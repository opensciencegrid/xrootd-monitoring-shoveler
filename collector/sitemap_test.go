package collector

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSiteTestLogger returns a quiet logger for the site resolution tests.
func newSiteTestLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	return logger
}

// TestSiteRegistryEmbeddedResolve verifies the embedded CRIC domains snapshot
// loads and resolves representative hosts, exercising the longest-suffix rule and
// the besttier-collapsed single-site cases.
func TestSiteRegistryEmbeddedResolve(t *testing.T) {
	r := NewSiteRegistry(newSiteTestLogger())

	cases := []struct {
		host       string
		wantSite   string
		wantStatus string
	}{
		{"xrootd.aglt2.org", "AGLT2", SiteStatusResolved},
		{"eosatlas.cern.ch", "CERN-PROD", SiteStatusResolved}, // besttier collapsed away BOINC
		// Longest suffix wins: the specific gla.scotgrid.ac.uk beats both the
		// ambiguous scotgrid.ac.uk and the broad ac.uk.
		{"n01.gla.scotgrid.ac.uk", "UKI-SCOTGRID-GLASGOW", SiteStatusResolved},
		// A host starting with 'f' must be treated as a name, not an IP literal.
		{"cmsxrootd.fnal.gov", "USCMS-FNAL-WC1", SiteStatusResolved},
		{"ft.uam.es", "UAM-LCG2", SiteStatusResolved},
		// Case and trailing dot are normalized.
		{"XROOTD.AGLT2.ORG.", "AGLT2", SiteStatusResolved},
	}
	for _, c := range cases {
		site, status := r.ResolveHost(c.host)
		assert.Equal(t, c.wantStatus, status, "status for %s", c.host)
		assert.Equal(t, c.wantSite, site, "site for %s", c.host)
	}
}

// TestSiteRegistryAmbiguous verifies that a longest-suffix hit shared by more
// than one RCSite reports the first listed site under the ambiguous status, and
// that we do NOT fall through to an even broader suffix once a match is found.
func TestSiteRegistryAmbiguous(t *testing.T) {
	r := NewSiteRegistry(newSiteTestLogger())

	// scotgrid.ac.uk maps to DURHAM+GLASGOW; must stop there and not fall back to
	// the single-site ac.uk (RAL-LCG2).
	site, status := r.ResolveHost("host.scotgrid.ac.uk")
	assert.Equal(t, SiteStatusAmbiguous, status)
	assert.Equal(t, "UKI-SCOTGRID-DURHAM", site)

	// desy.de maps to DESY-HH+DESY-ZN.
	site, status = r.ResolveHost("dcache.desy.de")
	assert.Equal(t, SiteStatusAmbiguous, status)
	assert.Equal(t, "DESY-HH", site)
}

// TestSiteRegistryUnresolvable verifies the no_host and unknown_domain outcomes.
func TestSiteRegistryUnresolvable(t *testing.T) {
	r := NewSiteRegistry(newSiteTestLogger())

	noHost := []string{
		"",                           // empty
		"unknown",                    // sentinel
		"localhost",                  // single label
		"192.41.230.5",               // IPv4 literal
		"2001:1458:303:18::100:50",   // IPv6 literal
		"[2001:1458:303:18::100:50]", // bracketed IPv6 literal
	}
	for _, h := range noHost {
		site, status := r.ResolveHost(h)
		assert.Equal(t, SiteStatusNoHost, status, "host %q", h)
		assert.Empty(t, site, "host %q", h)
	}

	site, status := r.ResolveHost("worker.no-such-domain.invalid")
	assert.Equal(t, SiteStatusUnknown, status)
	assert.Empty(t, site)
}

// TestSiteRegistryLoadRejectsBadData verifies a bad load leaves the previous map
// intact (fail-open on refresh).
func TestSiteRegistryLoadRejectsBadData(t *testing.T) {
	r := NewSiteRegistry(newSiteTestLogger())
	require.Error(t, r.Load([]byte("not json")))
	require.Error(t, r.Load([]byte("{}")), "empty map must be rejected")

	// Still resolves from the embedded snapshot.
	site, status := r.ResolveHost("xrootd.aglt2.org")
	assert.Equal(t, SiteStatusResolved, status)
	assert.Equal(t, "AGLT2", site)
}

// TestSiteEnricherDirection verifies that src/dst sites are ordered by data-flow
// direction: on read the server is the source, on write the client is.
func TestSiteEnricherDirection(t *testing.T) {
	e := &siteRecordEnricher{domains: NewSiteRegistry(newSiteTestLogger())}
	ctx := context.Background()

	// Read: client at CERN reads from an AGLT2 server -> src=AGLT2, dst=CERN.
	read := &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		clientHostname: "wn01.cern.ch",
		Read:           1024,
	}
	e.Enrich(ctx, read)
	assert.Equal(t, "AGLT2", read.srcSite)
	assert.Equal(t, SiteStatusResolved, read.srcSiteStatus)
	assert.Equal(t, "CERN-PROD", read.dstSite)
	assert.Equal(t, SiteStatusResolved, read.dstSiteStatus)

	// Write: same endpoints, client writes to the server -> src=CERN, dst=AGLT2.
	write := &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		clientHostname: "wn01.cern.ch",
		Write:          1024,
	}
	e.Enrich(ctx, write)
	assert.Equal(t, "CERN-PROD", write.srcSite)
	assert.Equal(t, "AGLT2", write.dstSite)
}

// TestSiteEnricherClientFallbackAndStatus verifies the client host falls back to
// Host when no DNS hostname is present, and that an IP-only client with no IP
// registry yields no_host.
func TestSiteEnricherClientFallbackAndStatus(t *testing.T) {
	e := &siteRecordEnricher{domains: NewSiteRegistry(newSiteTestLogger())}
	ctx := context.Background()

	// clientHostname empty but Host is already a name -> used directly.
	rec := &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		Host:           "grid.cyfronet.pl",
		Read:           1,
	}
	e.Enrich(ctx, rec)
	assert.Equal(t, "AGLT2", rec.srcSite)
	assert.Equal(t, "CYFRONET-LCG2", rec.dstSite)

	// Client is only an IP literal -> no_host, empty dst site.
	rec = &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		Host:           "192.41.230.5",
		Read:           1,
	}
	e.Enrich(ctx, rec)
	assert.Equal(t, "AGLT2", rec.srcSite)
	assert.Empty(t, rec.dstSite)
	assert.Equal(t, SiteStatusNoHost, rec.dstSiteStatus)
}

// TestEndpointIP covers the helper the "ip" method uses to extract an endpoint's
// address: an address (bracketed / zoned / IPv4-mapped forms included) yields its
// canonical IP; a name or the "unknown" sentinel yields nil.
func TestEndpointIP(t *testing.T) {
	cases := map[string]string{
		"192.0.2.5":        "192.0.2.5",
		"[2001:db8::1]":    "2001:db8::1",
		"::ffff:192.0.2.7": "192.0.2.7", // IPv4-mapped -> dotted IPv4
		"fe80::1%eth0":     "fe80::1",   // zoned literal
		"wn-42.to.infn.it": "",          // a name, not an address
		"fnal.gov":         "",          // starts with 'f' but is a name
		"unknown":          "",
		"":                 "",
	}
	for in, want := range cases {
		got := ""
		if ip := endpointIP(in); ip != nil {
			got = ip.String()
		}
		assert.Equal(t, want, got, "endpointIP(%q)", in)
	}
}

// TestSiteEnricherRegisteredAfterDNS verifies the enricher pipeline order: DNS
// must run before site resolution so resolved host names are available.
func TestSiteEnricherRegisteredAfterDNS(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:                 time.Minute,
		MaxEntries:          10,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         time.Minute,
		DNSTimeout:          time.Second,
		SiteRegistry:        NewSiteRegistry(newSiteTestLogger()),
		Logger:              newSiteTestLogger(),
	})
	defer c.Stop()

	var names []string
	for _, e := range c.enrichers {
		names = append(names, e.Name())
	}
	require.Equal(t, []string{"dns", "site"}, names)
}

// TestSiteFieldsOnlyOnWLCGRecord is the end-to-end statement of where the
// resolved sites are allowed to appear: the WLCG record carries them, the plain
// collector record does not, and a record that is not WLCG-bound is not resolved
// at all.
func TestSiteFieldsOnlyOnWLCGRecord(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:            time.Minute,
		MaxEntries:     10,
		SiteRegistry:   NewSiteRegistry(newSiteTestLogger()),
		SiteIPRegistry: NewIPSiteRegistry(newSiteTestLogger()),
		SiteLocalSite:  "CERN-PROD",
		WLCGVOs:        []string{"cms"},
		Logger:         newSiteTestLogger(),
	})
	defer c.Stop()

	newRecord := func(vo string) *CollectorRecord {
		return &CollectorRecord{
			ServerHostname: "eosatlas.cern.ch",
			ServerIP:       "128.142.1.1",
			Host:           "xrootd.aglt2.org",
			Read:           4096,
			VO:             vo,
		}
	}

	results := make(chan EnrichedRecord, 1)
	destination := EnrichmentDestination{Results: results, WLCGExchange: "wlcg"}

	// A WLCG-bound record: resolved, and the sites are on the emitted payload.
	c.EnqueueForEnrichment(newRecord("cms"), destination)
	wlcg := requireEnriched(t, results)
	assert.Equal(t, "wlcg", wlcg.Exchange)
	assert.Equal(t, "CERN-PROD", wlcg.Record.srcSite)
	assert.Equal(t, SiteStatusResolvedConfig, wlcg.Record.srcSiteStatus)
	assert.Equal(t, "AGLT2", wlcg.Record.dstSite)

	payload := decodePayload(t, wlcg.Payload)
	assert.Equal(t, "CERN-PROD", payload["src_site"])
	assert.Equal(t, "AGLT2", payload["dst_site"])
	assert.Equal(t, SiteStatusResolvedConfig, payload["src_site_status"])

	// A record that is not WLCG-bound: never resolved, and the plain collector
	// record it serializes to has no site fields at all.
	c.EnqueueForEnrichment(newRecord("atlas"), destination)
	plain := requireEnriched(t, results)
	assert.Empty(t, plain.Exchange)
	assert.Empty(t, plain.Record.srcSite, "a non-WLCG record is not resolved")
	assert.Empty(t, plain.Record.srcSiteStatus)

	payload = decodePayload(t, plain.Payload)
	for _, field := range []string{"src_site", "dst_site", "src_site_status", "dst_site_status"} {
		assert.NotContains(t, payload, field, "the collector record must not carry %s", field)
	}
}

// requireEnriched takes the next enriched record off the pipeline, failing the
// test rather than hanging if the enrichment workers produce nothing.
func requireEnriched(t *testing.T, results <-chan EnrichedRecord) EnrichedRecord {
	t.Helper()
	select {
	case enriched := <-results:
		return enriched
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an enriched record")
		return EnrichedRecord{}
	}
}

func decodePayload(t *testing.T, payload []byte) map[string]interface{} {
	t.Helper()
	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(payload, &decoded))
	return decoded
}

// TestSiteResolutionDisabled verifies that leaving SiteRegistry nil registers no
// site enricher at all, so records keep empty src/dst site fields.
func TestSiteResolutionDisabled(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:        time.Minute,
		MaxEntries: 10,
		Logger:     newSiteTestLogger(),
	})
	defer c.Stop()

	assert.Empty(t, c.enrichers, "no enrichers without DNS or site resolution")
}

// TestSiteRegistryEmptySiteListNoPanic covers a domains document that lists a
// suffix with no sites. Such an entry is dropped at load, and the suffix walk
// treats it as no match at all rather than indexing an empty slice — the
// collector installs no recover(), so this would otherwise take the process down
// from an enrichment worker on operator-supplied site.source data.
func TestSiteRegistryEmptySiteListNoPanic(t *testing.T) {
	r := &SiteRegistry{logger: newSiteTestLogger(), domains: map[string][]string{}}

	// The empty entry is dropped at load; only the usable one survives.
	require.NoError(t, r.Load([]byte(`{"example.org": [], "sub.example.org": ["SITE-A"]}`)))
	site, status := r.ResolveHost("host.sub.example.org")
	assert.Equal(t, SiteStatusResolved, status)
	assert.Equal(t, "SITE-A", site)

	// The suffix with the empty list must not match, and must not panic.
	assert.NotPanics(t, func() {
		site, status = r.ResolveHost("host.example.org")
	})
	assert.Equal(t, SiteStatusUnknown, status)
	assert.Empty(t, site)

	// Defence in depth: even a map built without Load must not panic.
	direct := &SiteRegistry{logger: newSiteTestLogger(), domains: map[string][]string{"example.org": {}}}
	assert.NotPanics(t, func() {
		site, status = direct.ResolveHost("host.example.org")
	})
	assert.Equal(t, SiteStatusUnknown, status)
	assert.Empty(t, site)
}

// TestSiteRegistryLoadDegenerateKeepsPreviousMap covers the fail-open contract
// for a document that is non-empty as JSON but yields no usable entries. It must
// be rejected and leave the working map in place, exactly like a parse error.
func TestSiteRegistryLoadDegenerateKeepsPreviousMap(t *testing.T) {
	for name, doc := range map[string]string{
		"blank and dot keys": `{"": ["SITE-A"], ".": ["SITE-B"]}`,
		"only empty lists":   `{"example.org": [], "example.net": []}`,
		"only blank sites":   `{"example.org": ["", "   "]}`,
		"empty object":       `{}`,
		"json null":          `null`,
	} {
		t.Run(name, func(t *testing.T) {
			r := NewSiteRegistry(newSiteTestLogger())
			before, statusBefore := r.ResolveHost("eosatlas.cern.ch")
			require.Equal(t, SiteStatusResolved, statusBefore)

			require.Error(t, r.Load([]byte(doc)), "degenerate document must be rejected")

			after, statusAfter := r.ResolveHost("eosatlas.cern.ch")
			assert.Equal(t, statusBefore, statusAfter, "a rejected load must not wipe the map")
			assert.Equal(t, before, after)
		})
	}
}

// TestSiteRegistryLoadTrimsBlankSites verifies blank site names are dropped from
// an otherwise usable entry, so a partially blank list cannot make a resolved
// answer an empty string.
func TestSiteRegistryLoadTrimsBlankSites(t *testing.T) {
	r := &SiteRegistry{logger: newSiteTestLogger(), domains: map[string][]string{}}
	require.NoError(t, r.Load([]byte(`{"example.org": ["", " SITE-A ", "  "]}`)))

	site, status := r.ResolveHost("host.example.org")
	assert.Equal(t, SiteStatusResolved, status, "one real site left means resolved, not ambiguous")
	assert.Equal(t, "SITE-A", site)
}
