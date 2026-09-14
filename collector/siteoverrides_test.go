package collector

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newOverrideEnricher builds an enricher whose sources deliberately disagree, so
// a test can prove the override won rather than merely agreed.
func newOverrideEnricher(t *testing.T, overrides map[string]string) *siteRecordEnricher {
	t.Helper()
	domains := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, domains.Load([]byte(`{"shared.example.net": ["SITE-FIRST", "SITE-SECOND"],
	                                          "example.org": ["SITE-SUFFIX"]}`)))
	hosts := NewHostSiteRegistry(newSiteTestLogger())
	require.NoError(t, hosts.Load([]byte(`{"SE": {"rcsite": "SITE-SE",
	                                      "protocols": {"p": {"endpoint": "root://se.example.org:1094"}}}}`)))
	return &siteRecordEnricher{
		domains:   domains,
		hosts:     hosts,
		overrides: NewSiteOverrides(overrides, newSiteTestLogger()),
		order:     DefaultSiteResolutionOrder,
		localSite: "SITE-LOCAL",
	}
}

// TestSiteOverridePinsAmbiguousHost is the case the feature exists for: a host
// CRIC reports ambiguously is pinned to one site and resolves definitively.
func TestSiteOverridePinsAmbiguousHost(t *testing.T) {
	e := newOverrideEnricher(t, nil)
	res := e.resolveEndpoint(siteEndpoint{host: "host.shared.example.net"})
	require.Equal(t, SiteStatusAmbiguous, res.status, "precondition: ambiguous without an override")

	e = newOverrideEnricher(t, map[string]string{"host.shared.example.net": "SITE-SECOND"})
	res = e.resolveEndpoint(siteEndpoint{host: "host.shared.example.net"})
	assert.Equal(t, SiteStatusResolvedOverride, res.status)
	assert.Equal(t, "SITE-SECOND", res.site)
	assert.Equal(t, SiteMethodOverride, res.method)
}

// TestSiteOverrideMatching covers how a key is matched: exact host, domain
// suffix, longest key wins, and case/trailing-dot normalisation.
func TestSiteOverrideMatching(t *testing.T) {
	// Every host below is ambiguous in the synthetic domains map, so the pin is
	// what decides it.
	e := newOverrideEnricher(t, map[string]string{
		"shared.example.net":      "SITE-BROAD",
		"host.shared.example.net": "SITE-EXACT",
		"  SHARED.EXAMPLE.NET.  ": "  SITE-BROAD  ", // trimmed + case-folded onto the same key
	})
	for host, want := range map[string]string{
		"host.shared.example.net": "SITE-EXACT", // longest key wins
		"any.shared.example.net":  "SITE-BROAD", // suffix key
		"shared.example.net":      "SITE-BROAD", // the key itself
	} {
		res := e.resolveEndpoint(siteEndpoint{host: host})
		assert.Equal(t, want, res.site, "host %s", host)
		assert.Equal(t, SiteStatusResolvedOverride, res.status, "host %s", host)
	}
}

// TestSiteOverrideCorrectsADefiniteAnswer is the core of the contract: a pin is
// the operator's own answer and wins outright, so it corrects CRIC rather than
// only settling a tie.
func TestSiteOverrideCorrectsADefiniteAnswer(t *testing.T) {
	plain := newOverrideEnricher(t, nil)
	require.Equal(t, "SITE-SE", plain.resolveEndpoint(siteEndpoint{host: "se.example.org"}).site,
		"precondition: CRIC answers this definitively")

	e := newOverrideEnricher(t, map[string]string{"se.example.org": "SITE-PINNED"})
	res := e.resolveEndpoint(siteEndpoint{host: "se.example.org"})
	assert.Equal(t, "SITE-PINNED", res.site, "the pin must beat a definite CRIC answer")
	assert.Equal(t, SiteMethodOverride, res.method)
	assert.Equal(t, SiteStatusResolvedOverride, res.status)
}

// TestSiteOverrideByAddress covers address and CIDR keys, which is how an
// endpoint with no usable host name gets pinned.
func TestSiteOverrideByAddress(t *testing.T) {
	e := newOverrideEnricher(t, map[string]string{
		"192.0.2.7":          "SITE-ONE-ADDR",
		"192.0.2.0/24":       "SITE-RANGE",
		"2001:db8::/32":      "SITE-V6",
		"pinned.example.org": "SITE-BY-NAME",
	})
	for _, tc := range []struct{ host, ip, want, why string }{
		{"", "192.0.2.7", "SITE-ONE-ADDR", "single address beats the range containing it"},
		{"", "192.0.2.9", "SITE-RANGE", "falls back to the enclosing CIDR"},
		{"", "2001:db8::1", "SITE-V6", "IPv6 CIDR"},
		{"", "198.51.100.1", "", "no key covers this address"},
		{"pinned.example.org", "192.0.2.9", "SITE-BY-NAME", "a named host beats a range that contains it"},
	} {
		res := e.resolveEndpoint(siteEndpoint{host: tc.host, ip: endpointIP(tc.ip)})
		if tc.want == "" {
			assert.NotEqual(t, SiteStatusResolvedOverride, res.status, tc.why)
			continue
		}
		assert.Equal(t, tc.want, res.site, tc.why)
		assert.Equal(t, SiteStatusResolvedOverride, res.status, tc.why)
	}
}

// TestSiteOverrideIsNotAnOrderMethod verifies "override" cannot be scheduled in
// resolution_order, and that pins still apply under a custom order that never
// mentions them.
func TestSiteOverrideIsNotAnOrderMethod(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(&bytes.Buffer{})
	assert.Equal(t,
		[]string{SiteMethodDomain},
		NormalizeSiteResolutionOrder([]string{"override", "domain"}, logger),
		`"override" must be rejected as an order method`)

	e := newOverrideEnricher(t, map[string]string{"shared.example.net": "SITE-SECOND"})
	e.order = []string{SiteMethodDomain}
	res := e.resolveEndpoint(siteEndpoint{host: "host.shared.example.net"})
	assert.Equal(t, "SITE-SECOND", res.site)
	assert.Equal(t, SiteStatusResolvedOverride, res.status)
	// ...and under an order that resolves it definitively, the pin still wins.
	e.order = []string{SiteMethodHostname}
	assert.Equal(t, SiteStatusResolvedOverride,
		e.resolveEndpoint(siteEndpoint{host: "host.shared.example.net"}).status)
}

// TestSiteOverrideMissLeavesAmbiguityIntact verifies a pin that does not match
// changes nothing: the ambiguous guess and its status survive.
func TestSiteOverrideMissLeavesAmbiguityIntact(t *testing.T) {
	e := newOverrideEnricher(t, map[string]string{"somewhere.else": "SITE-X"})
	res := e.resolveEndpoint(siteEndpoint{host: "host.shared.example.net"})
	assert.Equal(t, SiteStatusAmbiguous, res.status)
	assert.Equal(t, "SITE-FIRST", res.site)
	assert.Empty(t, res.method, "an ambiguous guess is credited to no method")
}

// TestSiteOverrideSettlesAmbiguityLaterMethodsCouldNotClear covers the ordering
// subtlety behind the override hook: an ambiguous hit from an early method is
// NOT erased by later methods that simply find nothing. worseSiteFailure ranks
// ambiguous above unknown_domain, so a named guess survives a later blank, the
// endpoint still finishes ambiguous, and the pin still gets its chance. Were the
// blank to win instead, the outcome would be unknown_domain and no pin would
// ever apply.
func TestSiteOverrideSettlesAmbiguityLaterMethodsCouldNotClear(t *testing.T) {
	// The SE map names two sites for this host; the domains map has never heard
	// of it, so "domain" (last in the order) contributes only unknown_domain.
	const seAmbiguous = `{"A": {"rcsite": "SITE-ALPHA", "protocols": {"p": {"endpoint": "root://shared.nowhere.test:1094"}}},
	                      "B": {"rcsite": "SITE-BETA",  "protocols": {"p": {"endpoint": "root://shared.nowhere.test:1094"}}}}`
	const unrelatedDomains = `{"unrelated.example.org": ["SITE-OTHER"]}`

	build := func(overrides map[string]string) *siteRecordEnricher {
		domains := NewSiteRegistry(newSiteTestLogger())
		require.NoError(t, domains.Load([]byte(unrelatedDomains)))
		hosts := NewHostSiteRegistry(newSiteTestLogger())
		require.NoError(t, hosts.Load([]byte(seAmbiguous)))
		return &siteRecordEnricher{
			domains:   domains,
			hosts:     hosts,
			ips:       NewIPSiteRegistry(newSiteTestLogger()),
			overrides: NewSiteOverrides(overrides, newSiteTestLogger()),
			order:     DefaultSiteResolutionOrder,
		}
	}

	res := build(nil).resolveEndpoint(siteEndpoint{host: "shared.nowhere.test"})
	require.Equal(t, SiteStatusAmbiguous, res.status,
		"a later non-match must not downgrade the ambiguous guess to unknown_domain")
	assert.Equal(t, "SITE-ALPHA", res.site)

	res = build(map[string]string{"shared.nowhere.test": "SITE-BETA"}).
		resolveEndpoint(siteEndpoint{host: "shared.nowhere.test"})
	assert.Equal(t, SiteStatusResolvedOverride, res.status)
	assert.Equal(t, "SITE-BETA", res.site)
	assert.Equal(t, SiteMethodOverride, res.method)
}

// TestAmbiguousInEveryMethodKeepsTheFirstGuess pins down which candidate is
// reported when more than one method is ambiguous: the earliest, since
// worseSiteFailure only replaces on a strictly worse rank. That keeps the guess
// attributable to the most-trusted source that produced one.
func TestAmbiguousInEveryMethodKeepsTheFirstGuess(t *testing.T) {
	const seAmbiguous = `{"A": {"rcsite": "SE-ONE", "protocols": {"p": {"endpoint": "root://both.example.net:1094"}}},
	                      "B": {"rcsite": "SE-TWO", "protocols": {"p": {"endpoint": "root://both.example.net:1094"}}}}`
	const domainsAmbiguous = `{"example.net": ["DOM-ONE", "DOM-TWO"]}`

	domains := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, domains.Load([]byte(domainsAmbiguous)))
	hosts := NewHostSiteRegistry(newSiteTestLogger())
	require.NoError(t, hosts.Load([]byte(seAmbiguous)))
	e := &siteRecordEnricher{domains: domains, hosts: hosts,
		ips: NewIPSiteRegistry(newSiteTestLogger()), order: DefaultSiteResolutionOrder}

	res := e.resolveEndpoint(siteEndpoint{host: "both.example.net"})
	assert.Equal(t, SiteStatusAmbiguous, res.status)
	assert.Equal(t, "SE-ONE", res.site, "hostname runs before domain, so its guess is the one kept")
}

// TestNewSiteOverridesRejectsUnusable verifies a map with nothing usable yields
// no registry at all, which drops the method rather than installing an empty one.
func TestNewSiteOverridesRejectsUnusable(t *testing.T) {
	assert.Nil(t, NewSiteOverrides(nil, newSiteTestLogger()))
	assert.Nil(t, NewSiteOverrides(map[string]string{}, newSiteTestLogger()))
	assert.Nil(t, NewSiteOverrides(map[string]string{"": "SITE", "  ": "", "x.y": "  "}, newSiteTestLogger()))
	assert.NotNil(t, NewSiteOverrides(map[string]string{"x.y": "SITE"}, newSiteTestLogger()))
}

// TestAmbiguityReporterNamesEachHostOnce verifies the operator gets an
// actionable line naming the candidates, and only one per host however many
// records arrive.
func TestAmbiguityReporterNamesEachHostOnce(t *testing.T) {
	logger := logrus.New()
	var buf bytes.Buffer
	logger.SetOutput(&buf)
	logger.SetLevel(logrus.WarnLevel)

	e := newOverrideEnricher(t, nil)
	e.ambig = newAmbiguityReporter(logger)
	for i := 0; i < 5; i++ {
		e.resolveEndpoint(siteEndpoint{host: "host.shared.example.net"})
	}

	out := buf.String()
	assert.Equal(t, 1, strings.Count(out, "is ambiguous between"), "one line per host, got: %s", out)
	assert.Contains(t, out, "SITE-FIRST, SITE-SECOND", "names the candidates")
	assert.Contains(t, out, "site.overrides", "tells the operator how to pin it")
}

// TestAmbiguityReporterIsBounded verifies the remembered set cannot grow with
// traffic, so a long tail of ambiguous hosts cannot leak memory.
func TestAmbiguityReporterIsBounded(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(&bytes.Buffer{})
	r := newAmbiguityReporter(logger)
	for i := 0; i < maxReportedAmbiguousHosts*2; i++ {
		host := strings.Repeat("a", i%50) + string(rune('a'+i%26)) + ".example.org"
		r.report(siteEndpoint{host: host}, "SITE", nil)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.LessOrEqual(t, len(r.reported), maxReportedAmbiguousHosts)
}

// TestAmbiguityReporterNamesHostlessEndpoints covers an endpoint with no usable
// host name: two CRIC netroute blocks of equal length claim one address. It is
// reported keyed by that address, which site.overrides accepts as a key, so the
// logged line is directly actionable.
func TestAmbiguityReporterNamesHostlessEndpoints(t *testing.T) {
	logger := logrus.New()
	var buf bytes.Buffer
	logger.SetOutput(&buf)
	logger.SetLevel(logrus.WarnLevel)

	const sharedCIDR = `{
      "SITE-AAA": {"netroutes": {"m": {"networks": {"ipv4": ["192.0.2.0/24"]}}}},
      "SITE-BBB": {"netroutes": {"m": {"networks": {"ipv4": ["192.0.2.0/24"]}}}}
    }`
	ips := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, ips.Load([]byte(sharedCIDR)))

	e := &siteRecordEnricher{
		domains: NewSiteRegistry(newSiteTestLogger()),
		hosts:   NewHostSiteRegistry(newSiteTestLogger()),
		ips:     ips,
		ambig:   newAmbiguityReporter(logger),
		order:   DefaultSiteResolutionOrder,
	}
	ep := siteEndpoint{host: "192.0.2.5", ip: endpointIP("192.0.2.5")}
	res := e.resolveEndpoint(ep)
	require.Equal(t, SiteStatusAmbiguous, res.status)

	out := buf.String()
	// logrus escapes the quotes in its text output, so match on content, not quoting.
	assert.Contains(t, out, "192.0.2.5", "names the address it could not resolve")
	assert.Contains(t, out, "SITE-AAA, SITE-BBB", "names both colliding sites")
	assert.Contains(t, out, "site.overrides", "offers a pin, which an address key can now express")
}
