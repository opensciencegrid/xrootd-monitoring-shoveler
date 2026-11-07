package collector

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cernWorkerNodes is the case this exists for: XRootD reports a CERN worker node
// by the unqualified name it saw, and the address behind it sits in a CRIC
// netroute owned by CERN-PROD.
var cernWorkerNodes = map[string][]string{
	"b9p04p7188": {"188.185.205.157"},
	"b9p25p9299": {"188.185.212.34"},
}

// newClientLookupCorrelator builds a correlator whose forward lookups answer
// from cernWorkerNodes and whose reverse lookups answer nothing, so a test sees
// the address path alone.
func newClientLookupCorrelator(t *testing.T) (*Correlator, *int) {
	t.Helper()

	calls := 0
	c := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:                 5 * time.Second,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              newSiteTestLogger(),
	})
	t.Cleanup(c.Stop)

	c.dnsResolver = &mockDNSResolver{
		hostFunc: func(_ context.Context, host string) ([]string, error) {
			calls++
			if addrs, ok := cernWorkerNodes[host]; ok {
				return addrs, nil
			}
			return nil, fmt.Errorf("no such host: %s", host)
		},
		lookupFunc: func(_ context.Context, _ string) ([]string, error) {
			return nil, fmt.Errorf("no PTR")
		},
	}
	return c, &calls
}

// TestClientNameLookupResolvesUnqualifiedNames covers the forward lookup itself:
// an unqualified client name gets an address, a name DNS cannot place does not,
// and neither is looked up twice.
func TestClientNameLookupResolvesUnqualifiedNames(t *testing.T) {
	c, calls := newClientLookupCorrelator(t)
	enricher := &dnsRecordEnricher{correlator: c}

	t.Run("a known worker node", func(t *testing.T) {
		record := &CollectorRecord{Host: "b9p04p7188", needsClientNameLookup: true, clientLookupName: "b9p04p7188"}
		enricher.Enrich(context.Background(), record)

		assert.Equal(t, "188.185.205.157", record.clientIP)
		assert.False(t, record.needsClientNameLookup, "the pending flag must be cleared")
	})

	t.Run("a name DNS cannot place", func(t *testing.T) {
		record := &CollectorRecord{Host: "wn-nowhere", needsClientNameLookup: true, clientLookupName: "wn-nowhere"}
		enricher.Enrich(context.Background(), record)

		assert.Empty(t, record.clientIP)
		assert.False(t, record.needsClientNameLookup)
	})

	t.Run("answers are cached, failures included", func(t *testing.T) {
		before := *calls
		for i := 0; i < 5; i++ {
			enricher.Enrich(context.Background(), &CollectorRecord{Host: "b9p04p7188", needsClientNameLookup: true, clientLookupName: "b9p04p7188"})
			enricher.Enrich(context.Background(), &CollectorRecord{Host: "wn-nowhere", needsClientNameLookup: true, clientLookupName: "wn-nowhere"})
		}
		assert.Equal(t, before, *calls, "a resolved name and an unresolvable one must both come from cache")
	})
}

// TestUnqualifiedClientResolvesToASite is the end of the chain: with the address
// in hand the site resolver places the client by CRIC netroute, so a CERN worker
// node reading from CERN EOS comes out LAN instead of UNKNOWN.
func TestUnqualifiedClientResolvesToASite(t *testing.T) {
	ips := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, ips.Load([]byte(`{"CERN-PROD": {"netroutes": {"a": {"networks": {"ipv4": ["188.185.192.0/18"]}}}}}`)))

	site := &siteRecordEnricher{
		domains:   NewSiteRegistry(newSiteTestLogger()),
		ips:       ips,
		localSite: "CERN-PROD",
		order:     DefaultSiteResolutionOrder,
	}
	traffic := &trafficRecordEnricher{classifier: newTrafficClassifier(TrafficConfig{Enabled: true})}

	// A read from a CERN EOS server by a worker node reported as a bare name.
	record := &CollectorRecord{
		Host:           "b9p04p7188",
		ServerHostname: "st-096-100gb-ip312-6fb70.cern.ch",
		ServerIP:       "188.184.1.1",
		Read:           664006,
	}

	t.Run("without the lookup it is UNKNOWN", func(t *testing.T) {
		unresolved := *record
		site.Enrich(context.Background(), &unresolved)
		traffic.Enrich(context.Background(), &unresolved)

		assert.Equal(t, SiteStatusNoHost, unresolved.dstSiteStatus, "the client end cannot be placed")
		assert.Equal(t, TrafficScopeUnknown, unresolved.trafficScope)
	})

	t.Run("with the resolved address it is LAN", func(t *testing.T) {
		resolved := *record
		resolved.clientIP = "188.185.205.157" // what the forward lookup returns

		site.Enrich(context.Background(), &resolved)
		traffic.Enrich(context.Background(), &resolved)

		assert.Equal(t, "CERN-PROD", resolved.srcSite, "server end, from the configured local site")
		assert.Equal(t, "CERN-PROD", resolved.dstSite, "client end, from the CRIC netroute")
		assert.Equal(t, SiteStatusResolvedIP, resolved.dstSiteStatus)
		assert.Equal(t, TrafficScopeLAN, resolved.trafficScope)
		assert.Equal(t, true, *siteInternalFromScope(resolved.trafficScope))
	})
}

// TestOnlyUnqualifiedClientsAreLookedUp guards the trigger: a client reported as
// an address or as an FQDN already has what the resolver needs, and must not
// cost an extra forward lookup.
func TestOnlyUnqualifiedClientsAreLookedUp(t *testing.T) {
	c, calls := newClientLookupCorrelator(t)
	enricher := &dnsRecordEnricher{correlator: c}

	before := *calls
	enricher.Enrich(context.Background(), &CollectorRecord{Host: "worker.example.org"})
	enricher.Enrich(context.Background(), &CollectorRecord{Host: "188.185.205.157"})
	assert.Equal(t, before, *calls, "only a record marked for the lookup may trigger one")

	// And the address of a client reported as a literal still reaches the resolver.
	assert.Equal(t, net.ParseIP("188.185.205.157"), endpointIP("188.185.205.157"))
}
