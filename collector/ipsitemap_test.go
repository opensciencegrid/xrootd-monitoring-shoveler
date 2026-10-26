package collector

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIPSiteRegistryEmbedded checks that the committed CRIC netroutes snapshot
// loads and behaves sensibly. It anchors the positive cases on stable site
// allocations; exact per-IP mappings are covered by the crafted-data tests below
// so they do not depend on the snapshot's contents (CRIC declares most sites only
// as narrow LHCOPN/LHCONE subnets).
func TestIPSiteRegistryEmbedded(t *testing.T) {
	r := NewIPSiteRegistry(newSiteTestLogger())

	// The committed snapshot must parse into a substantial route table.
	assert.Greater(t, len(r.routes), 100, "embedded snapshot should load many CIDR blocks")

	// Cross-check against CRIC's own resolveip.monit API: the sample IP it
	// resolves to CERN-PROD (via CERN-PROD-LHCOPNE) must resolve the same way
	// locally. This validates that local longest-prefix containment agrees with
	// CRIC's server-side resolution.
	site, status := r.ResolveIP(net.ParseIP("2001:1458:303:18::100:50"))
	assert.Equal(t, SiteStatusResolvedIP, status)
	assert.Equal(t, "CERN-PROD", site)

	site, status = r.ResolveIP(net.ParseIP("2001:48a8:68f7:1::9")) // AGLT2 IPv6 /48
	assert.Equal(t, SiteStatusResolvedIP, status)
	assert.Equal(t, "AGLT2", site)

	_, status = r.ResolveIP(net.ParseIP("8.8.8.8")) // public IP, not in any WLCG block
	assert.Equal(t, SiteStatusUnknownIP, status)

	_, status = r.ResolveIP(nil)
	assert.Equal(t, SiteStatusNoHost, status)
}

// TestIPSiteLongestPrefix verifies that a more specific block wins over a broader
// one that also contains the IP.
func TestIPSiteLongestPrefix(t *testing.T) {
	doc := []byte(`{
		"SITE-BROAD": {"netroutes": {"r": {"networks": {"ipv4": ["10.0.0.0/8"]}}}},
		"SITE-SPECIFIC": {"netroutes": {"r": {"networks": {"ipv4": ["10.1.2.0/24"]}}}}
	}`)
	r := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, r.Load(doc))

	site, status := r.ResolveIP(net.ParseIP("10.1.2.5")) // inside both; /24 wins
	assert.Equal(t, SiteStatusResolvedIP, status)
	assert.Equal(t, "SITE-SPECIFIC", site)

	site, status = r.ResolveIP(net.ParseIP("10.9.9.9")) // only the /8 contains it
	assert.Equal(t, SiteStatusResolvedIP, status)
	assert.Equal(t, "SITE-BROAD", site)
}

// TestIPSiteAmbiguousSharedRange verifies that two sites declaring the same
// most-specific block yield the ambiguous status, naming the lexicographically
// first of them so the answer does not depend on route-table iteration order.
func TestIPSiteAmbiguousSharedRange(t *testing.T) {
	doc := []byte(`{
		"SITE-A": {"netroutes": {"r": {"networks": {"ipv4": ["192.0.2.0/24"]}}}},
		"SITE-B": {"netroutes": {"r": {"networks": {"ipv4": ["192.0.2.0/24"]}}}}
	}`)
	r := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, r.Load(doc))

	site, status := r.ResolveIP(net.ParseIP("192.0.2.5"))
	assert.Equal(t, SiteStatusAmbiguous, status)
	assert.Equal(t, "SITE-A", site)

	// Same table, many times over: the route slice is built by ranging a map, so
	// a guess that depended on that order would flap between calls.
	for i := 0; i < 50; i++ {
		require.NoError(t, r.Load(doc))
		site, status = r.ResolveIP(net.ParseIP("192.0.2.5"))
		assert.Equal(t, SiteStatusAmbiguous, status)
		assert.Equal(t, "SITE-A", site)
	}
}

// TestIPSiteLoadRejectsBadData verifies fail-open behaviour: a parse error or a
// document with no usable CIDRs leaves the previous table intact.
func TestIPSiteLoadRejectsBadData(t *testing.T) {
	r := NewIPSiteRegistry(newSiteTestLogger())
	require.Error(t, r.Load([]byte("not json")))
	require.Error(t, r.Load([]byte(`{"S": {"netroutes": {}}}`)), "no CIDRs must be rejected")

	// Still resolves from the embedded snapshot.
	site, status := r.ResolveIP(net.ParseIP("128.142.1.1"))
	assert.Equal(t, SiteStatusResolvedIP, status)
	assert.Equal(t, "CERN-PROD", site)
}

// TestSiteEnricherIPMethod verifies that when the name-based methods miss but
// the endpoint has an IP, the "ip" method resolves it, and that an exact host
// name still takes precedence (it comes first in the default order). It uses a
// crafted IP table over a documentation range (TEST-NET-3) so it does not depend
// on the snapshot.
func TestSiteEnricherIPMethod(t *testing.T) {
	ipReg := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, ipReg.Load([]byte(
		`{"TEST-SITE":{"netroutes":{"r":{"networks":{"ipv4":["203.0.113.0/24"]}}}}}`)))

	e := &siteRecordEnricher{
		domains: NewSiteRegistry(newSiteTestLogger()),
		ips:     ipReg,
	}
	ctx := context.Background()

	// The server resolves by name (AGLT2). The client is a bare IP with no name,
	// so the name-based methods yield no_host and "ip" resolves it to TEST-SITE.
	rec := &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		ServerIP:       "198.51.100.9", // TEST-NET-2, not in the crafted table
		Host:           "203.0.113.9",  // TEST-NET-3 literal, no PTR name
		Read:           1,
	}
	e.Enrich(ctx, rec)

	assert.Equal(t, "AGLT2", rec.srcSite)
	assert.Equal(t, SiteStatusResolved, rec.srcSiteStatus, "server resolved by name, not IP")
	assert.Equal(t, "TEST-SITE", rec.dstSite)
	assert.Equal(t, SiteStatusResolvedIP, rec.dstSiteStatus, "client recovered by the ip method")
}

// TestSiteEnricherNoIPRegistry verifies the enricher still works with the "ip"
// method disabled (ips == nil): unresolvable endpoints stay unresolved.
func TestSiteEnricherNoIPRegistry(t *testing.T) {
	e := &siteRecordEnricher{domains: NewSiteRegistry(newSiteTestLogger())}
	rec := &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		Host:           "128.142.55.7", // would resolve by IP, but there is no IP registry
		Read:           1,
	}
	e.Enrich(context.Background(), rec)

	assert.Equal(t, "AGLT2", rec.srcSite)
	assert.Empty(t, rec.dstSite)
	assert.Equal(t, SiteStatusNoHost, rec.dstSiteStatus)
}
