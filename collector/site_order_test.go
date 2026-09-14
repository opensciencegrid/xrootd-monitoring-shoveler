package collector

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syntheticDomains is a two-entry domains map where the full host name and its
// parent suffix deliberately point at different sites, so a test can tell an
// exact-hostname hit from a suffix hit.
const syntheticDomains = `{
  "exact.example.org": ["SITE-EXACT"],
  "example.org": ["SITE-SUFFIX"]
}`

// syntheticNetroutes declares one CIDR block owned by a third site, so a test
// can tell an IP hit from either domain hit.
const syntheticNetroutes = `{
  "SITE-IP": {"netroutes": {"main": {"networks": {"ipv4": ["192.0.2.0/24"]}}}}
}`

// syntheticSE lists the same full host as a CRIC SE protocol endpoint, which is
// what the hostname method matches on (no longer the domains-map exact key).
const syntheticSE = `{
  "SE-EXACT": {"rcsite": "SITE-EXACT", "protocols": {"p": {"endpoint": "root://exact.example.org:1094"}}}
}`

// syntheticSharedDomains maps one suffix to two sites, for the ambiguous case.
const syntheticSharedDomains = `{
  "shared.example.net": ["SITE-FIRST", "SITE-SECOND"]
}`

// newOrderedEnricher builds an enricher over the synthetic maps with the given
// method order.
func newOrderedEnricher(t *testing.T, order []string) *siteRecordEnricher {
	t.Helper()

	domains := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, domains.Load([]byte(syntheticDomains)))
	hosts := NewHostSiteRegistry(newSiteTestLogger())
	require.NoError(t, hosts.Load([]byte(syntheticSE)))
	ips := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, ips.Load([]byte(syntheticNetroutes)))

	return &siteRecordEnricher{domains: domains, hosts: hosts, ips: ips, order: order}
}

// TestSiteResolutionOrderDecidesWinner verifies that the configured order, not
// the resolver's internals, decides which identifier wins when several would
// resolve the same endpoint to different sites.
func TestSiteResolutionOrderDecidesWinner(t *testing.T) {
	ep := siteEndpoint{host: "exact.example.org", ip: net.ParseIP("192.0.2.5")}

	cases := []struct {
		name     string
		order    []string
		wantSite string
		wantStat string
	}{
		{"hostname first", []string{SiteMethodHostname, SiteMethodIP, SiteMethodDomain}, "SITE-EXACT", SiteStatusResolvedHostname},
		{"ip first", []string{SiteMethodIP, SiteMethodHostname, SiteMethodDomain}, "SITE-IP", SiteStatusResolvedIP},
		{"domain first", []string{SiteMethodDomain, SiteMethodHostname, SiteMethodIP}, "SITE-EXACT", SiteStatusResolved},
		{"ip only", []string{SiteMethodIP}, "SITE-IP", SiteStatusResolvedIP},
	}
	for _, c := range cases {
		got := newOrderedEnricher(t, c.order).resolveEndpoint(ep)
		assert.Equal(t, c.wantSite, got.site, c.name)
		assert.Equal(t, c.wantStat, got.status, c.name)
	}

	// "domain first" above resolves to SITE-EXACT because the suffix walk starts
	// at the full host name; the suffix entry only wins for a host the exact
	// lookup misses, which is where ip-before-domain becomes visible.
	other := siteEndpoint{host: "other.example.org", ip: net.ParseIP("192.0.2.5")}

	ipBeforeDomain := newOrderedEnricher(t, DefaultSiteResolutionOrder).resolveEndpoint(other)
	assert.Equal(t, "SITE-IP", ipBeforeDomain.site, "default order puts the IP range ahead of the domain suffix")
	assert.Equal(t, SiteMethodIP, ipBeforeDomain.method)

	domainBeforeIP := newOrderedEnricher(t, []string{SiteMethodHostname, SiteMethodDomain, SiteMethodIP}).resolveEndpoint(other)
	assert.Equal(t, "SITE-SUFFIX", domainBeforeIP.site)
	assert.Equal(t, SiteMethodDomain, domainBeforeIP.method)
}

// TestSiteLocalSiteResolvesServer verifies the "config" method: with the
// collector's own site configured, the reporting server is labelled from the
// config and no lookup is consulted, while the remote client still is.
func TestSiteLocalSiteResolvesServer(t *testing.T) {
	e := newOrderedEnricher(t, DefaultSiteResolutionOrder)
	e.localSite = "CERN-PROD"
	ctx := context.Background()

	// The server's own name would resolve to SITE-EXACT, but config comes first.
	rec := &CollectorRecord{
		ServerHostname: "exact.example.org",
		clientHostname: "other.example.org",
		Read:           1,
	}
	e.Enrich(ctx, rec)
	assert.Equal(t, "CERN-PROD", rec.srcSite, "server takes the configured local site")
	assert.Equal(t, SiteStatusResolvedConfig, rec.srcSiteStatus)
	assert.Equal(t, "SITE-SUFFIX", rec.dstSite, "the client is still resolved by lookup")
	assert.Equal(t, SiteStatusResolved, rec.dstSiteStatus)

	// With no local site configured the step is a no-op and the server falls
	// through to the host name.
	e.localSite = ""
	e.Enrich(ctx, rec)
	assert.Equal(t, "SITE-EXACT", rec.srcSite)
	assert.Equal(t, SiteStatusResolvedHostname, rec.srcSiteStatus)
}

// TestSiteLocalSiteLANClients verifies that a client on a private address is
// placed at the collector's site only when that is enabled — CRIC declares no
// worker-node ranges, so it is the only way those clients resolve.
func TestSiteLocalSiteLANClients(t *testing.T) {
	ctx := context.Background()
	newRecord := func() *CollectorRecord {
		return &CollectorRecord{
			ServerHostname: "exact.example.org",
			Host:           "10.1.2.3", // private worker node, no PTR
			Read:           1,
		}
	}

	e := newOrderedEnricher(t, DefaultSiteResolutionOrder)
	e.localSite = "CERN-PROD"
	e.localSiteLANClients = true

	rec := newRecord()
	e.Enrich(ctx, rec)
	assert.Equal(t, "CERN-PROD", rec.dstSite)
	assert.Equal(t, SiteStatusResolvedConfig, rec.dstSiteStatus)

	e.localSiteLANClients = false
	rec = newRecord()
	e.Enrich(ctx, rec)
	assert.Empty(t, rec.dstSite, "left unresolved rather than assumed local")
	assert.Equal(t, SiteStatusNoHost, rec.dstSiteStatus)

	// A public client address is never covered by the config method.
	e.localSiteLANClients = true
	public := &CollectorRecord{ServerHostname: "exact.example.org", Host: "198.51.100.7", Read: 1}
	e.Enrich(ctx, public)
	assert.Empty(t, public.dstSite)
}

// TestSiteResolutionFailureReason verifies the reported reason when every method
// misses: the informative name-side reason wins over the IP one, so no_host keeps
// meaning "no usable name and no matching range either".
func TestSiteResolutionFailureReason(t *testing.T) {
	e := newOrderedEnricher(t, DefaultSiteResolutionOrder)

	// A name outside the map, with an address outside every declared range.
	named := e.resolveEndpoint(siteEndpoint{host: "wn.no-such-domain.invalid", ip: net.ParseIP("198.51.100.7")})
	assert.Equal(t, SiteStatusUnknown, named.status)
	assert.Empty(t, named.method)

	// An address-only endpoint outside every declared range.
	bare := e.resolveEndpoint(siteEndpoint{host: "198.51.100.7", ip: net.ParseIP("198.51.100.7")})
	assert.Equal(t, SiteStatusNoHost, bare.status)

	// An endpoint we know nothing about at all.
	empty := e.resolveEndpoint(siteEndpoint{})
	assert.Equal(t, SiteStatusNoHost, empty.status)
}

// TestSiteAmbiguousGuessSurvivesTheOrder verifies what an ambiguous match is
// worth once the whole order has run: it is not a hit, so later methods still
// get their turn and a definite answer displaces it — but if nothing definite
// turns up, the guessed site is what the endpoint reports, under the ambiguous
// status that marks it as one.
func TestSiteAmbiguousGuessSurvivesTheOrder(t *testing.T) {
	domains := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, domains.Load([]byte(syntheticSharedDomains)))
	ips := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, ips.Load([]byte(syntheticNetroutes)))
	e := &siteRecordEnricher{domains: domains, ips: ips, order: DefaultSiteResolutionOrder}

	// Nothing else can resolve this address, so the guess is all there is.
	only := e.resolveEndpoint(siteEndpoint{host: "wn01.shared.example.net", ip: net.ParseIP("198.51.100.7")})
	assert.Equal(t, SiteStatusAmbiguous, only.status)
	assert.Equal(t, "SITE-FIRST", only.site)
	// Still not a resolution: no method may claim credit for a guess.
	assert.Empty(t, only.method)

	// Same ambiguous name, but now the address lands in a declared range: the
	// definite IP hit must win outright rather than being ranked against a guess.
	beaten := e.resolveEndpoint(siteEndpoint{host: "wn01.shared.example.net", ip: net.ParseIP("192.0.2.5")})
	assert.Equal(t, SiteStatusResolvedIP, beaten.status)
	assert.Equal(t, "SITE-IP", beaten.site)
	assert.Equal(t, SiteMethodIP, beaten.method)
}

// TestNormalizeSiteResolutionOrder verifies the config is sanitized rather than
// trusted: names are case-insensitive, unknown and repeated ones are dropped, and
// an order left with nothing usable falls back to the default.
func TestNormalizeSiteResolutionOrder(t *testing.T) {
	logger := newSiteTestLogger()

	assert.Equal(t, DefaultSiteResolutionOrder, NormalizeSiteResolutionOrder(nil, logger))
	assert.Equal(t, DefaultSiteResolutionOrder, NormalizeSiteResolutionOrder([]string{"geoip", ""}, logger),
		"an order with no known method must not disable resolution")

	assert.Equal(t,
		[]string{SiteMethodIP, SiteMethodDomain},
		NormalizeSiteResolutionOrder([]string{" IP ", "geoip", "domain", "ip"}, logger))

	// An order may legitimately leave methods out, e.g. config-only.
	assert.Equal(t, []string{SiteMethodConfig}, NormalizeSiteResolutionOrder([]string{"config"}, logger))
}

// TestSiteRegistryDomainWalkPrefersExactKey verifies the domain method's walk
// starts at the full host name, so an exact key in the domains map still wins
// over its parent suffix. This is the coverage the removed ResolveExactHost
// helper used to carry: the "hostname" method now matches CRIC SE endpoints, and
// nothing else needed an exact-only lookup on the domain map.
func TestSiteRegistryDomainWalkPrefersExactKey(t *testing.T) {
	r := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, r.Load([]byte(syntheticDomains)))

	// The full host is itself a key and must beat the parent suffix.
	site, status := r.ResolveHost("exact.example.org")
	assert.Equal(t, "SITE-EXACT", site)
	assert.Equal(t, SiteStatusResolved, status)

	// No exact key, so the walk falls back to the parent suffix.
	site, status = r.ResolveHost("other.example.org")
	assert.Equal(t, "SITE-SUFFIX", site)
	assert.Equal(t, SiteStatusResolved, status)

	// A two-label host that is itself a key still matches.
	site, status = r.ResolveHost("example.org")
	assert.Equal(t, "SITE-SUFFIX", site)
	assert.Equal(t, SiteStatusResolved, status)

	// Unusable hosts are rejected before any lookup.
	site, status = r.ResolveHost("192.0.2.5")
	assert.Empty(t, site)
	assert.Equal(t, SiteStatusNoHost, status)
}
