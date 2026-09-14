package collector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostFromSEEndpoint(t *testing.T) {
	cases := map[string]string{
		"root://xrootd.aglt2.org:1094":              "xrootd.aglt2.org",
		"davs://webdav.aglt2.org:2880":              "webdav.aglt2.org",
		"srm://head01.aglt2.org:8443/srm/managerv2": "head01.aglt2.org",
		"root://EOSATLAS.CERN.CH:1094":              "eosatlas.cern.ch",
		"xrootd.aglt2.org:1094":                     "xrootd.aglt2.org",
		"xrootd.aglt2.org":                          "xrootd.aglt2.org",
		"root://xrootd.aglt2.org:1094/store":        "xrootd.aglt2.org",
		"/atlasdatadisk/rucio/":                     "",
		"":                                          "",
		"192.0.2.5:1094":                            "",
		"root://192.0.2.5:1094":                     "",
		"root://[2001:db8::1]:1094":                 "",
	}
	for ep, want := range cases {
		assert.Equal(t, want, hostFromSEEndpoint(ep), ep)
	}
}

// TestHostSiteRegistryEmbedded checks that the committed CRIC SE snapshot loads
// and exact-matches storage hosts without falling through to a domain suffix.
func TestHostSiteRegistryEmbedded(t *testing.T) {
	r := NewHostSiteRegistry(newSiteTestLogger())

	assert.Greater(t, len(r.hosts), 100, "embedded snapshot should load many SE hosts")

	site, status := r.ResolveHost("xrootd.aglt2.org")
	assert.Equal(t, SiteStatusResolvedHostname, status)
	assert.Equal(t, "AGLT2", site)

	site, status = r.ResolveHost("eosatlas.cern.ch")
	assert.Equal(t, SiteStatusResolvedHostname, status)
	assert.Equal(t, "CERN-PROD", site)

	// Scheme/port must not be part of the lookup key; the enricher passes a bare host.
	site, status = r.ResolveHost("XROOTD.AGLT2.ORG.")
	assert.Equal(t, SiteStatusResolvedHostname, status)
	assert.Equal(t, "AGLT2", site)

	// A site domain is not itself an SE endpoint: hostname is exact, not a suffix.
	_, status = r.ResolveHost("aglt2.org")
	assert.Equal(t, SiteStatusUnknown, status)

	_, status = r.ResolveHost("wn01.cern.ch")
	assert.Equal(t, SiteStatusUnknown, status)

	_, status = r.ResolveHost("192.0.2.5")
	assert.Equal(t, SiteStatusNoHost, status)
}

func TestHostSiteRegistryLoadStripsEndpoint(t *testing.T) {
	doc := []byte(`{
		"SE-A": {
			"rcsite": "SITE-A",
			"protocols": {
				"xrootd": {"endpoint": "root://se.example.org:1094"},
				"webdav": {"endpoint": "davs://se.example.org:2880/store"}
			}
		}
	}`)
	r := NewHostSiteRegistry(newSiteTestLogger())
	require.NoError(t, r.Load(doc))

	site, status := r.ResolveHost("se.example.org")
	assert.Equal(t, SiteStatusResolvedHostname, status)
	assert.Equal(t, "SITE-A", site)
}

func TestHostSiteRegistryAmbiguousSharedHost(t *testing.T) {
	doc := []byte(`{
		"SE-B": {"rcsite": "SITE-B", "protocols": {"p": {"endpoint": "root://shared.example.org:1094"}}},
		"SE-A": {"rcsite": "SITE-A", "protocols": {"p": {"endpoint": "davs://shared.example.org:2880"}}}
	}`)
	r := NewHostSiteRegistry(newSiteTestLogger())
	require.NoError(t, r.Load(doc))

	site, status := r.ResolveHost("shared.example.org")
	assert.Equal(t, SiteStatusAmbiguous, status)
	assert.Equal(t, "SITE-A", site, "lexicographically first site is the guess")
}

func TestHostSiteLoadRejectsBadData(t *testing.T) {
	r := NewHostSiteRegistry(newSiteTestLogger())
	require.Error(t, r.Load([]byte("not json")))
	require.Error(t, r.Load([]byte(`{"S": {"rcsite": "", "protocols": {}}}`)), "no endpoints must be rejected")

	site, status := r.ResolveHost("xrootd.aglt2.org")
	assert.Equal(t, SiteStatusResolvedHostname, status)
	assert.Equal(t, "AGLT2", site)
}

// TestSiteEnricherHostnameMethod verifies that an SE endpoint host resolves via
// hostname (not domain suffix) and that a worker-node name still falls through
// to the domain method.
func TestSiteEnricherHostnameMethod(t *testing.T) {
	hosts := NewHostSiteRegistry(newSiteTestLogger())
	require.NoError(t, hosts.Load([]byte(`{
		"SE-A": {"rcsite": "SITE-SE", "protocols": {"p": {"endpoint": "root://se.example.org:1094"}}}
	}`)))
	domains := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, domains.Load([]byte(`{"example.org": ["SITE-SUFFIX"]}`)))

	e := &siteRecordEnricher{domains: domains, hosts: hosts}
	ctx := context.Background()

	rec := &CollectorRecord{
		ServerHostname: "se.example.org",
		clientHostname: "wn01.example.org",
		Read:           1,
	}
	e.Enrich(ctx, rec)

	assert.Equal(t, "SITE-SE", rec.srcSite)
	assert.Equal(t, SiteStatusResolvedHostname, rec.srcSiteStatus)
	assert.Equal(t, "SITE-SUFFIX", rec.dstSite)
	assert.Equal(t, SiteStatusResolved, rec.dstSiteStatus)
}

func TestSiteEnricherNoHostRegistry(t *testing.T) {
	e := &siteRecordEnricher{domains: NewSiteRegistry(newSiteTestLogger())}
	rec := &CollectorRecord{
		ServerHostname: "xrootd.aglt2.org",
		clientHostname: "wn01.cern.ch",
		Read:           1,
	}
	e.Enrich(context.Background(), rec)

	assert.Equal(t, "AGLT2", rec.srcSite)
	assert.Equal(t, SiteStatusResolved, rec.srcSiteStatus, "falls through to domain when hostname is disabled")
	assert.Equal(t, "CERN-PROD", rec.dstSite)
}

// TestHostSiteRegistryParsesUnreducedAPIShape loads a verbatim slice of the live
// CRIC service/query/?json&type=SE response — every field, not the reduced form
// the committed snapshot uses. It guards the claim that an unreduced response
// parses identically: the decoder must ignore the many fields we do not read, and
// must still find rcsite and the protocol endpoints.
//
// It also pins down "aprotocols", whose real shape is {"read_wan": ["<name>"]} --
// lists of protocol names, not protocol objects. Declaring it protocols-shaped
// made encoding/json reject the entire live document, so this fixture keeps the
// real shape to make sure it stays merely ignored.
func TestHostSiteRegistryParsesUnreducedAPIShape(t *testing.T) {
	const liveShape = `{
      "AGLT2_SE": {
        "aprotocols": {}, "arch": "", "auth": [], "country": "United States",
        "country_code": "US", "description": "", "federation": "US-AGLT2", "id": 2068,
        "impl": "", "in_report": false, "info_url": "", "is_virtual": true,
        "last_modified": "2020-01-15T15:15:10.111533",
        "metric_profile": {"is_accessible": null, "is_valid": null, "last_generated": null},
        "name": "AGLT2_SE",
        "protocols": {
          "AGLT2_SE-SRM-head01.aglt2.org": {
            "basepath": "", "doortype": "", "endpoint": "srm://head01.aglt2.org:8443",
            "flavour": "SRM", "id": 1149, "impl": "", "in_report": false,
            "is_monitored": true, "name": "AGLT2_SE-SRM-head01.aglt2.org",
            "state": "ACTIVE", "status": "", "token_support": false, "version": null
          }
        },
        "rcsite": "AGLT2", "rcsite_state": "ACTIVE", "resources": {},
        "state": "ACTIVE", "type": "SE", "usage": {}, "version": null, "vos": []
      },
      "AGLT2_SE_0_ATLAS": {
        "aprotocols": {}, "arch": "Disk", "impl": "dCache",
        "info_url": "http://head01.aglt2.org:3880/api/v1/srr",
        "protocols": {
          "AGLT2_SE_0_ATLAS-WEBDAV-webdav.aglt2.org": {
            "endpoint": "davs://webdav.aglt2.org:2880", "flavour": "WEBDAV",
            "id": 394, "impl": "dcache", "state": "ACTIVE", "status": "production",
            "version": "11.2.1"
          }
        },
        "rcsite": "AGLT2", "rcsite_state": "ACTIVE", "state": "ACTIVE", "type": "SE"
      },
      "NO_RCSITE_SE": {
        "protocols": {"x": {"endpoint": "root://orphan.example.org:1094", "state": "ACTIVE"}},
        "rcsite": "", "state": "ACTIVE", "type": "SE"
      },
      "MIXED_SE": {
        "aprotocols": {"read_wan": ["live"]},
        "protocols": {
          "live": {"endpoint": "root://live.example.org:1094", "state": "ACTIVE"},
          "retired": {"endpoint": "root://retired.example.org:1094", "state": "DISABLED", "status": "DISABLED"}
        },
        "rcsite": "MIXED-SITE", "state": "ACTIVE", "type": "SE"
      }
    }`

	r := &HostSiteRegistry{logger: newSiteTestLogger(), hosts: map[string][]string{}}
	require.NoError(t, r.Load([]byte(liveShape)))

	for host, want := range map[string]string{
		"head01.aglt2.org": "AGLT2", // srm:// endpoint, port stripped
		"webdav.aglt2.org": "AGLT2", // davs:// on a second SE of the same site
		"live.example.org": "MIXED-SITE",
	} {
		site, status := r.ResolveHost(host)
		assert.Equal(t, want, site, "host %s", host)
		assert.Equal(t, SiteStatusResolvedHostname, status, "host %s", host)
	}

	// A lifecycle field does not exclude an endpoint: the host->site fact holds
	// whether or not that particular door is still in service.
	site, status := r.ResolveHost("retired.example.org")
	assert.Equal(t, "MIXED-SITE", site)
	assert.Equal(t, SiteStatusResolvedHostname, status)

	// A service with no rcsite has nothing to map its host to, so it is skipped.
	site, status = r.ResolveHost("orphan.example.org")
	assert.Empty(t, site)
	assert.Equal(t, SiteStatusUnknown, status)

	// info_url is not an endpoint and must not become a resolvable host on its own
	// account — head01 is only present because a protocol endpoint names it.
	assert.NotContains(t, r.hosts, "wlcg-cric.cern.ch")
}
