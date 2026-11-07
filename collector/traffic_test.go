package collector

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultTrafficEnricher is the classifier as a WLCG-mode collector runs it:
// production defaults, every record classified (no WLCG gate, which the
// correlator supplies).
func defaultTrafficEnricher() *trafficRecordEnricher {
	return &trafficRecordEnricher{classifier: newTrafficClassifier(TrafficConfig{Enabled: true})}
}

// trafficRecord builds a file-close record with both endpoints resolved to the
// given sites. A site of "" is left unresolved with a no_host status, which is
// how an endpoint the resolver could not name arrives here.
func trafficRecord(srcSite, dstSite string) *CollectorRecord {
	record := &CollectorRecord{
		User:          "cmsuser",
		userInfoKnown: true,
		Protocol:      "xrootd",
		Filename:      "/store/data/file.root",
		Read:          1024,
	}
	record.srcSite, record.srcSiteStatus = srcSite, SiteStatusResolved
	record.dstSite, record.dstSiteStatus = dstSite, SiteStatusResolved
	if srcSite == "" {
		record.srcSiteStatus = SiteStatusNoHost
	}
	if dstSite == "" {
		record.dstSiteStatus = SiteStatusNoHost
	}
	return record
}

// TestTrafficScopeFromResolvedSites covers the topology rules and the invariant
// that site_internal_traffic is nothing but a reading of the scope: LAN is
// internal, WAN is not, and an endpoint that did not resolve leaves it with no
// answer at all rather than a false.
func TestTrafficScopeFromResolvedSites(t *testing.T) {
	cases := []struct {
		name         string
		src, dst     string
		srcSt, dstSt string
		wantScope    string
		wantInternal *bool
	}{
		{
			name: "same site is LAN", src: "CERN-PROD", dst: "CERN-PROD",
			srcSt: SiteStatusResolved, dstSt: SiteStatusResolvedConfig,
			wantScope: TrafficScopeLAN, wantInternal: boolValue(true),
		},
		{
			name: "different sites are WAN", src: "CERN-PROD", dst: "IN2P3-CC",
			srcSt: SiteStatusResolvedHostname, dstSt: SiteStatusResolvedIP,
			wantScope: TrafficScopeWAN, wantInternal: boolValue(false),
		},
		{
			name: "unresolved source is UNKNOWN", src: "", dst: "CERN-PROD",
			srcSt: SiteStatusNoHost, dstSt: SiteStatusResolved,
			wantScope: TrafficScopeUnknown, wantInternal: nil,
		},
		{
			name: "unresolved destination is UNKNOWN", src: "CERN-PROD", dst: "",
			srcSt: SiteStatusResolved, dstSt: SiteStatusUnknown,
			wantScope: TrafficScopeUnknown, wantInternal: nil,
		},
		{
			name: "both unresolved is UNKNOWN", src: "", dst: "",
			srcSt: SiteStatusNoHost, dstSt: SiteStatusUnknownIP,
			wantScope: TrafficScopeUnknown, wantInternal: nil,
		},
		{
			// An ambiguous match names a site but is explicitly a guess, so it must
			// not produce a definite LAN — even when both ends guessed the same one.
			name: "ambiguous match does not settle the topology", src: "SITE-FIRST", dst: "SITE-FIRST",
			srcSt: SiteStatusAmbiguous, dstSt: SiteStatusResolved,
			wantScope: TrafficScopeUnknown, wantInternal: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			scope := classifyTrafficScope(c.src, c.srcSt, c.dst, c.dstSt)
			assert.Equal(t, c.wantScope, scope)
			assert.Equal(t, c.wantInternal, siteInternalFromScope(scope))
		})
	}
}

// TestTrafficDimensionsAreIndependent walks the topology/origin grid: neither
// classification may filter or imply the other, and a cross-site operation that
// matches an internal signal must still come out as WAN and internal.
func TestTrafficDimensionsAreIndependent(t *testing.T) {
	cases := []struct {
		name         string
		src, dst     string
		user         string
		filename     string
		wantScope    string
		wantInternal *bool
		wantXRootD   bool
	}{
		{
			name: "same site, ordinary user", src: "CERN-PROD", dst: "CERN-PROD", user: "cmsuser",
			wantScope: TrafficScopeLAN, wantInternal: boolValue(true), wantXRootD: false,
		},
		{
			name: "same site, root", src: "CERN-PROD", dst: "CERN-PROD", user: "root",
			wantScope: TrafficScopeLAN, wantInternal: boolValue(true), wantXRootD: true,
		},
		{
			name: "same site, job-agent account", src: "CERN-PROD", dst: "CERN-PROD", user: "3",
			wantScope: TrafficScopeLAN, wantInternal: boolValue(true), wantXRootD: true,
		},
		{
			name: "different sites, ordinary user", src: "CERN-PROD", dst: "IN2P3-CC", user: "cmsuser",
			wantScope: TrafficScopeWAN, wantInternal: boolValue(false), wantXRootD: false,
		},
		{
			name: "different sites, root", src: "CERN-PROD", dst: "IN2P3-CC", user: "root",
			wantScope: TrafficScopeWAN, wantInternal: boolValue(false), wantXRootD: true,
		},
		{
			name: "unresolved topology still classifies the origin", src: "", dst: "", user: "root",
			wantScope: TrafficScopeUnknown, wantInternal: nil, wantXRootD: true,
		},
		{
			name: "unresolved topology, ordinary user", src: "", dst: "", user: "cmsuser",
			wantScope: TrafficScopeUnknown, wantInternal: nil, wantXRootD: false,
		},
		{
			// MonALISA's daemon + "/replicate:" case: the account is not a system
			// one, the path is what says this is XRootD's own work.
			name: "replication across sites", src: "CERN-PROD", dst: "IN2P3-CC", user: "daemon",
			filename:  "/store/data/replicate:0001/file.root",
			wantScope: TrafficScopeWAN, wantInternal: boolValue(false), wantXRootD: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			record := trafficRecord(c.src, c.dst)
			record.User = c.user
			if c.filename != "" {
				record.Filename = c.filename
			}

			defaultTrafficEnricher().Enrich(context.Background(), record)

			assert.Equal(t, c.wantScope, record.trafficScope)
			assert.Equal(t, c.wantXRootD, record.xrootdInternal)
			assert.Equal(t, c.wantInternal, siteInternalFromScope(record.trafficScope))
		})
	}
}

// TestXRootDInternalSignals pins down exactly which inputs count as XRootD's
// own work, and that nothing else does.
func TestXRootDInternalSignals(t *testing.T) {
	cases := []struct {
		name          string
		user          string
		userDN        string
		userInfoKnown bool
		protocol      string
		appInfo       string
		filename      string
		wantInternal  bool
		wantSignal    string
	}{
		{
			name: "root is a system account", user: "root", userInfoKnown: true,
			wantInternal: true, wantSignal: TrafficSignalInternalUser,
		},
		{
			name: "system accounts match case-insensitively by default", user: "ROOT", userInfoKnown: true,
			wantInternal: true, wantSignal: TrafficSignalInternalUser,
		},
		{
			name: "first job-agent id", user: "1", userInfoKnown: true,
			wantInternal: true, wantSignal: TrafficSignalJobAgent,
		},
		{
			name: "last job-agent id", user: "8", userInfoKnown: true,
			wantInternal: true, wantSignal: TrafficSignalJobAgent,
		},
		{
			name: "an id past the range is an ordinary account", user: "9", userInfoKnown: true,
			wantInternal: false,
		},
		{
			name: "an account that merely starts with a digit is not an agent", user: "1cms", userInfoKnown: true,
			wantInternal: false,
		},
		{
			// user_dn is where the system accounts actually show up in production:
			// the auth stream's n= value, usually a mapped account name.
			name: "root in user_dn", user: "cmsplt01", userInfoKnown: true, userDN: "root",
			wantInternal: true, wantSignal: TrafficSignalInternalUser,
		},
		{
			name: "a system account in a real DN", user: "cmsplt01", userInfoKnown: true,
			userDN:       "/DC=ch/DC=cern/OU=Organic Units/CN=root",
			wantInternal: true, wantSignal: TrafficSignalInternalUser,
		},
		{
			name: "an ordinary account in user_dn", user: "cmsplt01", userInfoKnown: true, userDN: "cms001",
			wantInternal: false,
		},
		{
			name: "an ordinary DN", user: "cmsplt01", userInfoKnown: true,
			userDN:       "/DC=ch/DC=cern/OU=Organic Units/CN=cmsprod",
			wantInternal: false,
		},
		{
			// MonALISA's exact daemon case, as it arrives here: the account is in
			// user_dn, the record's own user is the hex fallback, and it is the
			// path that says this is XRootD's own work.
			name: "daemon in user_dn replicating", user: "AAAAAAAk", userDN: "daemon",
			filename:     "/store/replicate:1/file.root",
			wantInternal: true, wantSignal: TrafficSignalReplication,
		},
		{
			// daemon is not a system account on its own, so an ordinary transfer
			// under it stays a user's, as in MonALISA.
			name: "daemon not replicating", user: "cmsplt01", userInfoKnown: true, userDN: "daemon",
			filename:     "/store/data/file.root",
			wantInternal: false,
		},
		{
			name: "replication protocol", user: "cmsuser", userInfoKnown: true, protocol: "replicate",
			wantInternal: true, wantSignal: TrafficSignalReplication,
		},
		{
			name: "replication path segment", user: "daemon", userInfoKnown: true,
			filename:     "/store/replicate:1234/file.root",
			wantInternal: true, wantSignal: TrafficSignalReplication,
		},
		{
			name: "replication appinfo", user: "daemon", userInfoKnown: true, appInfo: "xrdcp/replicate:9",
			wantInternal: true, wantSignal: TrafficSignalReplication,
		},
		{
			name: "no user but a replication signal", user: "", protocol: "xrootd",
			filename:     "/store/replicate:1234/file.root",
			wantInternal: true, wantSignal: TrafficSignalReplication,
		},
		{
			// The whole point of userInfoKnown: with no correlated user info the
			// account is a hex of the numeric user id, so a low id reads as a
			// job-agent account. A missing input is not a signal.
			name: "a hex user-id fallback is not a job agent", user: "4", userInfoKnown: false,
			wantInternal: false,
		},
		{
			name: "nothing known about the record", user: "", protocol: "unknown",
			wantInternal: false,
		},
		{
			name: "an ordinary transfer", user: "cmsuser", userInfoKnown: true, protocol: "xrootd",
			filename:     "/store/data/file.root",
			wantInternal: false,
		},
		{
			// A path that only contains the letters must not match: the prefix is
			// matched at the start of a path segment.
			name: "a path that merely mentions replication", user: "cmsuser", userInfoKnown: true,
			filename:     "/store/user/cms/my-replicate-notes/file.root",
			wantInternal: false,
		},
	}

	classifier := newTrafficClassifier(TrafficConfig{Enabled: true})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			record := &CollectorRecord{
				User:          c.user,
				UserDN:        c.userDN,
				userInfoKnown: c.userInfoKnown,
				Protocol:      c.protocol,
				AppInfo:       c.appInfo,
				Filename:      c.filename,
			}

			internal, signal := classifier.ClassifyXRootDInternal(record)
			assert.Equal(t, c.wantInternal, internal)
			if c.wantInternal {
				assert.Equal(t, c.wantSignal, signal)
			}
		})
	}
}

// fakeStorageEndpoints is a stand-in for the deferred storage-server discovery,
// so the seam the classifier leaves for it is exercised.
type fakeStorageEndpoints map[string]bool

func (f fakeStorageEndpoints) IsStorageEndpoint(host string) bool { return f[host] }

// TestStorageToStorageIsOffUnlessWired covers the deferred fourth signal: it
// contributes nothing while no storage-endpoint set is configured, which is the
// production case, and is a plain internal signal once one is.
func TestStorageToStorageIsOffUnlessWired(t *testing.T) {
	record := trafficRecord("CERN-PROD", "CERN-PROD")
	record.User = "cmsuser"
	record.ServerHostname = "se01.cern.ch"
	record.clientHostname = "se02.cern.ch"

	offByDefault := newTrafficClassifier(TrafficConfig{Enabled: true})
	internal, _ := offByDefault.ClassifyXRootDInternal(record)
	assert.False(t, internal, "no storage endpoint set is wired in production")

	wired := newTrafficClassifier(TrafficConfig{Enabled: true})
	wired.storage = fakeStorageEndpoints{"se01.cern.ch": true, "se02.cern.ch": true}
	internal, signal := wired.ClassifyXRootDInternal(record)
	assert.True(t, internal)
	assert.Equal(t, TrafficSignalStorage, signal)

	// One end unknown to the set is not storage-to-storage.
	wired.storage = fakeStorageEndpoints{"se01.cern.ch": true}
	internal, _ = wired.ClassifyXRootDInternal(record)
	assert.False(t, internal)
}

// TestTrafficConfigRules covers the configurable rules: custom values are
// honoured and the production defaults stay where they are.
func TestTrafficConfigRules(t *testing.T) {
	t.Run("defaults are root, 1-8 and replicate", func(t *testing.T) {
		cfg := TrafficConfig{Enabled: true}.withDefaults()
		assert.Equal(t, []string{"root"}, cfg.InternalUsers)
		assert.Equal(t, []string{"replicate"}, cfg.ReplicationPrefixes)
		assert.Equal(t, 1, cfg.JobAgentMin)
		assert.Equal(t, 8, cfg.JobAgentMax)
	})

	t.Run("custom accounts and prefixes", func(t *testing.T) {
		classifier := newTrafficClassifier(TrafficConfig{
			Enabled:             true,
			InternalUsers:       []string{"xrootd", "storage"},
			JobAgentMin:         100,
			JobAgentMax:         102,
			ReplicationPrefixes: []string{"xfer"},
		})

		internal, signal := classifier.ClassifyXRootDInternal(&CollectorRecord{User: "xrootd", userInfoKnown: true})
		assert.True(t, internal)
		assert.Equal(t, TrafficSignalInternalUser, signal)

		internal, signal = classifier.ClassifyXRootDInternal(&CollectorRecord{User: "101", userInfoKnown: true})
		assert.True(t, internal)
		assert.Equal(t, TrafficSignalJobAgent, signal)

		internal, signal = classifier.ClassifyXRootDInternal(&CollectorRecord{Filename: "/store/xfer:1/f.root"})
		assert.True(t, internal)
		assert.Equal(t, TrafficSignalReplication, signal)

		// The defaults are replaced, not added to.
		internal, _ = classifier.ClassifyXRootDInternal(&CollectorRecord{User: "root", userInfoKnown: true})
		assert.False(t, internal, "root is not a system account under a custom list")
		internal, _ = classifier.ClassifyXRootDInternal(&CollectorRecord{User: "3", userInfoKnown: true})
		assert.False(t, internal, "3 is outside the custom job-agent range")
		internal, _ = classifier.ClassifyXRootDInternal(&CollectorRecord{Filename: "/store/replicate:1/f.root"})
		assert.False(t, internal, "replicate is not a marker under a custom list")
	})

	t.Run("case-sensitive matching", func(t *testing.T) {
		classifier := newTrafficClassifier(TrafficConfig{Enabled: true, CaseSensitive: true})

		internal, _ := classifier.ClassifyXRootDInternal(&CollectorRecord{User: "root", userInfoKnown: true})
		assert.True(t, internal)
		internal, _ = classifier.ClassifyXRootDInternal(&CollectorRecord{User: "ROOT", userInfoKnown: true})
		assert.False(t, internal)
	})

	t.Run("a max below the min turns the job-agent rule off", func(t *testing.T) {
		classifier := newTrafficClassifier(TrafficConfig{Enabled: true, JobAgentMin: 1, JobAgentMax: 0})

		internal, _ := classifier.ClassifyXRootDInternal(&CollectorRecord{User: "3", userInfoKnown: true})
		assert.False(t, internal)
		internal, _ = classifier.ClassifyXRootDInternal(&CollectorRecord{User: "root", userInfoKnown: true})
		assert.True(t, internal, "the account rule is unaffected")
	})
}

// TestTrafficEnricherOnlyTouchesWLCGRecords verifies the WLCG gate: a record
// that will not be converted is left unclassified, so it costs nothing and
// carries none of the fields.
func TestTrafficEnricherOnlyTouchesWLCGRecords(t *testing.T) {
	enricher := defaultTrafficEnricher()
	enricher.wlcgOnly = func(record *CollectorRecord) bool { return record.VO == "cms" }

	skipped := trafficRecord("CERN-PROD", "CERN-PROD")
	skipped.VO = "dune"
	enricher.Enrich(context.Background(), skipped)
	assert.Empty(t, skipped.trafficScope)

	converted := trafficRecord("CERN-PROD", "CERN-PROD")
	converted.VO = "cms"
	enricher.Enrich(context.Background(), converted)
	assert.Equal(t, TrafficScopeLAN, converted.trafficScope)
}

// TestTrafficSerialization covers the emitted document: the three public fields
// are there, the helper classifications the pipeline computes on the way are
// not, and "user" is whatever it always was.
func TestTrafficSerialization(t *testing.T) {
	marshal := func(t *testing.T, record *CollectorRecord) map[string]interface{} {
		t.Helper()
		wlcg, err := ConvertToWLCG(record, testWLCGMetadata(), nil)
		require.NoError(t, err)
		payload, err := wlcg.ToJSON()
		require.NoError(t, err)

		var doc map[string]interface{}
		require.NoError(t, json.Unmarshal(payload, &doc))
		return doc
	}

	t.Run("a classified LAN record", func(t *testing.T) {
		record := trafficRecord("CERN-PROD", "CERN-PROD")
		record.User = "root"
		record.UserDN = "/DC=ch/DC=cern/OU=Organic Units/CN=cmsprod"
		defaultTrafficEnricher().Enrich(context.Background(), record)

		doc := marshal(t, record)

		assert.Equal(t, true, doc["site_internal_traffic"])
		assert.Equal(t, TrafficScopeLAN, doc["traffic_scope"])
		assert.Equal(t, true, doc["xrootd_internal_traffic"])

		// The user identity is published as it always was, not replaced by the
		// classification it fed.
		assert.Equal(t, "cmsprod", doc["user"])

		// The helper classifications stay inside the classifier.
		assert.NotContains(t, doc, "user_generated")
		assert.NotContains(t, doc, "storage_to_storage")
	})

	t.Run("a cross-site user transfer", func(t *testing.T) {
		record := trafficRecord("CERN-PROD", "IN2P3-CC")
		defaultTrafficEnricher().Enrich(context.Background(), record)

		doc := marshal(t, record)

		assert.Equal(t, false, doc["site_internal_traffic"])
		assert.Equal(t, TrafficScopeWAN, doc["traffic_scope"])
		assert.Equal(t, false, doc["xrootd_internal_traffic"])
	})

	t.Run("an unresolved topology leaves site_internal_traffic off", func(t *testing.T) {
		record := trafficRecord("", "CERN-PROD")
		record.User = "root"
		defaultTrafficEnricher().Enrich(context.Background(), record)

		doc := marshal(t, record)

		assert.NotContains(t, doc, "site_internal_traffic",
			"an unresolved topology must not be published as a false")
		assert.Equal(t, TrafficScopeUnknown, doc["traffic_scope"])
		assert.Equal(t, true, doc["xrootd_internal_traffic"], "the origin is classified independently")
	})

	t.Run("an unclassified record carries none of the fields", func(t *testing.T) {
		doc := marshal(t, trafficRecord("CERN-PROD", "CERN-PROD"))

		assert.NotContains(t, doc, "site_internal_traffic")
		assert.NotContains(t, doc, "traffic_scope")
		assert.NotContains(t, doc, "xrootd_internal_traffic")
	})
}

// boolValue is a test helper for the pointer site_internal_traffic carries.
func boolValue(v bool) *bool { return &v }

// enricherNames lists the enrichment stages a correlator registered, in order.
func enricherNames(c *Correlator) []string {
	names := make([]string, 0, len(c.enrichers))
	for _, enricher := range c.enrichers {
		names = append(names, enricher.Name())
	}
	return names
}

// TestTrafficEnricherRegistration covers the wiring: the classification is part
// of WLCG mode, so it is registered only when that mode is on and the feature
// is not switched off, and it runs after the site resolver whose answers it
// reads.
func TestTrafficEnricherRegistration(t *testing.T) {
	newCorrelator := func(t *testing.T, config CorrelatorConfig) *Correlator {
		t.Helper()
		config.TTL = 5 * time.Second
		config.Logger = newSiteTestLogger()
		c := NewCorrelatorWithConfig(config)
		t.Cleanup(c.Stop)
		return c
	}

	t.Run("off outside WLCG mode", func(t *testing.T) {
		c := newCorrelator(t, CorrelatorConfig{Traffic: TrafficConfig{Enabled: true}})
		assert.NotContains(t, enricherNames(c), "traffic")
	})

	t.Run("off when the feature is disabled", func(t *testing.T) {
		c := newCorrelator(t, CorrelatorConfig{WLCGEnabled: true})
		assert.NotContains(t, enricherNames(c), "traffic")
	})

	t.Run("on in WLCG mode, after the site resolver", func(t *testing.T) {
		c := newCorrelator(t, CorrelatorConfig{
			WLCGEnabled:  true,
			Traffic:      TrafficConfig{Enabled: true},
			SiteRegistry: NewSiteRegistry(newSiteTestLogger()),
		})
		assert.Equal(t, []string{"site", "traffic"}, enricherNames(c))
	})
}
