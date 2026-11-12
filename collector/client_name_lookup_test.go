package collector

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cernServer is a CERN EOS server, the reporting end in these tests.
const cernServer = "st-096-100gb-ip312-6fb70.cern.ch"

// workerNodes are CERN worker nodes as XRootD reports them, unqualified, with
// the address the site's search list resolves them to. The addresses sit in a
// CRIC netroute owned by CERN-PROD.
var workerNodes = map[string][]string{
	"b9p04p7188": {"188.185.205.157"},
	"b9p25p9299": {"188.185.212.34"},
}

// workerNodePTRs are the PTR names behind those addresses. b9p25p9299 has none.
var workerNodePTRs = map[string]string{
	"188.185.205.157": "b9p04p7188.cern.ch.",
	"127.0.0.1":       cernServer + ".", // the reporting server in recordForHost
}

// lookupLog records the names and addresses a test resolver was asked for.
type lookupLog struct {
	names, addrs []string
}

// newTestResolver answers from workerNodes and workerNodePTRs, and logs each
// question.
func newTestResolver(log *lookupLog) *mockDNSResolver {
	return &mockDNSResolver{
		hostFunc: func(_ context.Context, host string) ([]string, error) {
			log.names = append(log.names, host)
			if addrs, ok := workerNodes[host]; ok {
				return addrs, nil
			}
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		},
		lookupFunc: func(_ context.Context, addr string) ([]string, error) {
			log.addrs = append(log.addrs, addr)
			if name, ok := workerNodePTRs[addr]; ok {
				return []string{name}, nil
			}
			return nil, &net.DNSError{Err: "no such host", Name: addr, IsNotFound: true}
		},
	}
}

// newClientLookupCorrelator builds a correlator with DNS enrichment on and the
// test resolver.
func newClientLookupCorrelator(t *testing.T) (*Correlator, *lookupLog) {
	t.Helper()

	c := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:                 5 * time.Second,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              newSiteTestLogger(),
	})
	t.Cleanup(c.Stop)

	log := &lookupLog{}
	c.dnsResolver = newTestResolver(log)
	return c, log
}

// bareClient is a record with an unqualified client name, marked for lookup.
func bareClient(name string) *CollectorRecord {
	return &CollectorRecord{Host: name, ServerHostname: cernServer, needsClientNameLookup: true, clientLookupName: name}
}

func lookUpAll(*CollectorRecord) bool { return true }

// TestClientNameLookupResolvesUnqualifiedNames covers the lookup itself: the
// name goes to the resolver as reported, the address and its PTR name land on
// the record, and every answer comes from cache the second time, a missing
// name and a missing PTR included.
func TestClientNameLookupResolvesUnqualifiedNames(t *testing.T) {
	c, log := newClientLookupCorrelator(t)
	enricher := &dnsRecordEnricher{correlator: c, clientNames: lookUpAll}

	t.Run("a known worker node", func(t *testing.T) {
		record := bareClient("b9p04p7188")
		enricher.Enrich(context.Background(), record)

		assert.Equal(t, "188.185.205.157", record.clientIP)
		assert.Equal(t, "b9p04p7188.cern.ch", record.clientHostname)
		assert.Equal(t, "cern.ch", record.UserDomain)
		assert.False(t, record.needsClientNameLookup, "the pending flag must be cleared")
	})

	t.Run("a worker node with no PTR record", func(t *testing.T) {
		record := bareClient("b9p25p9299")
		enricher.Enrich(context.Background(), record)

		assert.Equal(t, "188.185.212.34", record.clientIP, "the address still feeds the ip method")
		assert.Empty(t, record.UserDomain)
	})

	t.Run("a name DNS cannot place", func(t *testing.T) {
		record := bareClient("wn-nowhere")
		enricher.Enrich(context.Background(), record)

		assert.Empty(t, record.clientIP)
		assert.False(t, record.needsClientNameLookup)
	})

	t.Run("every answer is cached", func(t *testing.T) {
		names, addrs := len(log.names), len(log.addrs)
		for i := 0; i < 5; i++ {
			for _, name := range []string{"b9p04p7188", "b9p25p9299", "wn-nowhere"} {
				enricher.Enrich(context.Background(), bareClient(name))
			}
		}
		assert.Equal(t, names, len(log.names), "no new forward lookups")
		assert.Equal(t, addrs, len(log.addrs), "no new PTR lookups, the missing one included")
	})

	assert.Equal(t, []string{"b9p04p7188", "b9p25p9299", "wn-nowhere"}, log.names, "names go to the resolver as reported")
}

// TestClientNameLookupRetriesTimeouts covers the failures that are not cached:
// a timeout says nothing about the name or its PTR, so the next record asks
// again.
func TestClientNameLookupRetriesTimeouts(t *testing.T) {
	c, _ := newClientLookupCorrelator(t)
	forward, reverse := 0, 0
	c.dnsResolver = &mockDNSResolver{
		hostFunc: func(_ context.Context, host string) ([]string, error) {
			forward++
			if forward == 1 {
				return nil, &net.DNSError{Err: "i/o timeout", Name: host, IsTimeout: true}
			}
			return workerNodes[host], nil
		},
		lookupFunc: func(_ context.Context, addr string) ([]string, error) {
			reverse++
			if reverse == 1 {
				return nil, &net.DNSError{Err: "i/o timeout", Name: addr, IsTimeout: true}
			}
			return []string{workerNodePTRs[addr]}, nil
		},
	}
	ctx := context.Background()

	assert.Equal(t, clientLookup{}, c.lookupClient(ctx, "b9p04p7188"), "the forward lookup times out")
	assert.Equal(t, clientLookup{addr: "188.185.205.157"}, c.lookupClient(ctx, "b9p04p7188"),
		"the address arrives; the PTR lookup times out")
	assert.Equal(t, clientLookup{addr: "188.185.205.157", hostname: "b9p04p7188.cern.ch"}, c.lookupClient(ctx, "b9p04p7188"),
		"neither timeout was cached")
	assert.Equal(t, []int{3, 2}, []int{forward, reverse})
}

// TestClientNameLookupSkips covers records the site resolver will not place:
// they are left alone and nothing reaches DNS.
func TestClientNameLookupSkips(t *testing.T) {
	c, log := newClientLookupCorrelator(t)

	for name, gate := range map[string]func(*CollectorRecord) bool{
		"no site resolution": nil,
		"not a WLCG record":  func(*CollectorRecord) bool { return false },
	} {
		record := bareClient("b9p04p7188")
		(&dnsRecordEnricher{correlator: c, clientNames: gate}).Enrich(context.Background(), record)
		assert.Empty(t, record.clientIP, name)
		assert.False(t, record.needsClientNameLookup, name)
	}

	assert.Empty(t, log.names)
	assert.Empty(t, log.addrs)
}

// TestNoClientLookupWithoutDNSEnrichment covers DNS enrichment turned off: a
// bare name keeps its host but is not marked for a lookup nothing would run.
func TestNoClientLookupWithoutDNSEnrichment(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{TTL: 5 * time.Second, Logger: newSiteTestLogger()})
	t.Cleanup(c.Stop)

	record := recordForHost(c, "b9p04p7188")
	assert.Equal(t, "b9p04p7188", record.clientHostname)
	assert.False(t, record.needsClientNameLookup)
	assert.False(t, record.NeedsEnrichment())
}

// TestUnqualifiedClientEndToEnd runs a close from a CERN worker node, reported
// by its bare name, through the correlator and every enricher to the published
// WLCG record: the server is placed by its PTR name, the client by the address
// its name resolves to, and the record is LAN.
func TestUnqualifiedClientEndToEnd(t *testing.T) {
	domains := NewSiteRegistry(newSiteTestLogger())
	require.NoError(t, domains.Load([]byte(`{"cern.ch": ["CERN-PROD"]}`)))
	ips := NewIPSiteRegistry(newSiteTestLogger())
	require.NoError(t, ips.Load([]byte(`{"CERN-PROD": {"netroutes": {"a": {"networks": {"ipv4": ["188.185.192.0/18"]}}}}}`)))

	newCorrelator := func(withSites bool) (*Correlator, *lookupLog) {
		cfg := CorrelatorConfig{
			TTL:                 5 * time.Second,
			EnableDNSEnrichment: true,
			DNSCacheTTL:         time.Hour,
			DNSTimeout:          2 * time.Second,
			Logger:              newSiteTestLogger(),
			WLCGEnabled:         true,
			Traffic:             TrafficConfig{Enabled: true},
		}
		if withSites {
			cfg.SiteRegistry, cfg.SiteIPRegistry = domains, ips
		}
		c := NewCorrelatorWithConfig(cfg)
		t.Cleanup(c.Stop)
		log := &lookupLog{}
		c.dnsResolver = newTestResolver(log)
		return c, log
	}

	publish := func(t *testing.T, c *Correlator) (string, map[string]interface{}) {
		results := make(chan EnrichedRecord, 1)
		c.processEnrichmentRequest(enrichmentRequest{
			record:      recordForHost(c, "b9p04p7188"),
			destination: EnrichmentDestination{Results: results, WLCGExchange: "wlcg"},
		})
		result := <-results
		var doc map[string]interface{}
		require.NoError(t, json.Unmarshal(result.Payload, &doc))
		return result.Exchange, doc
	}

	t.Run("with site resolution it is LAN", func(t *testing.T) {
		c, log := newCorrelator(true)
		exchange, doc := publish(t, c)

		assert.Equal(t, "wlcg", exchange)
		assert.Equal(t, []string{"b9p04p7188"}, log.names)
		assert.Equal(t, "CERN-PROD", doc["src_site"])
		assert.Equal(t, "CERN-PROD", doc["dst_site"])
		assert.Equal(t, TrafficScopeLAN, doc["traffic_scope"])
		assert.Equal(t, true, doc["site_internal_traffic"])
	})

	t.Run("without site resolution nothing is looked up", func(t *testing.T) {
		c, log := newCorrelator(false)
		_, doc := publish(t, c)

		assert.Empty(t, log.names)
		assert.Equal(t, TrafficScopeUnknown, doc["traffic_scope"])
	})
}

// recordForHost builds the record a close produces for a client XRootD reported
// as host.
func recordForHost(c *Correlator, host string) *CollectorRecord {
	serverID := "1234567890#127.0.0.1:1234"
	userInfo := parser.UserInfo{Protocol: "root", Username: "jdoe", Pid: 1, Sid: 1, Host: host}
	c.userMap.Set(BuildUserInfoKey(serverID, userInfo), &UserState{UserID: 100, UserInfo: userInfo})
	c.dictMap.Set(BuildDictIDKey(serverID, 100), userInfo)

	state := &FileState{FileID: 1, UserID: 100, Filename: "/store/f.root", ServerID: serverID}
	closeRec := parser.FileCloseRecord{Header: parser.FileHeader{RecType: 'c', FileId: 1, UserId: 100}}
	packet := &parser.Packet{Header: parser.Header{ServerStart: 1234567890}, RemoteAddr: "127.0.0.1:1234"}
	return c.createCorrelatedRecord(state, closeRec, packet, time.Now().Unix())
}

// TestClientHostKind checks which path each kind of client host takes. Names
// that start with "f" or a digit used to be taken for addresses, which lost
// their domain and kept the unqualified ones from being looked up.
func TestClientHostKind(t *testing.T) {
	c, _ := newClientLookupCorrelator(t)

	for _, host := range []string{"fermicloud12.fnal.gov", "fts3.cern.ch", "1wn.example.org"} {
		t.Run("name "+host, func(t *testing.T) {
			record := recordForHost(c, host)
			assert.Equal(t, host, record.clientHostname)
			assert.NotEmpty(t, record.UserDomain)
			assert.False(t, record.needsDNSEnrichment)
			assert.False(t, record.needsClientNameLookup)
		})
	}

	for _, host := range []string{"fwn042", "1wn-b07", "b9p04p7188"} {
		t.Run("unqualified "+host, func(t *testing.T) {
			record := recordForHost(c, host)
			assert.True(t, record.needsClientNameLookup)
			assert.Equal(t, host, record.clientLookupName)
			assert.False(t, record.needsDNSEnrichment)
		})
	}

	for host, ip := range map[string]string{
		"192.0.2.10":          "192.0.2.10",
		"[2001:db8::1]":       "2001:db8::1",
		"fd00::1":             "fd00::1",
		"fe80::1%eth0":        "fe80::1",
		"[::ffff:192.0.2.10]": "192.0.2.10",
		"::ffff:192.0.2.11":   "192.0.2.11",
	} {
		t.Run("address "+host, func(t *testing.T) {
			record := recordForHost(c, host)
			assert.True(t, record.needsDNSEnrichment)
			assert.Equal(t, ip, record.enrichmentIP)
			assert.False(t, record.needsClientNameLookup)
		})
	}
}
