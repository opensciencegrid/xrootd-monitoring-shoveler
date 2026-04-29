package collector

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestEndToEnd_DNSEnrichmentFlow tests the complete flow of DNS enrichment
// from packet processing through to record publishing
func TestEndToEnd_DNSEnrichmentFlow(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create mock DNS resolver
	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			switch addr {
			case "192.0.2.1":
				return []string{"test-host.example.com"}, nil
			case "192.168.1.100":
				return []string{"server.example.com"}, nil
			default:
				return nil, &net.DNSError{Err: "no such host", Name: addr}
			}
		},
	}

	// Create correlator with DNS enrichment enabled
	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          10000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}
	correlator := NewCorrelatorWithConfig(config)
	correlator.dnsResolver = mockResolver
	defer correlator.Stop()

	// Create a packet with an IP address in the host field
	serverStart := int32(1639505770)
	remoteAddr := "192.168.1.100:1094"

	// Create user packet with IP address host
	userPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: serverStart,
		},
		RemoteAddr: remoteAddr,
		PacketType: parser.PacketTypeUser,
		UserRecord: &parser.UserRecord{
			DictId: 100,
			UserInfo: parser.UserInfo{
				Protocol: "root",
				Username: "testuser",
				Pid:      1234,
				Sid:      5678,
				Host:     "192.0.2.1", // IP address
			},
			AuthInfo: parser.AuthInfo{
				DN:  "/DC=org/DC=example/OU=People/CN=Test User",
				Org: "cms",
			},
		},
	}

	// Process user packet
	_, err := correlator.ProcessPacket(userPacket)
	assert.NoError(t, err)

	// Create open packet
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeOpen,
			FileId:  1,
			UserId:  100,
		},
		FileSize: 1024,
		User:     100,
		Lfn:      []byte("/test/file.root"),
	}

	openPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: serverStart,
		},
		RemoteAddr:  remoteAddr,
		FileRecords: []interface{}{openRec},
	}

	// Process open packet
	_, err = correlator.ProcessPacket(openPacket)
	assert.NoError(t, err)

	// Create close packet
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  1,
			UserId:  100,
		},
		Xfr: parser.StatXFR{
			Read:  1024,
			Write: 0,
			Readv: 0,
		},
	}

	closePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: serverStart,
		},
		RemoteAddr:  remoteAddr,
		FileRecords: []interface{}{closeRec},
	}

	// Process close packet - should return a record that needs DNS enrichment
	records, err := correlator.ProcessPacket(closePacket)
	assert.NoError(t, err)
	assert.Len(t, records, 1)

	record := records[0]

	// Verify the record needs enrichment
	assert.True(t, record.NeedsEnrichment(), "Record should need enrichment")

	// Simulate what the main.go publish loop does with a timeout to avoid hanging
	doneCh := make(chan EnrichedRecord, 1)
	correlator.EnqueueForEnrichment(record, EnrichmentDestination{Results: doneCh})

	// Wait for enrichment to complete with timeout
	var enrichedRecord *CollectorRecord
	select {
	case enrichedMsg := <-doneCh:
		enrichedRecord = enrichedMsg.Record
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for DNS enrichment to complete")
	}

	// Verify the enriched record has the user domain set
	assert.NotNil(t, enrichedRecord)
	assert.Equal(t, "example.com", enrichedRecord.UserDomain, "UserDomain should be extracted from DNS result")
	assert.Equal(t, "server.example.com", enrichedRecord.ServerHostname, "ServerHostname should be resolved via DNS")

	// Verify DNS was called for both user IP and server IP
	assert.Equal(t, int64(2), mockResolver.lookupCount.Load(), "DNS resolver should have been called twice")
}

// TestEndToEnd_DNSEnrichmentCacheHit tests that cached DNS results don't trigger async enrichment
func TestEndToEnd_DNSEnrichmentCacheHit(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create mock DNS resolver
	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			if addr == "192.0.2.1" {
				return []string{"cached-host.example.com"}, nil
			}
			return nil, &net.DNSError{Err: "no such host", Name: addr}
		},
	}

	// Create correlator with DNS enrichment enabled
	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          10000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}
	correlator := NewCorrelatorWithConfig(config)
	correlator.dnsResolver = mockResolver
	defer correlator.Stop()

	// Pre-populate the DNS cache
	correlator.dnsCache.Set("192.0.2.1", "cached-host.example.com")
	correlator.dnsCache.Set("192.168.1.100", "server.example.com")

	// Create packets with the same IP
	serverStart := int32(1639505770)
	remoteAddr := "192.168.1.100:1094"

	// Create user packet with IP address host
	userPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: serverStart,
		},
		RemoteAddr: remoteAddr,
		PacketType: parser.PacketTypeUser,
		UserRecord: &parser.UserRecord{
			DictId: 100,
			UserInfo: parser.UserInfo{
				Protocol: "root",
				Username: "testuser",
				Pid:      1234,
				Sid:      5678,
				Host:     "192.0.2.1", // IP address
			},
		},
	}

	// Process user packet
	_, err := correlator.ProcessPacket(userPacket)
	assert.NoError(t, err)

	// Create open and close packets
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeOpen,
			FileId:  1,
			UserId:  100,
		},
		FileSize: 1024,
		User:     100,
		Lfn:      []byte("/test/file.root"),
	}

	openPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: serverStart,
		},
		RemoteAddr:  remoteAddr,
		FileRecords: []interface{}{openRec},
	}

	_, err = correlator.ProcessPacket(openPacket)
	assert.NoError(t, err)

	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  1,
			UserId:  100,
		},
		Xfr: parser.StatXFR{
			Read:  1024,
			Write: 0,
			Readv: 0,
		},
	}

	closePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: serverStart,
		},
		RemoteAddr:  remoteAddr,
		FileRecords: []interface{}{closeRec},
	}

	// Process close packet
	records, err := correlator.ProcessPacket(closePacket)
	assert.NoError(t, err)
	assert.Len(t, records, 1)

	record := records[0]

	// Verify the record does NOT need enrichment (cache hit)
	assert.False(t, record.NeedsEnrichment(), "Record should NOT need enrichment (cache hit)")

	// Verify the domain was set from cache
	assert.Equal(t, "example.com", record.UserDomain, "UserDomain should be set from cache")
	assert.Equal(t, "server.example.com", record.ServerHostname, "ServerHostname should be set from cache")

	// Verify DNS was NOT called (cache hit)
	assert.Equal(t, int64(0), mockResolver.lookupCount.Load(), "DNS resolver should NOT have been called (cache hit)")
}

// TestEndToEnd_WLCGDNSEnrichment verifies that DNS enrichment correctly populates
// server_host in the WLCG JSON payload produced by buildEnrichedRecord.
func TestEndToEnd_WLCGDNSEnrichment(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			switch addr {
			case "128.104.227.159":
				return []string{"cmssrv138.hep.wisc.edu."}, nil
			case "192.0.2.1":
				return []string{"client.example.com."}, nil
			default:
				return nil, &net.DNSError{Err: "no such host", Name: addr}
			}
		},
	}

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          10000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}
	correlator := NewCorrelatorWithConfig(config)
	correlator.dnsResolver = mockResolver
	defer correlator.Stop()

	serverStart := int32(1639505770)
	remoteAddr := "128.104.227.159:1094"

	// User packet — client IP that resolves to a domain
	userPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: serverStart,
		},
		RemoteAddr: remoteAddr,
		PacketType: parser.PacketTypeUser,
		UserRecord: &parser.UserRecord{
			DictId: 100,
			UserInfo: parser.UserInfo{
				Protocol: "https",
				Username: "cmsprod",
				Host:     "192.0.2.1",
			},
			AuthInfo: parser.AuthInfo{
				Org: "cms",
			},
		},
	}
	_, err := correlator.ProcessPacket(userPacket)
	assert.NoError(t, err)

	// Open packet for a /store path (WLCG-eligible)
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{RecType: parser.RecTypeOpen, FileId: 1, UserId: 100},
		FileSize: 1024,
		User:     100,
		Lfn:      []byte("/store/data/Run2026/file.root"),
	}
	openPacket := &parser.Packet{
		Header:      parser.Header{Code: parser.PacketTypeFStat, ServerStart: serverStart},
		RemoteAddr:  remoteAddr,
		FileRecords: []interface{}{openRec},
	}
	_, err = correlator.ProcessPacket(openPacket)
	assert.NoError(t, err)

	// Close packet
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{RecType: parser.RecTypeClose, FileId: 1, UserId: 100},
		Xfr:    parser.StatXFR{Read: 1024},
	}
	closePacket := &parser.Packet{
		Header:      parser.Header{Code: parser.PacketTypeFStat, ServerStart: serverStart},
		RemoteAddr:  remoteAddr,
		FileRecords: []interface{}{closeRec},
	}
	records, err := correlator.ProcessPacket(closePacket)
	assert.NoError(t, err)
	assert.Len(t, records, 1)

	record := records[0]

	// Verify the record is classified as a WLCG packet
	assert.True(t, IsWLCGPacket(record), "record should be a WLCG packet")
	assert.True(t, record.NeedsEnrichment(), "record should need DNS enrichment")

	// Route through the enrichment pipeline
	doneCh := make(chan EnrichedRecord, 1)
	correlator.EnqueueForEnrichment(record, EnrichmentDestination{
		Results:      doneCh,
		WLCGExchange: "wlcg-exchange",
	})

	var enrichedMsg EnrichedRecord
	select {
	case enrichedMsg = <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for DNS enrichment")
	}

	// The enriched record must have the resolved server hostname
	assert.Equal(t, "cmssrv138.hep.wisc.edu", enrichedMsg.Record.ServerHostname,
		"ServerHostname should be DNS-resolved")

	// The payload must be WLCG JSON with server_host = resolved hostname
	assert.Equal(t, "wlcg-exchange", enrichedMsg.Exchange, "exchange should be WLCG exchange")

	var wlcgData map[string]interface{}
	assert.NoError(t, json.Unmarshal(enrichedMsg.Payload, &wlcgData))
	assert.Equal(t, "cmssrv138.hep.wisc.edu", wlcgData["server_host"],
		"server_host in WLCG JSON must be the DNS-resolved hostname, not the raw IP")
	assert.Equal(t, "128.104.227.159", wlcgData["server_ip"],
		"server_ip should retain the raw IP address")
	assert.Equal(t, "wisc.edu", wlcgData["server_domain"],
		"server_domain should be extracted from the resolved hostname")
}
