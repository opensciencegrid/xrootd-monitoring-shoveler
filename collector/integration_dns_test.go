package collector

import (
	"context"
	"net"
	"sync"
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
			if addr == "192.0.2.1" {
				return []string{"test-host.example.com"}, nil
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
		DNSWorkers:          5,
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

	// Verify the record needs DNS enrichment
	assert.True(t, record.NeedsDNSEnrichment(), "Record should need DNS enrichment")

	// Simulate what the main.go publish loop does
	var wg sync.WaitGroup
	wg.Add(1)

	var enrichedRecord *CollectorRecord
	correlator.EnrichRecordAsync(record, func(r *CollectorRecord) {
		enrichedRecord = r
		wg.Done()
	})

	// Wait for enrichment to complete
	wg.Wait()

	// Verify the enriched record has the user domain set
	assert.NotNil(t, enrichedRecord)
	assert.Equal(t, "example.com", enrichedRecord.UserDomain, "UserDomain should be extracted from DNS result")

	// Verify DNS was actually called
	assert.Equal(t, 1, mockResolver.lookupCount, "DNS resolver should have been called once")
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
		DNSWorkers:          5,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}
	correlator := NewCorrelatorWithConfig(config)
	correlator.dnsResolver = mockResolver
	defer correlator.Stop()

	// Pre-populate the DNS cache
	correlator.dnsCache.Set("192.0.2.1", "cached-host.example.com")

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

	// Verify the record does NOT need DNS enrichment (cache hit)
	assert.False(t, record.NeedsDNSEnrichment(), "Record should NOT need DNS enrichment (cache hit)")

	// Verify the domain was set from cache
	assert.Equal(t, "example.com", record.UserDomain, "UserDomain should be set from cache")

	// Verify DNS was NOT called (cache hit)
	assert.Equal(t, 0, mockResolver.lookupCount, "DNS resolver should NOT have been called (cache hit)")
}
