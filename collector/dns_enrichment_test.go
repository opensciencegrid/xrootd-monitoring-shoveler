package collector

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// mockDNSResolver implements DNSResolver for testing
type mockDNSResolver struct {
	lookupFunc  func(ctx context.Context, addr string) ([]string, error)
	lookupCount atomic.Int64
}

func (m *mockDNSResolver) LookupAddr(ctx context.Context, addr string) ([]string, error) {
	m.lookupCount.Add(1)
	if m.lookupFunc != nil {
		return m.lookupFunc(ctx, addr)
	}
	return []string{"test.example.com."}, nil
}

// TestDNSEnrichment_Disabled tests that DNS enrichment is skipped when disabled
func TestDNSEnrichment_Disabled(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: false, // Disabled
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	// Verify enrichment returns empty string when disabled
	hostname, needsAsync := c.enrichWithDNSSync("192.0.2.1")
	assert.Equal(t, "", hostname, "DNS enrichment should return empty when disabled")
	assert.False(t, needsAsync, "Should not need async when disabled")

	// Verify no DNS cache was created
	assert.Nil(t, c.dnsCache, "DNS cache should not be created when disabled")
}

// TestDNSEnrichment_CacheHit tests fast-path cache hit (no goroutine)
func TestDNSEnrichment_CacheHit(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			return []string{"cache-test.example.com."}, nil
		},
	}
	c.dnsResolver = mockResolver

	// Prime the cache
	c.dnsCache.Set("192.0.2.1", "cached.example.com")

	// Perform enrichment - should hit cache
	hostname, needsAsync := c.enrichWithDNSSync("192.0.2.1")
	assert.Equal(t, "cached.example.com", hostname, "Should return cached hostname")
	assert.False(t, needsAsync, "Should not need async on cache hit")

	// Verify no DNS lookup was performed (cache hit)
	assert.Equal(t, int64(0), mockResolver.lookupCount.Load(), "DNS lookup should not be called on cache hit")
}

// TestDNSEnrichment_CacheMiss_Success tests cache miss with successful lookup
func TestDNSEnrichment_CacheMiss_Success(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			return []string{"resolved.example.com."}, nil
		},
	}
	c.dnsResolver = mockResolver

	// Perform sync check - should miss cache and indicate async needed
	hostname, needsAsync := c.enrichWithDNSSync("192.0.2.2")
	assert.Equal(t, "", hostname, "Should return empty on cache miss")
	assert.True(t, needsAsync, "Should need async on cache miss")

	// Perform blocking enrichment
	result := c.enrichWithDNSBlocking("192.0.2.2")
	assert.Equal(t, "resolved.example.com", result, "Should return resolved hostname")

	// Verify DNS lookup was performed
	assert.Equal(t, int64(1), mockResolver.lookupCount.Load(), "DNS lookup should be called on cache miss")

	// Verify result was cached
	val, exists := c.dnsCache.Get("192.0.2.2")
	assert.True(t, exists, "Result should be cached")
	assert.Equal(t, "resolved.example.com", val.(string), "Cached value should match resolved hostname")

	// Second call should hit cache (no additional lookup)
	mockResolver.lookupCount.Store(0)
	hostname2, needsAsync2 := c.enrichWithDNSSync("192.0.2.2")
	assert.Equal(t, "resolved.example.com", hostname2, "Should return cached hostname")
	assert.False(t, needsAsync2, "Should not need async on cache hit")
	assert.Equal(t, int64(0), mockResolver.lookupCount.Load(), "Second call should hit cache")
}

// TestDNSEnrichment_CacheMiss_Timeout tests cache miss with timeout
func TestDNSEnrichment_CacheMiss_Timeout(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          100 * time.Millisecond, // Short timeout
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			// Simulate slow DNS server
			select {
			case <-time.After(1 * time.Second):
				return []string{"slow.example.com."}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	c.dnsResolver = mockResolver

	// Perform blocking enrichment - should timeout
	result := c.enrichWithDNSBlocking("192.0.2.3")
	assert.Equal(t, "", result, "Should return empty on timeout")

	// Verify DNS lookup was attempted
	assert.Equal(t, int64(1), mockResolver.lookupCount.Load(), "DNS lookup should be attempted")
}

// TestDNSEnrichment_CacheMiss_Failure tests cache miss with DNS failure
func TestDNSEnrichment_CacheMiss_Failure(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			return nil, errors.New("DNS lookup failed")
		},
	}
	c.dnsResolver = mockResolver

	// Perform blocking enrichment - should fail gracefully
	result := c.enrichWithDNSBlocking("192.0.2.4")
	assert.Equal(t, "", result, "Should return empty on DNS failure")

	// Verify DNS lookup was attempted
	assert.Equal(t, int64(1), mockResolver.lookupCount.Load(), "DNS lookup should be attempted")
}

// TestDNSEnrichment_CacheTTL tests that cache entries expire after TTL
func TestDNSEnrichment_CacheTTL(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         500 * time.Millisecond, // Short TTL for testing
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	lookupNum := 0
	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			lookupNum++
			return []string{"ttl-test.example.com."}, nil
		},
	}
	c.dnsResolver = mockResolver

	// First lookup - cache miss (blocking)
	result1 := c.enrichWithDNSBlocking("192.0.2.5")
	assert.Equal(t, "ttl-test.example.com", result1)
	assert.Equal(t, int64(1), mockResolver.lookupCount.Load())

	// Immediate second lookup - cache hit
	mockResolver.lookupCount.Store(0)
	hostname2, needsAsync2 := c.enrichWithDNSSync("192.0.2.5")
	assert.Equal(t, "ttl-test.example.com", hostname2)
	assert.False(t, needsAsync2, "Should hit cache")
	assert.Equal(t, int64(0), mockResolver.lookupCount.Load(), "Should hit cache")

	// Wait for cache entry to expire and verify a new lookup occurs
	assert.Eventually(t, func() bool {
		mockResolver.lookupCount.Store(0)
		_, needsAsync := c.enrichWithDNSSync("192.0.2.5")
		return needsAsync
	}, 5*time.Second, 100*time.Millisecond, "Cache entry should eventually expire")

	// Now do a blocking lookup to get fresh result
	mockResolver.lookupCount.Store(0)
	result3 := c.enrichWithDNSBlocking("192.0.2.5")
	assert.Equal(t, "ttl-test.example.com", result3)
	assert.Equal(t, int64(1), mockResolver.lookupCount.Load(), "Should do new lookup after TTL expiry")
}

// TestDNSEnrichment_Concurrency tests multiple concurrent DNS lookups
func TestDNSEnrichment_Concurrency(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			// Simulate some work
			time.Sleep(50 * time.Millisecond)
			return []string{addr + ".example.com."}, nil
		},
	}
	c.dnsResolver = mockResolver

	// Launch multiple concurrent enrichments
	const numRequests = 10
	results := make(chan string, numRequests)

	for i := 0; i < numRequests; i++ {
		ip := "192.0.2." + strconv.Itoa(i)
		go func(ipAddr string) {
			results <- c.enrichWithDNSBlocking(ipAddr)
		}(ip)
	}

	// Collect all results
	successCount := 0
	for i := 0; i < numRequests; i++ {
		result := <-results
		if result != "" {
			successCount++
		}
	}

	// All requests should succeed (workers handle them concurrently)
	assert.Equal(t, numRequests, successCount, "All concurrent lookups should succeed")
	assert.GreaterOrEqual(t, mockResolver.lookupCount.Load(), int64(numRequests), "All lookups should be performed")
}

// TestDNSEnrichment_Integration tests full integration with record creation
func TestDNSEnrichment_Integration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			// Simulate realistic DNS response
			if addr == "192.0.2.10" {
				return []string{"client.university.edu."}, nil
			}
			if addr == "127.0.0.1" {
				return []string{"localhost."}, nil
			}
			return nil, errors.New("not found")
		},
	}
	c.dnsResolver = mockResolver

	// Create a user with IP address in host field
	userInfo := parser.UserInfo{
		Protocol: "http",
		Username: "testuser",
		Pid:      12345,
		Sid:      1,
		Host:     "192.0.2.10", // IP address - should be enriched
	}

	userState := &UserState{
		UserID:   100,
		UserInfo: userInfo,
		AuthInfo: parser.AuthInfo{
			DN:          "CN=Test User",
			Org:         "TestVO",
			InetVersion: "4",
		},
	}

	// Store user state
	serverID := "1234567890#127.0.0.1:1234"
	userInfoKey := BuildUserInfoKey(serverID, userInfo)
	c.userMap.Set(userInfoKey, userState)

	dictKey := BuildDictIDKey(serverID, 100)
	c.dictMap.Set(dictKey, userInfo)

	// Create a file state
	fileState := &FileState{
		FileID:   1,
		UserID:   100,
		OpenTime: time.Now().Unix(),
		FileSize: 12345,
		Filename: "/test/file.txt",
		ServerID: serverID,
	}

	// Create a close record
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: 'c', // close record
			FileId:  1,
			UserId:  100,
		},
		Ops: parser.StatOPS{
			Read:  10,
			Write: 5,
		},
		Xfr: parser.StatXFR{
			Read:  1024,
			Write: 512,
		},
	}

	packet := &parser.Packet{
		Header: parser.Header{
			ServerStart: 1234567890,
		},
		RemoteAddr: "127.0.0.1:1234",
	}

	// Create correlated record - with new async API, cache miss means record won't have userDomain yet
	record := c.createCorrelatedRecord(fileState, closeRec, packet)

	// Check if record needs async enrichment
	if record.NeedsEnrichment() {
		// Enrich asynchronously and wait for result
		done := make(chan EnrichedRecord, 1)
		c.EnqueueForEnrichment(record, EnrichmentDestination{Results: done})

		// Wait for enrichment
		select {
		case msg := <-done:
			record = msg.Record
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for DNS enrichment")
		}
	}

	// Verify DNS enrichment happened
	assert.NotEmpty(t, record.UserDomain, "User domain should be populated from DNS enrichment")
	assert.Equal(t, "university.edu", record.UserDomain, "User domain should be extracted from enriched hostname")

	// Verify DNS lookup was performed (once for user IP, once for server IP)
	assert.Equal(t, int64(2), mockResolver.lookupCount.Load(), "DNS lookup should be performed for user and server IPs")

	// Verify result was cached for user IP
	val, exists := c.dnsCache.Get("192.0.2.10")
	assert.True(t, exists, "DNS result for user IP should be cached")
	assert.Equal(t, "client.university.edu", val.(string))

	// Verify result was cached for server IP
	serverVal, serverExists := c.dnsCache.Get("127.0.0.1")
	assert.True(t, serverExists, "DNS result for server IP should be cached")
	assert.Equal(t, "localhost", serverVal.(string), "Server IP should resolve to localhost in mock")
}

// TestDNSEnrichment_ShutdownCleanup tests that workers exit cleanly on shutdown
func TestDNSEnrichment_ShutdownCleanup(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)

	// Start some work
	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			time.Sleep(100 * time.Millisecond)
			return []string{"test.example.com."}, nil
		},
	}
	c.dnsResolver = mockResolver

	// Launch a lookup (async, won't wait for result)
	go c.enrichWithDNSBlocking("192.0.2.20")

	// Immediately stop - should not hang or panic
	time.Sleep(10 * time.Millisecond)
	c.Stop()

	// If we get here without hanging, the test passes
	assert.True(t, true, "Stop should complete without hanging")
}

// TestDNSEnrichment_EmptyIP tests enrichment with empty IP
func TestDNSEnrichment_EmptyIP(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	// Empty IP should return empty immediately (no lookup)
	hostname, needsAsync := c.enrichWithDNSSync("")
	assert.Equal(t, "", hostname, "Empty IP should return empty hostname")
	assert.False(t, needsAsync, "Should not need async for empty IP")
}

// TestDNSEnrichment_InvalidIP tests enrichment with invalid IP
func TestDNSEnrichment_InvalidIP(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          2 * time.Second,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			return nil, errors.New("invalid IP")
		},
	}
	c.dnsResolver = mockResolver

	// Invalid IP should be handled gracefully
	result := c.enrichWithDNSBlocking("not-an-ip")
	assert.Equal(t, "", result, "Invalid IP should return empty hostname")
}

// TestEnrichmentQueue_BoundedConcurrency verifies that enrichment worker concurrency
// is capped by the worker pool size even when many records are submitted concurrently.
func TestEnrichmentQueue_BoundedConcurrency(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	const numWorkers = 3
	const numRecords = 50

	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          5 * time.Second,
		EnrichmentWorkers:   numWorkers,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	var activeLookups atomic.Int64
	var maxConcurrentLookups atomic.Int64
	c.dnsResolver = &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			active := activeLookups.Add(1)
			for {
				currentMax := maxConcurrentLookups.Load()
				if active <= currentMax {
					break
				}
				if maxConcurrentLookups.CompareAndSwap(currentMax, active) {
					break
				}
			}

			select {
			case <-time.After(25 * time.Millisecond):
			case <-ctx.Done():
				activeLookups.Add(-1)
				return nil, ctx.Err()
			}

			activeLookups.Add(-1)
			return []string{"host.example.com."}, nil
		},
	}

	results := make(chan EnrichedRecord, numRecords)

	for i := 0; i < numRecords; i++ {
		record := &CollectorRecord{
			needsDNSEnrichment: true,
			enrichmentIP:       "192.0.2." + strconv.Itoa(i%200),
		}
		go c.EnqueueForEnrichment(record, EnrichmentDestination{Results: results})
	}

	// All records must be emitted within a generous timeout
	received := 0
	timeout := time.After(10 * time.Second)
	for received < numRecords {
		select {
		case <-results:
			received++
		case <-timeout:
			t.Fatalf("Only %d/%d callbacks received before timeout", received, numRecords)
		}
	}
	assert.Equal(t, numRecords, received)
	assert.LessOrEqual(t, maxConcurrentLookups.Load(), int64(numWorkers), "lookups should not exceed worker count")
}

// TestEnrichmentQueue_NonBlockingEnqueue verifies records can be queued quickly
// even when workers are blocked on enrichment.
func TestEnrichmentQueue_NonBlockingEnqueue(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	const numRecords = 1000
	config := CorrelatorConfig{
		TTL:                 5 * time.Minute,
		MaxEntries:          1000,
		EnableDNSEnrichment: true,
		DNSCacheTTL:         1 * time.Hour,
		DNSTimeout:          30 * time.Second,
		EnrichmentWorkers:   1,
		Logger:              logger,
	}

	c := NewCorrelatorWithConfig(config)
	defer c.Stop()

	releaseLookup := make(chan struct{})
	c.dnsResolver = &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			select {
			case <-releaseLookup:
				return []string{"queued.example.com."}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}

	results := make(chan EnrichedRecord, numRecords)
	destination := EnrichmentDestination{Results: results}

	start := time.Now()
	for i := 0; i < numRecords; i++ {
		record := &CollectorRecord{
			needsDNSEnrichment: true,
			enrichmentIP:       "192.0.2." + strconv.Itoa(i),
		}
		c.EnqueueForEnrichment(record, destination)
	}
	enqueueDuration := time.Since(start)

	// Enqueue should not stall waiting for worker completion.
	assert.Less(t, enqueueDuration, 2*time.Second, "queue enqueue should be non-blocking")

	// Allow the blocked worker to proceed and drain backlog.
	close(releaseLookup)

	received := 0
	timeout := time.After(15 * time.Second)
	for received < numRecords {
		select {
		case <-results:
			received++
		case <-timeout:
			t.Fatalf("Only %d/%d records received before timeout", received, numRecords)
		}
	}
	assert.Equal(t, numRecords, received)
}
