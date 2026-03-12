package collector

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// Benchmark DNS enrichment with cache hits
func BenchmarkDNSEnrichment_CacheHit(b *testing.B) {
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

	// Prime the cache
	c.dnsCache.Set("192.0.2.1", "cached.example.com")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.enrichWithDNSSync("192.0.2.1")
	}
}

// Benchmark DNS enrichment with mock resolver (cache misses)
func BenchmarkDNSEnrichment_CacheMiss(b *testing.B) {
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

	// Use a fast mock resolver
	mockResolver := &mockDNSResolver{
		lookupFunc: func(ctx context.Context, addr string) ([]string, error) {
			return []string{"fast.example.com."}, nil
		},
	}
	c.dnsResolver = mockResolver

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Use different IPs to avoid cache hits
			ip := "192.0.2." + strconv.Itoa(i%10)
			// Benchmark the sync check only (async would require waiting)
			_, _ = c.enrichWithDNSSync(ip)
			i++
		}
	})
}

// Benchmark disabled DNS enrichment (baseline)
func BenchmarkDNSEnrichment_Disabled(b *testing.B) {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.enrichWithDNSSync("192.0.2.1")
	}
}
