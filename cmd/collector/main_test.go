package main

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/collector"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/connectors"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
)

type mockOutputConnector struct {
	mu        sync.Mutex
	writes    int
	exchanges []string
}

func (m *mockOutputConnector) Write(_ []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes++
	m.exchanges = append(m.exchanges, "")
	return nil
}

func (m *mockOutputConnector) WriteToExchange(_ []byte, exchange string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes++
	m.exchanges = append(m.exchanges, exchange)
	return nil
}

func (m *mockOutputConnector) Close() error { return nil }
func (m *mockOutputConnector) Sync() error  { return nil }

func (m *mockOutputConnector) snapshot() (int, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.exchanges))
	copy(cp, m.exchanges)
	return m.writes, cp
}

func makeGStreamPacket(remoteAddr string, streamType byte) *parser.Packet {
	return &parser.Packet{
		RemoteAddr: remoteAddr,
		GStreamRecord: &parser.GStreamRecord{
			StreamType: streamType,
			Events: []map[string]interface{}{
				{"msg": "hello"},
			},
		},
	}
}

func TestHandleParsedPacket_EnqueuesGStreamPacket(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cor := collector.NewCorrelator(30*time.Second, 100, logger)
	defer cor.Stop()

	queue := make(chan *parser.Packet, 2)
	var dropped int64

	handleParsedPacket(
		makeGStreamPacket("server.example:1094", 'C'),
		cor,
		queue,
		&dropped,
		collector.EnrichmentDestination{},
		logger,
	)

	if got := len(queue); got != 1 {
		t.Fatalf("expected queue length 1, got %d", got)
	}
	if got := atomic.LoadInt64(&dropped); got != 0 {
		t.Fatalf("expected dropped=0, got %d", got)
	}
}

func TestHandleParsedPacket_DropsWhenGStreamQueueFull(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cor := collector.NewCorrelator(30*time.Second, 100, logger)
	defer cor.Stop()

	queue := make(chan *parser.Packet, 1)
	queue <- makeGStreamPacket("server.example:1094", 'C')
	var dropped int64

	handleParsedPacket(
		makeGStreamPacket("server.example:1094", 'C'),
		cor,
		queue,
		&dropped,
		collector.EnrichmentDestination{},
		logger,
	)

	if got := len(queue); got != 1 {
		t.Fatalf("expected queue length to remain 1, got %d", got)
	}
	if got := atomic.LoadInt64(&dropped); got != 1 {
		t.Fatalf("expected dropped=1, got %d", got)
	}
}

func TestGStreamWorkers_ProcessAndEmit(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := &shoveler.Config{}
	cfg.State.GStreamWorkers = 1
	cfg.State.GStreamQueueSize = 8
	cfg.AmqpExchange = "default-exchange"
	cfg.AmqpExchangeCache = "cache-exchange"
	cfg.AmqpExchangeTCP = "tcp-exchange"
	cfg.AmqpExchangeTPC = "tpc-exchange"

	cor := collector.NewCorrelator(30*time.Second, 100, logger)
	defer cor.Stop()

	mockOut := &mockOutputConnector{}
	var output connectors.OutputConnector = mockOut

	gstreamQueue, wg, _ := startGStreamWorkers(cor, cfg, output, logger)
	gstreamQueue <- makeGStreamPacket("server.example:1094", 'C')
	close(gstreamQueue)
	wg.Wait()

	writes, exchanges := mockOut.snapshot()
	if writes != 1 {
		t.Fatalf("expected 1 emitted gstream event, got %d", writes)
	}
	if len(exchanges) != 1 || exchanges[0] != "cache-exchange" {
		t.Fatalf("expected exchange cache-exchange, got %#v", exchanges)
	}
}

// TestWarnInertNameMethods covers when the collector tells an operator that
// name-based site resolution cannot answer for the server endpoint. The warning
// must fire on the shipped defaults (DNS off, no local_site) and stay quiet
// whenever the server end is already covered — otherwise it trains operators to
// ignore it.
func TestWarnInertNameMethods(t *testing.T) {
	defaultOrder := []string{"config", "hostname", "ip", "domain"}

	cases := []struct {
		name     string
		site     shoveler.SiteConfig
		dnsOn    bool
		wantWarn bool
	}{
		{
			name:     "shipped defaults: DNS off and no local_site",
			site:     shoveler.SiteConfig{Enabled: true, ResolutionOrder: defaultOrder},
			wantWarn: true,
		},
		{
			name:     "local_site set: config resolves the server end first",
			site:     shoveler.SiteConfig{Enabled: true, LocalSite: "CERN-PROD", ResolutionOrder: defaultOrder},
			wantWarn: false,
		},
		{
			name:     "local_site set but config method dropped from the order",
			site:     shoveler.SiteConfig{Enabled: true, LocalSite: "CERN-PROD", ResolutionOrder: []string{"hostname", "ip", "domain"}},
			wantWarn: true,
		},
		{
			name:     "DNS on: names are available for the server end",
			site:     shoveler.SiteConfig{Enabled: true, ResolutionOrder: defaultOrder},
			dnsOn:    true,
			wantWarn: false,
		},
		{
			name:     "no name-based method requested",
			site:     shoveler.SiteConfig{Enabled: true, ResolutionOrder: []string{"config", "ip"}},
			wantWarn: false,
		},
		{
			name:     "site resolution disabled entirely",
			site:     shoveler.SiteConfig{Enabled: false, ResolutionOrder: defaultOrder},
			wantWarn: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger := logrus.New()
			var buf strings.Builder
			logger.SetOutput(&buf)
			logger.SetLevel(logrus.WarnLevel)

			cfg := &shoveler.Config{Site: tc.site}
			cfg.State.EnableDNSEnrichment = tc.dnsOn

			warnInertNameMethods(cfg, logger)

			warned := strings.Contains(buf.String(), "enable_dns_enrichment")
			if warned != tc.wantWarn {
				t.Errorf("warned = %v, want %v; log: %s", warned, tc.wantWarn, buf.String())
			}
		})
	}
}
