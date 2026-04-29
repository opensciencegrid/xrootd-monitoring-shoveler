package main

import (
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
