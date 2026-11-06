package main

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/collector"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/connectors"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/input"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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

type testPacketSource struct {
	packets chan input.PacketWithAddr
}

func (s *testPacketSource) Start() error                                 { return nil }
func (s *testPacketSource) Stop() error                                  { return nil }
func (s *testPacketSource) PacketsWithAddr() <-chan input.PacketWithAddr { return s.packets }

func packetsByServer(t *testing.T, serverIP string) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	counts := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != "shoveler_packets_by_server_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := make(map[string]string)
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["server_ip"] == serverIP {
				counts[labels["packet_type"]] = metric.GetCounter().GetValue()
			}
		}
	}
	return counts
}

func TestProcessPackets_PerServerMetricsBeforeDispatch(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enabled bool
		ip      string
	}{
		{name: "enabled", enabled: true, ip: "198.51.100.97"},
		{name: "disabled", ip: "203.0.113.97"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetLevel(logrus.ErrorLevel)
			cor := collector.NewCorrelatorWithConfig(collector.CorrelatorConfig{
				TTL: time.Minute, PerServerMetrics: tt.enabled, Logger: logger,
			})
			defer cor.Stop()

			gstream := make([]byte, 24)
			gstream[0] = parser.PacketTypeGStream
			binary.BigEndian.PutUint16(gstream[2:4], uint16(len(gstream)))
			gstream[16] = 'T'
			xfr := []byte{parser.PacketTypeXFR, 0, 0, 8, 0, 0, 0, 1}
			data := [][]byte{gstream, gstream, xfr, []byte("<statistics/>"), {}}
			source := &testPacketSource{packets: make(chan input.PacketWithAddr, len(data))}
			for _, packet := range data {
				// Verify metrics use the same canonical IP as emitted records.
				source.packets <- input.PacketWithAddr{Data: packet, RemoteAddr: "[::ffff:" + tt.ip + "]:1094"}
			}
			close(source.packets)

			// With no worker draining this queue, one gstream packet must be dropped.
			queue := make(chan *parser.Packet, 1)
			var dropped int64
			before := packetsByServer(t, tt.ip)
			beforeParsed := testutil.ToFloat64(shoveler.PacketsParsedOK)
			beforeErrors := testutil.ToFloat64(shoveler.ParseErrors)
			processPackets(source, cor, queue, &dropped, collector.EnrichmentDestination{}, logger)
			require.Equal(t, int64(1), dropped)
			require.Equal(t, beforeParsed+4, testutil.ToFloat64(shoveler.PacketsParsedOK))
			require.Equal(t, beforeErrors+1, testutil.ToFloat64(shoveler.ParseErrors))

			want := make(map[string]float64)
			for stream, count := range before {
				want[stream] = count
			}
			if tt.enabled {
				want["gstream"] += 2
				want["xfr"]++
				want["xml"]++
			}
			require.Equal(t, want, packetsByServer(t, tt.ip), "count parsed packets before dispatch, including queue drops")

			close(queue)
			for packet := range queue {
				_, _, err := cor.ProcessGStreamPacket(packet)
				require.NoError(t, err)
			}
			require.Equal(t, want, packetsByServer(t, tt.ip), "workers must not count packets again")
		})
	}
}
