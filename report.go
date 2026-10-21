package shoveler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Health reports to the shoveler registry. Every registry.report_interval
// seconds the shoveler POSTs its counters since it started, and its queue
// size, to registry.report_url, authenticated with the access token the
// registry issued it (the one it uses for RabbitMQ). The registry charts them
// and flags a shoveler that stops reporting, receives no packets, or stops
// publishing.

// processStart tells the registry which reports come from the same process:
// the counters restart from zero with each new one.
var processStart = time.Now().UTC()

// HealthReport is the body of a POST to registry.report_url.
type HealthReport struct {
	StartedAt       string            `json:"started_at"`
	IntervalSeconds int               `json:"interval_seconds"`
	Version         string            `json:"version,omitempty"`
	Metrics         map[string]uint64 `json:"metrics"`
}

// NewHealthReport snapshots the metrics the registry understands.
func NewHealthReport(intervalSeconds int) HealthReport {
	return HealthReport{
		StartedAt:       processStart.Format(time.RFC3339),
		IntervalSeconds: intervalSeconds,
		Version:         ShovelerVersion,
		Metrics: map[string]uint64{
			"packets_received":    metricValue(PacketsReceived),
			"validations_failed":  metricValue(ValidationsFailed),
			"messages_published":  metricValue(MessagesPublished),
			"rabbitmq_reconnects": metricValue(RabbitmqReconnects),
			"queue_size":          metricValue(QueueSize),
		},
	}
}

// metricValue reads a counter or gauge's current value.
func metricValue(m prometheus.Metric) uint64 {
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		return 0
	}
	if out.Counter != nil {
		return uint64(out.Counter.GetValue())
	}
	return uint64(out.Gauge.GetValue())
}

// Reporter sends health reports to the registry.
type Reporter struct {
	URL       string
	Interval  time.Duration
	TokenFile string
	Client    *http.Client
	failing   bool
}

// StartReporter starts reporting in the background if registry.report_url is set.
func StartReporter(config *Config) {
	if config.Registry.ReportURL == "" {
		return
	}
	r := &Reporter{
		URL:       config.Registry.ReportURL,
		Interval:  time.Duration(config.Registry.ReportInterval) * time.Second,
		TokenFile: config.Registry.TokenLocation,
		Client:    &http.Client{Timeout: 30 * time.Second},
	}
	log.Infoln("Reporting health to", r.URL, "every", r.Interval)
	go r.Run(context.Background())
}

// Run reports every Interval until ctx is done.
func (r *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.report(ctx)
		}
	}
}

// report sends one report. Failures are logged once until reports succeed
// again, so an unreachable registry doesn't flood the log.
func (r *Reporter) report(ctx context.Context) {
	err := r.Send(ctx, NewHealthReport(int(r.Interval/time.Second)))
	switch {
	case err != nil && !r.failing:
		log.Warningln("Failed to send a health report to the registry (will keep trying):", err)
	case err != nil:
		log.Debugln("Health report failed again:", err)
	case r.failing:
		log.Infoln("Health reports to the registry are succeeding again")
	default:
		log.Debugln("Sent a health report to the registry")
	}
	r.failing = err != nil
}

// Send POSTs one report with the current access token.
func (r *Reporter) Send(ctx context.Context, report HealthReport) error {
	token, err := os.ReadFile(r.TokenFile)
	if err != nil {
		return fmt.Errorf("reading the access token: %w", err)
	}
	body, err := json.Marshal(report)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "xrootd-monitoring-shoveler/"+ShovelerVersion)
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("registry answered %s: %s", resp.Status, strings.TrimSpace(string(detail)))
	}
	return nil
}
