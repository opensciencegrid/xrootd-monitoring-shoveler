package shoveler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Health reports to the shoveler registry (docs/registry-protocol.md). Once
// the registry has issued the shoveler an access token, the shoveler POSTs
// its counters since it started, and its queue size, to amqp.oauth.report_url
// every amqp.oauth.report_interval seconds, authenticated with that token.
// The registry charts them and flags a shoveler that stops reporting,
// receives no packets, or stops publishing. Reporting never affects shoveling.

const (
	minReportInterval = 10   // seconds; the registry's bounds for interval_seconds
	maxReportInterval = 3600 // seconds
)

// Wait before trying again when the registry has no report endpoint (404),
// e.g. a registry that predates health reports. A var so tests can shorten it.
var reportMissingInterval = time.Hour

// processStart tells the registry which reports come from the same process:
// the counters restart from zero with each new one.
var processStart = time.Now().UTC()

// HealthReport is the body of a POST to the report endpoint.
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

// defaultReportURL is the report endpoint beside a registry's token endpoint
// (Shoveler-registry serves .../token and .../report), or "" when tokenURL
// doesn't end in /token.
func defaultReportURL(tokenURL string) string {
	u, err := url.Parse(tokenURL)
	if err != nil || u.Host == "" || !strings.HasSuffix(u.Path, "/token") {
		return ""
	}
	u.Path = strings.TrimSuffix(u.Path, "token") + "report"
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	return u.String()
}

// Reporter sends health reports to the registry.
type Reporter struct {
	URL       string
	Interval  time.Duration
	TokenFile string // the access token the OAuth manager keeps fresh
	Client    *http.Client

	lastProblem string // the current failure streak's kind, to log it once
}

// StartReporter sends health reports in the background once ready is closed,
// that is once the OAuth manager has written the first access token: a
// shoveler still awaiting approval has nothing to report with. It does
// nothing unless OAuth and amqp.oauth.report_health are on.
func StartReporter(config *Config, ready <-chan struct{}) {
	startReporter(context.Background(), config, ready)
}

func startReporter(ctx context.Context, config *Config, ready <-chan struct{}) {
	if !config.OAuthEnabled || !config.OAuthReportHealth {
		return
	}
	r := &Reporter{
		URL:       config.OAuthReportURL,
		Interval:  time.Duration(config.OAuthReportInterval) * time.Second,
		TokenFile: config.AmqpToken,
		Client:    &http.Client{Timeout: 30 * time.Second},
	}
	go func() {
		select {
		case <-ready:
		case <-ctx.Done():
			return
		}
		log.Infoln("Reporting health to", r.URL, "every", r.Interval)
		r.Run(ctx)
	}()
}

// Run reports now, then every Interval, until ctx is done.
func (r *Reporter) Run(ctx context.Context) {
	for {
		wait := r.report(ctx)
		if !sleepCtx(ctx, wait) {
			return
		}
	}
}

// report sends one report and returns how long to wait before the next.
// Each kind of failure is logged once, until reports succeed again or fail
// differently, so an unreachable registry doesn't flood the log.
func (r *Reporter) report(ctx context.Context) time.Duration {
	err := r.Send(ctx, NewHealthReport(int(r.Interval/time.Second)))
	if ctx.Err() != nil {
		return r.Interval
	}
	problem := ""
	var ee *endpointError
	if errors.As(err, &ee) {
		problem = strconv.Itoa(ee.Status) + " " + ee.Code
	} else if err != nil {
		problem = "error"
	}
	missing := ee != nil && ee.Status == http.StatusNotFound
	switch {
	case problem == r.lastProblem && err != nil:
		log.Debugln("Health report failed again:", err)
	case missing:
		log.Warningln("The registry has no health report endpoint at", r.URL,
			"(404), so the shoveler will try again hourly. Set amqp.oauth.report_url,",
			"or amqp.oauth.report_health: false to stop sending reports.")
	case err != nil:
		log.Warningln("Failed to send a health report to the registry (will keep trying):", err)
	case r.lastProblem != "":
		log.Infoln("Health reports to the registry are succeeding again")
	default:
		log.Debugln("Sent a health report to the registry")
	}
	r.lastProblem = problem
	if missing {
		return reportMissingInterval
	}
	return r.Interval
}

// Send POSTs one report with the current access token. A registry error
// comes back as an *endpointError.
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
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return newEndpointError(resp.StatusCode, detail)
	}
	return nil
}
