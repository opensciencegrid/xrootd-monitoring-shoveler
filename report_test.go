package shoveler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeToken(t *testing.T, contents string) string {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	return path
}

func TestHealthReportSnapshotsMetrics(t *testing.T) {
	before := NewHealthReport(60)
	PacketsReceived.Add(3)
	ValidationsFailed.Inc()
	MessagesPublished.Add(2)
	QueueSize.Set(7)

	report := NewHealthReport(60)
	assert.Equal(t, before.Metrics["packets_received"]+3, report.Metrics["packets_received"])
	assert.Equal(t, before.Metrics["validations_failed"]+1, report.Metrics["validations_failed"])
	assert.Equal(t, before.Metrics["messages_published"]+2, report.Metrics["messages_published"])
	assert.Equal(t, uint64(7), report.Metrics["queue_size"])
	assert.Contains(t, report.Metrics, "rabbitmq_reconnects")
	// Same process, same started_at.
	assert.Equal(t, before.StartedAt, report.StartedAt)
	_, err := time.Parse(time.RFC3339, report.StartedAt)
	assert.NoError(t, err)
}

func TestReporterSendsReportWithToken(t *testing.T) {
	var got HealthReport
	var auth, contentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		auth = r.Header.Get("Authorization")
		contentType = r.Header.Get("Content-Type")
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	r := &Reporter{URL: server.URL, TokenFile: writeToken(t, "abc.def.ghi\n"), Client: server.Client()}
	require.NoError(t, r.Send(context.Background(), NewHealthReport(30)))
	assert.Equal(t, "Bearer abc.def.ghi", auth)
	assert.Equal(t, "application/json", contentType)
	assert.Equal(t, 30, got.IntervalSeconds)
	assert.Len(t, got.Metrics, 5)
}

func TestReporterSurfacesRegistryErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_token"}`))
	}))
	defer server.Close()

	r := &Reporter{URL: server.URL, TokenFile: writeToken(t, "expired"), Client: server.Client()}
	err := r.Send(context.Background(), NewHealthReport(60))
	assert.ErrorContains(t, err, `registry answered 401 Unauthorized: {"error":"invalid_token"}`)
}

func TestReporterNeedsTokenFile(t *testing.T) {
	r := &Reporter{URL: "http://127.0.0.1:1", TokenFile: filepath.Join(t.TempDir(), "missing"), Client: http.DefaultClient}
	assert.ErrorContains(t, r.Send(context.Background(), NewHealthReport(60)), "reading the access token")
}

func TestReporterRunsOnInterval(t *testing.T) {
	reports := make(chan HealthReport, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report HealthReport
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&report))
		reports <- report
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Reporter{URL: server.URL, Interval: 20 * time.Millisecond,
		TokenFile: writeToken(t, "tok"), Client: server.Client()}
	go r.Run(ctx)
	for i := 0; i < 2; i++ {
		select {
		case <-reports:
		case <-time.After(5 * time.Second):
			t.Fatal("no report sent")
		}
	}
}

func writeConfig(t *testing.T, yaml string) string {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))
	return path
}

func TestRegistryConfig(t *testing.T) {
	defer viper.Reset()
	config := Config{}
	config.ReadConfigWithPath(writeConfig(t, `
amqp:
  url: amqps://broker.example.org/xrd-mon
  token_location: /run/shoveler/token
registry:
  report_url: https://registry.example.org/report
  report_interval: 30
`))
	assert.Equal(t, "https://registry.example.org/report", config.Registry.ReportURL)
	assert.Equal(t, 30, config.Registry.ReportInterval)
	// The AMQP token is the one the registry issued.
	assert.Equal(t, "/run/shoveler/token", config.Registry.TokenLocation)
}

func TestRegistryConfigDefaults(t *testing.T) {
	defer viper.Reset()
	config := Config{}
	config.ReadConfigWithPath(writeConfig(t, `
amqp:
  url: amqps://broker.example.org/xrd-mon
registry:
  report_interval: 5
`))
	assert.Empty(t, config.Registry.ReportURL) // reporting is off
	assert.Equal(t, 60, config.Registry.ReportInterval)
	assert.Equal(t, "/etc/xrootd-monitoring-shoveler/token", config.Registry.TokenLocation)
}
