package shoveler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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

func TestDefaultReportURL(t *testing.T) {
	for tokenURL, want := range map[string]string{
		"https://registry.example.org/token":         "https://registry.example.org/report",
		"https://example.org/shoveler-auth/token":    "https://example.org/shoveler-auth/report",
		"http://127.0.0.1:18080/token":               "http://127.0.0.1:18080/report",
		"https://registry.example.org/token?x=1":     "https://registry.example.org/report",
		"https://kc.example.org/realms/r/token/info": "",
		"https://registry.example.org/":              "",
		"not a url":                                  "",
	} {
		assert.Equal(t, want, defaultReportURL(tokenURL), tokenURL)
	}
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
	var ee *endpointError
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, http.StatusUnauthorized, ee.Status)
	assert.Equal(t, "invalid_token", ee.Code)
}

func TestReporterNeedsTokenFile(t *testing.T) {
	r := &Reporter{URL: "http://127.0.0.1:1", TokenFile: filepath.Join(t.TempDir(), "missing"), Client: http.DefaultClient}
	assert.ErrorContains(t, r.Send(context.Background(), NewHealthReport(60)), "reading the access token")
}

// countingRegistry answers every report with status and counts them.
func countingRegistry(t *testing.T, status int) (*httptest.Server, func() int) {
	var mu sync.Mutex
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server, func() int { mu.Lock(); defer mu.Unlock(); return count }
}

// runReporter runs r until the test ends, and waits for it to stop before
// the test's other cleanups (registered earlier, so run later) undo anything.
func runReporter(t *testing.T, r *Reporter) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestReporterReportsAtOnceThenOnInterval(t *testing.T) {
	server, count := countingRegistry(t, http.StatusNoContent)
	runReporter(t, &Reporter{URL: server.URL, Interval: 20 * time.Millisecond,
		TokenFile: writeToken(t, "tok"), Client: server.Client()})
	require.Eventually(t, func() bool { return count() >= 3 }, 5*time.Second, 5*time.Millisecond)
}

func TestReporterBacksOffWithoutAReportEndpoint(t *testing.T) {
	old := reportMissingInterval
	reportMissingInterval = time.Hour
	t.Cleanup(func() { reportMissingInterval = old })

	server, count := countingRegistry(t, http.StatusNotFound)
	runReporter(t, &Reporter{URL: server.URL, Interval: 5 * time.Millisecond,
		TokenFile: writeToken(t, "tok"), Client: server.Client()})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, count(), "an older registry is asked hourly, not every interval")
}

func TestReporterStartsWithTheFirstOAuthToken(t *testing.T) {
	// End to end against the fake registry: no report while the client
	// awaits approval, then reports carrying the token the registry issued.
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.pendingCount = 2
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())
	config.OAuthReportHealth = true
	config.OAuthReportURL = defaultReportURL(fr.tokenURL())
	config.OAuthReportInterval = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	startReporter(ctx, config, ready)
	waitReady(t, ready)

	require.Eventually(t, func() bool {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		return len(fr.reports) > 0
	}, 5*time.Second, 10*time.Millisecond)
	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 3, fr.tokenCount, "reporting waited for approval")
	assert.Equal(t, "Bearer fake-access-token", fr.reports[0].Authorization)
	assert.Equal(t, 1, fr.reports[0].Body.IntervalSeconds)
	assert.Contains(t, fr.reports[0].Body.Metrics, "messages_published")
}

func TestReporterIsOffWithoutOAuthOrWhenDisabled(t *testing.T) {
	server, count := countingRegistry(t, http.StatusNoContent)
	ready := make(chan struct{})
	close(ready)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, config := range []*Config{
		{OAuthEnabled: false, OAuthReportHealth: true, OAuthReportURL: server.URL, OAuthReportInterval: 1},
		{OAuthEnabled: true, OAuthReportHealth: false, OAuthReportURL: server.URL, OAuthReportInterval: 1},
	} {
		config.AmqpToken = writeToken(t, "tok")
		startReporter(ctx, config, ready)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, count())
}

func readOAuthConfig(t *testing.T, oauthYAML string) *Config {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    registration_url: https://registry.example.org/register\n" +
		"    state_file: " + filepath.Join(dir, "registration.json") + "\n" +
		oauthYAML
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))
	c := &Config{}
	c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	return c
}

func TestReportConfigDefaults(t *testing.T) {
	c := readOAuthConfig(t, "    token_url: https://registry.example.org/token\n")
	assert.True(t, c.OAuthReportHealth, "on by default with OAuth")
	assert.Equal(t, "https://registry.example.org/report", c.OAuthReportURL)
	assert.Equal(t, 60, c.OAuthReportInterval)
}

func TestReportConfigOverrides(t *testing.T) {
	c := readOAuthConfig(t, ""+
		"    token_url: https://registry.example.org/token\n"+
		"    report_url: https://elsewhere.example.org/health\n"+
		"    report_interval: 30\n")
	assert.Equal(t, "https://elsewhere.example.org/health", c.OAuthReportURL)
	assert.Equal(t, 30, c.OAuthReportInterval)

	c = readOAuthConfig(t, ""+
		"    token_url: https://registry.example.org/token\n"+
		"    report_health: false\n")
	assert.False(t, c.OAuthReportHealth)

	c = readOAuthConfig(t, ""+
		"    token_url: https://registry.example.org/token\n"+
		"    report_interval: 5\n")
	assert.Equal(t, 60, c.OAuthReportInterval, "out of range falls back to 60")
}

func TestReportConfigOffWhenNoURLCanBeDerived(t *testing.T) {
	c := readOAuthConfig(t, "    token_url: https://kc.example.org/realms/r/protocol/openid-connect/token-endpoint\n")
	assert.False(t, c.OAuthReportHealth)
	assert.Empty(t, c.OAuthReportURL)
}

func TestReportConfigFromEnv(t *testing.T) {
	t.Setenv("SHOVELER_AMQP_OAUTH_REPORT_HEALTH", "false")
	c := readOAuthConfig(t, "    token_url: https://registry.example.org/token\n")
	assert.False(t, c.OAuthReportHealth)
}
