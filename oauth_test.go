package shoveler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oauthTestConfig builds a Config wired to the given endpoints with files under
// a temp dir.
func oauthTestConfig(t *testing.T, tokenURL, regURL string) *Config {
	t.Helper()
	dir := t.TempDir()
	return &Config{
		OAuthEnabled:         true,
		OAuthTokenURL:        tokenURL,
		OAuthRegistrationURL: regURL,
		OAuthBootstrapToken:  "bootstrap-initial-access-token",
		OAuthPrivateKeyFile:  filepath.Join(dir, "oauth", "private.pem"),
		OAuthStateFile:       filepath.Join(dir, "oauth", "registration.json"),
		OAuthClientName:      "shoveler",
		AmqpToken:            filepath.Join(dir, "token"),
	}
}

// shortOAuthDelays shrinks the retry and pending intervals for the test.
func shortOAuthDelays(t *testing.T) {
	t.Helper()
	oldRetry, oldPending, oldRejected := oauthRetryBackoff, oauthPendingInterval, oauthRejectedInterval
	oauthRetryBackoff, oauthPendingInterval, oauthRejectedInterval = 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() {
		oauthRetryBackoff, oauthPendingInterval, oauthRejectedInterval = oldRetry, oldPending, oldRejected
	})
}

// fakeRegistry is an httptest server with /register and /token handlers that
// records what it received.
type fakeRegistry struct {
	server *httptest.Server

	mu             sync.Mutex
	regCount       int
	regBearer      string
	regBody        map[string]interface{}
	regStatuses    []int // statuses to return from /register before succeeding
	regBare        bool  // send regStatuses with a plain HTML body, as a proxy would
	regBearers     []string
	tokenCount     int
	tokenAssertion string
	tokenGrantType string
	tokenScope     string
	pendingCount   int // /token answers authorization_pending this many times
	limitedCount   int // /token answers 429 invalid_client this many times
	accessToken    string
	expiresIn      int
	reports        []fakeReport // health reports received at /report
}

// fakeReport is one health report the fake registry received.
type fakeReport struct {
	Authorization string
	Body          HealthReport
}

func newFakeRegistry(t *testing.T) *fakeRegistry {
	t.Helper()
	fr := &fakeRegistry{accessToken: "fake-access-token", expiresIn: 300}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		fr.regCount++
		fr.regBearer = r.Header.Get("Authorization")
		fr.regBearers = append(fr.regBearers, fr.regBearer)
		fr.regBody = nil
		_ = json.NewDecoder(r.Body).Decode(&fr.regBody)
		if len(fr.regStatuses) > 0 {
			status := fr.regStatuses[0]
			fr.regStatuses = fr.regStatuses[1:]
			if fr.regBare {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(status)
				_, _ = w.Write([]byte("<html>Not Found</html>"))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"invalid_token","error_description":"forced"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"client_id": "generated-client-id"})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		fr.tokenCount++
		_ = r.ParseForm()
		fr.tokenAssertion = r.Form.Get("client_assertion")
		fr.tokenGrantType = r.Form.Get("grant_type")
		fr.tokenScope = r.Form.Get("scope")
		w.Header().Set("Content-Type", "application/json")
		if fr.limitedCount > 0 {
			fr.limitedCount--
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		if fr.pendingCount > 0 {
			fr.pendingCount--
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"authorization_pending","error_description":"awaiting admin approval"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": fr.accessToken,
			"expires_in":   fr.expiresIn,
			"token_type":   "Bearer",
		})
	})
	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		report := fakeReport{Authorization: r.Header.Get("Authorization")}
		_ = json.NewDecoder(r.Body).Decode(&report.Body)
		fr.reports = append(fr.reports, report)
		w.WriteHeader(http.StatusNoContent)
	})
	fr.server = httptest.NewServer(mux)
	t.Cleanup(fr.server.Close)
	return fr
}

func (fr *fakeRegistry) tokenURL() string { return fr.server.URL + "/token" }
func (fr *fakeRegistry) regURL() string   { return fr.server.URL + "/register" }

// waitReady waits for the first token to be written.
func waitReady(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first token")
	}
}

func TestOAuthRegistrationAndTokenFetch(t *testing.T) {
	fr := newFakeRegistry(t)
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Registration assertions.
	require.Equal(t, 1, fr.regCount, "registration should be called exactly once")
	assert.Equal(t, "Bearer bootstrap-initial-access-token", fr.regBearer, "bootstrap token must be sent as bearer")
	assert.Equal(t, "private_key_jwt", fr.regBody["token_endpoint_auth_method"])
	assert.Equal(t, "shoveler", fr.regBody["client_name"])

	// The request must carry a jwks with the public key (n and e present).
	jwks, ok := fr.regBody["jwks"].(map[string]interface{})
	require.True(t, ok, "request must include jwks")
	keys, ok := jwks["keys"].([]interface{})
	require.True(t, ok)
	require.Len(t, keys, 1)
	key0 := keys[0].(map[string]interface{})
	assert.Equal(t, "RSA", key0["kty"])
	require.NotEmpty(t, key0["n"], "jwks must include modulus")
	require.NotEmpty(t, key0["e"], "jwks must include exponent")
	// The kid is the RFC 7638 thumbprint of the registered key, so the
	// registry can recompute it.
	assert.Equal(t, rsaThumbprint(key0["n"].(string), key0["e"].(string)), key0["kid"])

	// No scope must be requested at registration time (authorization is
	// granted registry-side on approval).
	_, hasScope := fr.regBody["scope"]
	assert.False(t, hasScope, "registration must not request a scope")

	// State file written 0600 with the returned client_id.
	requireFileMode(t, config.OAuthStateFile, 0600)
	state, err := loadRegistrationState(config.OAuthStateFile)
	require.NoError(t, err)
	assert.Equal(t, "generated-client-id", state.ClientID)

	// Private key PEM written 0600.
	requireFileMode(t, config.OAuthPrivateKeyFile, 0600)

	// Token written atomically with 0600.
	requireFileMode(t, config.AmqpToken, 0600)
	tokenData, err := os.ReadFile(config.AmqpToken)
	require.NoError(t, err)
	assert.Equal(t, "fake-access-token", string(tokenData))

	// The client_assertion must be a parseable JWT signed by the generated key
	// with the right iss/sub/aud/kid.
	assert.Equal(t, "client_credentials", fr.tokenGrantType)
	require.NotEmpty(t, fr.tokenAssertion)
	privKey, err := loadPrivateKey(config.OAuthPrivateKeyFile)
	require.NoError(t, err)
	var claims jwt.RegisteredClaims
	parsed, err := jwt.ParseWithClaims(fr.tokenAssertion, &claims, func(token *jwt.Token) (interface{}, error) {
		return &privKey.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	assert.Equal(t, "generated-client-id", claims.Issuer)
	assert.Equal(t, "generated-client-id", claims.Subject)
	assert.Equal(t, jwt.ClaimStrings{fr.tokenURL()}, claims.Audience)
	var raw jwt.MapClaims
	_, _, err = new(jwt.Parser).ParseUnverified(fr.tokenAssertion, &raw)
	require.NoError(t, err)
	assert.IsType(t, "", raw["aud"], "aud must be a single string, per the protocol")
	assert.NotEmpty(t, claims.ID, "assertion must carry a jti")
	assert.Equal(t, key0["kid"], parsed.Header["kid"])
}

func TestOAuthRegistrationSkippedWhenStateExists(t *testing.T) {
	fr := newFakeRegistry(t)
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	// Pre-create a private key and state file so registration should be skipped.
	key, err := generateTestKey()
	require.NoError(t, err)
	require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, key))
	require.NoError(t, saveRegistrationState(config.OAuthStateFile, &registrationState{ClientID: "pre-existing-client"}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 0, fr.regCount, "registration must be skipped when a state file exists")

	// The assertion must be signed with the pre-existing client_id and the
	// key's thumbprint.
	require.NotEmpty(t, fr.tokenAssertion)
	var claims jwt.RegisteredClaims
	parsed, _, err := new(jwt.Parser).ParseUnverified(fr.tokenAssertion, &claims)
	require.NoError(t, err)
	assert.Equal(t, "pre-existing-client", claims.Issuer)
	assert.Equal(t, jwkThumbprint(&key.PublicKey), parsed.Header["kid"])
}

// A key left by an interrupted registration with the same token is reused,
// so the registry can return the same client.
func TestOAuthRegistrationRetryReusesKey(t *testing.T) {
	fr := newFakeRegistry(t)
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	key, err := generateTestKey()
	require.NoError(t, err)
	require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, key))
	require.NoError(t, savePendingRegistration(config.OAuthStateFile+".pending", tokenDigest(config.OAuthBootstrapToken)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	require.Equal(t, 1, fr.regCount)
	key0 := fr.regBody["jwks"].(map[string]interface{})["keys"].([]interface{})[0].(map[string]interface{})
	assert.Equal(t, jwkThumbprint(&key.PublicKey), key0["kid"], "a retried registration must present the same key")
	assert.NoFileExists(t, config.OAuthStateFile+".pending", "the marker is removed once registered")
}

// Re-registering after the client was revoked (state removed, new token) must
// not reuse the old key, which may have leaked.
func TestOAuthNewRegistrationGetsNewKey(t *testing.T) {
	fr := newFakeRegistry(t)
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	oldKey, err := generateTestKey()
	require.NoError(t, err)
	require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, oldKey))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	key0 := fr.regBody["jwks"].(map[string]interface{})["keys"].([]interface{})[0].(map[string]interface{})
	assert.NotEqual(t, jwkThumbprint(&oldKey.PublicKey), key0["kid"], "a new registration must use a new key")
	newKey, err := loadPrivateKey(config.OAuthPrivateKeyFile)
	require.NoError(t, err)
	assert.Equal(t, jwkThumbprint(&newKey.PublicKey), key0["kid"])
}

func TestOAuthWaitsWhileAuthorizationPending(t *testing.T) {
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.pendingCount = 3
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 4, fr.tokenCount, "should poll until approved")
	assert.Equal(t, 1, fr.regCount, "pending approval must not trigger re-registration")
	tokenData, err := os.ReadFile(config.AmqpToken)
	require.NoError(t, err)
	assert.Equal(t, "fake-access-token", string(tokenData))
}

func TestOAuthRegistrationWithoutTokenWaitsForApproval(t *testing.T) {
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.pendingCount = 2 // approved on the third poll
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())
	config.OAuthBootstrapToken = ""

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	require.Equal(t, 1, fr.regCount)
	assert.Empty(t, fr.regBearer, "no Authorization header without a token")
	assert.Equal(t, 3, fr.tokenCount)
	state, err := loadRegistrationState(config.OAuthStateFile)
	require.NoError(t, err)
	assert.Equal(t, "generated-client-id", state.ClientID)
}

func TestOAuthRateLimitIsTransient(t *testing.T) {
	// The registry answers 429 with an invalid_client body; it must be retried
	// on the short backoff, not treated as a revoked client.
	shortOAuthDelays(t)
	oauthPendingInterval = time.Hour
	fr := newFakeRegistry(t)
	fr.limitedCount = 2
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 3, fr.tokenCount)
}

func TestOAuthRegistrationRetriesTransientErrors(t *testing.T) {
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.regStatuses = []int{http.StatusServiceUnavailable, http.StatusTooManyRequests}
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 3, fr.regCount)
}

// A registry rejection must not exit (systemd would restart-loop it); the
// shoveler waits, re-reads the token file, and registers once it changes.
func TestOAuthRejectedRegistrationWaitsForNewToken(t *testing.T) {
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.regStatuses = []int{http.StatusUnauthorized, http.StatusUnauthorized}
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())
	tokenFile := filepath.Join(t.TempDir(), "bootstrap.token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("used-token\n"), 0600))
	config.OAuthBootstrapToken = ""
	config.OAuthBootstrapTokenFile = tokenFile

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)

	// Rejected twice; the operator then supplies a new token.
	require.Eventually(t, func() bool {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		return fr.regCount >= 2
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, os.WriteFile(tokenFile, []byte("new-token\n"), 0600))
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, "Bearer used-token", fr.regBearers[0])
	assert.Equal(t, "Bearer new-token", fr.regBearers[len(fr.regBearers)-1])
	state, err := loadRegistrationState(config.OAuthStateFile)
	require.NoError(t, err)
	assert.Equal(t, "generated-client-id", state.ClientID)
}

// A bare 4xx (e.g. a proxy's 404 page while the registry is redeployed) is
// not the registry rejecting the request, so it is retried.
func TestOAuthRegistrationRetriesProxyErrors(t *testing.T) {
	shortOAuthDelays(t)
	oauthRejectedInterval = time.Hour // must not be used
	fr := newFakeRegistry(t)
	fr.regStatuses = []int{http.StatusNotFound, http.StatusForbidden}
	fr.regBare = true
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 3, fr.regCount)
}

func TestOAuthRunStopsOnCancel(t *testing.T) {
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.pendingCount = 1 << 30 // never approved
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	m, err := newOAuthManager(config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- m.run(ctx, make(chan struct{})) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

func TestEndpointErrorPermanent(t *testing.T) {
	assert.True(t, (&endpointError{Status: 400}).permanent())
	assert.True(t, (&endpointError{Status: 401}).permanent())
	assert.True(t, (&endpointError{Status: 404}).permanent())
	assert.False(t, (&endpointError{Status: 408}).permanent())
	assert.False(t, (&endpointError{Status: 429}).permanent())
	assert.False(t, (&endpointError{Status: 500}).permanent())
	assert.False(t, (&endpointError{Status: 503}).permanent())

	e := newEndpointError(400, []byte(`{"error":"authorization_pending","error_description":"wait"}`))
	assert.Equal(t, "authorization_pending", e.Code)
	assert.Equal(t, "wait", e.Description)

	e = newEndpointError(502, []byte("bad gateway\n"))
	assert.Empty(t, e.Code)
	assert.Equal(t, "bad gateway", e.Description)
}

func TestRSAThumbprintRFC7638(t *testing.T) {
	// Example key and expected thumbprint from RFC 7638 section 3.1.
	n := "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
	assert.Equal(t, "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs", rsaThumbprint(n, "AQAB"))
}

func TestNextRefreshInterval(t *testing.T) {
	// ~half of the lifetime.
	assert.Equal(t, 150*time.Second, nextRefreshInterval(300*time.Second))
	// Floor at 30s when half would be smaller.
	assert.Equal(t, oauthRefreshFloor, nextRefreshInterval(40*time.Second))
	assert.Equal(t, oauthRefreshFloor, nextRefreshInterval(10*time.Second))
	// Fallback when no expiry is known.
	assert.Equal(t, oauthRefreshFallback, nextRefreshInterval(0))
}

func TestOAuthConfigParsing(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	stateFile := filepath.Join(dir, "registration.json")
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    token_url: https://kc.example.org/token\n" +
		"    registration_url: https://kc.example.org/register\n" +
		"    bootstrap_token: yaml-bootstrap\n" +
		"    state_file: " + stateFile + "\n" +
		"    client_name: my-shoveler\n" +
		"    scopes:\n" +
		"      - scope-a\n" +
		"      - scope-b\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	c := &Config{}
	c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")

	assert.True(t, c.OAuthEnabled)
	assert.Equal(t, "https://kc.example.org/token", c.OAuthTokenURL)
	assert.Equal(t, "https://kc.example.org/register", c.OAuthRegistrationURL)
	assert.Equal(t, "yaml-bootstrap", c.OAuthBootstrapToken)
	assert.Equal(t, "my-shoveler", c.OAuthClientName)
	assert.Equal(t, []string{"scope-a", "scope-b"}, c.OAuthScopes)
}

func TestOAuthConfigParsingFromEnv(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	stateFile := filepath.Join(dir, "registration.json")
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    state_file: " + stateFile + "\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	t.Setenv("SHOVELER_AMQP_OAUTH_TOKEN_URL", "https://env.example.org/token")
	t.Setenv("SHOVELER_AMQP_OAUTH_REGISTRATION_URL", "https://env.example.org/register")
	t.Setenv("SHOVELER_AMQP_OAUTH_BOOTSTRAP_TOKEN", "env-bootstrap")

	c := &Config{}
	c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")

	assert.Equal(t, "https://env.example.org/token", c.OAuthTokenURL)
	assert.Equal(t, "https://env.example.org/register", c.OAuthRegistrationURL)
	assert.Equal(t, "env-bootstrap", c.OAuthBootstrapToken)
}

func TestOAuthConfigAllowsFirstRunWithoutToken(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	// Enabled, required URLs present, no token and no state file: the shoveler
	// registers without a token and waits for approval.
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    token_url: https://registry.example.org/token\n" +
		"    registration_url: https://registry.example.org/register\n" +
		"    state_file: " + filepath.Join(dir, "does-not-exist.json") + "\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	c := &Config{}
	assert.NotPanics(t, func() {
		c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	})
	assert.Empty(t, c.OAuthBootstrapToken)
}

// A configured token file that can't be used must stop startup, never turn
// into a registration that waits for approval; once registered it isn't read.
func TestOAuthBootstrapTokenFileProblems(t *testing.T) {
	for _, tc := range []struct {
		name       string
		contents   *string
		registered bool
		wantErr    string
	}{
		{"missing on first run", nil, false, "can't read bootstrap_token_file"},
		{"empty on first run", ptr(" \n"), false, "is empty"},
		{"missing once registered", nil, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := oauthTestConfig(t, "https://r.example.org/token", "https://r.example.org/register")
			config.OAuthBootstrapToken = "inline-token"
			config.OAuthBootstrapTokenFile = filepath.Join(t.TempDir(), "bootstrap.token")
			if tc.contents != nil {
				require.NoError(t, os.WriteFile(config.OAuthBootstrapTokenFile, []byte(*tc.contents), 0600))
			}
			if tc.registered {
				key, err := generateTestKey()
				require.NoError(t, err)
				require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, key))
				require.NoError(t, saveRegistrationState(config.OAuthStateFile, &registrationState{ClientID: "c"}))
			}
			_, err := newOAuthManager(config)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// A state file that can't be checked (e.g. permission denied) is an error,
// not a first run: re-registering would replace a working client.
func TestOAuthStateFileStatErrorIsNotFirstRun(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	config := oauthTestConfig(t, "https://r.example.org/token", "https://r.example.org/register")
	dir := filepath.Dir(config.OAuthStateFile)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, os.Chmod(dir, 0))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	_, err := newOAuthManager(config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can't check OAuth state file")
}

func TestOAuthConfigValidationSucceedsWithStateFileOnly(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	stateFile := filepath.Join(dir, "registration.json")
	require.NoError(t, saveRegistrationState(stateFile, &registrationState{ClientID: "c"}))

	configFile := filepath.Join(dir, "config.yaml")
	// Enabled, required URLs present, NO bootstrap token, but a state file exists.
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    token_url: https://kc.example.org/token\n" +
		"    registration_url: https://kc.example.org/register\n" +
		"    state_file: " + stateFile + "\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	c := &Config{}
	assert.NotPanics(t, func() {
		c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	}, "subsequent run with only a state file must succeed")
	assert.Empty(t, c.OAuthBootstrapToken)
}

func TestOAuthBootstrapTokenFileWins(t *testing.T) {
	config := oauthTestConfig(t, "https://r.example.org/token", "https://r.example.org/register")
	config.OAuthBootstrapToken = "inline-bootstrap"
	config.OAuthBootstrapTokenFile = filepath.Join(t.TempDir(), "bootstrap.token")
	require.NoError(t, os.WriteFile(config.OAuthBootstrapTokenFile, []byte("  file-bootstrap\n"), 0600))

	m, err := newOAuthManager(config)
	require.NoError(t, err)
	assert.Equal(t, "file-bootstrap", m.bootstrapToken, "bootstrap_token_file must win and be trimmed")
}

// generateTestKey produces a small RSA key for tests.
func generateTestKey() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, 2048)
}

// requireFileMode asserts the file at path has exactly the given permission bits.
func requireFileMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, want, info.Mode().Perm(), "unexpected permissions on %s", path)
}

func TestOAuthDefaultPathsUseStateDir(t *testing.T) {
	for _, tc := range []struct {
		name      string
		oauth     string
		tokenLine string
		wantToken string
	}{
		{"oauth defaults to the state dir", "true", "", "/var/lib/xrootd-monitoring-shoveler/token"},
		{"explicit token_location wins", "true", "  token_location: /srv/token\n", "/srv/token"},
		{"without oauth the token stays in /etc", "false", "", "/etc/xrootd-monitoring-shoveler/token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			dir := t.TempDir()
			configFile := filepath.Join(dir, "config.yaml")
			yaml := "" +
				"mq: amqp\n" +
				"amqp:\n" +
				"  url: amqps://example.com/vhost\n" +
				tc.tokenLine +
				"  oauth:\n" +
				"    enabled: " + tc.oauth + "\n" +
				"    token_url: https://registry.example.org/token\n" +
				"    registration_url: https://registry.example.org/register\n"
			require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

			c := &Config{}
			c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
			assert.Equal(t, tc.wantToken, c.AmqpToken)
			if c.OAuthEnabled {
				assert.Equal(t, "/var/lib/xrootd-monitoring-shoveler/oauth/private.pem", c.OAuthPrivateKeyFile)
				assert.Equal(t, "/var/lib/xrootd-monitoring-shoveler/oauth/registration.json", c.OAuthStateFile)
			}
		})
	}
}

func TestOAuthConfigRejectsCredentialsInURL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://user:pass@example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    token_url: https://registry.example.org/token\n" +
		"    registration_url: https://registry.example.org/register\n" +
		"    state_file: " + filepath.Join(dir, "registration.json") + "\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	c := &Config{}
	assert.Panics(t, func() {
		c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	}, "URL credentials would bypass the OAuth token")
}

// The packaged config.yaml must not pin token_location, or OAuth mode would
// write the token under /etc, which the service user can't write.
func TestShippedConfigLeavesTokenLocationDefault(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	data, err := os.ReadFile("config/config.yaml")
	require.NoError(t, err)
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(configFile, data, 0644))

	t.Setenv("SHOVELER_MQ", "amqp")
	t.Setenv("SHOVELER_AMQP_URL", "amqps://broker.example.org/vhost")
	t.Setenv("SHOVELER_AMQP_OAUTH_ENABLED", "true")
	t.Setenv("SHOVELER_AMQP_OAUTH_TOKEN_URL", "https://registry.example.org/token")
	t.Setenv("SHOVELER_AMQP_OAUTH_REGISTRATION_URL", "https://registry.example.org/register")

	c := &Config{}
	c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	assert.Equal(t, "/var/lib/xrootd-monitoring-shoveler/token", c.AmqpToken)
}

func TestOAuthDefaultRegistryURLs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		urls      string
		wantToken string
		wantReg   string
	}{
		{"defaults to the OSG registry", "",
			"https://shoveler-auth.osg-htc.org/token", "https://shoveler-auth.osg-htc.org/register"},
		{"explicit URLs win", "" +
			"    token_url: https://registry.example.org/token\n" +
			"    registration_url: https://registry.example.org/register\n",
			"https://registry.example.org/token", "https://registry.example.org/register"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				tc.urls +
				"    state_file: " + filepath.Join(dir, "registration.json") + "\n"
			require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

			c := &Config{}
			c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
			assert.Equal(t, tc.wantToken, c.OAuthTokenURL)
			assert.Equal(t, tc.wantReg, c.OAuthRegistrationURL)
			assert.Equal(t, defaultReportURL(tc.wantToken), c.OAuthReportURL, "health reports follow the token URL")
		})
	}
}

// testAccessToken builds an (unverified) access token for sub expiring in d.
func testAccessToken(t *testing.T, sub string, d time.Duration) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   sub,
		Audience:  jwt.ClaimStrings{"my_rabbit_server", "other"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(d)),
	}).SignedString([]byte("test"))
	require.NoError(t, err)
	return tok
}

// registeredConfig returns a config with a saved registration for client.
func registeredConfig(t *testing.T, fr *fakeRegistry, client string) *Config {
	t.Helper()
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())
	key, err := generateTestKey()
	require.NoError(t, err)
	require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, key))
	require.NoError(t, saveRegistrationState(config.OAuthStateFile, &registrationState{ClientID: client}))
	return config
}

// After a restart a still-valid token on disk lets AMQP start without the
// registry, e.g. during a registry outage.
func TestOAuthResumeUsesValidTokenOnDisk(t *testing.T) {
	fr := newFakeRegistry(t)
	config := registeredConfig(t, fr, "client-1")
	require.NoError(t, os.WriteFile(config.AmqpToken, []byte(testAccessToken(t, "client-1", time.Hour)), 0600))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 0, fr.tokenCount, "the token on disk is good for an hour; no fetch yet")
}

func TestOAuthResumeIgnoresUnusableTokenOnDisk(t *testing.T) {
	for _, tc := range []struct {
		name  string
		token func(t *testing.T) string
	}{
		{"another client's", func(t *testing.T) string { return testAccessToken(t, "someone-else", time.Hour) }},
		{"nearly expired", func(t *testing.T) string { return testAccessToken(t, "client-1", 10*time.Second) }},
		{"garbage", func(t *testing.T) string { return "not-a-jwt" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fr := newFakeRegistry(t)
			config := registeredConfig(t, fr, "client-1")
			require.NoError(t, os.WriteFile(config.AmqpToken, []byte(tc.token(t)), 0600))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ready, err := StartOAuthManager(ctx, config)
			require.NoError(t, err)
			waitReady(t, ready)

			fr.mu.Lock()
			defer fr.mu.Unlock()
			assert.Equal(t, 1, fr.tokenCount)
		})
	}
}

// When the token can't be written, only the write is retried; the registry
// isn't asked for a new token every time.
func TestOAuthTokenWriteFailureRetriesWriteOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())
	tokenDir := filepath.Join(t.TempDir(), "tokens")
	require.NoError(t, os.MkdirAll(tokenDir, 0500))
	t.Cleanup(func() { _ = os.Chmod(tokenDir, 0700) })
	config.AmqpToken = filepath.Join(tokenDir, "token")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // ~20 write retries
	select {
	case <-ready:
		t.Fatal("ready before the token could be written")
	default:
	}
	require.NoError(t, os.Chmod(tokenDir, 0700))
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 1, fr.tokenCount, "write failures must not mint new tokens")
}

func TestLifetimeFromJWTAcceptsArrayAudience(t *testing.T) {
	remaining := lifetimeFromJWT(testAccessToken(t, "c", time.Hour))
	assert.InDelta(t, time.Hour.Seconds(), remaining.Seconds(), 5)
}

// readTestConfig reads YAML config text the way the shoveler does.
func readTestConfig(t *testing.T, yaml string) *Config {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	configFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))
	c := &Config{}
	c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	return c
}

// Setting one endpoint points the other at the same registry, so a
// registration token for one registry is never sent to another.
func TestOAuthEndpointsFollowEachOther(t *testing.T) {
	c := readTestConfig(t, "mq: amqp\namqp:\n  url: amqps://example.com/vhost\n  oauth:\n    enabled: true\n"+
		"    token_url: https://custom.example.org/auth/token\n")
	assert.Equal(t, "https://custom.example.org/auth/register", c.OAuthRegistrationURL)
	assert.Equal(t, "https://custom.example.org/auth/report", c.OAuthReportURL)

	c = readTestConfig(t, "mq: amqp\namqp:\n  url: amqps://example.com/vhost\n  oauth:\n    enabled: true\n"+
		"    registration_url: https://custom.example.org/register\n")
	assert.Equal(t, "https://custom.example.org/token", c.OAuthTokenURL)

	assert.Panics(t, func() {
		readTestConfig(t, "mq: amqp\namqp:\n  url: amqps://example.com/vhost\n  oauth:\n    enabled: true\n"+
			"    token_url: https://custom.example.org/oauth2/access\n")
	}, "an endpoint the other can't be derived from must not fall back to the OSG registry")
}

// Upgraded installs keep the old config.yaml, which pinned token_location
// under /etc where the service user can't write.
func TestOAuthIgnoresLegacyTokenLocation(t *testing.T) {
	c := readTestConfig(t, "mq: amqp\namqp:\n  url: amqps://example.com/vhost\n"+
		"  token_location: /etc/xrootd-monitoring-shoveler/token\n  oauth:\n    enabled: true\n")
	assert.Equal(t, "/var/lib/xrootd-monitoring-shoveler/token", c.AmqpToken)

	c = readTestConfig(t, "mq: amqp\namqp:\n  url: amqps://example.com/vhost\n"+
		"  token_location: /etc/xrootd-monitoring-shoveler/token\n")
	assert.Equal(t, "/etc/xrootd-monitoring-shoveler/token", c.AmqpToken, "unchanged without OAuth")
}

// The collector's rabbitmq input can't use the OAuth publish token.
func TestOAuthInputNeedsItsOwnCredentials(t *testing.T) {
	base := "mq: amqp\ninput:\n  type: rabbitmq\n%samqp:\n  url: amqps://example.com/vhost\n  oauth:\n    enabled: %s\n"
	assert.Panics(t, func() { readTestConfig(t, fmt.Sprintf(base, "", "true")) })

	c := readTestConfig(t, fmt.Sprintf(base, "  broker_url: amqps://user:pw@in.example.com/vhost\n", "true"))
	assert.Empty(t, c.Input.TokenLocation)

	c = readTestConfig(t, fmt.Sprintf(base, "  token_location: /etc/input.token\n", "true"))
	assert.Equal(t, "/etc/input.token", c.Input.TokenLocation)

	c = readTestConfig(t, fmt.Sprintf(base, "", "false"))
	assert.Equal(t, c.AmqpToken, c.Input.TokenLocation, "without OAuth the input shares the AMQP token as before")
}

func TestParseBrokerURLHidesCredentials(t *testing.T) {
	_, err := parseBrokerURL("amqps://user:s3cret@broker.example.org/vhost\n")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "s3cret")
}
