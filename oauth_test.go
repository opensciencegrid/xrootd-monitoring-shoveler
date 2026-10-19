package shoveler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
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
	oldRetry, oldPending := oauthRetryBackoff, oauthPendingInterval
	oauthRetryBackoff, oauthPendingInterval = 10*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { oauthRetryBackoff, oauthPendingInterval = oldRetry, oldPending })
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
	tokenCount     int
	tokenAssertion string
	tokenGrantType string
	tokenScope     string
	pendingCount   int // /token answers authorization_pending this many times
	accessToken    string
	expiresIn      int
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
		fr.regBody = nil
		_ = json.NewDecoder(r.Body).Decode(&fr.regBody)
		if len(fr.regStatuses) > 0 {
			status := fr.regStatuses[0]
			fr.regStatuses = fr.regStatuses[1:]
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
	var claims jwt.StandardClaims
	parsed, err := jwt.ParseWithClaims(fr.tokenAssertion, &claims, func(token *jwt.Token) (interface{}, error) {
		return &privKey.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	assert.Equal(t, "generated-client-id", claims.Issuer)
	assert.Equal(t, "generated-client-id", claims.Subject)
	assert.Equal(t, fr.tokenURL(), claims.Audience)
	assert.NotEmpty(t, claims.Id, "assertion must carry a jti")
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
	var claims jwt.StandardClaims
	parsed, _, err := new(jwt.Parser).ParseUnverified(fr.tokenAssertion, &claims)
	require.NoError(t, err)
	assert.Equal(t, "pre-existing-client", claims.Issuer)
	assert.Equal(t, jwkThumbprint(&key.PublicKey), parsed.Header["kid"])
}

func TestOAuthRegistrationReusesExistingKey(t *testing.T) {
	fr := newFakeRegistry(t)
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	// A key but no state: e.g. a previous run crashed after generating the key.
	key, err := generateTestKey()
	require.NoError(t, err)
	require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, key))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)
	waitReady(t, ready)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	require.Equal(t, 1, fr.regCount)
	key0 := fr.regBody["jwks"].(map[string]interface{})["keys"].([]interface{})[0].(map[string]interface{})
	assert.Equal(t, jwkThumbprint(&key.PublicKey), key0["kid"], "registration must present the existing key")
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

func TestOAuthRegistrationPermanentErrorStops(t *testing.T) {
	shortOAuthDelays(t)
	fr := newFakeRegistry(t)
	fr.regStatuses = []int{http.StatusUnauthorized}
	config := oauthTestConfig(t, fr.tokenURL(), fr.regURL())

	m, err := newOAuthManager(config)
	require.NoError(t, err)

	err = m.run(context.Background(), make(chan struct{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_token")
	assert.NoFileExists(t, config.OAuthStateFile)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	assert.Equal(t, 1, fr.regCount, "a 4xx must not be retried")
	assert.Equal(t, 0, fr.tokenCount)
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

func TestOAuthConfigValidationFailsFastOnFirstRun(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	// Enabled, required URLs present, but no bootstrap token and no state file.
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    token_url: https://kc.example.org/token\n" +
		"    registration_url: https://kc.example.org/register\n" +
		"    state_file: " + filepath.Join(dir, "does-not-exist.json") + "\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	c := &Config{}
	assert.Panics(t, func() {
		c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	}, "first run without a bootstrap token must fail fast")
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
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "bootstrap.token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("  file-bootstrap\n"), 0600))

	configFile := filepath.Join(dir, "config.yaml")
	yaml := "" +
		"mq: amqp\n" +
		"amqp:\n" +
		"  url: amqps://example.com/vhost\n" +
		"  oauth:\n" +
		"    enabled: true\n" +
		"    token_url: https://kc.example.org/token\n" +
		"    registration_url: https://kc.example.org/register\n" +
		"    bootstrap_token: inline-bootstrap\n" +
		"    bootstrap_token_file: " + tokenFile + "\n" +
		"    state_file: " + filepath.Join(dir, "registration.json") + "\n"
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0644))

	c := &Config{}
	c.ReadConfigWithPathAndPrefix(configFile, "SHOVELER")
	assert.Equal(t, "file-bootstrap", c.OAuthBootstrapToken, "bootstrap_token_file must win and be trimmed")
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
