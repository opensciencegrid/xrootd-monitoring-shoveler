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

// fakeKeycloak returns an httptest server with /register and /token handlers,
// and records what it received.
type fakeKeycloak struct {
	server          *httptest.Server
	regCount        int
	regBearer       string
	regBody         map[string]interface{}
	tokenAssertion  string
	tokenGrantType  string
	tokenScope      string
	accessToken     string
	expiresIn       int
	registrationErr int // if non-zero, /register returns this status
}

func newFakeKeycloak(t *testing.T) *fakeKeycloak {
	t.Helper()
	fk := &fakeKeycloak{accessToken: "fake-access-token", expiresIn: 300}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		fk.regCount++
		fk.regBearer = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&fk.regBody)
		if fk.registrationErr != 0 {
			w.WriteHeader(fk.registrationErr)
			_, _ = w.Write([]byte(`{"error":"forced"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"client_id":                 "generated-client-id",
			"registration_access_token": "reg-access-token",
			"registration_client_uri":   "https://kc.example.org/clients/generated-client-id",
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		fk.tokenAssertion = r.Form.Get("client_assertion")
		fk.tokenGrantType = r.Form.Get("grant_type")
		fk.tokenScope = r.Form.Get("scope")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": fk.accessToken,
			"expires_in":   fk.expiresIn,
			"token_type":   "Bearer",
		})
	})
	fk.server = httptest.NewServer(mux)
	t.Cleanup(fk.server.Close)
	return fk
}

func (fk *fakeKeycloak) tokenURL() string { return fk.server.URL + "/token" }
func (fk *fakeKeycloak) regURL() string   { return fk.server.URL + "/register" }

func TestOAuthRegistrationAndTokenFetch(t *testing.T) {
	fk := newFakeKeycloak(t)
	config := oauthTestConfig(t, fk.tokenURL(), fk.regURL())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first token")
	}

	// Registration assertions.
	require.Equal(t, 1, fk.regCount, "registration should be called exactly once")
	assert.Equal(t, "Bearer bootstrap-initial-access-token", fk.regBearer, "bootstrap token must be sent as bearer")
	assert.Equal(t, "private_key_jwt", fk.regBody["token_endpoint_auth_method"])

	// The request must carry a jwks with the public key (n and e present).
	jwks, ok := fk.regBody["jwks"].(map[string]interface{})
	require.True(t, ok, "request must include jwks")
	keys, ok := jwks["keys"].([]interface{})
	require.True(t, ok)
	require.Len(t, keys, 1)
	key0 := keys[0].(map[string]interface{})
	assert.Equal(t, "RSA", key0["kty"])
	assert.NotEmpty(t, key0["n"], "jwks must include modulus")
	assert.NotEmpty(t, key0["e"], "jwks must include exponent")
	assert.NotEmpty(t, key0["kid"], "jwks must include kid")

	// No scope must be requested at registration time (authorization is
	// admin-attached).
	_, hasScope := fk.regBody["scope"]
	assert.False(t, hasScope, "registration must not request a scope")

	// State file written 0600 with returned values.
	requireFileMode(t, config.OAuthStateFile, 0600)
	state, err := loadRegistrationState(config.OAuthStateFile)
	require.NoError(t, err)
	assert.Equal(t, "generated-client-id", state.ClientID)
	assert.Equal(t, "reg-access-token", state.RegistrationAccessToken)
	assert.NotEmpty(t, state.Kid)

	// Private key PEM written 0600.
	requireFileMode(t, config.OAuthPrivateKeyFile, 0600)

	// Token written atomically with 0600.
	requireFileMode(t, config.AmqpToken, 0600)
	tokenData, err := os.ReadFile(config.AmqpToken)
	require.NoError(t, err)
	assert.Equal(t, "fake-access-token", string(tokenData))

	// The client_assertion must be a parseable JWT signed by the generated key
	// with the right iss/sub/aud.
	assert.Equal(t, "client_credentials", fk.tokenGrantType)
	require.NotEmpty(t, fk.tokenAssertion)
	privKey, err := loadPrivateKey(config.OAuthPrivateKeyFile)
	require.NoError(t, err)
	var claims jwt.StandardClaims
	parsed, err := jwt.ParseWithClaims(fk.tokenAssertion, &claims, func(token *jwt.Token) (interface{}, error) {
		return &privKey.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	assert.Equal(t, "generated-client-id", claims.Issuer)
	assert.Equal(t, "generated-client-id", claims.Subject)
	assert.Equal(t, fk.tokenURL(), claims.Audience)
	assert.Equal(t, state.Kid, parsed.Header["kid"])
}

func TestOAuthRegistrationSkippedWhenStateExists(t *testing.T) {
	fk := newFakeKeycloak(t)
	config := oauthTestConfig(t, fk.tokenURL(), fk.regURL())

	// Pre-create a private key and state file so registration should be skipped.
	require.NoError(t, os.MkdirAll(filepath.Dir(config.OAuthStateFile), 0700))
	key, err := generateTestKey()
	require.NoError(t, err)
	require.NoError(t, savePrivateKey(config.OAuthPrivateKeyFile, key))
	require.NoError(t, saveRegistrationState(config.OAuthStateFile, &registrationState{
		ClientID: "pre-existing-client",
		Kid:      "pre-existing-kid",
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready, err := StartOAuthManager(ctx, config)
	require.NoError(t, err)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first token")
	}

	assert.Equal(t, 0, fk.regCount, "registration must be skipped when a state file exists")

	// The assertion must be signed with the pre-existing client_id and kid.
	require.NotEmpty(t, fk.tokenAssertion)
	var claims jwt.StandardClaims
	parsed, _, err := new(jwt.Parser).ParseUnverified(fk.tokenAssertion, &claims)
	require.NoError(t, err)
	assert.Equal(t, "pre-existing-client", claims.Issuer)
	assert.Equal(t, "pre-existing-kid", parsed.Header["kid"])
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
	require.NoError(t, saveRegistrationState(stateFile, &registrationState{ClientID: "c", Kid: "k"}))

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
