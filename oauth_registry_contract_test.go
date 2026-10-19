package shoveler

// Contract test against a live shoveler registry (docs/registry-protocol.md).
// Skipped unless SHOVELER_REGISTRY_URL is set. Each run needs two fresh,
// unused registration tokens for the same site:
//
//	SHOVELER_REGISTRY_URL=http://127.0.0.1:18080 \
//	SHOVELER_REGISTRY_BOOTSTRAP=<token> SHOVELER_REGISTRY_BOOTSTRAP2=<token> \
//	go test -run TestRegistryContract -v .

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func registryContractConfig(t *testing.T, base, bootstrap string) *Config {
	t.Helper()
	dir := t.TempDir()
	return &Config{
		OAuthEnabled:         true,
		OAuthTokenURL:        base + "/token",
		OAuthRegistrationURL: base + "/register",
		OAuthBootstrapToken:  bootstrap,
		OAuthPrivateKeyFile:  filepath.Join(dir, "oauth", "private.pem"),
		OAuthStateFile:       filepath.Join(dir, "oauth", "registration.json"),
		OAuthClientName:      "shoveler@contract-test.example",
		// The registry must tolerate (and may ignore) requested scopes.
		OAuthScopes: []string{"my_rabbit_server.write:*/xrd-shoveled"},
		AmqpToken:   filepath.Join(dir, "token"),
	}
}

// postAssertion sends a raw client-assertion token request.
func postAssertion(t *testing.T, tokenURL, assertion string) (*http.Response, *endpointError) {
	t.Helper()
	form := url.Values{
		"grant_type":            {"client_credentials"},
		"client_assertion_type": {oauthAssertionType},
		"client_assertion":      {assertion},
	}
	resp, err := http.PostForm(tokenURL, form)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return resp, nil
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	return resp, newEndpointError(resp.StatusCode, body[:n])
}

func TestRegistryContract(t *testing.T) {
	base := strings.TrimRight(os.Getenv("SHOVELER_REGISTRY_URL"), "/")
	if base == "" {
		t.Skip("SHOVELER_REGISTRY_URL not set")
	}
	bootstrap := os.Getenv("SHOVELER_REGISTRY_BOOTSTRAP")
	bootstrap2 := os.Getenv("SHOVELER_REGISTRY_BOOTSTRAP2")
	require.NotEmpty(t, bootstrap, "SHOVELER_REGISTRY_BOOTSTRAP must be set")
	require.NotEmpty(t, bootstrap2, "SHOVELER_REGISTRY_BOOTSTRAP2 must be set")

	oldVersion := ShovelerVersion
	ShovelerVersion = "contract-test" // sends software_version: unknown metadata must be accepted
	t.Cleanup(func() { ShovelerVersion = oldVersion })

	config := registryContractConfig(t, base, bootstrap)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var m *oauthManager
	tokenOK := false // negative /token checks are meaningless until a token can be issued
	t.Run("register and fetch first token", func(t *testing.T) {
		var err error
		m, err = newOAuthManager(config)
		require.NoError(t, err)
		ready := make(chan struct{})
		runErr := make(chan error, 1)
		go func() { runErr <- m.run(ctx, ready) }()
		select {
		case <-ready:
		case err := <-runErr:
			t.Fatalf("client stopped: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("no access token within 10s (pending? check the shoveler log above)")
		}
		tokenOK = true
		require.NotEmpty(t, m.clientID)
		state, err := loadRegistrationState(config.OAuthStateFile)
		require.NoError(t, err)
		assert.Equal(t, m.clientID, state.ClientID)

		token, err := os.ReadFile(config.AmqpToken)
		require.NoError(t, err)
		var claims jwt.MapClaims
		_, _, err = new(jwt.Parser).ParseUnverified(string(token), &claims)
		require.NoError(t, err, "access token must be a JWT")
		assert.NotEmpty(t, claims["exp"], "access token needs exp")
		assert.NotEmpty(t, claims["aud"], "access token needs aud for RabbitMQ")
	})
	require.NotNil(t, m)
	cancel()

	t.Run("token response carries a lifetime", func(t *testing.T) {
		_, lifetime, err := m.fetchToken(context.Background())
		require.NoError(t, err)
		assert.Greater(t, lifetime, oauthRefreshFloor, "expires_in (or exp) should give a usable lifetime")
	})

	t.Run("restart resumes from state without bootstrap", func(t *testing.T) {
		restarted := *config
		restarted.OAuthBootstrapToken = ""
		m2, err := newOAuthManager(&restarted)
		require.NoError(t, err)
		assert.Equal(t, m.clientID, m2.clientID)
		_, _, err = m2.fetchToken(context.Background())
		assert.NoError(t, err)
	})

	t.Run("idempotent re-registration with the same key", func(t *testing.T) {
		clientID, err := m.doRegister(context.Background())
		require.NoError(t, err, "a used token re-presented with the same key must return the same client")
		assert.Equal(t, m.clientID, clientID)
	})

	t.Run("used token with a different key is a permanent error", func(t *testing.T) {
		other := &oauthManager{config: config, httpClient: m.httpClient}
		key, err := generateTestKey()
		require.NoError(t, err)
		other.privateKey, other.kid = key, jwkThumbprint(&key.PublicKey)
		_, err = other.doRegister(context.Background())
		requirePermanent(t, err)
	})

	t.Run("garbage bootstrap token is a permanent error", func(t *testing.T) {
		bad := *config
		bad.OAuthBootstrapToken = "not-a-real-token"
		other := &oauthManager{config: &bad, httpClient: m.httpClient}
		key, err := generateTestKey()
		require.NoError(t, err)
		other.privateKey, other.kid = key, jwkThumbprint(&key.PublicKey)
		_, err = other.doRegister(context.Background())
		requirePermanent(t, err)
	})

	t.Run("weak key is rejected permanently", func(t *testing.T) {
		cfg2 := registryContractConfig(t, base, bootstrap2)
		weak := &oauthManager{config: cfg2, httpClient: m.httpClient}
		key, err := rsaKey(1024)
		require.NoError(t, err)
		weak.privateKey, weak.kid = key, jwkThumbprint(&key.PublicKey)
		_, err = weak.doRegister(context.Background())
		requirePermanent(t, err)
	})

	t.Run("second token still registers after a rejected attempt", func(t *testing.T) {
		cfg2 := registryContractConfig(t, base, bootstrap2)
		m2, err := newOAuthManager(cfg2)
		require.NoError(t, err)
		clientID, err := m2.doRegister(context.Background())
		require.NoError(t, err, "a 400 must not consume the registration token")
		assert.NotEqual(t, m.clientID, clientID)
	})

	t.Run("replayed assertion is invalid_client", func(t *testing.T) {
		if !tokenOK {
			t.Skip("token endpoint does not issue tokens yet")
		}
		assertion, err := buildClientAssertion(m.clientID, config.OAuthTokenURL, m.kid, m.privateKey)
		require.NoError(t, err)
		_, e := postAssertion(t, config.OAuthTokenURL, assertion)
		require.Nil(t, e, "first use should succeed")
		_, e = postAssertion(t, config.OAuthTokenURL, assertion)
		require.NotNil(t, e)
		assert.Equal(t, oauthErrInvalidClient, e.Code)
	})

	t.Run("assertion signed by another key is invalid_client", func(t *testing.T) {
		if !tokenOK {
			t.Skip("token endpoint does not issue tokens yet")
		}
		key, err := generateTestKey()
		require.NoError(t, err)
		assertion, err := buildClientAssertion(m.clientID, config.OAuthTokenURL, m.kid, key)
		require.NoError(t, err)
		_, e := postAssertion(t, config.OAuthTokenURL, assertion)
		require.NotNil(t, e)
		assert.Equal(t, oauthErrInvalidClient, e.Code)
	})

	t.Run("wrong audience is invalid_client", func(t *testing.T) {
		if !tokenOK {
			t.Skip("token endpoint does not issue tokens yet")
		}
		assertion, err := buildClientAssertion(m.clientID, base+"/not-the-token-endpoint", m.kid, m.privateKey)
		require.NoError(t, err)
		_, e := postAssertion(t, config.OAuthTokenURL, assertion)
		require.NotNil(t, e)
		assert.Equal(t, oauthErrInvalidClient, e.Code)
	})
}

func requirePermanent(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	ee, ok := err.(*endpointError)
	require.True(t, ok, "expected an HTTP error response, got %v", err)
	assert.True(t, ee.permanent(), "expected a permanent (4xx) rejection, got %v", ee)
}

func rsaKey(bits int) (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, bits)
}
