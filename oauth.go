package shoveler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
)

const (
	// Lifetime of the short-lived client-assertion JWT presented at the token
	// endpoint.
	oauthAssertionTTL = 60 * time.Second

	// Lower bound on how soon we re-fetch an access token.
	oauthRefreshFloor = 30 * time.Second

	// Used when the token response carries no usable expiry information.
	oauthRefreshFallback = 10 * time.Minute

	// Delay between retries when a fetch (or token-file write) fails.
	oauthRetryBackoff = 15 * time.Second

	// client_assertion_type for private_key_jwt client authentication.
	oauthAssertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
)

// registrationState is the persisted result of dynamic client registration. It
// is everything the shoveler needs to authenticate (and later self-rotate its
// key) without the one-time bootstrap token.
type registrationState struct {
	ClientID                string `json:"client_id"`
	RegistrationAccessToken string `json:"registration_access_token"`
	RegistrationClientURI   string `json:"registration_client_uri"`
	Kid                     string `json:"kid"`
}

// registrationResponse captures the fields we care about from Keycloak's
// dynamic client registration response (RFC 7591).
type registrationResponse struct {
	ClientID                string `json:"client_id"`
	RegistrationAccessToken string `json:"registration_access_token"`
	RegistrationClientURI   string `json:"registration_client_uri"`
}

// tokenResponse captures the fields we care about from the token endpoint.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// oauthManager keeps the token file populated with a fresh access token fetched
// via private_key_jwt. It deliberately writes to config.AmqpToken so the
// existing file-watch/restart machinery in amqp.go handles connection rotation.
type oauthManager struct {
	config     *Config
	privateKey *rsa.PrivateKey
	kid        string
	state      *registrationState
	httpClient *http.Client
}

// StartOAuthManager performs one-time bootstrap self-registration (if the
// shoveler has not yet registered) and then launches the steady-state token
// fetch loop in a goroutine. The returned channel is closed once the first
// access token has been written to config.AmqpToken, so startup can block on it
// before StartAMQP does its initial stat/read of the token file.
func StartOAuthManager(ctx context.Context, config *Config) (<-chan struct{}, error) {
	m := &oauthManager{
		config:     config,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}

	if fileExists(config.OAuthStateFile) {
		// Phase 2 only: resume from persisted state, skip registration.
		state, err := loadRegistrationState(config.OAuthStateFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load OAuth state file %s: %w", config.OAuthStateFile, err)
		}
		key, err := loadPrivateKey(config.OAuthPrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load OAuth private key %s: %w", config.OAuthPrivateKeyFile, err)
		}
		m.state = state
		m.kid = state.Kid
		m.privateKey = key
		log.Infoln("OAuth: resuming from persisted registration, client_id:", state.ClientID)
	} else {
		// Phase 1: register identity + public key, persist the result.
		if err := m.register(ctx); err != nil {
			return nil, fmt.Errorf("OAuth bootstrap registration failed: %w", err)
		}
		log.Infoln("OAuth: bootstrap registration complete, client_id:", m.state.ClientID)
	}

	ready := make(chan struct{})
	go m.run(ctx, ready)
	return ready, nil
}

// register generates (or loads) the RSA keypair, calls the dynamic client
// registration endpoint with the bootstrap token, and persists the resulting
// client_id + registration access token. The private key never leaves the host.
func (m *oauthManager) register(ctx context.Context) error {
	var key *rsa.PrivateKey
	if fileExists(m.config.OAuthPrivateKeyFile) {
		loaded, err := loadPrivateKey(m.config.OAuthPrivateKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load existing private key: %w", err)
		}
		key = loaded
		log.Infoln("OAuth: reusing existing private key at", m.config.OAuthPrivateKeyFile)
	} else {
		generated, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return fmt.Errorf("failed to generate RSA key: %w", err)
		}
		if err := savePrivateKey(m.config.OAuthPrivateKeyFile, generated); err != nil {
			return fmt.Errorf("failed to persist private key: %w", err)
		}
		key = generated
		log.Infoln("OAuth: generated new private key at", m.config.OAuthPrivateKeyFile)
	}
	m.privateKey = key
	m.kid = uuid.NewString()

	state, err := m.doRegister(ctx)
	if err != nil {
		return err
	}
	state.Kid = m.kid
	if err := saveRegistrationState(m.config.OAuthStateFile, state); err != nil {
		return fmt.Errorf("failed to persist registration state: %w", err)
	}
	m.state = state
	return nil
}

// doRegister performs the HTTP dynamic client registration call.
func (m *oauthManager) doRegister(ctx context.Context) (*registrationState, error) {
	// IMPORTANT: we register identity + public key only. We do NOT request any
	// scope — the per-site exchange authorization is attached admin-side after
	// approval.
	body := map[string]interface{}{
		"client_name":                m.config.OAuthClientName,
		"grant_types":                []string{"client_credentials"},
		"token_endpoint_auth_method": "private_key_jwt",
		"jwks":                       buildJWKS(&m.privateKey.PublicKey, m.kid),
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.OAuthRegistrationURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.config.OAuthBootstrapToken)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("registration endpoint returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var rr registrationResponse
	if err := json.Unmarshal(respBody, &rr); err != nil {
		return nil, fmt.Errorf("failed to parse registration response: %w", err)
	}
	if rr.ClientID == "" {
		return nil, fmt.Errorf("registration response missing client_id")
	}

	return &registrationState{
		ClientID:                rr.ClientID,
		RegistrationAccessToken: rr.RegistrationAccessToken,
		RegistrationClientURI:   rr.RegistrationClientURI,
	}, nil
}

// run is the steady-state fetch loop. It fetches an access token, writes it
// atomically to the token file, signals readiness on the first success, and
// schedules the next fetch. Transient failures are logged and retried; the
// process is never killed so the on-disk queue can buffer in the meantime.
func (m *oauthManager) run(ctx context.Context, ready chan struct{}) {
	first := true
	for {
		token, lifetime, err := m.fetchToken(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warningln("OAuth: failed to fetch access token, will retry:", err)
			if !sleepCtx(ctx, oauthRetryBackoff) {
				return
			}
			continue
		}

		if err := writeFileAtomic(m.config.AmqpToken, []byte(token), 0600); err != nil {
			log.Warningln("OAuth: failed to write token file, will retry:", err)
			if !sleepCtx(ctx, oauthRetryBackoff) {
				return
			}
			continue
		}

		if first {
			close(ready)
			first = false
			log.Infoln("OAuth: wrote initial access token to", m.config.AmqpToken)
		} else {
			log.Debugln("OAuth: refreshed access token at", m.config.AmqpToken)
		}

		interval := nextRefreshInterval(lifetime)
		log.Debugln("OAuth: next token refresh in", interval)
		if !sleepCtx(ctx, interval) {
			return
		}
	}
}

// fetchToken builds a client-assertion JWT and exchanges it for an access token
// via the client_credentials grant. It returns the token and its lifetime.
func (m *oauthManager) fetchToken(ctx context.Context) (string, time.Duration, error) {
	assertion, err := buildClientAssertion(m.state.ClientID, m.config.OAuthTokenURL, m.kid, m.privateKey)
	if err != nil {
		return "", 0, fmt.Errorf("failed to build client assertion: %w", err)
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_assertion_type", oauthAssertionType)
	form.Set("client_assertion", assertion)
	if len(m.config.OAuthScopes) > 0 {
		form.Set("scope", strings.Join(m.config.OAuthScopes, " "))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.OAuthTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var tr tokenResponse
	if err := json.Unmarshal(respBody, &tr); err != nil {
		return "", 0, fmt.Errorf("failed to parse token response: %w", err)
	}
	if tr.AccessToken == "" {
		return "", 0, fmt.Errorf("token response missing access_token")
	}

	lifetime := time.Duration(tr.ExpiresIn) * time.Second
	if lifetime <= 0 {
		lifetime = lifetimeFromJWT(tr.AccessToken)
	}
	return tr.AccessToken, lifetime, nil
}

// buildClientAssertion produces an RS256 JWT proving possession of the
// registered private key, in the style of cmd/createtoken/main.go.
func buildClientAssertion(clientID, tokenURL, kid string, key *rsa.PrivateKey) (string, error) {
	now := time.Now()
	claims := jwt.StandardClaims{
		Issuer:    clientID,
		Subject:   clientID,
		Audience:  tokenURL,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(oauthAssertionTTL).Unix(),
		Id:        uuid.NewString(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	return token.SignedString(key)
}

// lifetimeFromJWT extracts the remaining lifetime from an access token's exp
// claim without verifying its signature. Returns 0 if no usable exp is present.
func lifetimeFromJWT(tokenString string) time.Duration {
	parser := new(jwt.Parser)
	var claims jwt.StandardClaims
	if _, _, err := parser.ParseUnverified(tokenString, &claims); err != nil {
		return 0
	}
	if claims.ExpiresAt == 0 {
		return 0
	}
	remaining := time.Until(time.Unix(claims.ExpiresAt, 0))
	if remaining <= 0 {
		return 0
	}
	return remaining
}

// nextRefreshInterval returns when to fetch the next token: ~half of the
// token's lifetime, never below the floor, and a fallback when no expiry is
// known.
func nextRefreshInterval(lifetime time.Duration) time.Duration {
	if lifetime <= 0 {
		return oauthRefreshFallback
	}
	half := lifetime / 2
	if half < oauthRefreshFloor {
		return oauthRefreshFloor
	}
	return half
}

// buildJWKS constructs a JWKS document containing the RSA public key, including
// the generated kid.
func buildJWKS(pub *rsa.PublicKey, kid string) map[string]interface{} {
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())
	return map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"use": "sig",
				"alg": "RS256",
				"kid": kid,
				"n":   n,
				"e":   e,
			},
		},
	}
}

// savePrivateKey writes the RSA private key as a PKCS#1 PEM file with 0600
// permissions (PKCS#1 to match cmd/createtoken's ParsePKCS1PrivateKey).
func savePrivateKey(path string, key *rsa.PrivateKey) error {
	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})
	return writeFileAtomic(path, pemBytes, 0600)
}

// loadPrivateKey reads a PKCS#1 PEM private key from disk.
func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in %s", path)
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

// loadRegistrationState reads the persisted registration state.
func loadRegistrationState(path string) (*registrationState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state registrationState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.ClientID == "" {
		return nil, fmt.Errorf("registration state %s missing client_id", path)
	}
	return &state, nil
}

// saveRegistrationState writes the registration state as 0600 JSON.
func saveRegistrationState(path string, state *registrationState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0600)
}

// writeFileAtomic writes data to a temp file in the target's directory, chmods
// it, and renames it over the target. The rename bumps the mod-time the AMQP
// token watcher polls for.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".oauth-tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// fileExists reports whether a path exists (and is statable).
func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// sleepCtx waits for d or until ctx is cancelled. It returns false if the
// context was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
