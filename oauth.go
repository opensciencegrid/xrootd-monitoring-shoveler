package shoveler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
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

// The wire protocol spoken here is documented in docs/registry-protocol.md.

const (
	// Lifetime of the short-lived client-assertion JWT presented at the token
	// endpoint.
	oauthAssertionTTL = 60 * time.Second

	// Lower bound on how soon we re-fetch an access token.
	oauthRefreshFloor = 30 * time.Second

	// Used when the token response carries no usable expiry information.
	oauthRefreshFallback = 10 * time.Minute

	// client_assertion_type for private_key_jwt client authentication.
	oauthAssertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

	// OAuth error codes the token endpoint uses to signal registration status.
	oauthErrAuthorizationPending = "authorization_pending"
	oauthErrInvalidClient        = "invalid_client"
)

// Delays between retries. They are vars so tests can shorten them.
var (
	// After a transient failure (network error, 5xx, token-file write).
	oauthRetryBackoff = 15 * time.Second

	// While the registration awaits approval, or the registry rejects the
	// client (disabled or revoked).
	oauthPendingInterval = 60 * time.Second

	// How often the "awaiting approval" warning is repeated, so it stays
	// visible in logs at the default (warning) level.
	oauthPendingReminder = time.Hour
)

// registrationState is the persisted result of registration: everything the
// shoveler needs to authenticate on later runs without the bootstrap token.
type registrationState struct {
	ClientID string `json:"client_id"`
}

// registrationResponse is the registry's reply to a registration request.
type registrationResponse struct {
	ClientID string `json:"client_id"`
}

// tokenResponse captures the fields we care about from the token endpoint.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// oauthErrorResponse is an RFC 6749 section 5.2 error body.
type oauthErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// endpointError is a non-2xx reply from the registration or token endpoint.
type endpointError struct {
	Status      int
	Code        string // OAuth "error" field, if the body carried one
	Description string // OAuth "error_description", or the raw body otherwise
}

func (e *endpointError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("status %d: %s: %s", e.Status, e.Code, e.Description)
	}
	return fmt.Sprintf("status %d: %s", e.Status, e.Description)
}

// permanent reports whether retrying the same request cannot succeed: a 4xx
// other than timeout / rate limiting.
func (e *endpointError) permanent() bool {
	return e.Status >= 400 && e.Status < 500 &&
		e.Status != http.StatusRequestTimeout && e.Status != http.StatusTooManyRequests
}

// newEndpointError builds an endpointError from a non-2xx response body.
func newEndpointError(status int, body []byte) *endpointError {
	e := &endpointError{Status: status}
	var oe oauthErrorResponse
	if json.Unmarshal(body, &oe) == nil && oe.Error != "" {
		e.Code = oe.Error
		e.Description = oe.ErrorDescription
	} else {
		e.Description = strings.TrimSpace(string(body))
	}
	return e
}

// oauthManager keeps the token file populated with a fresh access token fetched
// via private_key_jwt. It deliberately writes to config.AmqpToken so the
// existing file-watch/restart machinery in amqp.go handles connection rotation.
type oauthManager struct {
	config     *Config
	privateKey *rsa.PrivateKey
	kid        string
	clientID   string // empty until registered
	httpClient *http.Client
}

// StartAMQPWithOAuth starts the OAuth manager and launches StartAMQP once the
// first access token is on disk. It returns immediately so packet intake can
// start; messages buffer in the on-disk queue until the registry issues a
// token, for example while a new registration awaits approval.
func StartAMQPWithOAuth(config *Config, queue *ConfirmationQueue) {
	ready, err := StartOAuthManager(context.Background(), config)
	if err != nil {
		log.Fatalln("Failed to start OAuth manager:", err)
	}
	go func() {
		<-ready
		log.Infoln("OAuth: initial access token ready, starting AMQP")
		StartAMQP(config, queue)
	}()
}

// StartOAuthManager loads (or creates) the shoveler's key and registration
// state, then launches the registration + token fetch loop in a goroutine. The
// returned channel is closed once the first access token has been written to
// config.AmqpToken. Errors returned here are local (unreadable key or state);
// a registry that permanently rejects the registration is fatal.
func StartOAuthManager(ctx context.Context, config *Config) (<-chan struct{}, error) {
	m, err := newOAuthManager(config)
	if err != nil {
		return nil, err
	}
	ready := make(chan struct{})
	go func() {
		if err := m.run(ctx, ready); err != nil {
			log.Fatalln("OAuth:", err)
		}
	}()
	return ready, nil
}

// newOAuthManager loads the persisted registration and key, or on first run
// loads-or-generates the key that registration will use.
func newOAuthManager(config *Config) (*oauthManager, error) {
	m := &oauthManager{
		config:     config,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}

	if fileExists(config.OAuthStateFile) {
		state, err := loadRegistrationState(config.OAuthStateFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load OAuth state file %s: %w", config.OAuthStateFile, err)
		}
		key, err := loadPrivateKey(config.OAuthPrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load OAuth private key %s: %w", config.OAuthPrivateKeyFile, err)
		}
		m.clientID = state.ClientID
		m.privateKey = key
		log.Infoln("OAuth: resuming from persisted registration, client_id:", state.ClientID)
	} else {
		// Reuse an existing key so a registration retried after a crash
		// presents the same public key (and the registry can treat it as
		// idempotent).
		if fileExists(config.OAuthPrivateKeyFile) {
			key, err := loadPrivateKey(config.OAuthPrivateKeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load existing private key: %w", err)
			}
			m.privateKey = key
			log.Infoln("OAuth: reusing existing private key at", config.OAuthPrivateKeyFile)
		} else {
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				return nil, fmt.Errorf("failed to generate RSA key: %w", err)
			}
			if err := savePrivateKey(config.OAuthPrivateKeyFile, key); err != nil {
				return nil, fmt.Errorf("failed to persist private key: %w", err)
			}
			m.privateKey = key
			log.Infoln("OAuth: generated new private key at", config.OAuthPrivateKeyFile)
		}
	}
	m.kid = jwkThumbprint(&m.privateKey.PublicKey)
	return m, nil
}

// run registers (if needed) and then runs the steady-state fetch loop: fetch
// an access token, write it atomically to the token file, signal readiness on
// the first success, and schedule the next fetch. Transient failures are
// retried so the on-disk queue can buffer in the meantime. It returns an error
// only when the registry permanently rejects the registration.
func (m *oauthManager) run(ctx context.Context, ready chan struct{}) error {
	if m.clientID == "" {
		if err := m.registerWithRetry(ctx); err != nil {
			return err
		}
		if m.clientID == "" { // cancelled before registration succeeded
			return nil
		}
	}

	first := true
	lastProblem := ""             // error code of the current failure streak, to avoid log spam
	var lastPendingWarn time.Time // when the "awaiting approval" warning was last logged
	for {
		token, lifetime, err := m.fetchToken(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ee *endpointError
			code := ""
			// A 429 is rate limiting whatever its error code says.
			if errors.As(err, &ee) && ee.Status != http.StatusTooManyRequests {
				code = ee.Code
			}
			wait := oauthRetryBackoff
			switch code {
			case oauthErrAuthorizationPending:
				if lastProblem != code || time.Since(lastPendingWarn) >= oauthPendingReminder {
					log.Warningln("OAuth: client_id", m.clientID, "is awaiting approval in the registry;",
						"approve the client whose key fingerprint is", m.kid, "- messages are queued until then")
					lastPendingWarn = time.Now()
				} else {
					log.Debugln("OAuth: still awaiting approval for client_id", m.clientID)
				}
				wait = oauthPendingInterval
			case oauthErrInvalidClient:
				if lastProblem != code {
					log.Errorln("OAuth: the registry does not accept client_id", m.clientID,
						"(disabled, revoked or unknown):", err, "- if it was disabled, re-enable it in the registry;",
						"if it was revoked, remove", m.config.OAuthStateFile, "and supply a new registration token")
				} else {
					log.Debugln("OAuth: registry still rejects client_id", m.clientID)
				}
				wait = oauthPendingInterval
			default:
				log.Warningln("OAuth: failed to fetch access token, will retry:", err)
			}
			lastProblem = code
			if !sleepCtx(ctx, wait) {
				return nil
			}
			continue
		}
		lastProblem = ""

		if err := writeFileAtomic(m.config.AmqpToken, []byte(token), 0600); err != nil {
			log.Warningln("OAuth: failed to write token file, will retry:", err)
			if !sleepCtx(ctx, oauthRetryBackoff) {
				return nil
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
			return nil
		}
	}
}

// registerWithRetry registers with the registry, retrying transient failures,
// and persists the resulting client_id.
func (m *oauthManager) registerWithRetry(ctx context.Context) error {
	for {
		clientID, err := m.doRegister(ctx)
		if err == nil {
			state := &registrationState{ClientID: clientID}
			if err := saveRegistrationState(m.config.OAuthStateFile, state); err != nil {
				// The registry has our key; a restart re-registers with the
				// same key and registration token.
				return fmt.Errorf("registered as client_id %s but failed to persist %s: %w",
					clientID, m.config.OAuthStateFile, err)
			}
			m.clientID = clientID
			if m.config.OAuthBootstrapToken == "" {
				log.Warningln("OAuth: registered without a registration token as client_id", clientID,
					"with key fingerprint", m.kid, "- a registry admin must approve it")
			} else {
				log.Infoln("OAuth: registration complete, client_id:", clientID)
			}
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		var ee *endpointError
		if errors.As(err, &ee) && ee.permanent() {
			if m.config.OAuthBootstrapToken == "" {
				return fmt.Errorf("registry rejected registration without a registration token (it may require one; check registration_url): %w", err)
			}
			return fmt.Errorf("registry rejected registration (check the registration token and registration_url): %w", err)
		}
		log.Warningln("OAuth: registration failed, will retry:", err)
		if !sleepCtx(ctx, oauthRetryBackoff) {
			return nil
		}
	}
}

// doRegister performs the HTTP registration call and returns the client_id.
func (m *oauthManager) doRegister(ctx context.Context) (string, error) {
	// Register identity + public key only. No scope is requested: the site's
	// exchange permissions are granted registry-side.
	body := map[string]interface{}{
		"client_name":                m.config.OAuthClientName,
		"grant_types":                []string{"client_credentials"},
		"token_endpoint_auth_method": "private_key_jwt",
		"jwks":                       buildJWKS(&m.privateKey.PublicKey, m.kid),
	}
	if ShovelerVersion != "" {
		body["software_version"] = ShovelerVersion
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.OAuthRegistrationURL, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	// Without a token the registration is open and the client waits for
	// approval in the registry.
	if m.config.OAuthBootstrapToken != "" {
		req.Header.Set("Authorization", "Bearer "+m.config.OAuthBootstrapToken)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", newEndpointError(resp.StatusCode, respBody)
	}

	var rr registrationResponse
	if err := json.Unmarshal(respBody, &rr); err != nil {
		return "", fmt.Errorf("failed to parse registration response: %w", err)
	}
	if rr.ClientID == "" {
		return "", fmt.Errorf("registration response missing client_id")
	}
	return rr.ClientID, nil
}

// fetchToken builds a client-assertion JWT and exchanges it for an access token
// via the client_credentials grant. It returns the token and its lifetime.
func (m *oauthManager) fetchToken(ctx context.Context) (string, time.Duration, error) {
	assertion, err := buildClientAssertion(m.clientID, m.config.OAuthTokenURL, m.kid, m.privateKey)
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
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, newEndpointError(resp.StatusCode, respBody)
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

// rsaJWKParams returns the base64url-encoded modulus and exponent of pub.
func rsaJWKParams(pub *rsa.PublicKey) (n, e string) {
	n = base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e = base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())
	return n, e
}

// jwkThumbprint returns the RFC 7638 SHA-256 thumbprint of an RSA public key.
// It is used as the key's kid, so the registry can recompute it from the
// registered JWK.
func jwkThumbprint(pub *rsa.PublicKey) string {
	n, e := rsaJWKParams(pub)
	return rsaThumbprint(n, e)
}

// rsaThumbprint hashes the RFC 7638 canonical form of an RSA JWK: the required
// members only, in lexicographic order, with no whitespace.
func rsaThumbprint(n, e string) string {
	canonical := `{"e":"` + e + `","kty":"RSA","n":"` + n + `"}`
	sum := sha256.Sum256([]byte(canonical))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// buildJWKS constructs a JWKS document containing the RSA public key.
func buildJWKS(pub *rsa.PublicKey, kid string) map[string]interface{} {
	n, e := rsaJWKParams(pub)
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
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
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
