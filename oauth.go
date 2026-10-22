package shoveler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
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

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// The wire protocol spoken here is documented in docs/registry-protocol.md.

const (
	// Lifetime of the short-lived client-assertion JWT presented at the token
	// endpoint.
	oauthAssertionTTL = 60 * time.Second

	// Lower bound on how soon we re-fetch an access token.
	oauthRefreshFloor = 30 * time.Second

	// Shortest token lifetime that rotates cleanly: refreshed at half its
	// lifetime and picked up by the AMQP token watcher within 10 s, it is
	// replaced well before it expires. Shorter tokens are used, with a
	// warning, but connections may briefly hold an expired one.
	oauthMinTokenLifetime = 2 * oauthRefreshFloor

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

	// After the registry rejects a registration (a bad or used token, or bad
	// metadata): how often to re-read the registration token and try again.
	oauthRejectedInterval = 10 * time.Minute
)

// registrationState is the persisted result of registration: everything the
// shoveler needs to authenticate on later runs without the bootstrap token.
type registrationState struct {
	ClientID string `json:"client_id"`
}

// pendingRegistration marks a registration in progress. A retry after a crash
// with the same registration token reuses the key, so the registry can return
// the same client; any other registration gets a new key.
type pendingRegistration struct {
	TokenSHA256 string `json:"token_sha256"`
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

// rejected reports whether the registry itself refused the request: a
// permanent 4xx carrying an OAuth error body. A bare 4xx (e.g. a proxy's 404
// page while the registry is redeployed) is treated as transient.
func (e *endpointError) rejected() bool {
	return e.Code != "" && e.permanent()
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
	config         *Config
	privateKey     *rsa.PrivateKey
	kid            string
	clientID       string             // empty until registered
	resumed        bool               // registered on an earlier run
	warnedLifetime time.Duration      // last too-short token lifetime warned about
	log            logrus.FieldLogger // nil means the package logger (tests set it)
	bootstrapToken string             // the registration token, read when registering
	httpClient     *http.Client
}

// StartAMQPWithOAuth starts the OAuth manager and launches StartAMQP once the
// first access token is on disk. It returns immediately so packet intake can
// start; messages buffer in the on-disk queue until the registry issues a
// token, for example while a new registration awaits approval. The returned
// channel is closed once that first token is on disk.
func StartAMQPWithOAuth(config *Config, queue *ConfirmationQueue) <-chan struct{} {
	ready, err := StartOAuthManager(context.Background(), config)
	if err != nil {
		log.Fatalln("Failed to start OAuth manager:", err)
	}
	go func() {
		<-ready
		log.Infoln("OAuth: initial access token ready, starting AMQP")
		StartAMQP(config, queue)
	}()
	return ready
}

// StartOAuthManager loads (or creates) the shoveler's key and registration
// state, then launches the registration + token fetch loop in a goroutine. The
// returned channel is closed once the first access token has been written to
// config.AmqpToken. Errors returned here are local (unreadable key, state or
// registration token); registry rejections are logged and retried.
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

	registered, err := pathExists(config.OAuthStateFile)
	if err != nil {
		// Not "absent": re-registering would replace a working client.
		return nil, fmt.Errorf("can't check OAuth state file %s: %w", config.OAuthStateFile, err)
	}
	if !registered {
		if err := m.prepareRegistration(); err != nil {
			return nil, err
		}
		return m, nil
	}
	state, err := loadRegistrationState(config.OAuthStateFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load OAuth state file %s: %w", config.OAuthStateFile, err)
	}
	key, err := loadPrivateKey(config.OAuthPrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load OAuth private key %s: %w", config.OAuthPrivateKeyFile, err)
	}
	m.clientID = state.ClientID
	m.resumed = true
	m.privateKey = key
	m.kid = jwkThumbprint(&key.PublicKey)
	log.Infoln("OAuth: resuming from persisted registration, client_id:", state.ClientID)
	return m, nil
}

// prepareRegistration reads the registration token and picks the key to
// register: the existing key only when retrying an interrupted registration
// with the same token, otherwise a new one. A key whose client was revoked
// (perhaps because it leaked) is therefore never registered again.
func (m *oauthManager) prepareRegistration() error {
	token, err := m.readBootstrapToken()
	if err != nil {
		return err
	}
	m.bootstrapToken = token
	digest := tokenDigest(token)

	m.privateKey = nil
	if pending, err := loadPendingRegistration(m.pendingFile()); err == nil && pending.TokenSHA256 == digest {
		if key, err := loadPrivateKey(m.config.OAuthPrivateKeyFile); err == nil {
			m.privateKey = key
			log.Infoln("OAuth: retrying an interrupted registration with the key at", m.config.OAuthPrivateKeyFile)
		}
	}
	if m.privateKey == nil {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return fmt.Errorf("failed to generate RSA key: %w", err)
		}
		if err := savePrivateKey(m.config.OAuthPrivateKeyFile, key); err != nil {
			return fmt.Errorf("failed to persist private key: %w", err)
		}
		m.privateKey = key
		log.Infoln("OAuth: generated new private key at", m.config.OAuthPrivateKeyFile)
	}
	m.kid = jwkThumbprint(&m.privateKey.PublicKey)
	if err := savePendingRegistration(m.pendingFile(), digest); err != nil {
		return fmt.Errorf("failed to persist %s: %w", m.pendingFile(), err)
	}
	if token == "" {
		log.Warningln("OAuth: no registration token configured; the shoveler will register and wait for a registry admin to approve it")
	}
	return nil
}

// readBootstrapToken returns the registration token from bootstrap_token_file
// (which wins) or bootstrap_token; "" means register without one. A configured
// file that is unreadable or empty is an error, never a silent switch to
// waiting for approval.
func (m *oauthManager) readBootstrapToken() (string, error) {
	path := m.config.OAuthBootstrapTokenFile
	if path == "" {
		return m.config.OAuthBootstrapToken, nil
	}
	const fix = "fix it, or remove bootstrap_token_file to register without a token and wait for approval"
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("can't read bootstrap_token_file %s (%s): %w", path, fix, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("bootstrap_token_file %s is empty (%s)", path, fix)
	}
	return token, nil
}

// pendingFile marks an in-progress registration; see pendingRegistration.
func (m *oauthManager) pendingFile() string {
	return m.config.OAuthStateFile + ".pending"
}

// tokenDigest identifies a registration token without storing it.
func tokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
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
	// After a restart, a token from the previous run that is still valid
	// lets AMQP start at once, even if the registry is unreachable.
	if m.resumed {
		if remaining := m.tokenOnDiskLifetime(); remaining > 2*oauthRefreshFloor {
			close(ready)
			first = false
			log.Infoln("OAuth: using the access token on disk, valid for", remaining.Round(time.Second))
			if !sleepCtx(ctx, nextRefreshInterval(remaining)) {
				return nil
			}
		}
	}
	lastProblem := ""             // error code of the current failure streak, to avoid log spam
	var lastPendingWarn time.Time // when the "awaiting approval" warning was last logged
	for {
		fetchedAt := time.Now()
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
		m.warnShortLifetime(lifetime)

		expiry := time.Now().Add(lifetime)
		if lifetime <= 0 {
			expiry = time.Now().Add(oauthRefreshFallback)
		}
		if !m.writeToken(ctx, token, expiry) {
			if ctx.Err() != nil {
				return nil
			}
			continue // the token expired before it could be written; fetch another
		}

		if first {
			close(ready)
			first = false
			log.Infoln("OAuth: wrote initial access token to", m.config.AmqpToken)
		} else {
			log.Debugln("OAuth: refreshed access token at", m.config.AmqpToken)
		}

		// Refresh on the schedule set when the token was fetched, so time
		// spent retrying the write counts against it.
		interval := refreshDelay(fetchedAt, lifetime, time.Now())
		log.Debugln("OAuth: next token refresh in", interval)
		if !sleepCtx(ctx, interval) {
			return nil
		}
	}
}

// warnShortLifetime warns, once per lifetime value, when tokens are too
// short-lived to rotate connections before they expire.
func (m *oauthManager) warnShortLifetime(lifetime time.Duration) {
	if lifetime <= 0 || lifetime >= oauthMinTokenLifetime || lifetime == m.warnedLifetime {
		return
	}
	m.warnedLifetime = lifetime
	logger := m.log
	if logger == nil {
		logger = log
	}
	logger.Warningln("OAuth: the registry issues tokens valid for only", lifetime.String()+",",
		"less than the", oauthMinTokenLifetime.String(), "needed to rotate connections in time;",
		"connections may briefly use an expired token on each rotation. Raise TOKEN_TTL_SECONDS on the registry.")
}

// refreshDelay is how long to wait at now before refreshing a token fetched
// at fetchedAt with the given lifetime; 0 if that refresh is already due.
func refreshDelay(fetchedAt time.Time, lifetime time.Duration, now time.Time) time.Duration {
	delay := fetchedAt.Add(nextRefreshInterval(lifetime)).Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

// writeToken writes the access token to the token file. When the write fails
// it retries only the write, not the fetch, until the token is about to
// expire. It reports whether the token was written.
func (m *oauthManager) writeToken(ctx context.Context, token string, expiry time.Time) bool {
	logged := false
	for {
		err := writeFileAtomic(m.config.AmqpToken, []byte(token), 0600)
		if err == nil {
			return true
		}
		if !logged {
			log.Errorln("OAuth: can't write the access token (the shoveler must be able to write",
				"the token_location directory), will retry:", err)
			logged = true
		}
		if time.Until(expiry) < 2*oauthRetryBackoff || !sleepCtx(ctx, oauthRetryBackoff) {
			return false
		}
	}
}

// tokenOnDiskLifetime returns how long the access token in the token file
// remains valid, or 0 if there is none or it wasn't issued to this client.
func (m *oauthManager) tokenOnDiskLifetime() time.Duration {
	data, err := os.ReadFile(m.config.AmqpToken)
	if err != nil {
		return 0
	}
	var claims jwt.RegisteredClaims
	if _, _, err := new(jwt.Parser).ParseUnverified(strings.TrimSpace(string(data)), &claims); err != nil {
		return 0
	}
	if claims.Subject != m.clientID || claims.ExpiresAt == nil {
		return 0
	}
	return time.Until(claims.ExpiresAt.Time)
}

// registerWithRetry registers with the registry and persists the resulting
// client_id. Transient failures are retried. When the registry rejects the
// registration (e.g. an expired or used token), retrying the same request
// can't help and exiting would only restart-loop, so it logs the rejection and
// re-reads the registration token every oauthRejectedInterval, so a new token
// takes effect without a restart. It returns an error only if the state
// can't be saved.
func (m *oauthManager) registerWithRetry(ctx context.Context) error {
	lastRejection := ""
	reprepare := false
	for {
		if reprepare {
			if err := m.prepareRegistration(); err != nil {
				log.Errorln("OAuth:", err)
				if !sleepCtx(ctx, oauthRejectedInterval) {
					return nil
				}
				continue
			}
			reprepare = false
		}
		clientID, err := m.doRegister(ctx)
		if err == nil {
			state := &registrationState{ClientID: clientID}
			if err := saveRegistrationState(m.config.OAuthStateFile, state); err != nil {
				// The registry has our key and the pending marker remains,
				// so a restart retries with the same key and token.
				return fmt.Errorf("registered as client_id %s but failed to persist %s: %w",
					clientID, m.config.OAuthStateFile, err)
			}
			_ = os.Remove(m.pendingFile())
			m.clientID = clientID
			if m.bootstrapToken == "" {
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
		if errors.As(err, &ee) && ee.rejected() {
			if msg := err.Error(); msg != lastRejection {
				hint := "supply a new registration token"
				if m.bootstrapToken == "" {
					hint = "this registry may require a registration token; supply one"
				}
				log.Errorln("OAuth: the registry rejected the registration:", err, "-", hint,
					"(the shoveler re-reads it every", oauthRejectedInterval.String()+"); check registration_url too")
				lastRejection = msg
			}
			reprepare = true
			if !sleepCtx(ctx, oauthRejectedInterval) {
				return nil
			}
			continue
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
	if m.bootstrapToken != "" {
		req.Header.Set("Authorization", "Bearer "+m.bootstrapToken)
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
	// MapClaims keeps aud a single string, as docs/registry-protocol.md
	// specifies (RegisteredClaims would encode it as an array).
	claims := jwt.MapClaims{
		"iss": clientID,
		"sub": clientID,
		"aud": tokenURL,
		"iat": now.Unix(),
		"exp": now.Add(oauthAssertionTTL).Unix(),
		"jti": uuid.NewString(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	return token.SignedString(key)
}

// lifetimeFromJWT extracts the remaining lifetime from an access token's exp
// claim without verifying its signature. Returns 0 if no usable exp is present.
func lifetimeFromJWT(tokenString string) time.Duration {
	parser := new(jwt.Parser)
	// RegisteredClaims accepts an array aud and a fractional exp.
	var claims jwt.RegisteredClaims
	if _, _, err := parser.ParseUnverified(tokenString, &claims); err != nil {
		return 0
	}
	if claims.ExpiresAt == nil {
		return 0
	}
	remaining := time.Until(claims.ExpiresAt.Time)
	if remaining <= 0 {
		return 0
	}
	return remaining
}

// nextRefreshInterval returns when to fetch the next token: half of the
// token's lifetime, and a fallback when no expiry is known.
func nextRefreshInterval(lifetime time.Duration) time.Duration {
	if lifetime <= 0 {
		return oauthRefreshFallback
	}
	// Half the lifetime: at least oauthRefreshFloor for any token of
	// oauthMinTokenLifetime or more. A shorter token is still refreshed at
	// half-life (not after it expires), but no more than once a second.
	half := lifetime / 2
	if half < time.Second {
		return time.Second
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

// pathExists reports whether path exists. Errors other than "not found"
// (e.g. permission denied) are returned rather than read as "absent".
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

// loadPendingRegistration reads the in-progress registration marker.
func loadPendingRegistration(path string) (*pendingRegistration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p pendingRegistration
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// savePendingRegistration writes the in-progress registration marker.
func savePendingRegistration(path, digest string) error {
	data, err := json.Marshal(pendingRegistration{TokenSHA256: digest})
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0600)
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
