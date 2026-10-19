# Shoveler Registry Protocol

This describes the contract between the shoveler's `amqp.oauth` mode (see
[oauth.go](../oauth.go)) and a shoveler registry, the service that onboards
shovelers and issues the short-lived JWTs they present to RabbitMQ. The
reference implementation is Shoveler-registry (the "Shoveler Auth Portal").

It uses standard OAuth 2.0 messages:

* registration: an [RFC 7591](https://datatracker.ietf.org/doc/html/rfc7591)-shaped request
* token issuance: the client-credentials grant ([RFC 6749 §4.4](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4))
  authenticated with `private_key_jwt` ([RFC 7523 §2.2](https://datatracker.ietf.org/doc/html/rfc7523#section-2.2))
* errors: [RFC 6749 §5.2](https://datatracker.ietf.org/doc/html/rfc6749#section-5.2) error bodies

`oauth_registry_contract_test.go` checks a live registry against this
contract using the shoveler's own client code.

## Lifecycle

```
site owner               registry                         shoveler
  |  log in, create         |                                 |
  |  registration token     |                                 |
  |------------------------>|                                 |
  |  write token to the shoveler host                         |
  |---------------------------------------------------------->|
  |                         |  POST /register (token, key)    |
  |                         |<--------------------------------|
  |                         |  201 {client_id}                |
  |                         |-------------------------------->|
  |                         |  POST /token (signed assertion) |
  |                         |<--------------------------------|
  |                         |  200 {access_token, expires_in} |
  |                         |-------------------------------->|   connects to RabbitMQ
```

**The registration token is the approval.** Only a member of the site's
COmanage group (`<COMANAGE_GROUP_PREFIX><site>`) can create one, so a client
registered with it is active immediately, bound to that site, with the
standard RabbitMQ permissions. There is no pending state.

## Registration tokens

Created by a site owner on the registry's dashboard and shown once. They are
single-use, expire after `REGISTRATION_TOKEN_TTL_SECONDS` (default 72 hours),
can be revoked from the dashboard, and are stored only as a SHA-256 hash.

The shoveler reads the token from `amqp.oauth.bootstrap_token_file` (or
`bootstrap_token`) and only needs it on the first run.

## Registration endpoint

Configured as `amqp.oauth.registration_url` (`<PORTAL_BASE_URL>/register`).

```http
POST /register
Authorization: Bearer <registration token>
Content-Type: application/json

{
  "client_name": "shoveler@xrootd.example.edu",
  "software_version": "1.4.0",
  "grant_types": ["client_credentials"],
  "token_endpoint_auth_method": "private_key_jwt",
  "jwks": {
    "keys": [{
      "kty": "RSA", "use": "sig", "alg": "RS256",
      "kid": "<RFC 7638 thumbprint>",
      "n": "<base64url modulus>", "e": "AQAB"
    }]
  }
}
```

* `client_name` is for display in the registry; it defaults to
  `shoveler@<hostname>` and the registry trims it to 100 characters.
* `software_version` is omitted when the build has no version. The registry
  ignores metadata it doesn't know.
* No `scope` is sent.
* `kid` is the [RFC 7638](https://datatracker.ietf.org/doc/html/rfc7638)
  SHA-256 thumbprint of the key. The registry stores whatever `kid` is sent and
  requires the same value in the assertion header later.

The registry requires: `token_endpoint_auth_method` = `private_key_jwt`;
`grant_types`, if present, exactly `["client_credentials"]`; no `jwks_uri`;
exactly one RSA key of at least 2048 bits with a non-empty `kid`, `use` absent
or `sig`, `alg` absent or `RS256`, and no private-key members.

**Success** (`201 Created`). The shoveler reads only `client_id`.

```json
{
  "client_id": "shoveler-3f9a1c0d2b4e6a81",
  "client_id_issued_at": 1792444800,
  "client_name": "shoveler@xrootd.example.edu",
  "grant_types": ["client_credentials"],
  "token_endpoint_auth_method": "private_key_jwt"
}
```

No `registration_access_token` is issued; RFC 7592 client management isn't
offered.

**Idempotent retry.** If the response is lost, the shoveler retries with the
same token and the same key (it saves the key before registering). The
registry returns the original client as long as the token hasn't expired or
been revoked. The same token with a different key is rejected.

**Errors:**

| Status | `error` | When | Shoveler behavior |
|---|---|---|---|
| 401 | `invalid_token` | token missing, unknown, expired, revoked, or used with a different key | logs the error and exits |
| 400 | `invalid_client_metadata` | the request fails validation (the token is not spent) | logs the error and exits |
| 429 | `invalid_request` | per-IP rate limit (`Retry-After` set) | retries every 15 s |
| 5xx or network error | | | retries every 15 s |

The shoveler treats every 4xx except 408 and 429 as permanent.

## Token endpoint

Configured as `amqp.oauth.token_url` (`<PORTAL_BASE_URL>/token`).

```http
POST /token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials
&client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer
&client_assertion=<JWT>
&scope=<space-separated, only if amqp.oauth.scopes is set>
```

The client assertion is an RS256 JWT:

| Field | Value | Registry check |
|---|---|---|
| header `kid` | the registered `kid` | must equal the stored `kid` |
| `iss`, `sub` | `client_id` | both equal a known, enabled client |
| `aud` | `token_url` exactly as configured | one of `PORTAL_BASE_URL/token`, `TOKEN_ISSUER/token` or `TOKEN_ISSUER`, compared exactly |
| `iat`, `exp` | now, now + 60 s | 60 s leeway; `exp` at most `ASSERTION_MAX_LIFETIME_SECONDS` (default 300) ahead |
| `jti` | random UUID | single use; a replay is rejected |

Only RS256 is accepted. Because `aud` is compared exactly, configure
`token_url` exactly as the registry shows it, with no trailing slash.

Any requested `scope` is ignored; the access token always carries the site's
standard permissions.

**Success** (`200`, `Cache-Control: no-store`):

```json
{ "access_token": "<JWT for RabbitMQ>", "token_type": "Bearer", "expires_in": 14400 }
```

The shoveler writes the token to `amqp.token_location` and refreshes at half
of `expires_in` (minimum 30 s; if `expires_in` is missing it reads `exp` from
the access token, falling back to 10 minutes).

**Errors:**

| Status | `error` | When | Shoveler behavior |
|---|---|---|---|
| 401 | `invalid_client` | unknown or disabled client, bad signature or `kid`, wrong `aud`, expired or too long-lived assertion, replayed `jti` (one uniform response) | logs an error once, retries every 60 s, keeps queuing |
| 429 | `invalid_client` | per-client rate limit (`Retry-After` set) | treated as rate limiting whatever the body says; retries every 15 s |
| 400 | `unsupported_grant_type` | | retries every 15 s |
| 5xx or network error | | | retries every 15 s |

The shoveler also understands `authorization_pending`, for a registry that
holds new clients for approval: it logs a warning once and polls every 60 s.
Shoveler-registry never sends it.

## Access tokens

The shoveler passes these to RabbitMQ as the AMQP password without inspecting
them. They're minted for
[`rabbitmq_auth_backend_oauth2`](https://www.rabbitmq.com/docs/oauth2):

* RS256, signed by a registry key published at
  `<TOKEN_ISSUER>/.well-known/jwks.json`, with a `kid` header;
* `iss` = `TOKEN_ISSUER`, `aud` = `RESOURCE_SERVER_ID`, `sub` = `client_id`;
* permissions in the claim named by `SCOPE_CLAIM` (RabbitMQ's
  `auth_oauth2.additional_scopes_key`), with the value `SCOPE_VALUE`;
* lifetime `TOKEN_TTL_SECONDS` (default 4 hours).

The registry also serves `/.well-known/openid-configuration`, advertising
`registration_endpoint`, `token_endpoint`, `jwks_uri`, and
`private_key_jwt` / `RS256` as the token-endpoint authentication method.

## Disabling and recovery

A client stops getting tokens (401 `invalid_client`) when it is disabled:

| Disabled by | Recovery |
|---|---|
| site owner, or idle expiry (unused for `IDLE_DAYS`) | the owner re-enables it on the dashboard; the shoveler picks it up on its next 60 s retry |
| registry admin | not re-enablable (the key may be compromised): create a new registration token, delete the shoveler's `state_file`, and restart it |

## Shoveler files

| Path (default) | Contents |
|---|---|
| `/etc/xrootd-monitoring-shoveler/oauth/bootstrap.token` | the registration token; can be deleted after the first successful start |
| `/etc/xrootd-monitoring-shoveler/oauth/private.pem` | RSA private key (PKCS#1, mode 0600); never leaves the host |
| `/etc/xrootd-monitoring-shoveler/oauth/registration.json` | `{"client_id": "..."}` (mode 0600) |
| `amqp.token_location` | current access token, rewritten atomically on each refresh |

The `oauth/` directory must survive restarts and redeploys. If the state file
exists, the shoveler skips registration. If only the key exists, it registers
again with that same key. If both are lost, create a new registration token.
