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

plus one registry-specific message, the [health report](#health-report-endpoint), a JSON POST authenticated with
the access token.

`oauth_registry_contract_test.go` checks a live registry against this
contract using the shoveler's own client code.

## Two ways to register

| | With a registration token | Without a token |
|---|---|---|
| Who acts first | a site owner creates a token on the registry's dashboard | nobody: the sysadmin just starts the shoveler |
| What the shoveler sends | `Authorization: Bearer <token>` | no `Authorization` header |
| Result | client active immediately, bound to the token's site | client **pending**, not bound to any site |
| Who approves | creating the token was the approval | a registry admin or site manager, on the dashboard |

```
with a token                                  without a token

site owner -> registry: create token          shoveler -> registry: POST /register (no token)
owner -> shoveler host: write token file      registry -> shoveler: 201 {client_id}   [pending]
shoveler -> registry: POST /register (token)  shoveler -> registry: POST /token
registry -> shoveler: 201 {client_id}         registry -> shoveler: 400 authorization_pending
shoveler -> registry: POST /token                (polls every 60 s; packets queue on disk)
registry -> shoveler: 200 {access_token}      approver -> registry: approve into a site
                                              shoveler -> registry: POST /token
                                              registry -> shoveler: 200 {access_token}
```

### Registration tokens

Created by a site owner (a member of the site's COmanage group
`<COMANAGE_GROUP_PREFIX><site>`) on the registry's dashboard and shown once.
They are single-use, expire after `REGISTRATION_TOKEN_TTL_SECONDS` (default 72
hours), can be revoked from the dashboard, and are stored only as a SHA-256
hash. The shoveler reads the token from `amqp.oauth.bootstrap_token_file` (or
`bootstrap_token`) and only needs it on the first run.

### Approving pending clients

In Shoveler-registry, registration without a token is controlled by
`ALLOW_PENDING_REGISTRATION` (default on), at most `MAX_PENDING_CLIENTS`
(default 100) may wait at once, and unapproved ones are deleted after
`PENDING_CLIENT_TTL_SECONDS` (default 7 days). With it off, a registration
without a token gets `401 invalid_token`.

The registry lists pending clients with their `client_name` (default
`shoveler@<hostname>`), `software_version`, source IP, registration time, and
key fingerprint (the RFC 7638 thumbprint, which is also the key's `kid`). The
shoveler logs the same fingerprint when it registers and every hour while it
waits, so the approver can confirm the request came from the real host.

The approver picks the site the client belongs to (one they manage, or any
site for a registry admin) and approves it. From then on it is a normal client
of that site. The shoveler's next poll, within 60 s, gets a token.

Denying a pending client deletes it. The registry also deletes pending clients
that nobody approves within a set time. Either way the
shoveler gets `invalid_client` from then on; to try again, delete its
`state_file` and restart it.

## Registration endpoint

Configured as `amqp.oauth.registration_url` (`<PORTAL_BASE_URL>/register`); defaults to the OSG registry,
`https://shoveler-auth.osg-htc.org/register`.

```http
POST /register
Authorization: Bearer <registration token>      (omitted when registering without a token)
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

**Success** (`201 Created`), for both kinds of registration. The shoveler
reads only `client_id`; the response doesn't say whether the client is
pending.

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
been revoked. The same token with a different key is rejected. A retried
registration without a token may create a second pending client; the extra
one is never approved and expires.

**A bad token is never downgraded.** If an `Authorization` header is present
but the token is unknown, expired, revoked or used, the answer is `401
invalid_token`, never a pending registration. The shoveler only omits the
header when no token is configured, and refuses to start if a configured
token file can't be read.

**Errors:**

| Status | `error` | When | Shoveler behavior |
|---|---|---|---|
| 401 | `invalid_token` | token unknown, expired, revoked, or used with a different key; or no token, on a registry that requires one | logs the error and exits |
| 400 | `invalid_client_metadata` | the request fails validation (the token is not spent) | logs the error and exits |
| 429 | `invalid_request` | per-IP rate limit (`Retry-After` set) | retries every 15 s |
| 503 | `temporarily_unavailable` | too many clients already pending approval | retries every 15 s |
| 5xx or network error | | | retries every 15 s |

The shoveler treats every 4xx except 408 and 429 as permanent.

## Token endpoint

Configured as `amqp.oauth.token_url` (`<PORTAL_BASE_URL>/token`); defaults to the OSG registry,
`https://shoveler-auth.osg-htc.org/token`.

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
| 400 | `authorization_pending` | a valid assertion from a client that hasn't been approved yet | logs a warning with the key fingerprint (then hourly), polls every 60 s, keeps queuing |
| 401 | `invalid_client` | unknown, denied or disabled client, bad signature or `kid`, wrong `aud`, expired or too long-lived assertion, replayed `jti` (one uniform response) | logs an error once, retries every 60 s, keeps queuing |
| 429 | `invalid_client` | per-client rate limit (`Retry-After` set) | treated as rate limiting whatever the body says; retries every 15 s |
| 400 | `unsupported_grant_type` | | retries every 15 s |
| 5xx or network error | | | retries every 15 s |

The registry must fully verify the assertion (signature, `kid`, `aud`,
lifetime, `jti`) before answering `authorization_pending`. Anything less gets
`invalid_client`, so only the key holder can learn that a client is pending.

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

## Health report endpoint

Configured as `amqp.oauth.report_url`, by default `token_url` with its final
`/token` replaced by `/report` (`<PORTAL_BASE_URL>/report`). Once the shoveler
has its first access token it reports right away, then every
`amqp.oauth.report_interval` seconds (default 60, from 10 to 3600), so the
registry can show whether it is running correctly. `amqp.oauth.report_health:
false` turns this off. Only the shoveler binary reports, not the collector.

```http
POST /report
Authorization: Bearer <access token from the token endpoint>
Content-Type: application/json

{
  "started_at": "2026-10-21T02:00:00Z",
  "interval_seconds": 60,
  "version": "2.4.0",
  "metrics": {
    "packets_received": 120345,
    "validations_failed": 12,
    "messages_published": 120333,
    "rabbitmq_reconnects": 0,
    "queue_size": 0
  }
}
```

* `started_at` (RFC 3339) is when the shoveler process started. Reports with the
  same value come from one process; the counters restart from zero with each
  new one, so the registry never computes a rate across a restart.
* `interval_seconds` is the configured report interval. The registry treats a
  shoveler as not reporting after 3 missed intervals (at least 3 minutes).
* `version` is omitted when the build has no version.
* `metrics` holds counters since `started_at`, and the current `queue_size`
  (messages waiting to be published, in memory or on disk). Any may be
  omitted. The registry ignores names it doesn't know, so a shoveler can send
  more.

The registry accepts only an access token it issued (its signature under a
`kid` it publishes, `iss` = `TOKEN_ISSUER`, `aud` = `RESOURCE_SERVER_ID`,
unexpired) whose `sub` is an approved, enabled client. A pending client has no
token, so it can't report.

**Success:** `204 No Content`.

**Errors:**

| Status | `error` | When | Shoveler behavior |
|---|---|---|---|
| 401 | `invalid_token` | missing, foreign, expired or malformed token, or a disabled or unknown client (one uniform response) | logs a warning once, keeps reporting every interval |
| 400 | `invalid_request` | malformed report; `error_description` says what | logs a warning once, keeps reporting |
| 429 | `invalid_request` | per-client rate limit (`REPORT_RATE_LIMIT` per `REPORT_RATE_WINDOW`, default 10 per 60 s; `Retry-After` set) | next report at the normal interval |
| 404 | | a registry without health reports | logs a warning once, asks again hourly |
| 5xx or network error | | | logs a warning once, keeps reporting |

A failure is logged again only when it changes kind, and recovery is logged
at info level. Reporting never affects shoveling.

The registry keeps reports for `METRICS_RETENTION_HOURS` (default 24), always
keeping each client's latest so it can say when a shoveler was last heard
from, and shows each shoveler as healthy, not reporting, receiving no packets
(none in the last 15 minutes), not publishing (packets received but none
published in the last 15 minutes), or never reported.

## Disabling and recovery

A client stops getting tokens (401 `invalid_client`) when it is disabled:

| Disabled by | Recovery |
|---|---|
| site owner, or idle expiry (unused for `IDLE_DAYS`) | the owner re-enables it on the dashboard; the shoveler picks it up on its next 60 s retry |
| registry admin | not re-enablable (the key may be compromised): delete the shoveler's `state_file` and restart it, with a new registration token or to wait for approval |

## Shoveler files

| Path (default) | Contents |
|---|---|
| `/var/lib/xrootd-monitoring-shoveler/bootstrap.token` (no default; set `bootstrap_token_file`) | the registration token, if one is used; must be readable by the service user; can be deleted after the first successful start |
| `/var/lib/xrootd-monitoring-shoveler/oauth/private.pem` | RSA private key (PKCS#1, mode 0600); never leaves the host |
| `/var/lib/xrootd-monitoring-shoveler/oauth/registration.json` | `{"client_id": "..."}` (mode 0600) |
| `/var/lib/xrootd-monitoring-shoveler/token` (`amqp.token_location`) | current access token, rewritten atomically on each refresh |

These all live in the service's state directory, `/var/lib/xrootd-monitoring-shoveler/`,
which the packages create for the `xrootd-monitoring-shoveler` user. It must
survive restarts and redeploys. If the state file
exists, the shoveler skips registration. If only the key exists, it registers
again with that same key. If both are lost, it registers as a new client:
with a new registration token, or without one and waits for approval again.
