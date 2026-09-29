# Design notes

## Resources and compatibility

An event is immutable except for `status`: `accepted -> delivered | dead_letter`. IDs are 128 random bits encoded as lowercase hex. Unknown request fields are rejected. Clients should ignore new response fields; changing required request fields or idempotency semantics needs a new API version.

The bearer key determines the tenant. Every resource query includes that tenant, and foreign or missing IDs both return 404. Keys are compared using constant-time comparison of their SHA-256 digests. Tenant isolation is enforced in the queries, without database row-level security.

`POST` returns 201 after commit. If the response is lost, the client can retry with the same key and body. A unique constraint on `(tenant_id, idempotency_key)` handles concurrent requests. The INSERT and SELECT use separate READ COMMITTED snapshots: a single CTE can detect a uniqueness conflict but fail to see the competing row.

A replay returns the same event ID with its current status and `Idempotency-Replayed: true`. The hash covers the raw request bytes, including whitespace. Payloads are stored as JSONB; unsupported values such as a null code point or an out-of-range number return 422. The webhook body is serialized from the stored event.

Errors use [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457.html) with a `code` field. Database failures return 503 without exposing database diagnostics or payloads.

## Transactional outbox

The event and delivery row are inserted in one transaction. Either both commit or neither does. Keeping the queue in PostgreSQL avoids a separate database/broker write that could fail halfway through.

Each worker claims a due row with [SKIP LOCKED](https://www.postgresql.org/docs/17/sql-select.html), increments the attempt number, and sets a 15-second lease. It commits before making the HTTP request. The client timeout is five seconds and the response-body drain is limited to 4 KiB. Each process runs four workers.

Completion checks the attempt number before updating the job. If another worker has reclaimed it, the old completion is ignored. Event status and attempt outcome are updated together. If that write fails, the lease eventually expires and the job may be sent again.

## Delivery semantics

2xx succeeds. Transport failures, 408, 429, and 5xx retry; other 3xx/4xx responses are terminal. Redirects are disabled. Backoff uses equal jitter with a 30-second cap. `Retry-After` supports seconds and HTTP dates, capped at 60 seconds.

The send budget is five attempts. If the last lease expires, a sixth claim marks the job as a dead letter without sending it. The attempt history has at most six records.

If a receiver commits its side effect and the worker crashes before recording success, the next attempt can repeat that side effect. Receivers must store the event ID and the side effect atomically to deduplicate safely. The demo receiver only keeps an in-memory set, which resets on restart. Failed jobs can exhaust the retry budget without ever reaching the receiver.

Each request includes `Dispatch-Event-ID`, `Dispatch-Attempt`, and `Dispatch-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256>`. The signature covers `<timestamp>.<raw HTTP body>`. The receiver checks it in constant time and rejects timestamps outside five minutes. This is a custom protocol, informed by [Stripe's webhook documentation](https://docs.stripe.com/webhooks).

## Operations

Request size, header size, database time, HTTP time, and worker count are bounded. Queue length, tenant request rate, and retained data are not. Keep this on a trusted local network until admission control is added.

Only the operator can set destination URLs. HTTPS is required outside local demo mode. Environment proxies and redirects are disabled. Self-service endpoints still need DNS rebinding protection, connected-IP checks, private-range restrictions, and tenant quotas.

Readiness checks the database connection, not the schema or receivers. The initial migration runs only when Docker creates a new database volume. Later schema changes need a migration runner.

SIGTERM drains HTTP requests, cancels workers, and waits for them before closing the pool. SIGKILL leaves leases for recovery.
