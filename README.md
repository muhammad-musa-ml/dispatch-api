# Dispatch

Webhook delivery in Go and PostgreSQL.

Dispatch stores an event and its delivery job in one transaction, then sends a signed webhook. Failed requests are retried. If a worker crashes, another worker can pick up the job after its lease expires. You can query an event's status and attempt history through the API.

Delivery is at least once with a fixed retry budget. Receivers need to deduplicate by event ID.

## Run

Requires Docker Compose and Python 3.10+ for the demo scripts.

```sh
docker compose up --build -d
python scripts/demo.py
python scripts/demo.py --crash
```

The demo receiver verifies signatures, fails the first two attempts with 503, and accepts the third. `--crash` kills and restarts the API after it accepts an event, then checks delivery, duplicate submission, key conflicts, and tenant isolation. Lease recovery can take 15 seconds.

The API binds to localhost:8080. The database and receiver have no published ports. The Compose credentials are for local use. `docker compose down` stops the services and keeps the database volume.

## Features

- Tenant-scoped bearer keys and idempotency keys.
- Atomic event and outbox writes in PostgreSQL.
- Four workers using `FOR UPDATE SKIP LOCKED`, expiring leases, and attempt numbers to reject stale completions.
- HMAC-SHA256 signatures, retry backoff with jitter, and `Retry-After` support.
- Five delivery attempts before a job becomes a dead letter.
- OpenAPI 3.1 contract, strict request validation, and RFC 9457 errors.
- PostgreSQL integration tests, race detection, and non-root Docker images.

## API

| Method | Path | Behavior |
|---|---|---|
| POST | `/v1/events` | 201 after event and outbox commit; 200 on same-body replay; 409 on conflicting key reuse |
| GET | `/v1/events/{id}` | Tenant-scoped event and current delivery status |
| GET | `/v1/events/{id}/attempts` | Tenant-scoped attempt history, ascending attempt number |
| GET | `/healthz` | Process liveness |
| GET | `/readyz` | Database connectivity |

```sh
curl -i http://localhost:8080/v1/events \
  -H 'Authorization: Bearer local-demo-key-alpha-000001' \
  -H 'Idempotency-Key: note-001' \
  -H 'Content-Type: application/json' \
  -d '{"type":"note.created","payload":{"note_id":"synthetic-001"}}'
```

Poll the returned `Location` for delivery status. The same key and body bytes return the same event ID and its current status. Changing whitespace also changes the request hash and returns 409. Keys remain valid for the lifetime of the event. A 201 means the event was committed, not that the receiver has accepted it.

See the [API spec](api/openapi.json) and [design notes](docs/design.md).

## Tests

Integration tests use a separate PostgreSQL service:

```sh
docker compose run --rm test
docker compose run --rm --no-deps test go vet ./...
docker compose up -d api
python scripts/benchmark.py --requests 200 --concurrency 16
```

The test command enables `-race` and requires a database connection. For local Go runs, set `TEST_DATABASE_URL` to a separate database without active workers. See [test results and benchmark details](docs/evidence.md).

## Architecture

```mermaid
flowchart LR
  Producer -->|POST + idempotency key| API[Go API]
  API -->|one transaction| PG[(PostgreSQL events + outbox)]
  PG -->|short lease claim| Workers[4 Go workers]
  Workers -->|signed POST| Receiver[Receiver]
  Workers -->|fenced result| PG
  Operator -->|GET event / attempts| API
```

PostgreSQL handles both storage and the queue. Workers release row locks before making HTTP requests. All instances must use the same target configuration.

## Limits

This currently runs locally. Targets are set by the operator, one per tenant. Clients cannot supply destination URLs. Changing a target changes where pending jobs are sent. HTTPS is required unless `ALLOW_HTTP_TARGETS=1` is set for the local demo.

There are no tenant rate limits, queue limits, key rotation, or automatic dead-letter replay yet. Delivery order is not guaranteed. The demo receiver deduplicates in memory, so that state is lost on restart. Keep the demo on a trusted local network.

Next steps are in the [roadmap](docs/roadmap.md), including a Kiln integration and tenant admission control.

MIT licensed.
