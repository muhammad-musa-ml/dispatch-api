# Test results

Recorded 2026-09-29 using the local Docker setup. Raw results are in [evidence/](evidence/).

## Checks

| Check | Result | Reproduction |
|---|---|---|
| Go tests with real PostgreSQL and race detector | 10 top-level tests; 28 passing test/subtest results; zero failures and zero skipped tests | `docker compose run --rm test` |
| Static checks | `go vet ./...` passed | `docker compose run --rm --no-deps test go vet ./...` |
| Dependency vulnerability scan | No vulnerabilities found after updating x/text to v0.39.0 | `docker compose run --rm --no-deps test go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...` |
| OpenAPI 3.1 document | Valid under openapi-spec-validator 0.9.0 | `python -m openapi_spec_validator api/openapi.json` |
| Docker build and running API/receiver | Both images built and both demos passed | `docker compose up --build -d` |
| Receiver failure demo | Two 503 responses, then a 204; final event delivered | `python scripts/demo.py` |
| SIGKILL recovery demo | Durable event retained its ID and reached delivered after the API process restarted | `python scripts/demo.py --crash` |

Raw Go output: [go-test.jsonl](evidence/go-test.jsonl). The count of 28 includes subtests and is not 28 independent top-level test functions.

The test suite exercises 32 concurrent same-key submissions, independent tenant key namespaces, hidden cross-tenant reads, transactional outbox cardinality, 16 competing claimers, forced lease expiry, stale completion rejection, signature verification, delayed retries, 400/302 terminal failures, 429/503 exhaustion, repeated crashes during exhaustion bookkeeping, unrepresentable JSON payloads, and duplicate receiver-side effects after ambiguous success.

Lease expiry is forced by moving its database timestamp in the integration test; this is a deterministic simulation, not a measured 15-second failover result. The live SIGKILL run [crash-recovery.json](evidence/crash-recovery.json) killed the process between recorded attempts. It proves durable restart recovery for that run, not that every possible crash instruction was exercised. The ambiguous-success test has the synthetic receiver commit its effect and then return 503; its in-memory dedupe fixture is not production receiver durability.

## Intake microbenchmark

[intake-benchmark.json](evidence/intake-benchmark.json) records one final run: 200 POST requests at client concurrency 16, a 57-byte synthetic payload, and 200 HTTP 201 responses. Observed elapsed time was 1.6535 seconds, about 121 accepted requests/second; median latency was 104.08 ms and p95 was 214.20 ms.

Environment: Windows host, Intel Core i7-13700HX (16 cores / 24 logical processors), Docker Linux engine with 24 CPUs and 15,550,533,632 bytes available memory, Go 1.26.8 linux/amd64, PostgreSQL 17 Alpine, pgx v5.11.0. Services did not have dedicated CPU/memory reservations. Other Go validation work shared the host during the final measurement, so this is especially unsuitable as an isolated performance comparison.

This is a closed-loop client microbenchmark through localhost and Docker networking, with workers and a deliberately failing receiver running. It measures durable intake, not time to delivery. It provides neither a production SLO nor evidence of sustainable capacity under open-loop load. A proper performance study needs a fixed hardware budget, idle baseline, warmup, repeated runs, longer workloads, queue-depth and resource measurements, and overload/error reporting.

## Not tested yet

- Real external consumer integration and production traffic.
- Multi-host scaling, regional failure, database restore, and rolling migrations.
- Tenant fairness and overload/admission behavior.
- Durable receiver deduplication and signing-key rotation.
