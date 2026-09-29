# Roadmap

The API and delivery workers are running. These are the next steps, in order. I will keep this updated as I work on this project

| Milestone | Why it matters | Exit evidence
|---|---|---|---|
| Admission control and fairness | Make overload behavior explicit | Per-tenant 429 with Retry-After; bounded backlog; a noisy tenant cannot starve a quiet one in a saved open-loop load test
| Real Kiln consumer | Demonstrate a useful producer/consumer integration | Synthetic `note.created` emitted from a Kiln adapter; receiver dedupe stored transactionally; duplicate/crash demo across both services
| Safe dead-letter replay | Complete an operator workflow | Replay resource with its own idempotency key, explicit retry budget and audit lineage; concurrent replay test; original attempts preserved
| Observability and capacity study | Replace implied scale with evidence | OpenTelemetry traces and Prometheus metrics without payloads/secrets; queue age, outcomes and retry counts; open-loop k6 test with p50/p95/p99, errors, hardware, limits, repetitions and raw results
| Self-service endpoints | Expand the API design challenge | Tenant-scoped CRUD, opaque cursor pagination, optimistic concurrency, signing-key rotation; SSRF/DNS-rebinding negative tests and quotas
| Deployment and recovery | Establish operations evidence | Versioned migrations, managed-secret injection, TLS ingress, backup/restore drill, two-process worker test, documented cost cap 

Start with admission control and a working consumer. Add a broker or split the service only if the load tests show a reason to do so.
