# Relay

Relay is a PostgreSQL-backed job queue that I built to practice reliable background processing and failure handling.

The system exposes a small REST API for enqueueing jobs, stores queue state in PostgreSQL, and runs jobs through separate worker processes. Workers claim jobs with `FOR UPDATE SKIP LOCKED`. Each claim gets an attempt number that acts as a fencing token, so a worker that loses its lease cannot later overwrite the result after another worker has taken over.

## What is included

- Java 21 multi-module Maven project
- Spring Boot REST server for enqueueing, status checks, and DLQ operations
- PostgreSQL-backed queue with leases, retries, idempotency keys, and a dead-letter queue
- Worker runtime with heartbeats, graceful shutdown, and metrics
- Integration and concurrency tests using Testcontainers
- Docker Compose setup with PostgreSQL, Prometheus, and Grafana
- Chaos test that kills or pauses workers and checks queue invariants

## Run locally

Requirements: Java 21, Maven 3.9+, Docker Desktop, and Node.js 18+.

```bash
mvn -B verify
mvn -B -DskipTests package
docker compose -f deploy/docker-compose.yml up -d --build
```

Enqueue a job:

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "content-type: application/json" \
  -d '{"tenant":"acme","queue":"default","type":"log","payload":{"message":"hello"},"idempotencyKey":"demo-1"}'
```

The API is available on port 8080, Prometheus on 9090, and Grafana on 3000.

## Main design decisions

- PostgreSQL is the source of truth, so claiming and state transitions can be protected with transactions and conditional updates.
- Leases allow jobs to be recovered after a worker crash or freeze.
- Fencing is enforced in SQL by matching both the job id and the claim attempt.
- Retry delays use exponential backoff with jitter.
- The delivery guarantee is at-least-once. Job handlers still need to be idempotent when side effects matter.

## Limitations and known flaws

This is a project for learning and demonstration, not a production-ready queue. The main gaps are:

1. The REST API has no authentication, authorization, rate limiting, or tenant isolation at the network boundary.
2. It depends on one PostgreSQL instance; there is no sharding, replication strategy, multi-region failover, or automated backup setup.
3. Workers poll the database instead of receiving pushed notifications, which adds latency and database load.
4. Completed jobs and old dead-letter entries are not cleaned up automatically, so storage grows without a retention policy.
5. At-least-once delivery can run a handler more than once after a lease expires; external side effects need their own idempotency protection.
6. Fairness between tenants and queues is limited. A busy queue can take most available worker capacity.
7. The sample worker handles a small set of job types and is not a general plugin or deployment model.
8. Configuration and database credentials in the local Compose file are development defaults, not secrets management.
9. Metrics and logs are useful for local inspection, but alert rules, tracing, and audit logging are not included.
10. Kubernetes manifests are included as a starting point but have not been validated on a real cluster.
11. Benchmarks were run on laptop hardware in Docker, so the throughput numbers are only rough comparisons and are not capacity guarantees.
12. The chaos tests exercise worker failure scenarios, but they do not prove correctness under PostgreSQL failure, disk loss, network partitions, or clock failures.
13. There is no job cancellation, dependency graph, scheduled-job service, or per-job result store.
14. The queue uses polling and database transactions for every state change, so a dedicated broker may be a better fit at much higher scale.

## Project layout

```text
relay-core/       Queue model, SQL repository, migrations, and core tests
relay-server/     REST API, reaper, and server metrics
relay-worker/     Worker runtime and example worker application
deploy/           Docker Compose, Prometheus, and Grafana configuration
bench/            Benchmarks and worker-failure chaos tests
```

The code and tests are intentionally kept in this repository so the implementation can be reviewed directly instead of relying on a long project report.

