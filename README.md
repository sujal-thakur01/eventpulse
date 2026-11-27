# EventPulse

EventPulse is a Go service for ingesting customer transaction events, computing reward points, and maintaining an auditable customer balance and ledger. Events are ingested over HTTP, partitioned by customer in Apache Kafka, persisted atomically in PostgreSQL, and queried via a GraphQL API.

## Overview

EventPulse models a single-binary pipeline composed of an HTTP ingestion and query server alongside a Kafka consumer worker:

- **Ingestion (`POST /events`)**: Validates incoming transaction payloads (`purchase`, `refund`, `bonus`) and publishes them to Apache Kafka keyed by `customer_id`.
- **Stream Processing (`internal/consumer`)**: Consumes messages partition-by-partition with explicit offset commits, retries transient failures with exponential backoff and equal jitter, and routes unrecoverable or retry-exhausted messages to a dead-letter queue (DLQ).
- **Persistence (`internal/store`)**: Applies embedded SQL migrations on startup and processes each event inside a single PostgreSQL transaction across `processed_events`, `customer_balances`, and `reward_ledger`.
- **Query API (`POST /graphql`)**: Serves customer reward balances and keyset-paginated transaction histories via `gqlgen`.

## Architecture

```mermaid
flowchart LR
    Client([HTTP Client]) -->|POST /events| Ingest[HTTP Ingestion Handler]
    Client -->|POST /graphql| GQL[gqlgen GraphQL Server]

    Ingest -->|Murmur2 key=customer_id, RequireAll| Kafka[(Kafka: transactions)]
    Kafka -->|FetchMessage / CommitMessages| Worker[Consumer Worker]

    Worker -->|ProcessTransaction| PG[(PostgreSQL)]
    PG --> Events[(processed_events)]
    PG --> Balances[(customer_balances)]
    PG --> Ledger[(reward_ledger)]

    Worker -.->|poison or retries_exhausted| DLQ[(Kafka: transactions.dlq)]
    GQL -->|GetCustomerBalance / ListTransactionHistory| PG
```

## Core Guarantees

### 1. Per-Customer Ordering via Partitioning

**How it works:** `queue.Producer` configures `kafka.Writer` with `kafka.Murmur2Balancer` and `RequiredAcks: kafka.RequireAll`, setting the Kafka message key to `ev.CustomerID`. All events for a given `customer_id` map deterministically to the same partition in publish order. On the consumer side, `consumer.Worker.Run` fetches and resolves one message at a time (`FetchMessage` -> process -> `CommitMessages`) and never fetches the next message while the current message is unresolved.

**Proven by:**
- [`TestProducerGuarantee1Integration`](internal/queue/producer_integration_test.go#L48) in [internal/queue/producer_integration_test.go](internal/queue/producer_integration_test.go#L48) — publishes events for multiple customers across a multi-partition Kafka topic and verifies each customer lands on a single partition in exact send order.
- [`TestWorkerRunRetriesSameMessageUntilCommitted`](internal/consumer/worker_test.go#L283) in [internal/consumer/worker_test.go](internal/consumer/worker_test.go#L283) — verifies that a message is fully resolved and committed before the next message is fetched.

### 2. At-Least-Once Delivery with Idempotency

**How it works:** The Kafka reader is configured with `CommitInterval: 0`, and `consumer.Worker` only calls `CommitMessages` after `ProcessTransaction` succeeds or the message is durably published to the DLQ. If the worker crashes before committing, Kafka redelivers the message. `PostgresStore.ProcessTransaction` inserts into `processed_events` with `ON CONFLICT (idempotency_key) DO NOTHING`; when `RowsAffected() == 0`, it returns `(false, nil)` without touching `customer_balances` or `reward_ledger`.

**Proven by:**
- [`TestProcessTransactionIntegration`](internal/store/postgres_integration_test.go#L87) (subtest `same event twice returns false on duplicate without changing balance or ledger`) in [internal/store/postgres_integration_test.go](internal/store/postgres_integration_test.go#L108).
- [`TestWorkerRunRetriesSameMessageUntilCommitted`](internal/consumer/worker_test.go#L283) in [internal/consumer/worker_test.go](internal/consumer/worker_test.go#L283).

### 3. Atomic Reward Updates

**How it works:** `PostgresStore.ProcessTransaction` executes the `processed_events` insert, `customer_balances` upsert, and `reward_ledger` insert inside a single PostgreSQL transaction (`s.pool.Begin` / `tx.Commit` with deferred `tx.Rollback`). If any statement fails—for example, a constraint violation or a ledger insert error—the entire transaction rolls back, preventing partial updates between balances and the audit ledger.

**Proven by:**
- [`TestProcessTransactionIntegration`](internal/store/postgres_integration_test.go#L87) (subtests `guarantee3_ledger_failure_rolls_back_balance_and_event` and `20_goroutines_one_customer`) in [internal/store/postgres_integration_test.go](internal/store/postgres_integration_test.go#L262).

### 4. Dead Letter Queue (DLQ) Separation

**How it works:** `consumer.Worker.handleMessage` separates non-retryable failures from transient ones via `isPoison`:
- **Poison (`reason: "poison"`)**: JSON decode failures (`ErrDecode`), domain validation errors (`domain.ErrInvalidEvent`), and insufficient-balance errors (`domain.ErrInsufficientBalance`) are published directly to the DLQ topic without retrying.
- **Transient failures (`reason: "retries_exhausted"`)**: Other errors are retried with bounded exponential backoff and equal jitter up to `MaxAttempts` (default `6`). If all attempts fail, the message is published to the DLQ with `reason: "retries_exhausted"`.

In both cases, the source Kafka offset is committed only after `DLQWriter.PublishDLQ` succeeds.

**Proven by:**
- [`TestWorkerHandleMessage`](internal/consumer/worker_test.go#L70) in [internal/consumer/worker_test.go](internal/consumer/worker_test.go#L70).

## Design Decisions and Tradeoffs

- **Exhausted retries route to the DLQ (`retries_exhausted`), which can reorder later events for that customer:** Rather than blocking a Kafka partition indefinitely on a persistently failing transient dependency, `Worker` sends the message to `transactions.dlq` with `reason: "retries_exhausted"` after `MaxAttempts` and commits the source offset. This preserves partition availability for other customers, at the cost that subsequent events for the affected customer are processed before the DLQ record is replayed.
- **Refunds exceeding the customer balance route to the DLQ as `poison`:** The `customer_balances` table enforces `CHECK (balance >= 0)` (`customer_balances_balance_check`, SQLSTATE `23514`). `PostgresStore` maps this violation to `domain.ErrInsufficientBalance`, and `isPoison` treats it as non-retryable so it goes straight to the DLQ with `reason: "poison"`. Retrying in place cannot succeed unless an independent credit arrives, and holding the offset would stall all other customers sharing the partition.
- **Idempotency keys must be valid UUIDs:** `TransactionEvent.Validate` parses `idempotency_key` using `uuid.Parse`. Enforcing UUID format guarantees a consistent, globally unique key structure from producers and prevents accidental collisions from arbitrary strings.
- **Points are 1 per 100 cents and refunds are negative:** `domain.CalculatePoints` computes `amount_cents / 100` (integer division) for `purchase` and `bonus` events, and `-(amount_cents / 100)` for `refund` events, requiring `amount_cents > 0`. Points are always derived server-side from `event_type` and `amount_cents`.
- **GraphQL pagination uses `(created_at, id)` instead of `occurred_at`:** `ListTransactionHistory` orders rows by `(created_at DESC, id DESC)` and encodes `(created_at, id)` into the opaque base64 cursor, backed by `idx_processed_events_customer_created`. Because `occurred_at` is client-supplied and events can arrive late or backdated, paginating on `occurred_at` would cause newly ingested historical events to be inserted behind an active cursor and skipped during traversal. Using server-assigned `created_at` with `id` as a tie-breaker provides a stable, monotonic keyset order.

## API

### Ingest Event (`POST /events`)

Accepts a JSON transaction event (up to 1 MiB). Returns `202 Accepted` once published to Kafka, `400 Bad Request` on malformed JSON or validation errors, and `500 Internal Server Error` if publishing fails.

```bash
curl -i -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "550e8400-e29b-41d4-a716-446655440000",
    "customer_id": "cust-123",
    "event_type": "purchase",
    "amount_cents": 2500,
    "occurred_at": "2026-11-27T10:00:00Z"
  }'
```

Supported `event_type` values: `purchase`, `refund`, `bonus`.

Additional HTTP endpoints:
- `GET /healthz` — returns `200 OK` (`ok`).
- `GET /playground` — serves the interactive GraphQL Playground.

### Query Data (`POST /graphql`)

Query a customer's current reward balance:

```bash
curl -s -X POST http://localhost:8080/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "query ($id: ID!) { customerBalance(customerId: $id) { customerId balance updatedAt } }",
    "variables": { "id": "cust-123" }
  }'
```

Query keyset-paginated transaction history (`first` defaults to `20` and is capped at `100` server-side):

```bash
curl -s -X POST http://localhost:8080/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "query ($id: ID!, $first: Int, $after: String) { transactionHistory(customerId: $id, first: $first, after: $after) { edges { cursor node { id idempotencyKey eventType amountCents points occurredAt createdAt } } pageInfo { hasNextPage endCursor } } }",
    "variables": { "id": "cust-123", "first": 20, "after": null }
  }'
```

## Local Setup

### Prerequisites

- Go 1.27+
- Docker and Docker Compose

### Run with Docker Compose

Start PostgreSQL, Apache Kafka, topic initialization (`transactions` with 3 partitions, `transactions.dlq` with 1 partition), and the EventPulse service:

```bash
docker compose up -d --build
```

### Run Locally Against Docker Infrastructure

Start only PostgreSQL and Kafka:

```bash
docker compose up -d postgres kafka kafka-init
```

Run the service from source:

```bash
go run ./cmd/eventpulse
```

Configuration is loaded from environment variables in `internal/config/config.go`:

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `DATABASE_URL` | `postgres://eventpulse:eventpulse@localhost:5432/eventpulse?sslmode=disable` | PostgreSQL connection string |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC` | `transactions` | Primary event topic |
| `KAFKA_DLQ_TOPIC` | `transactions.dlq` | Dead-letter queue topic |
| `KAFKA_GROUP` | `eventpulse-consumer` | Kafka consumer group ID |

## Running Tests

### Unit Tests

Unit tests cover HTTP handler validation (`cmd/eventpulse/main_test.go`) and consumer worker retry/DLQ/commit behavior (`internal/consumer/worker_test.go`) without external dependencies:

```bash
go test -race ./...
```

### Integration Tests

Integration tests are gated behind the `integration` build tag and use `testcontainers-go` to start ephemeral PostgreSQL (`postgres:16-alpine`) and Kafka (`confluentinc/confluent-local:7.5.0`) containers. Ensure Docker is running, then run:

```bash
go test -tags=integration -race ./...
```

## CI

The [Jenkinsfile](Jenkinsfile) defines three stages:

1. **Lint**: Runs `golangci-lint run --build-tags=integration` using `golangci/golangci-lint:v2.15.0`.
2. **Test**: Runs `go test -tags=integration -race -timeout 12m ./...` inside `golang:1.27` with `/var/run/docker.sock` mounted for Testcontainers, archiving `test-output.txt`.
3. **Docker Build**: Builds the multi-stage container image from [Dockerfile](Dockerfile) tagged `eventpulse:${BUILD_NUMBER}`.
