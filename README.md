# EventPulse

Go service that ingests customer transaction events over HTTP, partitions them in Kafka, stores reward points atomically in PostgreSQL, and serves balances over GraphQL.

![Go](https://img.shields.io/badge/Go-1.27-00ADD8?style=flat-square&logo=go&logoColor=white) ![Kafka](https://img.shields.io/badge/Kafka-3.9-231F20?style=flat-square&logo=apachekafka&logoColor=white) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white) ![GraphQL](https://img.shields.io/badge/GraphQL-gqlgen-E10098?style=flat-square&logo=graphql&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

## Guarantees

| Guarantee | How it works | Proof test |
| --- | --- | --- |
| Per customer ordering | Producer uses `Murmur2Balancer` keyed by `customer_id` with `RequireAll`; worker resolves one message before fetching the next | `internal/queue/producer_integration_test.go` (`TestProducerGuarantee1Integration`), `internal/consumer/worker_test.go` (`TestWorkerRunRetriesSameMessageUntilCommitted`) |
| At least once delivery | `CommitInterval` is `0` so offsets commit only after DB commit or DLQ publish; `processed_events` ignores duplicate `idempotency_key` inserts | `internal/store/postgres_integration_test.go` (`TestProcessTransactionIntegration/same_event_twice_returns_false_on_duplicate_without_changing_balance_or_ledger`), `internal/consumer/worker_test.go` (`TestWorkerRunRetriesSameMessageUntilCommitted`) |
| Atomic reward updates | `ProcessTransaction` inserts the event, upserts the balance, and writes the ledger in one PostgreSQL transaction | `internal/store/postgres_integration_test.go` (`TestProcessTransactionIntegration/guarantee3_ledger_failure_rolls_back_balance_and_event`) |
| Dead letter separation | Poison errors go straight to `transactions.dlq`; transient errors retry up to 6 times with backoff before DLQ | `internal/consumer/worker_test.go` (`TestWorkerHandleMessage`) |

## Event Flow

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant Kafka
    participant Worker
    participant Postgres
    participant DLQ
    Client->>HTTP: POST /events
    HTTP->>Kafka: Produce (key=customer_id, RequireAll)
    HTTP-->>Client: 202 Accepted
    Kafka->>Worker: FetchMessage
    alt Poison (decode, validation, or balance error)
        Worker->>DLQ: PublishDLQ (reason=poison)
        Worker->>Kafka: CommitMessages
    else Transient error
        loop Up to 6 attempts with backoff and jitter
            Worker->>Postgres: ProcessTransaction
            Postgres-->>Worker: error
        end
        Worker->>DLQ: PublishDLQ (reason=retries_exhausted)
        Worker->>Kafka: CommitMessages
    else Success or duplicate
        Worker->>Postgres: ProcessTransaction (1 TX)
        Postgres-->>Worker: committed
        Worker->>Kafka: CommitMessages
    end
```

## Consumer Decision

```mermaid
flowchart TD
    Fetch[FetchMessage] --> Decode{Decode JSON?}
    Decode -->|fail| Poison["Publish DLQ (poison)"]
    Decode -->|ok| Validate{Valid UUID and fields?}
    Validate -->|fail| Poison
    Validate -->|ok| Process["ProcessTransaction in Postgres"]
    Process -->|ok or duplicate| Commit[CommitMessages]
    Process -->|insufficient balance| Poison
    Process -->|transient error| Retry{Attempts < 6?}
    Retry -->|yes| Backoff["Sleep with backoff and jitter"] --> Process
    Retry -->|no| Exhausted["Publish DLQ (retries_exhausted)"]
    Poison --> DLQOk{DLQ publish ok?}
    Exhausted --> DLQOk
    DLQOk -->|yes| Commit
    DLQOk -->|no| NoCommit["Return error, no commit"]
```

## Schema

```mermaid
erDiagram
    customer_balances ||--o{ processed_events : "customer_id (logical)"
    customer_balances ||--o{ reward_ledger : "customer_id (logical)"
    processed_events ||--|| reward_ledger : idempotency_key
    processed_events {
        bigint id PK
        text idempotency_key UK
        text customer_id
        text event_type
        bigint amount_cents
        bigint points
        timestamptz occurred_at
        timestamptz created_at
    }
    customer_balances {
        text customer_id PK
        bigint balance
        timestamptz updated_at
    }
    reward_ledger {
        bigint id PK
        text customer_id
        text idempotency_key FK
        bigint points_delta
        bigint balance_after
        timestamptz created_at
    }
```

## Quick Start

Start PostgreSQL, Kafka (`transactions` with 3 partitions, `transactions.dlq` with 1 partition), and EventPulse:

```bash
docker compose up -d --build
```

Send an event:

```bash
curl -i -X POST http://localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"550e8400-e29b-41d4-a716-446655440000","customer_id":"cust-123","event_type":"purchase","amount_cents":2500,"occurred_at":"2026-11-27T10:00:00Z"}'
```

Query the balance:

```bash
curl -s -X POST http://localhost:8080/graphql -H 'Content-Type: application/json' \
  -d '{"query":"query ($id: ID!) { customerBalance(customerId: $id) { customerId balance updatedAt } }","variables":{"id":"cust-123"}}'
```

## Screenshots

| Jenkins pipeline | GraphQL playground | DLQ demo |
| --- | --- | --- |
| ![Jenkins pipeline](docs/images/jenkins-pipeline.png) | ![GraphQL playground](docs/images/graphql-playground.png) | ![DLQ demo](docs/images/dlq-demo.png) |
| Three stage CI run with lint, integration tests, and Docker build. | Interactive query for customer balance and paginated history. | Poison and exhausted retry events routed to the dead letter topic. |

## Design Decisions

| Decision | Why |
| --- | --- |
| Exhausted retries go to DLQ | Keeps the partition open for other customers instead of stalling forever on one failing event. |
| Negative balance refunds are poison | `CHECK (balance >= 0)` rejects them and retrying cannot pass without a later credit. |
| UUID required for `idempotency_key` | Enforces a consistent unique format across producers and avoids accidental collisions. |
| Server side points (`amount_cents / 100`) | Keeps point math consistent across `purchase`, `bonus`, and negative `refund` events. |
| Cursor uses `(created_at, id)` | Prevents late or backdated `occurred_at` events from slipping behind an active cursor. |

## API

<details>
<summary>HTTP endpoints: POST /events, GET /healthz, GET /playground</summary>

- `POST /events`: accepts JSON up to 1 MiB for `purchase`, `refund`, or `bonus`. Returns `202` on publish, `400` on bad JSON or validation error, `500` on Kafka error.
- `GET /healthz`: returns `200 OK` (`ok`).
- `GET /playground`: serves the interactive GraphQL Playground.

</details>

<details>
<summary>GraphQL queries: POST /graphql</summary>

- `customerBalance(customerId: ID!)`: returns `customerId`, `balance`, and `updatedAt`.
- `transactionHistory(customerId: ID!, first: Int, after: String)`: keyset paginated history (`first` defaults to `20`, max `100`).

```bash
curl -s -X POST http://localhost:8080/graphql -H 'Content-Type: application/json' \
  -d '{"query":"query ($id: ID!, $first: Int, $after: String) { transactionHistory(customerId: $id, first: $first, after: $after) { edges { cursor node { id idempotencyKey eventType amountCents points occurredAt createdAt } } pageInfo { hasNextPage endCursor } } }","variables":{"id":"cust-123","first":20,"after":null}}'
```

</details>

## Layout, Config, Tests, and CI

```text
cmd/eventpulse/      HTTP server, entrypoint, handler tests
internal/config/     Environment configuration
internal/consumer/   Kafka worker, backoff retry, DLQ routing
internal/domain/     Event validation and points calculation
internal/queue/      Kafka producer and integration tests
internal/store/      PostgreSQL store and integration tests
migrations/          Embedded SQL schema (001_init.sql)
docs/images/         Pipeline, playground, and DLQ screenshots
```

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `DATABASE_URL` | `postgres://eventpulse:eventpulse@localhost:5432/eventpulse?sslmode=disable` | PostgreSQL connection string |
| `KAFKA_BROKERS` | `localhost:9092` | Comma separated Kafka broker addresses |
| `KAFKA_TOPIC` | `transactions` | Primary event topic |
| `KAFKA_DLQ_TOPIC` | `transactions.dlq` | Dead letter queue topic |
| `KAFKA_GROUP` | `eventpulse-consumer` | Kafka consumer group ID |

Run unit tests with `go test -race ./...`. Run integration tests (requires Docker for Testcontainers `postgres:16-alpine` and `confluentinc/confluent-local:7.5.0`) with `go test -tags=integration -race ./...`.

`Jenkinsfile` runs three stages: Lint (`golangci/golangci-lint:v2.15.0` with `--build-tags=integration`), Test (`golang:1.27` with Docker socket, archives `test-output.txt`), and Docker Build (`eventpulse:${BUILD_NUMBER}`).

## Known Limits

- No end to end crash restart test.
- No load numbers.
- Single node Kafka in Docker Compose.
