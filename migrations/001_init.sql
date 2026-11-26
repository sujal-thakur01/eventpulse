-- 001_init.sql: Initial schema for EventPulse

CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('purchase', 'refund', 'bonus')),
    amount_cents BIGINT NOT NULL,
    points BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_customer_created
    ON processed_events (customer_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS customer_balances (
    customer_id TEXT PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reward_ledger (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE REFERENCES processed_events(idempotency_key),
    points_delta BIGINT NOT NULL,
    balance_after BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reward_ledger_customer_created
    ON reward_ledger (customer_id, created_at DESC, id DESC);
