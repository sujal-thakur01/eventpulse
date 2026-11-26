package domain

import "time"

// TransactionEvent represents an incoming customer transaction event.
type TransactionEvent struct {
	IdempotencyKey string    `json:"idempotency_key"`
	CustomerID     string    `json:"customer_id"`
	EventType      string    `json:"event_type"`
	AmountCents    int64     `json:"amount_cents"`
	Points         int64     `json:"points"`
	OccurredAt     time.Time `json:"occurred_at"`
}

// CustomerBalance represents a customer's current reward point balance.
type CustomerBalance struct {
	CustomerID string    `json:"customer_id"`
	Balance    int64     `json:"balance"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// LedgerEntry represents an immutable reward ledger record.
type LedgerEntry struct {
	ID             int64     `json:"id"`
	CustomerID     string    `json:"customer_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	PointsDelta    int64     `json:"points_delta"`
	BalanceAfter   int64     `json:"balance_after"`
	CreatedAt      time.Time `json:"created_at"`
}
