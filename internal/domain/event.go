package domain

import (
	"fmt"
	"time"
)

const (
	EventTypePurchase = "purchase"
	EventTypeRefund   = "refund"
	EventTypeBonus    = "bonus"

	// MaxPageSize caps GraphQL pagination page size.
	MaxPageSize = 100
)

// TransactionEvent represents an incoming customer transaction event.
// Points are calculated server-side from EventType and AmountCents.
type TransactionEvent struct {
	IdempotencyKey string    `json:"idempotency_key"`
	CustomerID     string    `json:"customer_id"`
	EventType      string    `json:"event_type"`
	AmountCents    int64     `json:"amount_cents"`
	OccurredAt     time.Time `json:"occurred_at"`
}

// CalculatePoints computes reward points from event type and amount in cents.
func CalculatePoints(eventType string, amountCents int64) (int64, error) {
	if amountCents <= 0 {
		return 0, fmt.Errorf("amount_cents must be positive, got %d", amountCents)
	}
	switch eventType {
	case EventTypePurchase, EventTypeBonus:
		return amountCents / 100, nil
	case EventTypeRefund:
		return -(amountCents / 100), nil
	default:
		return 0, fmt.Errorf("unsupported event_type %q", eventType)
	}
}

// ProcessedEvent represents a persisted transaction event with server-calculated points.
type ProcessedEvent struct {
	ID             int64     `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	CustomerID     string    `json:"customer_id"`
	EventType      string    `json:"event_type"`
	AmountCents    int64     `json:"amount_cents"`
	Points         int64     `json:"points"`
	OccurredAt     time.Time `json:"occurred_at"`
	CreatedAt      time.Time `json:"created_at"`
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
