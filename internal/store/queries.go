package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

const DefaultPageSize = 20

// TransactionEdge pairs a persisted transaction with its opaque pagination cursor.
type TransactionEdge struct {
	Cursor string
	Node   domain.ProcessedEvent
}

// TransactionPage represents a keyset-paginated slice of customer transactions.
type TransactionPage struct {
	Edges       []TransactionEdge
	HasNextPage bool
	EndCursor   *string
}

type transactionCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        int64     `json:"id"`
}

// EncodeTransactionCursor encodes a (created_at, id) keyset position into an opaque base64 cursor.
func EncodeTransactionCursor(createdAt time.Time, id int64) (string, error) {
	raw, err := json.Marshal(transactionCursor{
		CreatedAt: createdAt.UTC(),
		ID:        id,
	})
	if err != nil {
		return "", fmt.Errorf("marshal transaction cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeTransactionCursor decodes an opaque base64 cursor into its (created_at, id) keyset position.
func DecodeTransactionCursor(cursor string) (time.Time, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var c transactionCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid cursor payload: %w", err)
	}
	if c.CreatedAt.IsZero() || c.ID <= 0 {
		return time.Time{}, 0, errors.New("invalid cursor fields")
	}
	return c.CreatedAt.UTC(), c.ID, nil
}

// GetCustomerBalance returns the current reward balance for a customer, or nil if not found.
func (s *PostgresStore) GetCustomerBalance(ctx context.Context, customerID string) (*domain.CustomerBalance, error) {
	var bal domain.CustomerBalance
	err := s.pool.QueryRow(ctx, `
		SELECT customer_id, balance, updated_at
		FROM customer_balances
		WHERE customer_id = $1
	`, customerID).Scan(&bal.CustomerID, &bal.Balance, &bal.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query customer_balance: %w", err)
	}
	return &bal, nil
}

// ListTransactionHistory returns a keyset-paginated page of transactions for a customer
// ordered by (created_at DESC, id DESC).
func (s *PostgresStore) ListTransactionHistory(
	ctx context.Context,
	customerID string,
	first *int,
	after *string,
) (TransactionPage, error) {
	limit := DefaultPageSize
	if first != nil {
		if *first <= 0 {
			return TransactionPage{}, errors.New("first must be positive")
		}
		limit = min(*first, domain.MaxPageSize)
	}

	var (
		rows pgx.Rows
		err  error
	)

	if after != nil && *after != "" {
		cursorCreatedAt, cursorID, decErr := DecodeTransactionCursor(*after)
		if decErr != nil {
			return TransactionPage{}, decErr
		}
		rows, err = s.pool.Query(ctx, `
			SELECT id, idempotency_key, customer_id, event_type, amount_cents, points, occurred_at, created_at
			FROM processed_events
			WHERE customer_id = $1
			  AND (created_at, id) < ($2, $3)
			ORDER BY created_at DESC, id DESC
			LIMIT $4
		`, customerID, cursorCreatedAt, cursorID, limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, idempotency_key, customer_id, event_type, amount_cents, points, occurred_at, created_at
			FROM processed_events
			WHERE customer_id = $1
			ORDER BY created_at DESC, id DESC
			LIMIT $2
		`, customerID, limit+1)
	}
	if err != nil {
		return TransactionPage{}, fmt.Errorf("query processed_events: %w", err)
	}
	defer rows.Close()

	events := make([]domain.ProcessedEvent, 0, limit+1)
	for rows.Next() {
		var ev domain.ProcessedEvent
		if err := rows.Scan(
			&ev.ID,
			&ev.IdempotencyKey,
			&ev.CustomerID,
			&ev.EventType,
			&ev.AmountCents,
			&ev.Points,
			&ev.OccurredAt,
			&ev.CreatedAt,
		); err != nil {
			return TransactionPage{}, fmt.Errorf("scan processed_event: %w", err)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return TransactionPage{}, fmt.Errorf("iterate processed_events: %w", err)
	}

	hasNextPage := len(events) > limit
	if hasNextPage {
		events = events[:limit]
	}

	edges := make([]TransactionEdge, 0, len(events))
	for _, ev := range events {
		cursor, err := EncodeTransactionCursor(ev.CreatedAt, ev.ID)
		if err != nil {
			return TransactionPage{}, err
		}
		edges = append(edges, TransactionEdge{
			Cursor: cursor,
			Node:   ev,
		})
	}

	var endCursor *string
	if len(edges) > 0 {
		c := edges[len(edges)-1].Cursor
		endCursor = &c
	}

	return TransactionPage{
		Edges:       edges,
		HasNextPage: hasNextPage,
		EndCursor:   endCursor,
	}, nil
}
