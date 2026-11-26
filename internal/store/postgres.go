package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
	"github.com/sujal-thakur01/eventpulse/migrations"
)

const (
	pgCheckViolationCode            = "23514"
	balanceCheckConstraintName      = "customer_balances_balance_check"
)

// PostgresStore wraps a pgxpool.Pool for database operations.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a connection pool and verifies connectivity.
func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

// Migrate applies embedded SQL migrations.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	return migrations.Apply(ctx, s.pool)
}

// Close closes the underlying connection pool.
func (s *PostgresStore) Close() {
	s.pool.Close()
}

// ProcessTransaction atomically records an event (idempotently via ON CONFLICT DO NOTHING),
// updates the customer's reward balance, and inserts a reward_ledger entry in one transaction.
// It returns true if the event was newly processed, or false if it was a duplicate.
func (s *PostgresStore) ProcessTransaction(ctx context.Context, ev domain.TransactionEvent) (bool, error) {
	if err := ev.Validate(); err != nil {
		return false, err
	}

	points, err := domain.CalculatePoints(ev.EventType, ev.AmountCents)
	if err != nil {
		return false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (
			idempotency_key, customer_id, event_type, amount_cents, points, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, ev.IdempotencyKey, ev.CustomerID, ev.EventType, ev.AmountCents, points, ev.OccurredAt)
	if err != nil {
		return false, classifyStoreError("insert processed_event", err)
	}

	if tag.RowsAffected() == 0 {
		return false, nil
	}

	var balanceAfter int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO customer_balances (customer_id, balance, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (customer_id) DO UPDATE
		SET balance = customer_balances.balance + EXCLUDED.balance,
		    updated_at = NOW()
		RETURNING balance
	`, ev.CustomerID, points).Scan(&balanceAfter); err != nil {
		return false, classifyStoreError("upsert customer_balance", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO reward_ledger (customer_id, idempotency_key, points_delta, balance_after)
		VALUES ($1, $2, $3, $4)
	`, ev.CustomerID, ev.IdempotencyKey, points, balanceAfter); err != nil {
		return false, classifyStoreError("insert reward_ledger", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit transaction: %w", err)
	}

	return true, nil
}

func classifyStoreError(op string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		pgErr.Code == pgCheckViolationCode &&
		pgErr.ConstraintName == balanceCheckConstraintName {
		return fmt.Errorf("%s: %w: %w", op, domain.ErrInsufficientBalance, pgErr)
	}
	return fmt.Errorf("%s: %w", op, err)
}
