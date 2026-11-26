//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startIntegrationStore(t *testing.T) *PostgresStore {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("eventpulse"),
		postgres.WithUsername("eventpulse"),
		postgres.WithPassword("eventpulse"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ctr.Terminate(context.Background())
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	st, err := NewPostgresStore(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(st.Close)

	require.NoError(t, st.Migrate(ctx))
	return st
}

func countCustomerRows(t *testing.T, ctx context.Context, st *PostgresStore, customerID string) (eventsCount, balanceCount, ledgerCount int) {
	t.Helper()

	require.NoError(t, st.pool.QueryRow(ctx, `SELECT COUNT(*) FROM processed_events WHERE customer_id = $1`, customerID).Scan(&eventsCount))
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT COUNT(*) FROM customer_balances WHERE customer_id = $1`, customerID).Scan(&balanceCount))
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT COUNT(*) FROM reward_ledger WHERE customer_id = $1`, customerID).Scan(&ledgerCount))
	return eventsCount, balanceCount, ledgerCount
}

// installLedgerFailTrigger creates a test-only trigger that raises an exception
// when an insert into reward_ledger targets customer_id 'cust-ledger-fail'.
func installLedgerFailTrigger(t *testing.T, ctx context.Context, st *PostgresStore) {
	t.Helper()
	_, err := st.pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION raise_on_ledger_fail_customer()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.customer_id = 'cust-ledger-fail' THEN
				RAISE EXCEPTION 'simulated ledger failure for cust-ledger-fail';
			END IF;
			RETURN NEW;
		END;
		$$;

		DROP TRIGGER IF EXISTS trig_ledger_fail ON reward_ledger;
		CREATE TRIGGER trig_ledger_fail
			BEFORE INSERT ON reward_ledger
			FOR EACH ROW
			EXECUTE FUNCTION raise_on_ledger_fail_customer();
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = st.pool.Exec(context.Background(), `
			DROP TRIGGER IF EXISTS trig_ledger_fail ON reward_ledger;
			DROP FUNCTION IF EXISTS raise_on_ledger_fail_customer();
		`)
	})
}

func TestProcessTransactionIntegration(t *testing.T) {
	st := startIntegrationStore(t)
	now := time.Date(2026, 11, 27, 1, 0, 0, 0, time.UTC)

	t.Run("table_driven_guarantees", func(t *testing.T) {
		type txStep struct {
			event         domain.TransactionEvent
			wantProcessed bool
			wantErr       error
		}

		cases := []struct {
			name             string
			customerID       string
			steps            []txStep
			wantBalance      int64
			wantEventsCount  int
			wantBalanceCount int
			wantLedgerCount  int
		}{
			{
				name:       "same event twice returns false on duplicate without changing balance or ledger",
				customerID: "cust-idempotent",
				steps: func() []txStep {
					ev := domain.TransactionEvent{
						IdempotencyKey: uuid.NewString(),
						CustomerID:     "cust-idempotent",
						EventType:      domain.EventTypePurchase,
						AmountCents:    5000,
						OccurredAt:     now,
					}
					return []txStep{
						{event: ev, wantProcessed: true, wantErr: nil},
						{event: ev, wantProcessed: false, wantErr: nil},
					}
				}(),
				wantBalance:      50,
				wantEventsCount:  1,
				wantBalanceCount: 1,
				wantLedgerCount:  1,
			},
			{
				name:       "refund bigger than zero balance returns ErrInsufficientBalance and writes nothing",
				customerID: "cust-refund-empty",
				steps: []txStep{
					{
						event: domain.TransactionEvent{
							IdempotencyKey: uuid.NewString(),
							CustomerID:     "cust-refund-empty",
							EventType:      domain.EventTypeRefund,
							AmountCents:    2500,
							OccurredAt:     now,
						},
						wantProcessed: false,
						wantErr:       domain.ErrInsufficientBalance,
					},
				},
				wantBalance:      0,
				wantEventsCount:  0,
				wantBalanceCount: 0,
				wantLedgerCount:  0,
			},
			{
				name:       "refund bigger than existing balance returns ErrInsufficientBalance and rolls back",
				customerID: "cust-refund-exceeds",
				steps: []txStep{
					{
						event: domain.TransactionEvent{
							IdempotencyKey: uuid.NewString(),
							CustomerID:     "cust-refund-exceeds",
							EventType:      domain.EventTypePurchase,
							AmountCents:    1000,
							OccurredAt:     now,
						},
						wantProcessed: true,
						wantErr:       nil,
					},
					{
						event: domain.TransactionEvent{
							IdempotencyKey: uuid.NewString(),
							CustomerID:     "cust-refund-exceeds",
							EventType:      domain.EventTypeRefund,
							AmountCents:    1500,
							OccurredAt:     now.Add(time.Second),
						},
						wantProcessed: false,
						wantErr:       domain.ErrInsufficientBalance,
					},
				},
				wantBalance:      10,
				wantEventsCount:  1,
				wantBalanceCount: 1,
				wantLedgerCount:  1,
			},
			{
				name:       "invalid UUID returns ErrInvalidEvent and writes nothing",
				customerID: "cust-invalid-uuid",
				steps: []txStep{
					{
						event: domain.TransactionEvent{
							IdempotencyKey: "not-a-uuid",
							CustomerID:     "cust-invalid-uuid",
							EventType:      domain.EventTypePurchase,
							AmountCents:    1000,
							OccurredAt:     now,
						},
						wantProcessed: false,
						wantErr:       domain.ErrInvalidEvent,
					},
				},
				wantBalance:      0,
				wantEventsCount:  0,
				wantBalanceCount: 0,
				wantLedgerCount:  0,
			},
			{
				name:       "unsupported event type returns ErrInvalidEvent and writes nothing",
				customerID: "cust-invalid-type",
				steps: []txStep{
					{
						event: domain.TransactionEvent{
							IdempotencyKey: uuid.NewString(),
							CustomerID:     "cust-invalid-type",
							EventType:      "chargeback",
							AmountCents:    1000,
							OccurredAt:     now,
						},
						wantProcessed: false,
						wantErr:       domain.ErrInvalidEvent,
					},
				},
				wantBalance:      0,
				wantEventsCount:  0,
				wantBalanceCount: 0,
				wantLedgerCount:  0,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				for i, step := range tc.steps {
					processed, err := st.ProcessTransaction(ctx, step.event)
					require.Equal(t, step.wantProcessed, processed, "step %d processed mismatch", i)
					if step.wantErr != nil {
						require.ErrorIs(t, err, step.wantErr, "step %d error mismatch", i)
						if errors.Is(step.wantErr, domain.ErrInsufficientBalance) {
							var pgErr *pgconn.PgError
							require.ErrorAs(t, err, &pgErr)
							require.Equal(t, "23514", pgErr.Code)
							require.Equal(t, balanceCheckConstraintName, pgErr.ConstraintName)
						}
					} else {
						require.NoError(t, err, "step %d unexpected error", i)
					}
				}

				evCount, balCount, ledCount := countCustomerRows(t, ctx, st, tc.customerID)
				require.Equal(t, tc.wantEventsCount, evCount)
				require.Equal(t, tc.wantBalanceCount, balCount)
				require.Equal(t, tc.wantLedgerCount, ledCount)

				if tc.wantBalanceCount > 0 {
					var gotBalance int64
					err := st.pool.QueryRow(ctx, `SELECT balance FROM customer_balances WHERE customer_id = $1`, tc.customerID).Scan(&gotBalance)
					require.NoError(t, err)
					require.Equal(t, tc.wantBalance, gotBalance)
				}
			})
		}
	})

	// Guarantee 3: reward balance and ledger entry updated in one transaction.
	// A test-only trigger simulates a ledger insert failure mid-transaction; the
	// balance upsert (which runs before the ledger insert) must also be rolled back.
	t.Run("guarantee3_ledger_failure_rolls_back_balance_and_event", func(t *testing.T) {
		ctx := context.Background()
		customerID := "cust-ledger-fail"

		installLedgerFailTrigger(t, ctx, st)

		ev := domain.TransactionEvent{
			IdempotencyKey: uuid.NewString(),
			CustomerID:     customerID,
			EventType:      domain.EventTypePurchase,
			AmountCents:    3000,
			OccurredAt:     now,
		}

		processed, err := st.ProcessTransaction(ctx, ev)
		require.False(t, processed)
		require.Error(t, err)

		evCount, balCount, ledCount := countCustomerRows(t, ctx, st, customerID)
		require.Equal(t, 0, evCount, "processed_events must be empty on rollback")
		require.Equal(t, 0, balCount, "customer_balances must be empty on rollback")
		require.Equal(t, 0, ledCount, "reward_ledger must be empty on rollback")
	})

	t.Run("20_goroutines_one_customer", func(t *testing.T) {
		ctx := context.Background()
		const workers = 20
		customerID := "cust-concurrent-20"

		type result struct {
			processed bool
			err       error
		}

		results := make(chan result, workers)
		var wg sync.WaitGroup
		var expectedSum int64

		startGate := make(chan struct{})
		for i := range workers {
			amountCents := int64((i + 1) * 1000)
			expectedSum += amountCents / 100
			ev := domain.TransactionEvent{
				IdempotencyKey: uuid.NewString(),
				CustomerID:     customerID,
				EventType:      domain.EventTypePurchase,
				AmountCents:    amountCents,
				OccurredAt:     now.Add(time.Duration(i) * time.Millisecond),
			}

			wg.Add(1)
			go func(event domain.TransactionEvent) {
				defer wg.Done()
				<-startGate
				processed, err := st.ProcessTransaction(ctx, event)
				results <- result{processed: processed, err: err}
			}(ev)
		}

		close(startGate)
		wg.Wait()
		close(results)

		for res := range results {
			require.NoError(t, res.err)
			require.True(t, res.processed)
		}

		evCount, balCount, ledCount := countCustomerRows(t, ctx, st, customerID)
		require.Equal(t, workers, evCount)
		require.Equal(t, 1, balCount)
		require.Equal(t, workers, ledCount)

		var finalBalance int64
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT balance FROM customer_balances WHERE customer_id = $1`,
			customerID,
		).Scan(&finalBalance))
		require.Equal(t, expectedSum, finalBalance)

		var lastBalanceAfter int64
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT balance_after FROM reward_ledger WHERE customer_id = $1 ORDER BY id DESC LIMIT 1`,
			customerID,
		).Scan(&lastBalanceAfter))
		require.Equal(t, expectedSum, lastBalanceAfter)
	})
}
