//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

func insertTestEvents(t *testing.T, ctx context.Context, st *PostgresStore, customerID string, count int, baseTime time.Time) []string {
	t.Helper()
	keys := make([]string, 0, count)
	for i := range count {
		key := uuid.NewString()
		processed, err := st.ProcessTransaction(ctx, domain.TransactionEvent{
			IdempotencyKey: key,
			CustomerID:     customerID,
			EventType:      domain.EventTypePurchase,
			AmountCents:    int64((i + 1) * 100),
			OccurredAt:     baseTime.Add(time.Duration(i) * time.Second),
		})
		require.NoError(t, err)
		require.True(t, processed)
		keys = append(keys, key)
	}
	return keys
}

func TestTransactionHistoryPaginationIntegration(t *testing.T) {
	st := startIntegrationStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 27, 2, 0, 0, 0, time.UTC)

	t.Run("paging_returns_every_row_once_with_no_gaps_or_duplicates", func(t *testing.T) {
		customerID := "cust-page-all-" + uuid.NewString()
		otherCustomerID := "cust-page-other-" + uuid.NewString()

		const totalRows = 25
		insertedKeys := insertTestEvents(t, ctx, st, customerID, totalRows, now)
		insertTestEvents(t, ctx, st, otherCustomerID, 5, now)

		// Force identical created_at on a subset of rows to verify (created_at, id) tie-breaking.
		_, err := st.pool.Exec(ctx, `
			UPDATE processed_events
			SET created_at = $1
			WHERE customer_id = $2
			  AND id IN (
				SELECT id FROM processed_events
				WHERE customer_id = $2
				ORDER BY id ASC
				LIMIT 6
			  )
		`, now, customerID)
		require.NoError(t, err)

		pageSize := 4
		var after *string
		seenKeys := make(map[string]struct{}, totalRows)
		var collected []domain.ProcessedEvent

		for {
			page, err := st.ListTransactionHistory(ctx, customerID, &pageSize, after)
			require.NoError(t, err)
			require.LessOrEqual(t, len(page.Edges), pageSize)

			for _, edge := range page.Edges {
				require.Equal(t, customerID, edge.Node.CustomerID)
				_, dup := seenKeys[edge.Node.IdempotencyKey]
				require.False(t, dup, "duplicate transaction %s", edge.Node.IdempotencyKey)
				seenKeys[edge.Node.IdempotencyKey] = struct{}{}
				collected = append(collected, edge.Node)
			}

			if !page.HasNextPage {
				break
			}
			require.NotNil(t, page.EndCursor)
			after = page.EndCursor
		}

		require.Len(t, collected, totalRows)
		for _, k := range insertedKeys {
			_, ok := seenKeys[k]
			require.True(t, ok, "missing transaction %s", k)
		}

		for i := 1; i < len(collected); i++ {
			prev := collected[i-1]
			curr := collected[i]
			ordered := prev.CreatedAt.After(curr.CreatedAt) ||
				(prev.CreatedAt.Equal(curr.CreatedAt) && prev.ID > curr.ID)
			require.True(t, ordered, "rows not strictly ordered by (created_at DESC, id DESC) at index %d", i)
		}
	})

	t.Run("cursor_stays_correct_when_new_rows_inserted_between_fetches", func(t *testing.T) {
		customerID := "cust-page-live-" + uuid.NewString()
		initialKeys := insertTestEvents(t, ctx, st, customerID, 7, now)
		initialSet := make(map[string]struct{}, len(initialKeys))
		for _, k := range initialKeys {
			initialSet[k] = struct{}{}
		}

		pageSize := 3
		page1, err := st.ListTransactionHistory(ctx, customerID, &pageSize, nil)
		require.NoError(t, err)
		require.Len(t, page1.Edges, 3)
		require.True(t, page1.HasNextPage)
		require.NotNil(t, page1.EndCursor)

		// Insert newer rows that sort ahead of page 1 while paging is in progress.
		newKeys := insertTestEvents(t, ctx, st, customerID, 3, now.Add(time.Minute))
		newSet := make(map[string]struct{}, len(newKeys))
		for _, k := range newKeys {
			newSet[k] = struct{}{}
		}

		page2, err := st.ListTransactionHistory(ctx, customerID, &pageSize, page1.EndCursor)
		require.NoError(t, err)
		require.Len(t, page2.Edges, 3)
		require.True(t, page2.HasNextPage)
		require.NotNil(t, page2.EndCursor)

		page3, err := st.ListTransactionHistory(ctx, customerID, &pageSize, page2.EndCursor)
		require.NoError(t, err)
		require.Len(t, page3.Edges, 1)
		require.False(t, page3.HasNextPage)

		seenInitial := make(map[string]struct{}, len(initialKeys))
		for _, p := range []TransactionPage{page1, page2, page3} {
			for _, edge := range p.Edges {
				_, isNew := newSet[edge.Node.IdempotencyKey]
				require.False(t, isNew, "newly inserted row %s leaked into existing cursor traversal", edge.Node.IdempotencyKey)
				_, dup := seenInitial[edge.Node.IdempotencyKey]
				require.False(t, dup, "duplicate row %s during live insert pagination", edge.Node.IdempotencyKey)
				seenInitial[edge.Node.IdempotencyKey] = struct{}{}
			}
		}
		require.Len(t, seenInitial, len(initialKeys))
		for k := range initialSet {
			_, ok := seenInitial[k]
			require.True(t, ok, "missing initial row %s", k)
		}

		// Fresh query without cursor includes both new and initial rows (10 total).
		fullSize := 100
		fullPage, err := st.ListTransactionHistory(ctx, customerID, &fullSize, nil)
		require.NoError(t, err)
		require.Len(t, fullPage.Edges, 10)
		require.False(t, fullPage.HasNextPage)
	})

	t.Run("default_and_max_page_size_and_customer_balance", func(t *testing.T) {
		missingBal, err := st.GetCustomerBalance(ctx, "cust-missing-"+uuid.NewString())
		require.NoError(t, err)
		require.Nil(t, missingBal)

		customerID := "cust-page-cap-" + uuid.NewString()
		insertTestEvents(t, ctx, st, customerID, domain.MaxPageSize+5, now)

		bal, err := st.GetCustomerBalance(ctx, customerID)
		require.NoError(t, err)
		require.NotNil(t, bal)
		require.Equal(t, customerID, bal.CustomerID)
		require.Positive(t, bal.Balance)

		defaultPage, err := st.ListTransactionHistory(ctx, customerID, nil, nil)
		require.NoError(t, err)
		require.Len(t, defaultPage.Edges, DefaultPageSize)
		require.True(t, defaultPage.HasNextPage)

		oversized := domain.MaxPageSize + 50
		cappedPage, err := st.ListTransactionHistory(ctx, customerID, &oversized, nil)
		require.NoError(t, err)
		require.Len(t, cappedPage.Edges, domain.MaxPageSize)
		require.True(t, cappedPage.HasNextPage)
	})
}
