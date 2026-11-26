package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

type fakeFetcher struct {
	committed []kafka.Message
	commitErr error
}

func (f *fakeFetcher) FetchMessage(_ context.Context) (kafka.Message, error) {
	return kafka.Message{}, context.Canceled
}

func (f *fakeFetcher) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = append(f.committed, msgs...)
	return nil
}

type fakeStore struct {
	fn    func(attempt int, ev domain.TransactionEvent) (bool, error)
	calls int
}

func (s *fakeStore) ProcessTransaction(_ context.Context, ev domain.TransactionEvent) (bool, error) {
	s.calls++
	return s.fn(s.calls, ev)
}

type fakeDLQ struct {
	records []DLQRecord
	err     error
}

func (d *fakeDLQ) PublishDLQ(_ context.Context, _ []byte, record DLQRecord) error {
	d.records = append(d.records, record)
	return d.err
}

func validEventJSON(t *testing.T) []byte {
	t.Helper()
	ev := domain.TransactionEvent{
		IdempotencyKey: uuid.NewString(),
		CustomerID:     "cust-1",
		EventType:      domain.EventTypePurchase,
		AmountCents:    2500,
		OccurredAt:     time.Date(2026, 11, 27, 1, 0, 0, 0, time.UTC),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return b
}

func TestWorkerHandleMessage(t *testing.T) {
	const maxAttempts = 3
	errTransient := errors.New("postgres connection reset")
	errDLQDown := errors.New("kafka dlq unavailable")

	cases := []struct {
		name            string
		payload         func(t *testing.T) []byte
		storeFn         func(attempt int, ev domain.TransactionEvent) (bool, error)
		dlqErr          error
		wantErr         bool
		wantStoreCalls  int
		wantDLQCount    int
		wantDLQReason   string
		wantDLQAttempts int
		wantCommitted   bool
		wantBackoffs    []time.Duration
	}{
		{
			name: "poison decode error goes to DLQ after exactly 1 attempt",
			payload: func(_ *testing.T) []byte {
				return []byte("{not-json")
			},
			storeFn: func(_ int, _ domain.TransactionEvent) (bool, error) {
				return false, errors.New("should not be called")
			},
			wantErr:         false,
			wantStoreCalls:  0,
			wantDLQCount:    1,
			wantDLQReason:   ReasonPoison,
			wantDLQAttempts: 1,
			wantCommitted:   true,
			wantBackoffs:    nil,
		},
		{
			name:    "poison ErrInvalidEvent goes to DLQ after exactly 1 attempt",
			payload: validEventJSON,
			storeFn: func(_ int, _ domain.TransactionEvent) (bool, error) {
				return false, fmt.Errorf("validate: %w", domain.ErrInvalidEvent)
			},
			wantErr:         false,
			wantStoreCalls:  1,
			wantDLQCount:    1,
			wantDLQReason:   ReasonPoison,
			wantDLQAttempts: 1,
			wantCommitted:   true,
			wantBackoffs:    nil,
		},
		{
			name:    "poison ErrInsufficientBalance goes to DLQ after exactly 1 attempt",
			payload: validEventJSON,
			storeFn: func(_ int, _ domain.TransactionEvent) (bool, error) {
				return false, fmt.Errorf("balance: %w", domain.ErrInsufficientBalance)
			},
			wantErr:         false,
			wantStoreCalls:  1,
			wantDLQCount:    1,
			wantDLQReason:   ReasonPoison,
			wantDLQAttempts: 1,
			wantCommitted:   true,
			wantBackoffs:    nil,
		},
		{
			name:    "transient failure succeeds on 3rd attempt with bounded backoff and no DLQ",
			payload: validEventJSON,
			storeFn: func(attempt int, _ domain.TransactionEvent) (bool, error) {
				if attempt < 3 {
					return false, errTransient
				}
				return true, nil
			},
			wantErr:        false,
			wantStoreCalls: 3,
			wantDLQCount:   0,
			wantCommitted:  true,
			wantBackoffs:   []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
		},
		{
			name:    "transient never succeeds and goes to DLQ after max attempts",
			payload: validEventJSON,
			storeFn: func(_ int, _ domain.TransactionEvent) (bool, error) {
				return false, errTransient
			},
			wantErr:         false,
			wantStoreCalls:  maxAttempts,
			wantDLQCount:    1,
			wantDLQReason:   ReasonRetriesExhausted,
			wantDLQAttempts: maxAttempts,
			wantCommitted:   true,
			wantBackoffs:    []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
		},
		{
			name:    "failed DLQ publish means no commit",
			payload: validEventJSON,
			storeFn: func(_ int, _ domain.TransactionEvent) (bool, error) {
				return false, domain.ErrInvalidEvent
			},
			dlqErr:          errDLQDown,
			wantErr:         true,
			wantStoreCalls:  1,
			wantDLQCount:    1,
			wantDLQReason:   ReasonPoison,
			wantDLQAttempts: 1,
			wantCommitted:   false,
			wantBackoffs:    nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &fakeFetcher{}
			st := &fakeStore{fn: tc.storeFn}
			dlq := &fakeDLQ{err: tc.dlqErr}
			var backoffs []time.Duration

			cfg := Config{
				MaxAttempts: maxAttempts,
				BaseBackoff: 10 * time.Millisecond,
				MaxBackoff:  25 * time.Millisecond,
				Sleep: func(_ context.Context, d time.Duration) error {
					backoffs = append(backoffs, d)
					return nil
				},
				Now: func() time.Time {
					return time.Date(2026, 11, 27, 1, 30, 0, 0, time.UTC)
				},
			}

			w := NewWorker(fetcher, st, dlq, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
			msg := kafka.Message{
				Topic:     "transactions",
				Partition: 1,
				Offset:    42,
				Key:       []byte("cust-1"),
				Value:     tc.payload(t),
			}

			err := w.handleMessage(context.Background(), msg)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.wantStoreCalls, st.calls)
			require.Len(t, dlq.records, tc.wantDLQCount)
			if tc.wantDLQCount > 0 {
				require.Equal(t, tc.wantDLQReason, dlq.records[0].Reason)
				require.Equal(t, tc.wantDLQAttempts, dlq.records[0].Attempts)
				require.Equal(t, "transactions", dlq.records[0].OriginalTopic)
				require.Equal(t, 1, dlq.records[0].Partition)
				require.Equal(t, int64(42), dlq.records[0].Offset)
			}

			if tc.wantCommitted {
				require.Len(t, fetcher.committed, 1)
			} else {
				require.Empty(t, fetcher.committed)
			}

			require.Equal(t, tc.wantBackoffs, backoffs)
		})
	}
}
