package consumer

import (
	"bytes"
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
			name:    "transient never succeeds returns error without DLQ or commit after max attempts",
			payload: validEventJSON,
			storeFn: func(_ int, _ domain.TransactionEvent) (bool, error) {
				return false, errTransient
			},
			wantErr:        true,
			wantStoreCalls: maxAttempts,
			wantDLQCount:   0,
			wantCommitted:  false,
			wantBackoffs:   []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
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
				Jitter: func(d time.Duration) time.Duration {
					return d
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

type runFetcher struct {
	msgs       []kafka.Message
	idx        int
	committed  []kafka.Message
	events     *[]string
	cancelFunc context.CancelFunc
}

func (f *runFetcher) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.idx >= len(f.msgs) {
		f.cancelFunc()
		return kafka.Message{}, context.Canceled
	}
	msg := f.msgs[f.idx]
	f.idx++
	*f.events = append(*f.events, fmt.Sprintf("fetch:%d", msg.Offset))
	return msg, nil
}

func (f *runFetcher) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, m := range msgs {
		*f.events = append(*f.events, fmt.Sprintf("commit:%d", m.Offset))
	}
	f.committed = append(f.committed, msgs...)
	return nil
}

type flakyDLQ struct {
	failuresRemaining int
	err               error
	records           []DLQRecord
	events            *[]string
}

func (d *flakyDLQ) PublishDLQ(_ context.Context, _ []byte, record DLQRecord) error {
	if d.failuresRemaining > 0 {
		d.failuresRemaining--
		*d.events = append(*d.events, fmt.Sprintf("dlq_fail:%d", record.Offset))
		return d.err
	}
	*d.events = append(*d.events, fmt.Sprintf("dlq_ok:%d", record.Offset))
	d.records = append(d.records, record)
	return nil
}

func TestWorkerRunRetriesSameMessageUntilCommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var events []string
	msg1 := kafka.Message{
		Topic:     "transactions",
		Partition: 0,
		Offset:    10,
		Key:       []byte("cust-1"),
		Value:     []byte("{not-json"),
	}
	msg2 := kafka.Message{
		Topic:     "transactions",
		Partition: 0,
		Offset:    11,
		Key:       []byte("cust-2"),
		Value:     validEventJSON(t),
	}

	fetcher := &runFetcher{
		msgs:       []kafka.Message{msg1, msg2},
		events:     &events,
		cancelFunc: cancel,
	}
	st := &fakeStore{
		fn: func(_ int, _ domain.TransactionEvent) (bool, error) {
			events = append(events, "store_ok")
			return true, nil
		},
	}
	dlq := &flakyDLQ{
		failuresRemaining: 2,
		err:               errors.New("dlq broker down"),
		events:            &events,
	}

	var backoffs []time.Duration
	cfg := Config{
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		Sleep: func(_ context.Context, d time.Duration) error {
			backoffs = append(backoffs, d)
			return nil
		},
		Jitter: func(d time.Duration) time.Duration {
			return d
		},
	}

	w := NewWorker(fetcher, st, dlq, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	err := w.Run(ctx)
	require.NoError(t, err)

	// msg1 must be fetched once, fail DLQ twice, succeed DLQ on 3rd try, and be committed
	// BEFORE msg2 is ever fetched.
	require.Equal(t, []string{
		"fetch:10",
		"dlq_fail:10",
		"dlq_fail:10",
		"dlq_ok:10",
		"commit:10",
		"fetch:11",
		"store_ok",
		"commit:11",
	}, events)

	require.Len(t, fetcher.committed, 2)
	require.Equal(t, int64(10), fetcher.committed[0].Offset)
	require.Equal(t, int64(11), fetcher.committed[1].Offset)
	require.Len(t, dlq.records, 1)
	require.Equal(t, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}, backoffs)
}

func TestWorkerDefaultsAndJitter(t *testing.T) {
	w := NewWorker(&fakeFetcher{}, &fakeStore{}, &fakeDLQ{}, nil, Config{})
	require.Equal(t, 6, w.cfg.MaxAttempts)
	require.Equal(t, 5*time.Second, w.cfg.MaxBackoff)

	for attempt := 1; attempt <= 6; attempt++ {
		d := w.backoffDuration(attempt)
		require.Greater(t, d, time.Duration(0))
		require.LessOrEqual(t, d, 5*time.Second)
	}
}

func TestWorkerTransientErrorNeverReachesDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var events []string
	msg1 := kafka.Message{
		Topic:     "transactions",
		Partition: 0,
		Offset:    20,
		Key:       []byte("cust-1"),
		Value:     validEventJSON(t),
	}
	msg2 := kafka.Message{
		Topic:     "transactions",
		Partition: 0,
		Offset:    21,
		Key:       []byte("cust-2"),
		Value:     validEventJSON(t),
	}

	fetcher := &runFetcher{
		msgs:       []kafka.Message{msg1, msg2},
		events:     &events,
		cancelFunc: cancel,
	}

	errTransient := errors.New("postgres unavailable")
	const maxAttempts = 3
	const transientFailures = 7 // Spans multiple handleMessage cycles in Run (> maxAttempts).

	st := &fakeStore{
		fn: func(attempt int, _ domain.TransactionEvent) (bool, error) {
			if attempt <= transientFailures {
				events = append(events, fmt.Sprintf("store_err:%d", attempt))
				return false, errTransient
			}
			events = append(events, fmt.Sprintf("store_ok:%d", attempt))
			return true, nil
		},
	}
	dlq := &fakeDLQ{}

	cfg := Config{
		MaxAttempts: maxAttempts,
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  20 * time.Millisecond,
		Sleep: func(_ context.Context, _ time.Duration) error {
			return nil
		},
		Jitter: func(d time.Duration) time.Duration {
			return d
		},
	}

	w := NewWorker(fetcher, st, dlq, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	err := w.Run(ctx)
	require.NoError(t, err)

	// Transient store errors must never be sent to the DLQ, even after exceeding MaxAttempts.
	require.Empty(t, dlq.records)
	require.Equal(t, []string{
		"fetch:20",
		"store_err:1",
		"store_err:2",
		"store_err:3",
		"store_err:4",
		"store_err:5",
		"store_err:6",
		"store_err:7",
		"store_ok:8",
		"commit:20",
		"fetch:21",
		"store_ok:9",
		"commit:21",
	}, events)
	require.Len(t, fetcher.committed, 2)
}

func TestWorkerLogsDuplicateSkipped(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	fetcher := &fakeFetcher{}
	st := &fakeStore{
		fn: func(_ int, _ domain.TransactionEvent) (bool, error) {
			return false, nil
		},
	}
	dlq := &fakeDLQ{}

	w := NewWorker(fetcher, st, dlq, logger, Config{})
	msg := kafka.Message{
		Topic:     "transactions",
		Partition: 2,
		Offset:    99,
		Key:       []byte("cust-1"),
		Value:     validEventJSON(t),
	}

	err := w.handleMessage(context.Background(), msg)
	require.NoError(t, err)
	require.Contains(t, logBuf.String(), `"msg":"duplicate_skipped"`)
	require.Empty(t, dlq.records)
	require.Len(t, fetcher.committed, 1)
}
