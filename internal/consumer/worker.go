package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

const (
	ReasonPoison = "poison"

	defaultMaxAttempts = 6
	defaultBaseBackoff = 100 * time.Millisecond
	defaultMaxBackoff  = 5 * time.Second
)

// MessageFetcher reads messages from Kafka and commits offsets explicitly.
// Implementations must use FetchMessage + CommitMessages, never ReadMessage.
type MessageFetcher interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// EventStore persists validated transaction events atomically.
type EventStore interface {
	ProcessTransaction(ctx context.Context, ev domain.TransactionEvent) (bool, error)
}

// DLQRecord captures the original message metadata and failure context for the DLQ.
type DLQRecord struct {
	OriginalTopic string    `json:"original_topic"`
	Partition     int       `json:"partition"`
	Offset        int64     `json:"offset"`
	Key           string    `json:"key,omitempty"`
	Payload       string    `json:"payload"`
	Reason        string    `json:"reason"`
	Error         string    `json:"error"`
	Attempts      int       `json:"attempts"`
	FailedAt      time.Time `json:"failed_at"`
}

// DLQWriter publishes failed messages to the dead-letter queue.
type DLQWriter interface {
	PublishDLQ(ctx context.Context, key []byte, record DLQRecord) error
}

// Config configures retry and backoff behavior for the consumer worker.
type Config struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Sleep       func(ctx context.Context, d time.Duration) error
	Jitter      func(d time.Duration) time.Duration
	Now         func() time.Time
}

func (c Config) withDefaults() Config {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = defaultBaseBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = defaultMaxBackoff
	}
	if c.Sleep == nil {
		c.Sleep = sleepWithContext
	}
	if c.Jitter == nil {
		c.Jitter = equalJitter
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Worker consumes transaction events with at-least-once semantics.
type Worker struct {
	fetcher MessageFetcher
	store   EventStore
	dlq     DLQWriter
	logger  *slog.Logger
	cfg     Config
}

// NewWorker creates a Worker with injected dependencies and no global state.
func NewWorker(fetcher MessageFetcher, store EventStore, dlq DLQWriter, logger *slog.Logger, cfg Config) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		fetcher: fetcher,
		store:   store,
		dlq:     dlq,
		logger:  logger,
		cfg:     cfg.withDefaults(),
	}
}

// Run continuously fetches, processes, and commits messages until ctx is canceled.
// It never fetches the next message while the current message is unresolved.
func (w *Worker) Run(ctx context.Context) error {
	for {
		msg, err := w.fetcher.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		for attempt := 1; ; attempt++ {
			err := w.handleMessage(ctx, msg)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("message handling failed without commit; retrying same message",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"attempt", attempt,
				"error", err,
			)
			if sleepErr := w.cfg.Sleep(ctx, w.backoffDuration(attempt)); sleepErr != nil {
				return nil
			}
		}
	}
}

func (w *Worker) handleMessage(ctx context.Context, msg kafka.Message) error {
	var ev domain.TransactionEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		decodeErr := fmt.Errorf("%w: %w", ErrDecode, err)
		return w.sendToDLQAndCommit(ctx, msg, ReasonPoison, decodeErr, 1)
	}

	var lastErr error
	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		created, err := w.store.ProcessTransaction(ctx, ev)
		if err == nil {
			if !created {
				w.logger.Info("duplicate_skipped",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"customer_id", ev.CustomerID,
					"idempotency_key", ev.IdempotencyKey,
				)
			}
			if commitErr := w.fetcher.CommitMessages(ctx, msg); commitErr != nil {
				return fmt.Errorf("commit offset: %w", commitErr)
			}
			return nil
		}

		if isPoison(err) {
			return w.sendToDLQAndCommit(ctx, msg, ReasonPoison, err, attempt)
		}

		lastErr = err
		if attempt < w.cfg.MaxAttempts {
			if sleepErr := w.cfg.Sleep(ctx, w.backoffDuration(attempt)); sleepErr != nil {
				return fmt.Errorf("retry backoff canceled: %w", sleepErr)
			}
		}
	}

	return fmt.Errorf("process transaction after %d attempts: %w", w.cfg.MaxAttempts, lastErr)
}

func (w *Worker) sendToDLQAndCommit(ctx context.Context, msg kafka.Message, reason string, cause error, attempts int) error {
	record := DLQRecord{
		OriginalTopic: msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Key:           string(msg.Key),
		Payload:       string(msg.Value),
		Reason:        reason,
		Error:         cause.Error(),
		Attempts:      attempts,
		FailedAt:      w.cfg.Now().UTC(),
	}

	if err := w.dlq.PublishDLQ(ctx, msg.Key, record); err != nil {
		return fmt.Errorf("publish dlq: %w", err)
	}

	if err := w.fetcher.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit offset after dlq: %w", err)
	}
	return nil
}

func (w *Worker) backoffDuration(attempt int) time.Duration {
	var d time.Duration
	if attempt <= 1 {
		d = min(w.cfg.BaseBackoff, w.cfg.MaxBackoff)
	} else {
		d = w.cfg.BaseBackoff
		for i := 1; i < attempt; i++ {
			if d >= w.cfg.MaxBackoff/2 {
				d = w.cfg.MaxBackoff
				break
			}
			d *= 2
		}
		d = min(d, w.cfg.MaxBackoff)
	}
	return w.cfg.Jitter(d)
}

func equalJitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + rand.N(d-half+1)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
