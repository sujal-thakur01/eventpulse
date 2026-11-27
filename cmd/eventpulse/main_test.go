package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

type stubPublisher struct {
	published []domain.TransactionEvent
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, ev domain.TransactionEvent) error {
	if s.err != nil {
		return s.err
	}
	s.published = append(s.published, ev)
	return nil
}

func TestPostEventsHandler(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("valid event returns 202 and publishes", func(t *testing.T) {
		pub := &stubPublisher{}
		mux := newMux(pub, logger)

		body := `{"idempotency_key":"` + uuid.NewString() + `","customer_id":"cust-1","event_type":"purchase","amount_cents":1500,"occurred_at":"2026-11-27T01:00:00Z"}`
		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusAccepted, rec.Code)
		require.Len(t, pub.published, 1)
		require.Equal(t, "cust-1", pub.published[0].CustomerID)
		require.Equal(t, int64(1500), pub.published[0].AmountCents)
	})

	t.Run("invalid json returns 400", func(t *testing.T) {
		pub := &stubPublisher{}
		mux := newMux(pub, logger)

		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString("{bad-json"))
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Empty(t, pub.published)
	})

	t.Run("validation failure returns 400", func(t *testing.T) {
		pub := &stubPublisher{}
		mux := newMux(pub, logger)

		body := `{"idempotency_key":"not-a-uuid","customer_id":"cust-1","event_type":"purchase","amount_cents":1500,"occurred_at":"2026-11-27T01:00:00Z"}`
		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Empty(t, pub.published)
	})

	t.Run("publish failure returns 500", func(t *testing.T) {
		pub := &stubPublisher{err: errors.New("kafka unavailable")}
		mux := newMux(pub, logger)

		body := `{"idempotency_key":"` + uuid.NewString() + `","customer_id":"cust-1","event_type":"purchase","amount_cents":1500,"occurred_at":"2026-11-27T01:00:00Z"}`
		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}
