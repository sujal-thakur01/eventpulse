package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		n            = flag.Int("n", 1000, "number of unique valid events to send")
		concurrency  = flag.Int("concurrency", 20, "number of concurrent HTTP workers")
		customers    = flag.Int("customers", 100, "number of distinct customers")
		targetURL    = flag.String("url", "http://localhost:8080/events", "POST /events endpoint URL")
		dsn          = flag.String("dsn", envOr("DATABASE_URL", "postgres://eventpulse:eventpulse@localhost:5432/eventpulse?sslmode=disable"), "PostgreSQL connection string")
		pollInterval = flag.Duration("poll-interval", 50*time.Millisecond, "interval to poll processed_events row count")
		timeout      = flag.Duration("timeout", 2*time.Minute, "timeout waiting for consumer to process all events")
	)
	flag.Parse()

	if *n <= 0 {
		return fmt.Errorf("-n must be > 0, got %d", *n)
	}
	if *concurrency <= 0 {
		return fmt.Errorf("-concurrency must be > 0, got %d", *concurrency)
	}
	if *customers <= 0 {
		return fmt.Errorf("-customers must be > 0, got %d", *customers)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	initialCount, err := countProcessedEvents(ctx, pool)
	if err != nil {
		return fmt.Errorf("query initial processed_events count: %w", err)
	}

	payloads, err := buildPayloads(*n, *customers)
	if err != nil {
		return fmt.Errorf("build event payloads: %w", err)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency,
			MaxIdleConnsPerHost: *concurrency,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	latencies := make([]time.Duration, *n)
	var nextIdx atomic.Int64
	var succeeded atomic.Int64
	var failed atomic.Int64

	sendStart := time.Now()
	var wg sync.WaitGroup
	workers := min(*concurrency, *n)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(nextIdx.Add(1) - 1)
				if idx >= *n {
					return
				}
				lat, err := sendEvent(ctx, client, *targetURL, payloads[idx])
				latencies[idx] = lat
				if err != nil {
					failed.Add(1)
					continue
				}
				succeeded.Add(1)
			}
		}()
	}
	wg.Wait()
	sendElapsed := time.Since(sendStart)

	okCount := succeeded.Load()
	failCount := failed.Load()
	targetCount := initialCount + okCount

	if err := waitForConsumer(ctx, pool, targetCount, *pollInterval); err != nil {
		return fmt.Errorf("wait for consumer: %w", err)
	}
	totalElapsed := time.Since(sendStart)

	slices.Sort(latencies)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	httpRPS := float64(okCount) / sendElapsed.Seconds()
	e2eRPS := float64(okCount) / totalElapsed.Seconds()

	fmt.Printf("events_sent=%d succeeded=%d failed=%d concurrency=%d customers=%d\n",
		*n, okCount, failCount, *concurrency, *customers)
	fmt.Printf("processed_events_before=%d processed_events_after=%d\n",
		initialCount, targetCount)
	fmt.Printf("http_duration=%s http_events_per_sec=%.2f\n",
		sendElapsed.Round(time.Millisecond), httpRPS)
	fmt.Printf("e2e_duration=%s e2e_events_per_sec=%.2f\n",
		totalElapsed.Round(time.Millisecond), e2eRPS)
	fmt.Printf("http_latency_p50=%s http_latency_p95=%s http_latency_p99=%s\n",
		p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))

	if failCount > 0 {
		return fmt.Errorf("%d HTTP requests failed", failCount)
	}
	return nil
}

func buildPayloads(n, customers int) ([][]byte, error) {
	payloads := make([][]byte, n)
	now := time.Now().UTC()
	for i := range n {
		ev := domain.TransactionEvent{
			IdempotencyKey: uuid.NewString(),
			CustomerID:     fmt.Sprintf("loadgen-cust-%03d", i%customers),
			EventType:      domain.EventTypePurchase,
			AmountCents:    int64(100 + (i%100)*10),
			OccurredAt:     now,
		}
		b, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		payloads[i] = b
	}
	return payloads, nil
}

func sendEvent(ctx context.Context, client *http.Client, targetURL string, payload []byte) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return time.Since(start), err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	lat := time.Since(start)

	if resp.StatusCode != http.StatusAccepted {
		return lat, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return lat, nil
}

func countProcessedEvents(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var count int64
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM processed_events").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func waitForConsumer(ctx context.Context, pool *pgxpool.Pool, targetCount int64, pollInterval time.Duration) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		count, err := countProcessedEvents(ctx, pool)
		if err != nil {
			return err
		}
		if count >= targetCount {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out at %d/%d processed_events: %w", count, targetCount, ctx.Err())
		case <-ticker.C:
		}
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
