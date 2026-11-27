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
		rate         = flag.Float64("rate", 0, "target events per second (0 for unlimited)")
		concurrency  = flag.Int("concurrency", 20, "number of concurrent HTTP workers")
		customers    = flag.Int("customers", 100, "number of distinct customers")
		targetURL    = flag.String("url", "http://localhost:8080/events", "POST /events endpoint URL")
		dsn          = flag.String("dsn", envOr("DATABASE_URL", "postgres://eventpulse:eventpulse@localhost:5432/eventpulse?sslmode=disable"), "PostgreSQL connection string")
		pollInterval = flag.Duration("poll-interval", 50*time.Millisecond, "interval to poll processed_events row count")
		timeout      = flag.Duration("timeout", 2*time.Minute, "timeout waiting for consumer to process all events")
		expectedOut  = flag.String("expected-out", "", "optional path to write expected points per customer as JSON")
	)
	flag.Parse()

	if *n <= 0 {
		return fmt.Errorf("-n must be > 0, got %d", *n)
	}
	if *rate < 0 {
		return fmt.Errorf("-rate must be >= 0, got %v", *rate)
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

	payloads, expectedPoints, err := buildPayloads(*n, *customers)
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

	limiter := newRateLimiter(*rate)
	latencies := make([]time.Duration, *n)
	var nextIdx atomic.Int64
	var succeeded atomic.Int64
	var failed atomic.Int64
	var retries atomic.Int64

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
				if err := limiter.Wait(ctx); err != nil {
					failed.Add(1)
					return
				}
				lat, attempts, err := sendEventWithRetry(ctx, client, *targetURL, payloads[idx])
				if attempts > 1 {
					retries.Add(int64(attempts - 1))
				}
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

	if *expectedOut != "" {
		if err := writeExpectedPoints(*expectedOut, expectedPoints); err != nil {
			return fmt.Errorf("write expected points: %w", err)
		}
	}

	slices.Sort(latencies)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	httpRPS := float64(okCount) / sendElapsed.Seconds()
	e2eRPS := float64(okCount) / totalElapsed.Seconds()

	fmt.Printf("events_sent=%d succeeded=%d failed=%d retries=%d rate=%.2f concurrency=%d customers=%d\n",
		*n, okCount, failCount, retries.Load(), *rate, *concurrency, *customers)
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

type rateLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newRateLimiter(rps float64) *rateLimiter {
	if rps <= 0 {
		return nil
	}
	interval := time.Duration(float64(time.Second) / rps)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	return &rateLimiter{
		interval: interval,
		next:     time.Now(),
	}
}

func (l *rateLimiter) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now.Add(-50 * time.Millisecond)) {
		l.next = now
	}
	target := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()

	wait := time.Until(target)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func buildPayloads(n, customers int) ([][]byte, map[string]int64, error) {
	payloads := make([][]byte, n)
	expectedPoints := make(map[string]int64, customers)
	now := time.Now().UTC()
	for i := range n {
		ev := domain.TransactionEvent{
			IdempotencyKey: uuid.NewString(),
			CustomerID:     fmt.Sprintf("loadgen-cust-%03d", i%customers),
			EventType:      domain.EventTypePurchase,
			AmountCents:    int64(100 + (i%100)*10),
			OccurredAt:     now,
		}
		pts, err := domain.CalculatePoints(ev.EventType, ev.AmountCents)
		if err != nil {
			return nil, nil, err
		}
		expectedPoints[ev.CustomerID] += pts

		b, err := json.Marshal(ev)
		if err != nil {
			return nil, nil, err
		}
		payloads[i] = b
	}
	return payloads, expectedPoints, nil
}

func writeExpectedPoints(path string, expected map[string]int64) error {
	data, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func sendEventWithRetry(ctx context.Context, client *http.Client, targetURL string, payload []byte) (time.Duration, int, error) {
	backoff := 50 * time.Millisecond
	const maxBackoff = 500 * time.Millisecond

	for attempt := 1; ; attempt++ {
		lat, err := sendEventOnce(ctx, client, targetURL, payload)
		if err == nil {
			return lat, attempt, nil
		}
		if ctx.Err() != nil {
			return lat, attempt, ctx.Err()
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lat, attempt, ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func sendEventOnce(ctx context.Context, client *http.Client, targetURL string, payload []byte) (time.Duration, error) {
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
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
