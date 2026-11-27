package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/segmentio/kafka-go"
	"github.com/sujal-thakur01/eventpulse/graph"
	"github.com/sujal-thakur01/eventpulse/internal/config"
	"github.com/sujal-thakur01/eventpulse/internal/consumer"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
	"github.com/sujal-thakur01/eventpulse/internal/queue"
	"github.com/sujal-thakur01/eventpulse/internal/store"
)

type eventPublisher interface {
	Publish(ctx context.Context, ev domain.TransactionEvent) error
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		logger.Error("failed to apply migrations", "error", err)
		os.Exit(1)
	}
	logger.Info("database migrations applied")

	producer := queue.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("failed to close kafka producer", "error", err)
		}
	}()

	dlqWriter := queue.NewDLQWriter(cfg.KafkaBrokers, cfg.KafkaDLQ)
	defer func() {
		if err := dlqWriter.Close(); err != nil {
			logger.Error("failed to close kafka dlq writer", "error", err)
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		GroupID:        cfg.KafkaGroup,
		Topic:          cfg.KafkaTopic,
		CommitInterval: 0,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Error("failed to close kafka reader", "error", err)
		}
	}()

	worker := consumer.NewWorker(reader, db, dlqWriter, logger, consumer.Config{})
	workerErrCh := make(chan error, 1)
	go func() {
		logger.Info("consumer worker started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)
		workerErrCh <- worker.Run(ctx)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           newMux(producer, db, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http server shutdown error", "error", err)
		}
		if err := <-workerErrCh; err != nil {
			logger.Error("consumer worker error", "error", err)
		}
	case err := <-errCh:
		if err != nil {
			logger.Error("http server error", "error", err)
			stop()
			<-workerErrCh
			os.Exit(1)
		}
	case err := <-workerErrCh:
		if err != nil {
			logger.Error("consumer worker error", "error", err)
			stop()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
			os.Exit(1)
		}
	}
}

func newMux(pub eventPublisher, st graph.Store, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var ev domain.TransactionEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, "invalid json payload", http.StatusBadRequest)
			return
		}
		if err := ev.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := pub.Publish(r.Context(), ev); err != nil {
			logger.Error("failed to publish event", "customer_id", ev.CustomerID, "error", err)
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	gqlSrv := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{Store: st},
	}))
	mux.Handle("POST /graphql", gqlSrv)
	mux.Handle("GET /playground", playground.Handler("EventPulse GraphQL", "/graphql"))

	return mux
}
