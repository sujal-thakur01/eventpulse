package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sujal-thakur01/eventpulse/internal/consumer"
)

// DLQWriter publishes failed event records to a dead-letter Kafka topic.
type DLQWriter struct {
	writer *kafka.Writer
}

// NewDLQWriter creates a Kafka writer for publishing DLQ records to the specified topic.
func NewDLQWriter(brokers []string, topic string) *DLQWriter {
	return &DLQWriter{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Murmur2Balancer{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// PublishDLQ serializes record as JSON and writes it to the DLQ topic with the original key.
func (w *DLQWriter) PublishDLQ(ctx context.Context, key []byte, record consumer.DLQRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal dlq record: %w", err)
	}

	msg := kafka.Message{
		Key:   key,
		Value: payload,
	}
	if err := w.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write dlq message: %w", err)
	}
	return nil
}

// Close closes the underlying Kafka writer.
func (w *DLQWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	return w.writer.Close()
}
