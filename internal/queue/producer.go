package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

// Producer publishes transaction events partitioned by customer ID.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Kafka producer configured with Murmur2 partitioning and RequireAll acks.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Murmur2Balancer{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

// Publish serializes and writes a transaction event keyed by customer_id.
func (p *Producer) Publish(ctx context.Context, ev domain.TransactionEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(ev.CustomerID),
		Value: payload,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
