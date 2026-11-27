//go:build integration

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/sujal-thakur01/eventpulse/internal/domain"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func createTopicAndWait(t *testing.T, ctx context.Context, broker, topic string, numPartitions int) {
	t.Helper()

	conn, err := kafka.Dial("tcp", broker)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	require.NoError(t, err)

	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)
	defer func() { _ = ctrlConn.Close() }()

	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) == numPartitions
	}, 15*time.Second, 200*time.Millisecond, "topic partitions did not become ready")
}

func TestProducerGuarantee1Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := kafkacontainer.Run(ctx, "confluentinc/confluent-local:7.5.0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ctr.Terminate(context.Background())
	})

	brokers, err := ctr.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	const (
		topic             = "transactions-guarantee-1"
		numPartitions     = 6
		numCustomers      = 10
		eventsPerCustomer = 20
	)

	createTopicAndWait(t, ctx, brokers[0], topic, numPartitions)

	producer := NewProducer(brokers, topic)
	t.Cleanup(func() {
		_ = producer.Close()
	})

	sentOrder := make(map[string][]string, numCustomers)
	baseTime := time.Date(2026, 11, 27, 1, 0, 0, 0, time.UTC)

	for seq := range eventsPerCustomer {
		for c := range numCustomers {
			customerID := fmt.Sprintf("cust-%d", c)
			idKey := uuid.NewString()
			ev := domain.TransactionEvent{
				IdempotencyKey: idKey,
				CustomerID:     customerID,
				EventType:      domain.EventTypePurchase,
				AmountCents:    int64((seq + 1) * 100),
				OccurredAt:     baseTime.Add(time.Duration(seq) * time.Millisecond),
			}
			sentOrder[customerID] = append(sentOrder[customerID], idKey)
			require.NoError(t, producer.Publish(ctx, ev))
		}
	}

	partitionsSeen := make(map[string]map[int]bool, numCustomers)
	receivedOrder := make(map[string][]string, numCustomers)
	totalRead := 0

	for p := range numPartitions {
		pConn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, p)
		require.NoError(t, err)
		first, last, err := pConn.ReadOffsets()
		require.NoError(t, err)
		_ = pConn.Close()

		if last <= first {
			continue
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p,
			MinBytes:  1,
			MaxBytes:  10e6,
		})
		require.NoError(t, reader.SetOffset(first))

		for offset := first; offset < last; offset++ {
			msg, err := reader.ReadMessage(ctx)
			require.NoError(t, err)

			var ev domain.TransactionEvent
			require.NoError(t, json.Unmarshal(msg.Value, &ev))

			if partitionsSeen[ev.CustomerID] == nil {
				partitionsSeen[ev.CustomerID] = make(map[int]bool)
			}
			partitionsSeen[ev.CustomerID][p] = true
			receivedOrder[ev.CustomerID] = append(receivedOrder[ev.CustomerID], ev.IdempotencyKey)
			totalRead++
		}
		_ = reader.Close()
	}

	require.Equal(t, numCustomers*eventsPerCustomer, totalRead)

	for c := range numCustomers {
		customerID := fmt.Sprintf("cust-%d", c)
		require.Len(t, partitionsSeen[customerID], 1, "customer %s landed on multiple partitions: %v", customerID, partitionsSeen[customerID])
		require.Equal(t, sentOrder[customerID], receivedOrder[customerID], "customer %s events out of sent order", customerID)
	}
}
