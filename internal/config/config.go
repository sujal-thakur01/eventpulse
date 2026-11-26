package config

import (
	"os"
	"strings"
)

// Config holds runtime configuration loaded from environment variables.
type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaDLQ     string
	KafkaGroup   string
}

// Load reads configuration from environment variables with sensible local defaults.
func Load() Config {
	return Config{
		HTTPAddr:     envOr("HTTP_ADDR", ":8080"),
		DatabaseURL:  envOr("DATABASE_URL", "postgres://eventpulse:eventpulse@localhost:5432/eventpulse?sslmode=disable"),
		KafkaBrokers: strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   envOr("KAFKA_TOPIC", "transactions"),
		KafkaDLQ:     envOr("KAFKA_DLQ_TOPIC", "transactions.dlq"),
		KafkaGroup:   envOr("KAFKA_GROUP", "eventpulse-consumer"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
