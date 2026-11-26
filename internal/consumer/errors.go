package consumer

import (
	"errors"

	"github.com/sujal-thakur01/eventpulse/internal/domain"
)

// ErrDecode indicates a Kafka message payload could not be decoded as JSON.
var ErrDecode = errors.New("decode event")

// isPoison returns true for unrecoverable errors that must go straight to the DLQ
// without retrying: decode failures, domain validation failures, and insufficient balance.
func isPoison(err error) bool {
	return errors.Is(err, ErrDecode) ||
		errors.Is(err, domain.ErrInvalidEvent) ||
		errors.Is(err, domain.ErrInsufficientBalance)
}
