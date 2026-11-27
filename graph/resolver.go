package graph

import (
	"context"

	"github.com/sujal-thakur01/eventpulse/internal/domain"
	"github.com/sujal-thakur01/eventpulse/internal/store"
)

// Store defines the persistence queries required by GraphQL resolvers.
type Store interface {
	GetCustomerBalance(ctx context.Context, customerID string) (*domain.CustomerBalance, error)
	ListTransactionHistory(ctx context.Context, customerID string, first *int, after *string) (store.TransactionPage, error)
}

// Resolver serves as dependency injection for GraphQL resolvers.
type Resolver struct {
	Store Store
}
