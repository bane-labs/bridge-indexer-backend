package indexer

import (
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// EventStorer is the storage interface required by event processors.
type EventStorer interface {
	UpdateOperationCompletion(bt types.BridgeType, dir types.Direction, nonce uint64, destBlock uint64, destTxHash string, completionTime time.Time) error
}

// Compile-time check: *datastore.OperationStore satisfies EventStorer.
var _ EventStorer = (*datastore.OperationStore)(nil)
