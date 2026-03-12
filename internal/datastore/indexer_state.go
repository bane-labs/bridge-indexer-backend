package datastore

import (
	"fmt"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// Key: "indexer_state:{chain}"

func indexerStateKey(chain string) string {
	return fmt.Sprintf("indexer_state:%s", chain)
}

// SaveIndexerState persists the indexer checkpoint for a chain.
func (s *Store) SaveIndexerState(state *types.IndexerState) error {
	return s.Set(indexerStateKey(state.Chain), state)
}

// GetIndexerState retrieves the indexer checkpoint for a chain.
func (s *Store) GetIndexerState(chain string) (*types.IndexerState, error) {
	var state types.IndexerState
	if err := s.Get(indexerStateKey(chain), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// SaveIndexerStateWithOperations atomically saves an indexer checkpoint together
// with one or more bridge operations. This satisfies the Metis requirement that
// checkpoint and operation data are written in the same transaction.
func (s *Store) SaveIndexerStateWithOperations(state *types.IndexerState, ops []*types.BridgeOperation) error {
	writeOps := make([]WriteOp, 0, 1+len(ops)*2)

	// Checkpoint.
	writeOps = append(writeOps, WriteOp{
		Key:   indexerStateKey(state.Chain),
		Value: state,
	})

	// Operations + status indexes.
	for _, op := range ops {
		opKey := operationKey(op.BridgeType, op.Direction, op.Nonce)
		idxKey := statusIndexKey(op.Status, op.BridgeType, op.Direction, op.Nonce)
		writeOps = append(writeOps,
			WriteOp{Key: opKey, Value: op},
			WriteOp{Key: idxKey, Value: []byte{}},
		)
	}

	return s.BatchWrite(writeOps)
}

// SaveIndexerStateWithOperationsAndExtra atomically saves an indexer checkpoint,
// bridge operations, and additional write operations (e.g., bridge state updates)
// in a single batch.
func (s *Store) SaveIndexerStateWithOperationsAndExtra(state *types.IndexerState, ops []*types.BridgeOperation, extra []WriteOp) error {
	writeOps := make([]WriteOp, 0, 1+len(ops)*2+len(extra))

	writeOps = append(writeOps, WriteOp{
		Key:   indexerStateKey(state.Chain),
		Value: state,
	})

	for _, op := range ops {
		opKey := operationKey(op.BridgeType, op.Direction, op.Nonce)
		idxKey := statusIndexKey(op.Status, op.BridgeType, op.Direction, op.Nonce)
		writeOps = append(writeOps,
			WriteOp{Key: opKey, Value: op},
			WriteOp{Key: idxKey, Value: []byte{}},
		)
	}

	writeOps = append(writeOps, extra...)

	return s.BatchWrite(writeOps)
}
