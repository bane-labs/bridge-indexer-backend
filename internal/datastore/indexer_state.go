package datastore

import (
	"encoding/json"
	"fmt"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	badger "github.com/dgraph-io/badger/v4"
)

// Key: "indexer_state:{chain}"

func indexerStateKey(chain string) string {
	return fmt.Sprintf("indexer_state:%s", chain)
}

// SaveNeoXLifecycleCheckpoint commits decoded evidence and the normal indexing
// checkpoint together. A rejected observation cannot advance the checkpoint.
func (s *Store) SaveNeoXLifecycleCheckpoint(state *types.IndexerState, ops []*types.BridgeOperation, extra []WriteOp, observations []LifecycleObservation) error {
	if state.Chain != "neo_x" {
		return fmt.Errorf("unexpected lifecycle checkpoint chain %q", state.Chain)
	}
	writeOps := []WriteOp{{Key: indexerStateKey(state.Chain), Value: state}}
	for _, op := range ops {
		normalized, err := s.reconcileDeferredCompletion(op, &writeOps)
		if err != nil {
			return err
		}
		if normalized.Status == types.StatusPending && s.operationAlreadyAdvanced(operationKeyForOp(normalized)) {
			continue
		}
		writeOps = append(writeOps, WriteOp{Key: operationKeyForOp(normalized), Value: normalized},
			WriteOp{Key: statusIndexKeyForOp(normalized.Status, normalized), Value: []byte{}})
	}
	writeOps = append(writeOps, extra...)
	return retryLifecycleWrite(s.db, func(txn *badger.Txn) error {
		if err := writeLifecycleObservations(txn, observations); err != nil {
			return err
		}
		for _, op := range writeOps {
			if op.Value == nil {
				if err := txn.Delete([]byte(op.Key)); err != nil {
					return err
				}
				continue
			}
			data, err := json.Marshal(op.Value)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(op.Key), data); err != nil {
				return err
			}
		}
		return nil
	})
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
//
// Operations that already exist with a more advanced status (completed/stuck)
// are skipped to prevent re-indexing from overwriting completion data.
func (s *Store) SaveIndexerStateWithOperations(state *types.IndexerState, ops []*types.BridgeOperation) error {
	writeOps := make([]WriteOp, 0, 1+len(ops)*4)

	// Checkpoint.
	writeOps = append(writeOps, WriteOp{
		Key:   indexerStateKey(state.Chain),
		Value: state,
	})

	// Operations + status indexes.
	for _, op := range ops {
		normalizedOp, err := s.reconcileDeferredCompletion(op, &writeOps)
		if err != nil {
			return err
		}
		opKey := operationKeyForOp(normalizedOp)
		if normalizedOp.Status == types.StatusPending && s.operationAlreadyAdvanced(opKey) {
			continue
		}
		idxKey := statusIndexKeyForOp(normalizedOp.Status, normalizedOp)
		writeOps = append(writeOps,
			WriteOp{Key: opKey, Value: normalizedOp},
			WriteOp{Key: idxKey, Value: []byte{}},
		)
	}

	return s.BatchWrite(writeOps)
}

// SaveIndexerStateWithOperationsAndExtra atomically saves an indexer checkpoint,
// bridge operations, and additional write operations (e.g., bridge state updates)
// in a single batch.
//
// Operations that already exist with a more advanced status (completed/stuck)
// are skipped to prevent re-indexing from overwriting completion data.
func (s *Store) SaveIndexerStateWithOperationsAndExtra(state *types.IndexerState, ops []*types.BridgeOperation, extra []WriteOp) error {
	writeOps := make([]WriteOp, 0, 1+len(ops)*4+len(extra))

	writeOps = append(writeOps, WriteOp{
		Key:   indexerStateKey(state.Chain),
		Value: state,
	})

	for _, op := range ops {
		normalizedOp, err := s.reconcileDeferredCompletion(op, &writeOps)
		if err != nil {
			return err
		}
		opKey := operationKeyForOp(normalizedOp)
		if normalizedOp.Status == types.StatusPending && s.operationAlreadyAdvanced(opKey) {
			continue
		}
		idxKey := statusIndexKeyForOp(normalizedOp.Status, normalizedOp)
		writeOps = append(writeOps,
			WriteOp{Key: opKey, Value: normalizedOp},
			WriteOp{Key: idxKey, Value: []byte{}},
		)
	}

	writeOps = append(writeOps, extra...)

	return s.BatchWrite(writeOps)
}

// operationAlreadyAdvanced returns true if the operation at opKey already exists
// with a completed or stuck status. This prevents re-indexing from overwriting
// completion data with a fresh pending operation.
func (s *Store) operationAlreadyAdvanced(opKey string) bool {
	var existing types.BridgeOperation
	if err := s.Get(opKey, &existing); err != nil {
		return false
	}
	return existing.Status == types.StatusCompleted || existing.Status == types.StatusStuck
}

func (s *Store) reconcileDeferredCompletion(op *types.BridgeOperation, writeOps *[]WriteOp) (*types.BridgeOperation, error) {
	if op.Status != types.StatusPending {
		return op, nil
	}

	var deferred deferredCompletion
	dcKey := deferredCompletionKey(op.BridgeType, op.Direction, op.Nonce, op.TokenContract, op.DestTokenContract)
	if err := s.Get(dcKey, &deferred); err != nil {
		// Backward compatibility for previously unscoped token deferred records.
		if op.BridgeType == types.BridgeTypeToken {
			fallbackKeys := []string{
				deferredCompletionKey(op.BridgeType, op.Direction, op.Nonce, nil, op.DestTokenContract),
				deferredCompletionKey(op.BridgeType, op.Direction, op.Nonce, nil, nil),
			}
			found := false
			for _, k := range fallbackKeys {
				if legacyErr := s.Get(k, &deferred); legacyErr == nil {
					dcKey = k
					found = true
					break
				} else if legacyErr != ErrNotFound {
					return nil, fmt.Errorf("get deferred completion %s: %w", k, legacyErr)
				}
			}
			if !found {
				if err == ErrNotFound {
					return op, nil
				}
				return nil, fmt.Errorf("get deferred completion %s: %w", dcKey, err)
			}
		} else if err == ErrNotFound {
			return op, nil
		} else {
			return nil, fmt.Errorf("get deferred completion %s: %w", dcKey, err)
		}
	}
	normalized := *op
	normalized.Status = types.StatusCompleted
	normalized.DestBlockHeight = &deferred.DestBlock
	destTx := deferred.DestTxHash
	normalized.DestTxHash = &destTx
	completionTime := deferred.CompletionTime
	normalized.CompletionTimestamp = &completionTime

	*writeOps = append(*writeOps, WriteOp{Key: dcKey, Value: nil})

	instanceID := InstanceIDFromOperation(&normalized)
	var existingMeta types.DirectionalBridgeState
	if err := s.Get(bridgeStateKey(instanceID), &existingMeta); err == ErrNotFound {
		*writeOps = append(*writeOps, WriteOp{
			Key: bridgeStateKey(instanceID),
			Value: &types.DirectionalBridgeState{
				InstanceID: instanceID,
				BridgeType: normalized.BridgeType,
				SrcChain:   normalized.SourceChain,
				DstChain:   normalized.DestinationChain,
				SrcToken:   normalized.TokenContract,
				DstToken:   normalized.DestTokenContract,
			},
		})
	} else if err != nil {
		return nil, fmt.Errorf("get bridge metadata %s: %w", instanceID, err)
	}

	next := BuildDestChainState(normalized.Nonce, deferred.DestBlock, deferred.DestTxHash, deferred.CompletionTime, normalized.Root)
	var currentDst types.ChainState
	if err := s.Get(bridgeDstPrefix+instanceID, &currentDst); err != nil || shouldReplaceChainState(&currentDst, next) {
		*writeOps = append(*writeOps, WriteOp{
			Key:   bridgeDstPrefix + instanceID,
			Value: next,
		})
	}

	return &normalized, nil
}
