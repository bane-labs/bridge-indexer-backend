package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// Key schema:
//   op:{bridge_type}:{direction}:{nonce}                          → BridgeOperation JSON
//   idx:status:{status}:{bridge_type}:{direction}:{nonce}         → [] (secondary index)

// OperationFilter defines query parameters for ListOperations.
type OperationFilter struct {
	BridgeType *types.BridgeType
	Direction  *types.Direction
	Status     *types.OperationStatus
	Limit      int
	Offset     int
}

// OperationStore provides bridge operation CRUD on top of Store.
type OperationStore struct {
	store *Store
}

// NewOperationStore creates a new OperationStore.
func NewOperationStore(store *Store) *OperationStore {
	return &OperationStore{store: store}
}

func operationKey(bt types.BridgeType, dir types.Direction, nonce uint64) string {
	return fmt.Sprintf("op:%s:%s:%d", bt, dir, nonce)
}

func statusIndexKey(status types.OperationStatus, bt types.BridgeType, dir types.Direction, nonce uint64) string {
	return fmt.Sprintf("idx:status:%s:%s:%s:%d", status, bt, dir, nonce)
}

// SaveOperation persists a bridge operation with its status index atomically.
func (os *OperationStore) SaveOperation(op *types.BridgeOperation) error {
	opKey := operationKey(op.BridgeType, op.Direction, op.Nonce)
	idxKey := statusIndexKey(op.Status, op.BridgeType, op.Direction, op.Nonce)

	return os.store.BatchWrite([]WriteOp{
		{Key: opKey, Value: op},
		{Key: idxKey, Value: []byte{}},
	})
}

// GetOperation retrieves a single operation by its composite key.
func (os *OperationStore) GetOperation(bt types.BridgeType, dir types.Direction, nonce uint64) (*types.BridgeOperation, error) {
	var op types.BridgeOperation
	if err := os.store.Get(operationKey(bt, dir, nonce), &op); err != nil {
		return nil, err
	}
	return &op, nil
}

// UpdateOperationCompletion transitions a pending operation to completed.
func (os *OperationStore) UpdateOperationCompletion(
	bt types.BridgeType,
	dir types.Direction,
	nonce uint64,
	destBlock uint64,
	destTxHash string,
	completionTime time.Time,
) error {
	op, err := os.GetOperation(bt, dir, nonce)
	if err != nil {
		return fmt.Errorf("get operation: %w", err)
	}

	oldStatus := op.Status
	op.DestBlockHeight = &destBlock
	op.DestTxHash = &destTxHash
	op.CompletionTimestamp = &completionTime
	op.Status = types.StatusCompleted

	opKey := operationKey(bt, dir, nonce)
	oldIdxKey := statusIndexKey(oldStatus, bt, dir, nonce)
	newIdxKey := statusIndexKey(types.StatusCompleted, bt, dir, nonce)

	return os.store.BatchWrite([]WriteOp{
		{Key: opKey, Value: op},
		{Key: oldIdxKey, Value: nil}, // delete old index
		{Key: newIdxKey, Value: []byte{}},
	})
}

// ListOperations returns operations matching the filter.
// Returns (results, totalMatchingCount, error).
func (os *OperationStore) ListOperations(filter OperationFilter) ([]types.BridgeOperation, int, error) {
	var all []types.BridgeOperation

	if filter.Status != nil {
		// Use secondary index for status-based queries.
		prefix := fmt.Sprintf("idx:status:%s:", *filter.Status)
		if err := os.store.ScanPrefix(prefix, func(key string, _ []byte) error {
			// Parse composite key: idx:status:{status}:{bridge_type}:{direction}:{nonce}
			parts := strings.SplitN(key, ":", 6)
			if len(parts) != 6 {
				return nil // skip malformed
			}
			bt := types.BridgeType(parts[3])
			dir := types.Direction(parts[4])

			if filter.BridgeType != nil && bt != *filter.BridgeType {
				return nil
			}
			if filter.Direction != nil && dir != *filter.Direction {
				return nil
			}

			opKey := fmt.Sprintf("op:%s:%s:%s", parts[3], parts[4], parts[5])
			var op types.BridgeOperation
			if err := os.store.Get(opKey, &op); err != nil {
				if errors.Is(err, ErrNotFound) {
					return nil // index orphan, skip
				}
				return err
			}
			all = append(all, op)
			return nil
		}); err != nil {
			return nil, 0, err
		}
	} else {
		// Full scan with in-memory filtering.
		prefix := "op:"
		if filter.BridgeType != nil {
			prefix = fmt.Sprintf("op:%s:", *filter.BridgeType)
		}

		if err := os.store.ScanPrefix(prefix, func(_ string, value []byte) error {
			var op types.BridgeOperation
			if err := json.Unmarshal(value, &op); err != nil {
				return err
			}
			if filter.BridgeType != nil && op.BridgeType != *filter.BridgeType {
				return nil
			}
			if filter.Direction != nil && op.Direction != *filter.Direction {
				return nil
			}
			all = append(all, op)
			return nil
		}); err != nil {
			return nil, 0, err
		}
	}

	total := len(all)

	// Apply offset/limit.
	offset := filter.Offset
	if offset > len(all) {
		offset = len(all)
	}
	all = all[offset:]

	if filter.Limit > 0 && filter.Limit < len(all) {
		all = all[:filter.Limit]
	}

	return all, total, nil
}

// DeleteOperation removes an operation and its status index.
func (os *OperationStore) DeleteOperation(bt types.BridgeType, dir types.Direction, nonce uint64) error {
	// Read existing to find current status for index cleanup.
	op, err := os.GetOperation(bt, dir, nonce)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil // already gone
		}
		return err
	}

	opKey := operationKey(bt, dir, nonce)
	idxKey := statusIndexKey(op.Status, bt, dir, nonce)

	return os.store.BatchWrite([]WriteOp{
		{Key: opKey, Value: nil},
		{Key: idxKey, Value: nil},
	})
}
