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
	store            *Store
	bridgeStateStore *BridgeStateStore
}

type deferredCompletion struct {
	DestBlock      uint64    `json:"dest_block"`
	DestTxHash     string    `json:"dest_tx_hash"`
	CompletionTime time.Time `json:"completion_time"`
}

// NewOperationStore creates a new OperationStore.
func NewOperationStore(store *Store, bridgeStateStore *BridgeStateStore) *OperationStore {
	return &OperationStore{store: store, bridgeStateStore: bridgeStateStore}
}

func operationKey(bt types.BridgeType, dir types.Direction, nonce uint64) string {
	return fmt.Sprintf("op:%s:%s:%d", bt, dir, nonce)
}

func statusIndexKey(status types.OperationStatus, bt types.BridgeType, dir types.Direction, nonce uint64) string {
	return fmt.Sprintf("idx:status:%s:%s:%s:%d", status, bt, dir, nonce)
}

func normalizeTokenKeyPart(token string) string {
	s := strings.TrimSpace(strings.ToLower(token))
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	if s == "" {
		return "unknown"
	}
	return s
}

func tokenPairSuffix(srcToken, dstToken *string) string {
	src := "unknown"
	dst := "unknown"
	if srcToken != nil {
		src = normalizeTokenKeyPart(*srcToken)
	}
	if dstToken != nil {
		dst = normalizeTokenKeyPart(*dstToken)
	}
	return src + ":" + dst
}

func operationKeyForOp(op *types.BridgeOperation) string {
	base := operationKey(op.BridgeType, op.Direction, op.Nonce)
	if op.BridgeType != types.BridgeTypeToken {
		return base
	}
	return base + ":" + tokenPairSuffix(op.TokenContract, op.DestTokenContract)
}

func statusIndexKeyForOp(status types.OperationStatus, op *types.BridgeOperation) string {
	base := statusIndexKey(status, op.BridgeType, op.Direction, op.Nonce)
	if op.BridgeType != types.BridgeTypeToken {
		return base
	}
	return base + ":" + tokenPairSuffix(op.TokenContract, op.DestTokenContract)
}

func deferredCompletionKey(bt types.BridgeType, dir types.Direction, nonce uint64, srcToken, dstToken *string) string {
	base := fmt.Sprintf("deferred_completion:%s:%s:%d", bt, dir, nonce)
	if bt != types.BridgeTypeToken {
		return base
	}
	return base + ":" + tokenPairSuffix(srcToken, dstToken)
}

// SaveOperation persists a bridge operation with its status index atomically.
func (os *OperationStore) SaveOperation(op *types.BridgeOperation) error {
	opKey := operationKeyForOp(op)
	idxKey := statusIndexKeyForOp(op.Status, op)

	return os.store.BatchWrite([]WriteOp{
		{Key: opKey, Value: op},
		{Key: idxKey, Value: []byte{}},
	})
}

// GetOperation retrieves a single operation by its composite key.
func (os *OperationStore) GetOperation(bt types.BridgeType, dir types.Direction, nonce uint64) (*types.BridgeOperation, error) {
	if bt == types.BridgeTypeToken {
		return os.getTokenOperationByDestToken(dir, nonce, "")
	}
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
	var op *types.BridgeOperation
	var err error
	if bt == types.BridgeTypeToken {
		op, err = os.getTokenOperationByDestToken(dir, nonce, "")
	} else {
		op, err = os.GetOperation(bt, dir, nonce)
	}
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("get operation: %w", err)
		}

		// Operation not found — persist completion for deferred reconciliation
		// when the source-side op is indexed later.
		if recErr := os.recordDeferredCompletion(bt, dir, nonce, nil, nil, destBlock, destTxHash, completionTime); recErr != nil {
			return fmt.Errorf("record deferred completion: %w", recErr)
		}

		// Keep best-effort destination-side bridge-state updates for native and
		// message bridges so sync visibility isn't lost during initial races.
		if os.bridgeStateStore != nil && bt != types.BridgeTypeToken {
			os.updateDestBridgeStateFallback(bt, dir, nonce, destBlock, destTxHash, completionTime)
		}
		return nil
	}

	oldStatus := op.Status
	op.DestBlockHeight = &destBlock
	op.DestTxHash = &destTxHash
	op.CompletionTimestamp = &completionTime
	op.Status = types.StatusCompleted

	opKey := operationKeyForOp(op)
	oldIdxKey := statusIndexKeyForOp(oldStatus, op)
	newIdxKey := statusIndexKeyForOp(types.StatusCompleted, op)

	writeOps := []WriteOp{
		{Key: opKey, Value: op},
		{Key: oldIdxKey, Value: nil}, // delete old index
		{Key: newIdxKey, Value: []byte{}},
	}

	if os.bridgeStateStore != nil {
		destStateOps, err := os.bridgeStateStore.BuildDestStateWriteOps(op, destBlock, destTxHash, completionTime, op.Root)
		if err != nil {
			return fmt.Errorf("build dest state write ops: %w", err)
		}
		writeOps = append(writeOps, destStateOps...)
	}

	return os.store.BatchWrite(writeOps)
}

func (os *OperationStore) UpdateTokenOperationCompletionByDestToken(
	dir types.Direction,
	nonce uint64,
	destToken string,
	destBlock uint64,
	destTxHash string,
	completionTime time.Time,
) error {
	op, err := os.getTokenOperationByDestToken(dir, nonce, destToken)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		dst := normalizeTokenKeyPart(destToken)
		if recErr := os.recordDeferredCompletion(types.BridgeTypeToken, dir, nonce, nil, &dst, destBlock, destTxHash, completionTime); recErr != nil {
			return fmt.Errorf("record token deferred completion: %w", recErr)
		}
		if os.bridgeStateStore != nil {
			os.updateTokenDestBridgeStateFallback(dir, nonce, dst, destBlock, destTxHash, completionTime)
		}
		return nil
	}

	oldStatus := op.Status
	op.DestBlockHeight = &destBlock
	op.DestTxHash = &destTxHash
	op.CompletionTimestamp = &completionTime
	op.Status = types.StatusCompleted

	opKey := operationKeyForOp(op)
	oldIdxKey := statusIndexKeyForOp(oldStatus, op)
	newIdxKey := statusIndexKeyForOp(types.StatusCompleted, op)

	writeOps := []WriteOp{
		{Key: opKey, Value: op},
		{Key: oldIdxKey, Value: nil},
		{Key: newIdxKey, Value: []byte{}},
	}
	if os.bridgeStateStore != nil {
		destStateOps, err := os.bridgeStateStore.BuildDestStateWriteOps(op, destBlock, destTxHash, completionTime, op.Root)
		if err != nil {
			return fmt.Errorf("build token dest state write ops: %w", err)
		}
		writeOps = append(writeOps, destStateOps...)
	}
	return os.store.BatchWrite(writeOps)
}

// UpdateMessageDepositMirror mirrors Neo->EVM message nonce/root observed on
// NeoX into directional bridge state for message deposits (neo_n3->neo_x).
// This is a best-effort fallback for historical windows where N3 MessageSend
// source ops are missing, while NeoX has execution/root progression.
func (os *OperationStore) UpdateMessageDepositMirror(
	nonce uint64,
	root string,
	observedBlock uint64,
	observedTxHash string,
	observedTime time.Time,
) error {
	if os.bridgeStateStore == nil {
		return nil
	}
	instanceID := BuildInstanceID(string(types.BridgeTypeMessage), "neo_n3", "neo_x", nil, nil)
	os.bridgeStateStore.ensureMetadata(instanceID, &types.BridgeOperation{
		BridgeType:       types.BridgeTypeMessage,
		Direction:        types.DirectionDeposit,
		SourceChain:      "neo_n3",
		DestinationChain: "neo_x",
		FromAddress:      "unknown",
		ToAddress:        "",
	})

	rootPtr := &root

	var currentSrc types.ChainState
	shouldUpdateSrc := true
	if err := os.store.Get(bridgeSrcPrefix+instanceID, &currentSrc); err == nil {
		if nonce < currentSrc.Nonce {
			shouldUpdateSrc = false
		} else if nonce == currentSrc.Nonce && currentSrc.Root != nil && *currentSrc.Root != "" {
			shouldUpdateSrc = false
		}
	}
	if shouldUpdateSrc {
		srcState := BuildSourceChainState(&types.BridgeOperation{
			Nonce:             nonce,
			Root:              rootPtr,
			SourceBlockHeight: 0,
			SourceTxHash:      "",
			Timestamp:         observedTime,
		})
		if err := os.store.Set(bridgeSrcPrefix+instanceID, srcState); err != nil {
			return err
		}
	}

	var currentDst types.ChainState
	shouldUpdateDst := true
	if err := os.store.Get(bridgeDstPrefix+instanceID, &currentDst); err == nil {
		if nonce < currentDst.Nonce {
			shouldUpdateDst = false
		} else if nonce == currentDst.Nonce && currentDst.Root != nil && *currentDst.Root != "" {
			shouldUpdateDst = false
		}
	}
	if shouldUpdateDst {
		dstState := BuildDestChainState(nonce, observedBlock, observedTxHash, observedTime, rootPtr)
		if err := os.store.Set(bridgeDstPrefix+instanceID, dstState); err != nil {
			return err
		}
	}
	return nil
}

func (os *OperationStore) updateTokenDestBridgeStateFallback(
	dir types.Direction,
	nonce uint64,
	dstTokenNorm string,
	destBlock uint64,
	destTxHash string,
	completionTime time.Time,
) {
	if dstTokenNorm == "" || dstTokenNorm == "unknown" {
		return
	}

	srcChain, dstChain := "neo_n3", "neo_x"
	if dir == types.DirectionWithdrawal {
		srcChain, dstChain = "neo_x", "neo_n3"
	}

	srcToken, ok := os.inferTokenSourceForDirection(srcChain, dstChain, dstTokenNorm)
	if !ok || srcToken == "" || srcToken == "unknown" {
		return
	}
	dstToken := dstTokenNorm
	instanceID := BuildInstanceID(string(types.BridgeTypeToken), srcChain, dstChain, &srcToken, &dstToken)
	os.bridgeStateStore.ensureMetadata(instanceID, &types.BridgeOperation{
		BridgeType:        types.BridgeTypeToken,
		SourceChain:       srcChain,
		DestinationChain:  dstChain,
		TokenContract:     &srcToken,
		DestTokenContract: &dstToken,
	})

	var current types.ChainState
	if err := os.store.Get(bridgeDstPrefix+instanceID, &current); err == nil && nonce <= current.Nonce {
		return
	}
	cs := BuildDestChainState(nonce, destBlock, destTxHash, completionTime, nil)
	_ = os.store.Set(bridgeDstPrefix+instanceID, cs)
}

func (os *OperationStore) inferTokenSourceForDirection(srcChain, dstChain, dstTokenNorm string) (string, bool) {
	states, err := os.bridgeStateStore.ListBridgeStates()
	if err != nil {
		return "", false
	}

	for _, st := range states {
		if st.BridgeType != types.BridgeTypeToken || st.SrcToken == nil || st.DstToken == nil {
			continue
		}
		if st.SrcChain == srcChain && st.DstChain == dstChain && normalizeTokenKeyPart(*st.DstToken) == dstTokenNorm {
			return normalizeTokenKeyPart(*st.SrcToken), true
		}
	}

	// infer from opposite direction when direct metadata doesn't exist yet
	for _, st := range states {
		if st.BridgeType != types.BridgeTypeToken || st.SrcToken == nil || st.DstToken == nil {
			continue
		}
		if st.SrcChain == dstChain && st.DstChain == srcChain && normalizeTokenKeyPart(*st.SrcToken) == dstTokenNorm {
			return normalizeTokenKeyPart(*st.DstToken), true
		}
	}
	return "", false
}

func (os *OperationStore) getTokenOperationByDestToken(dir types.Direction, nonce uint64, destToken string) (*types.BridgeOperation, error) {
	// Backward compatibility: legacy token key without pair suffix.
	var legacy types.BridgeOperation
	if err := os.store.Get(operationKey(types.BridgeTypeToken, dir, nonce), &legacy); err == nil {
		if destToken == "" || normalizeTokenKeyPart(strVal(legacy.DestTokenContract)) == normalizeTokenKeyPart(destToken) {
			return &legacy, nil
		}
	}

	prefix := operationKey(types.BridgeTypeToken, dir, nonce) + ":"
	var matches []types.BridgeOperation
	if err := os.store.ScanPrefix(prefix, func(_ string, value []byte) error {
		var op types.BridgeOperation
		if err := json.Unmarshal(value, &op); err != nil {
			return err
		}
		if destToken != "" && normalizeTokenKeyPart(strVal(op.DestTokenContract)) != normalizeTokenKeyPart(destToken) {
			return nil
		}
		matches = append(matches, op)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		// Fallback for legacy/unscoped records where token fields were absent.
		if destToken != "" {
			return os.getTokenOperationByDestToken(dir, nonce, "")
		}
		return nil, ErrNotFound
	}
	if len(matches) == 1 {
		return &matches[0], nil
	}

	for i := range matches {
		if matches[i].Status != types.StatusCompleted {
			return &matches[i], nil
		}
	}
	return &matches[0], nil
}

func strVal(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func (os *OperationStore) recordDeferredCompletion(
	bt types.BridgeType,
	dir types.Direction,
	nonce uint64,
	srcToken *string,
	dstToken *string,
	destBlock uint64,
	destTxHash string,
	completionTime time.Time,
) error {
	key := deferredCompletionKey(bt, dir, nonce, srcToken, dstToken)
	var existing deferredCompletion
	if err := os.store.Get(key, &existing); err == nil {
		// Keep the newest completion record for this nonce.
		if existing.DestBlock > destBlock {
			return nil
		}
	} else if err != ErrNotFound {
		return err
	}

	return os.store.Set(key, &deferredCompletion{
		DestBlock:      destBlock,
		DestTxHash:     destTxHash,
		CompletionTime: completionTime,
	})
}

// updateDestBridgeStateFallback updates the destination-side bridge state for
// native and message bridges even when the source operation doesn't exist yet.
// This prevents the race condition where the destination chain indexes
// completion events before the source chain creates the pending operations.
// Token bridges are excluded because the instance ID requires token addresses
// that are not available without the source operation.
func (os *OperationStore) updateDestBridgeStateFallback(
	bt types.BridgeType,
	dir types.Direction,
	nonce uint64,
	destBlock uint64,
	destTxHash string,
	completionTime time.Time,
) {
	src, dst := "neo_n3", "neo_x"
	if dir == types.DirectionWithdrawal {
		src, dst = dst, src
	}
	instanceID := BuildInstanceID(string(bt), src, dst, nil, nil)

	os.bridgeStateStore.ensureMetadata(instanceID, nil)

	// Read only the dest key — no risk of clobbering source state.
	var current types.ChainState
	if err := os.store.Get(bridgeDstPrefix+instanceID, &current); err == nil {
		if nonce <= current.Nonce {
			return
		}
	}

	cs := BuildDestChainState(nonce, destBlock, destTxHash, completionTime, nil)
	_ = os.store.Set(bridgeDstPrefix+instanceID, cs)
}

// ListOperations returns operations matching the filter.
// Returns (results, totalMatchingCount, error).
func (os *OperationStore) ListOperations(filter OperationFilter) ([]types.BridgeOperation, int, error) {
	results := make([]types.BridgeOperation, 0)
	total := 0
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	limit := filter.Limit
	if limit < 0 {
		limit = 0
	}

	if filter.Status != nil {
		// Use secondary index for status-based queries.
		prefix := fmt.Sprintf("idx:status:%s:", *filter.Status)
		if err := os.store.ScanPrefix(prefix, func(key string, _ []byte) error {
			parts := strings.Split(key, ":")
			if len(parts) < 6 {
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

			total++
			if total <= offset {
				return nil
			}
			if limit > 0 && len(results) >= limit {
				return nil
			}

			opKey := fmt.Sprintf("op:%s:%s:%s", parts[3], parts[4], parts[5])
			if len(parts) > 6 {
				opKey += ":" + strings.Join(parts[6:], ":")
			}
			var op types.BridgeOperation
			if err := os.store.Get(opKey, &op); err != nil {
				if errors.Is(err, ErrNotFound) {
					return nil // index orphan, skip
				}
				return err
			}
			results = append(results, op)
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

			total++
			if total <= offset {
				return nil
			}
			if limit > 0 && len(results) >= limit {
				return nil
			}

			results = append(results, op)
			return nil
		}); err != nil {
			return nil, 0, err
		}
	}

	return results, total, nil
}

// GetOperationsByNonce returns all operations across bridge types/directions for a nonce.
func (os *OperationStore) GetOperationsByNonce(nonce uint64) ([]types.BridgeOperation, error) {
	var out []types.BridgeOperation
	err := os.store.ScanPrefix("op:", func(_ string, value []byte) error {
		var op types.BridgeOperation
		if err := json.Unmarshal(value, &op); err != nil {
			return err
		}
		if op.Nonce == nonce {
			out = append(out, op)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CountOperations returns the count of operations matching the filter.
func (os *OperationStore) CountOperations(filter OperationFilter) (int, error) {
	count := 0
	if filter.Status != nil {
		prefix := fmt.Sprintf("idx:status:%s:", *filter.Status)
		err := os.store.ScanPrefix(prefix, func(key string, _ []byte) error {
			parts := strings.Split(key, ":")
			if len(parts) < 6 {
				return nil
			}
			bt := types.BridgeType(parts[3])
			dir := types.Direction(parts[4])
			if filter.BridgeType != nil && bt != *filter.BridgeType {
				return nil
			}
			if filter.Direction != nil && dir != *filter.Direction {
				return nil
			}
			count++
			return nil
		})
		return count, err
	}

	prefix := "op:"
	if filter.BridgeType != nil {
		prefix = fmt.Sprintf("op:%s:", *filter.BridgeType)
	}

	err := os.store.ScanPrefix(prefix, func(_ string, value []byte) error {
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
		count++
		return nil
	})
	return count, err
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

	opKey := operationKeyForOp(op)
	idxKey := statusIndexKeyForOp(op.Status, op)

	return os.store.BatchWrite([]WriteOp{
		{Key: opKey, Value: nil},
		{Key: idxKey, Value: nil},
	})
}

func (os *OperationStore) MarkStuck(bt types.BridgeType, dir types.Direction, nonce uint64) error {
	op, err := os.GetOperation(bt, dir, nonce)
	if err != nil {
		return err
	}

	oldStatus := op.Status
	op.Status = types.StatusStuck

	opKey := operationKeyForOp(op)
	oldIdxKey := statusIndexKeyForOp(oldStatus, op)
	newIdxKey := statusIndexKeyForOp(types.StatusStuck, op)

	return os.store.BatchWrite([]WriteOp{
		{Key: opKey, Value: op},
		{Key: oldIdxKey, Value: nil},
		{Key: newIdxKey, Value: []byte{}},
	})
}
