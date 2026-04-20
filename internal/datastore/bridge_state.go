package datastore

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// Key schema:
//   bridge_state:{instanceID}     → DirectionalBridgeState metadata (may contain legacy embedded states)
//   bridge_src:{instanceID}       → ChainState for source side
//   bridge_dst:{instanceID}       → ChainState for destination side
//
// Source and destination chain states are stored under independent keys so
// that concurrent updates from two indexers never clobber each other.

const (
	bridgeStatePrefix = "bridge_state:"
	bridgeSrcPrefix   = "bridge_src:"
	bridgeDstPrefix   = "bridge_dst:"
)

// BridgeStateStore provides CRUD for directional bridge state on top of Store.
type BridgeStateStore struct {
	store *Store
}

// NewBridgeStateStore creates a new BridgeStateStore.
func NewBridgeStateStore(store *Store) *BridgeStateStore {
	return &BridgeStateStore{store: store}
}

// BuildInstanceID produces a canonical instance identifier.
// For native/message bridges: "{bridgeType}|{srcChain}->{dstChain}"
// For token bridges: "{bridgeType}|{srcChain}->{dstChain}|{srcToken}|{dstToken}"
// If bridgeType is "token", both token addresses are always encoded (using
// "unknown" as a sentinel for nil) to prevent different token pairs from
// colliding on the same instance ID.
func BuildInstanceID(bridgeType, srcChain, dstChain string, srcToken, dstToken *string) string {
	base := fmt.Sprintf("%s|%s->%s", bridgeType, srcChain, dstChain)
	if bridgeType == string(types.BridgeTypeToken) {
		src := "unknown"
		dst := "unknown"
		if srcToken != nil {
			src = normalizeTokenAddress(*srcToken)
		}
		if dstToken != nil {
			dst = normalizeTokenAddress(*dstToken)
		}
		return fmt.Sprintf("%s|%s|%s", base, src, dst)
	}
	return base
}

func normalizeTokenAddress(v string) string {
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	s = strings.ToLower(s)
	if s == "" {
		return "unknown"
	}
	return s
}

func bridgeStateKey(instanceID string) string {
	return bridgeStatePrefix + instanceID
}

// SaveBridgeState persists a directional bridge state, writing metadata and
// source/dest chain states to their independent keys atomically.
func (bs *BridgeStateStore) SaveBridgeState(state *types.DirectionalBridgeState) error {
	ops := []WriteOp{{Key: bridgeStateKey(state.InstanceID), Value: state}}
	if state.SourceState != nil {
		ops = append(ops, WriteOp{Key: bridgeSrcPrefix + state.InstanceID, Value: state.SourceState})
	}
	if state.DestState != nil {
		ops = append(ops, WriteOp{Key: bridgeDstPrefix + state.InstanceID, Value: state.DestState})
	}
	return bs.store.BatchWrite(ops)
}

// GetBridgeState retrieves a directional bridge state by instance ID.
// Source and dest chain states are read from their independent keys and
// overlaid on top of any legacy embedded values.
// Returns ErrNotFound when absent.
func (bs *BridgeStateStore) GetBridgeState(instanceID string) (*types.DirectionalBridgeState, error) {
	var state types.DirectionalBridgeState
	if err := bs.store.Get(bridgeStateKey(instanceID), &state); err != nil {
		return nil, err
	}
	var srcCS types.ChainState
	if err := bs.store.Get(bridgeSrcPrefix+instanceID, &srcCS); err == nil {
		state.SourceState = &srcCS
	}
	var dstCS types.ChainState
	if err := bs.store.Get(bridgeDstPrefix+instanceID, &dstCS); err == nil {
		state.DestState = &dstCS
	}
	return &state, nil
}

// ListBridgeStates returns all directional bridge states via prefix scan.
func (bs *BridgeStateStore) ListBridgeStates() ([]types.DirectionalBridgeState, error) {
	merged := make(map[string]*types.DirectionalBridgeState)
	err := bs.store.ScanPrefix(bridgeStatePrefix, func(_ string, value []byte) error {
		var s types.DirectionalBridgeState
		if err := json.Unmarshal(value, &s); err != nil {
			return err
		}

		rawID := s.InstanceID
		canonicalID := rawID
		if s.BridgeType == types.BridgeTypeToken {
			canonicalID = BuildInstanceID(string(s.BridgeType), s.SrcChain, s.DstChain, s.SrcToken, s.DstToken)
			if canonicalID == "" {
				canonicalID = rawID
			}
		}
		s.InstanceID = canonicalID

		if s.BridgeType == types.BridgeTypeToken && s.SrcToken != nil {
			n := normalizeTokenAddress(*s.SrcToken)
			s.SrcToken = &n
		}
		if s.BridgeType == types.BridgeTypeToken && s.DstToken != nil {
			n := normalizeTokenAddress(*s.DstToken)
			s.DstToken = &n
		}

		// Read side states from raw ID and canonical ID; keep highest nonce.
		if cs := bs.readChainStateMaybe(bridgeSrcPrefix + rawID); cs != nil {
			s.SourceState = cs
		}
		if canonicalID != rawID {
			if cs := bs.readChainStateMaybe(bridgeSrcPrefix + canonicalID); cs != nil {
				s.SourceState = pickHigherChainState(s.SourceState, cs)
			}
		}

		if cs := bs.readChainStateMaybe(bridgeDstPrefix + rawID); cs != nil {
			s.DestState = cs
		}
		if canonicalID != rawID {
			if cs := bs.readChainStateMaybe(bridgeDstPrefix + canonicalID); cs != nil {
				s.DestState = pickHigherChainState(s.DestState, cs)
			}
		}

		if existing, ok := merged[canonicalID]; ok {
			existing.SourceState = pickHigherChainState(existing.SourceState, s.SourceState)
			existing.DestState = pickHigherChainState(existing.DestState, s.DestState)
			return nil
		}

		sCopy := s
		merged[canonicalID] = &sCopy
		return nil
	})
	if err != nil {
		return nil, err
	}
	states := make([]types.DirectionalBridgeState, 0, len(merged))
	for _, st := range merged {
		states = append(states, *st)
	}
	return states, nil
}

func (bs *BridgeStateStore) readChainStateMaybe(key string) *types.ChainState {
	var cs types.ChainState
	if err := bs.store.Get(key, &cs); err != nil {
		return nil
	}
	return &cs
}

func pickHigherChainState(a, b *types.ChainState) *types.ChainState {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if b.Nonce > a.Nonce {
		return b
	}
	return a
}

// UpdateSourceState updates the source-side chain state for the given instance.
// If no state exists yet, one is created and populated from the operation metadata.
func (bs *BridgeStateStore) UpdateSourceState(instanceID string, cs *types.ChainState, op *types.BridgeOperation) error {
	bs.ensureMetadata(instanceID, op)
	return bs.store.Set(bridgeSrcPrefix+instanceID, cs)
}

// UpdateDestState updates the destination-side chain state for the given instance.
// If no state exists yet, one is created and populated from the operation metadata.
func (bs *BridgeStateStore) UpdateDestState(instanceID string, cs *types.ChainState, op *types.BridgeOperation) error {
	bs.ensureMetadata(instanceID, op)
	return bs.store.Set(bridgeDstPrefix+instanceID, cs)
}

// ensureMetadata creates the metadata key if it doesn't exist yet.
func (bs *BridgeStateStore) ensureMetadata(instanceID string, op *types.BridgeOperation) {
	var existing types.DirectionalBridgeState
	if err := bs.store.Get(bridgeStateKey(instanceID), &existing); err == nil {
		return
	}
	meta := &types.DirectionalBridgeState{InstanceID: instanceID}
	if op != nil {
		meta.BridgeType = op.BridgeType
		meta.SrcChain = op.SourceChain
		meta.DstChain = op.DestinationChain
		meta.SrcToken = op.TokenContract
		meta.DstToken = op.DestTokenContract
	}
	_ = bs.store.Set(bridgeStateKey(instanceID), meta)
}

// BuildSourceChainState constructs a ChainState from a BridgeOperation for the source side.
func BuildSourceChainState(op *types.BridgeOperation) *types.ChainState {
	return &types.ChainState{
		Nonce:       op.Nonce,
		Root:        op.Root,
		BlockNumber: op.SourceBlockHeight,
		TxHash:      op.SourceTxHash,
		ObservedAt:  op.Timestamp,
		IndexedAt:   time.Now(),
	}
}

// BuildDestChainState constructs a ChainState from completion data for the destination side.
func BuildDestChainState(nonce, destBlock uint64, destTxHash string, completionTime time.Time, root *string) *types.ChainState {
	return &types.ChainState{
		Nonce:       nonce,
		Root:        root,
		BlockNumber: destBlock,
		TxHash:      destTxHash,
		ObservedAt:  completionTime,
		IndexedAt:   time.Now(),
	}
}

func messageNeoXWithdrawalCompletedNeedsSourceState(op *types.BridgeOperation) bool {
	if op.BridgeType != types.BridgeTypeMessage || op.Direction != types.DirectionWithdrawal {
		return false
	}
	if op.Status != types.StatusCompleted {
		return false
	}
	return types.MessageAMBTypeSkipsNeoXWithdrawalDest(op.AMBMessageType)
}

// BuildSourceStateWriteOps builds WriteOps for atomically updating source-side bridge state
// for a set of pending operations. Only writes to bridge_src keys, never touching bridge_dst.
// Completed NeoX MessageSend ops that are STORE_ONLY or RESULT also update source state
// (they never become pending; source nonce still advances on NeoX).
// For those same STORE_ONLY/RESULT ops, the dest state is also advanced to the same nonce
// so the bridge instance appears synced (no N3 execution is expected or needed).
func (bs *BridgeStateStore) BuildSourceStateWriteOps(ops []*types.BridgeOperation) ([]WriteOp, error) {
	var writeOps []WriteOp
	ensured := make(map[string]bool)
	for _, op := range ops {
		skipsDest := messageNeoXWithdrawalCompletedNeedsSourceState(op)
		if op.Status != types.StatusPending && !skipsDest {
			continue
		}
		instanceID := InstanceIDFromOperation(op)
		if !ensured[instanceID] {
			ensured[instanceID] = true
			var existing types.DirectionalBridgeState
			if err := bs.store.Get(bridgeStateKey(instanceID), &existing); err == ErrNotFound {
				writeOps = append(writeOps, WriteOp{
					Key: bridgeStateKey(instanceID),
					Value: &types.DirectionalBridgeState{
						InstanceID: instanceID,
						BridgeType: op.BridgeType,
						SrcChain:   op.SourceChain,
						DstChain:   op.DestinationChain,
						SrcToken:   op.TokenContract,
						DstToken:   op.DestTokenContract,
					},
				})
			} else if err != nil {
				return nil, fmt.Errorf("get bridge state %s: %w", instanceID, err)
			}
		}
		next := BuildSourceChainState(op)
		var current types.ChainState
		if err := bs.store.Get(bridgeSrcPrefix+instanceID, &current); err == nil {
			if shouldReplaceChainState(&current, next) {
				writeOps = append(writeOps, WriteOp{Key: bridgeSrcPrefix + instanceID, Value: next})
			}
		} else if err == ErrNotFound {
			writeOps = append(writeOps, WriteOp{Key: bridgeSrcPrefix + instanceID, Value: next})
		} else {
			return nil, fmt.Errorf("get source chain state %s: %w", instanceID, err)
		}
		// STORE_ONLY/RESULT NeoX withdrawals never get N3 execution. Advance the
		// destination state to the same nonce so the sync computer reports synced.
		if skipsDest {
			nextDst := BuildDestChainState(op.Nonce, op.SourceBlockHeight, op.SourceTxHash, op.Timestamp, op.Root)
			var currentDst types.ChainState
			if err := bs.store.Get(bridgeDstPrefix+instanceID, &currentDst); err == nil {
				if shouldReplaceChainState(&currentDst, nextDst) {
					writeOps = append(writeOps, WriteOp{Key: bridgeDstPrefix + instanceID, Value: nextDst})
				}
			} else if err == ErrNotFound {
				writeOps = append(writeOps, WriteOp{Key: bridgeDstPrefix + instanceID, Value: nextDst})
			} else {
				return nil, fmt.Errorf("get dest chain state %s: %w", instanceID, err)
			}
		}
	}
	return writeOps, nil
}

// BuildDestStateWriteOps builds WriteOps for atomically updating destination-side bridge state.
// Only writes to bridge_dst keys, never touching bridge_src.
func (bs *BridgeStateStore) BuildDestStateWriteOps(op *types.BridgeOperation, destBlock uint64, destTxHash string, completionTime time.Time, root *string) ([]WriteOp, error) {
	instanceID := InstanceIDFromOperation(op)
	var writeOps []WriteOp
	var existing types.DirectionalBridgeState
	if err := bs.store.Get(bridgeStateKey(instanceID), &existing); err == ErrNotFound {
		writeOps = append(writeOps, WriteOp{
			Key: bridgeStateKey(instanceID),
			Value: &types.DirectionalBridgeState{
				InstanceID: instanceID,
				BridgeType: op.BridgeType,
				SrcChain:   op.SourceChain,
				DstChain:   op.DestinationChain,
				SrcToken:   op.TokenContract,
				DstToken:   op.DestTokenContract,
			},
		})
	}
	next := BuildDestChainState(op.Nonce, destBlock, destTxHash, completionTime, root)
	var current types.ChainState
	if err := bs.store.Get(bridgeDstPrefix+instanceID, &current); err == nil {
		if !shouldReplaceChainState(&current, next) {
			return writeOps, nil
		}
	} else if err != ErrNotFound {
		return nil, fmt.Errorf("get destination chain state %s: %w", instanceID, err)
	}
	writeOps = append(writeOps, WriteOp{
		Key:   bridgeDstPrefix + instanceID,
		Value: next,
	})
	return writeOps, nil
}

func shouldReplaceChainState(current, next *types.ChainState) bool {
	if current == nil {
		return true
	}
	if next == nil {
		return false
	}
	if next.Nonce > current.Nonce {
		return true
	}
	if next.Nonce < current.Nonce {
		return false
	}
	// Same nonce: only replace when upgrading root from missing -> present.
	currentRoot := current.Root != nil && *current.Root != ""
	nextRoot := next.Root != nil && *next.Root != ""
	return !currentRoot && nextRoot
}

// InstanceIDFromOperation derives the instance ID from a bridge operation.
func InstanceIDFromOperation(op *types.BridgeOperation) string {
	return BuildInstanceID(
		string(op.BridgeType),
		op.SourceChain,
		op.DestinationChain,
		op.TokenContract,
		op.DestTokenContract,
	)
}

// RebuildFromOperations scans all persisted operations and rebuilds bridge
// state so that source/dest nonces reflect the highest nonce actually present
// in the datastore. This corrects drift caused by operations that were indexed
// before bridge-state tracking was wired into the indexers, or completions
// that were resolved by the backfiller without a corresponding state update.
func (bs *BridgeStateStore) RebuildFromOperations(logger *slog.Logger) error {
	type instanceAgg struct {
		maxSrcOp *types.BridgeOperation
		maxDstOp *types.BridgeOperation
	}
	instances := make(map[string]*instanceAgg)

	err := bs.store.ScanPrefix("op:", func(_ string, value []byte) error {
		var op types.BridgeOperation
		if err := json.Unmarshal(value, &op); err != nil {
			return nil
		}

		id := BuildInstanceID(
			string(op.BridgeType),
			op.SourceChain,
			op.DestinationChain,
			op.TokenContract,
			op.DestTokenContract,
		)

		agg := instances[id]
		if agg == nil {
			agg = &instanceAgg{}
			instances[id] = agg
		}

		if agg.maxSrcOp == nil || op.Nonce > agg.maxSrcOp.Nonce {
			opCopy := op
			agg.maxSrcOp = &opCopy
		}

		// STORE_ONLY/RESULT NeoX withdrawals are completed without a dest tx.
		// Include them as valid dest-state candidates so the rebuild reflects
		// that those nonces were fully processed.
		isSkipsDest := op.Status == types.StatusCompleted && messageNeoXWithdrawalCompletedNeedsSourceState(&op)
		if op.Status == types.StatusCompleted && (op.DestTxHash != nil || isSkipsDest) {
			if agg.maxDstOp == nil || op.Nonce > agg.maxDstOp.Nonce {
				opCopy := op
				agg.maxDstOp = &opCopy
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("scan operations: %w", err)
	}

	updated := 0
	for id, agg := range instances {
		state, err := bs.GetBridgeState(id)
		if err != nil && err != ErrNotFound {
			return fmt.Errorf("get bridge state %s: %w", id, err)
		}
		if err == ErrNotFound {
			state = &types.DirectionalBridgeState{InstanceID: id}
			if agg.maxSrcOp != nil {
				state.BridgeType = agg.maxSrcOp.BridgeType
				state.SrcChain = agg.maxSrcOp.SourceChain
				state.DstChain = agg.maxSrcOp.DestinationChain
				state.SrcToken = agg.maxSrcOp.TokenContract
				state.DstToken = agg.maxSrcOp.DestTokenContract
			}
		}

		changed := false

		if agg.maxSrcOp != nil && (state.SourceState == nil || agg.maxSrcOp.Nonce > state.SourceState.Nonce) {
			state.SourceState = BuildSourceChainState(agg.maxSrcOp)
			changed = true
		}

		if agg.maxDstOp != nil && (state.DestState == nil || agg.maxDstOp.Nonce > state.DestState.Nonce) {
			ct := time.Now()
			if agg.maxDstOp.CompletionTimestamp != nil {
				ct = *agg.maxDstOp.CompletionTimestamp
			}
			// For STORE_ONLY/RESULT ops there is no dest block/tx; fall back to source.
			destBlock := agg.maxDstOp.SourceBlockHeight
			if agg.maxDstOp.DestBlockHeight != nil {
				destBlock = *agg.maxDstOp.DestBlockHeight
			}
			destTxHash := agg.maxDstOp.SourceTxHash
			if agg.maxDstOp.DestTxHash != nil {
				destTxHash = *agg.maxDstOp.DestTxHash
			}
			state.DestState = BuildDestChainState(
				agg.maxDstOp.Nonce,
				destBlock,
				destTxHash,
				ct,
				agg.maxDstOp.Root,
			)
			changed = true
		}

		if changed {
			if err := bs.SaveBridgeState(state); err != nil {
				return fmt.Errorf("save bridge state %s: %w", id, err)
			}
			updated++
		}
	}

	if logger != nil {
		logger.Info("Bridge state rebuild complete", "instances_updated", updated, "total_instances", len(instances))
	}
	return nil
}
