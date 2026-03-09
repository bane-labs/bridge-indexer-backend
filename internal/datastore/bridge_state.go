package datastore

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// Key schema: bridge_state:{instanceID}

const bridgeStatePrefix = "bridge_state:"

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
func BuildInstanceID(bridgeType, srcChain, dstChain string, srcToken, dstToken *string) string {
	base := fmt.Sprintf("%s|%s->%s", bridgeType, srcChain, dstChain)
	if srcToken != nil && dstToken != nil {
		return fmt.Sprintf("%s|%s|%s", base, *srcToken, *dstToken)
	}
	return base
}

func bridgeStateKey(instanceID string) string {
	return bridgeStatePrefix + instanceID
}

// SaveBridgeState persists a directional bridge state.
func (bs *BridgeStateStore) SaveBridgeState(state *types.DirectionalBridgeState) error {
	return bs.store.Set(bridgeStateKey(state.InstanceID), state)
}

// GetBridgeState retrieves a directional bridge state by instance ID.
// Returns ErrNotFound when absent.
func (bs *BridgeStateStore) GetBridgeState(instanceID string) (*types.DirectionalBridgeState, error) {
	var state types.DirectionalBridgeState
	if err := bs.store.Get(bridgeStateKey(instanceID), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// ListBridgeStates returns all directional bridge states via prefix scan.
func (bs *BridgeStateStore) ListBridgeStates() ([]types.DirectionalBridgeState, error) {
	var states []types.DirectionalBridgeState
	err := bs.store.ScanPrefix(bridgeStatePrefix, func(_ string, value []byte) error {
		var s types.DirectionalBridgeState
		if err := json.Unmarshal(value, &s); err != nil {
			return err
		}
		states = append(states, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return states, nil
}

// UpdateSourceState updates the source-side chain state for the given instance.
// If no state exists yet, one is created with the provided metadata.
func (bs *BridgeStateStore) UpdateSourceState(instanceID string, cs *types.ChainState) error {
	state, err := bs.GetBridgeState(instanceID)
	if err != nil {
		if err != ErrNotFound {
			return fmt.Errorf("get bridge state: %w", err)
		}
		// Create new state — caller must ensure instanceID is well-formed.
		state = &types.DirectionalBridgeState{
			InstanceID: instanceID,
		}
	}
	state.SourceState = cs
	return bs.SaveBridgeState(state)
}

// UpdateDestState updates the destination-side chain state for the given instance.
// If no state exists yet, one is created with the provided metadata.
func (bs *BridgeStateStore) UpdateDestState(instanceID string, cs *types.ChainState) error {
	state, err := bs.GetBridgeState(instanceID)
	if err != nil {
		if err != ErrNotFound {
			return fmt.Errorf("get bridge state: %w", err)
		}
		state = &types.DirectionalBridgeState{
			InstanceID: instanceID,
		}
	}
	state.DestState = cs
	return bs.SaveBridgeState(state)
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
func BuildDestChainState(nonce, destBlock uint64, destTxHash string, completionTime time.Time) *types.ChainState {
	return &types.ChainState{
		Nonce:       nonce,
		BlockNumber: destBlock,
		TxHash:      destTxHash,
		ObservedAt:  completionTime,
		IndexedAt:   time.Now(),
	}
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
