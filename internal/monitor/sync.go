package monitor

import (
	"log/slog"
	"sort"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// SyncComputer derives per-instance directional sync from persisted bridge state.
type SyncComputer struct {
	bridgeStateStore *datastore.BridgeStateStore
	logger           *slog.Logger
}

func NewSyncComputer(bridgeStateStore *datastore.BridgeStateStore, logger *slog.Logger) *SyncComputer {
	return &SyncComputer{
		bridgeStateStore: bridgeStateStore,
		logger:           logger,
	}
}

func (s *SyncComputer) ComputeAll() ([]types.DirectionalInstanceSync, error) {
	states, err := s.bridgeStateStore.ListBridgeStates()
	if err != nil {
		return nil, err
	}

	result := make([]types.DirectionalInstanceSync, 0, len(states))
	for i := range states {
		result = append(result, s.computeOne(states[i]))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (s *SyncComputer) computeOne(state types.DirectionalBridgeState) types.DirectionalInstanceSync {
	out := types.DirectionalInstanceSync{
		ID:         state.InstanceID,
		BridgeType: state.BridgeType,
		SrcChain:   state.SrcChain,
		DstChain:   state.DstChain,
		SrcToken:   state.SrcToken,
		DstToken:   state.DstToken,
	}

	if state.SourceState == nil || state.DestState == nil {
		out.Status = types.DirectionalSyncStatusUnknown
		out.Reason = types.DirectionalSyncReasonMissingState
		if state.SourceState != nil {
			out.SrcNonce = &state.SourceState.Nonce
			out.SrcRoot = state.SourceState.Root
		}
		if state.DestState != nil {
			out.DstNonce = &state.DestState.Nonce
			out.DstRoot = state.DestState.Root
		}
		return out
	}

	srcNonce := state.SourceState.Nonce
	dstNonce := state.DestState.Nonce
	delta := int64(srcNonce) - int64(dstNonce)
	out.DeltaNonce = &delta
	out.SrcNonce = &srcNonce
	out.DstNonce = &dstNonce
	out.SrcRoot = state.SourceState.Root
	out.DstRoot = state.DestState.Root

	switch {
	case srcNonce == dstNonce && rootsEqual(state.SourceState.Root, state.DestState.Root):
		out.Status = types.DirectionalSyncStatusSynced
		out.Reason = types.DirectionalSyncReasonSynced
	case srcNonce > dstNonce:
		out.Status = types.DirectionalSyncStatusSourceAhead
		out.Reason = types.DirectionalSyncReasonSourceNonceGreater
	case srcNonce == dstNonce:
		out.Status = types.DirectionalSyncStatusRootMismatch
		out.Reason = types.DirectionalSyncReasonRootMismatchAtEqualNonce
	default:
		// Destination should not be ahead. Mark unknown and log invariant.
		out.Status = types.DirectionalSyncStatusUnknown
		out.Reason = types.DirectionalSyncReasonInvariantViolation
		if s.logger != nil {
			s.logger.Warn(
				"Directional sync invariant violated: destination nonce ahead",
				"instance_id", state.InstanceID,
				"src_nonce", srcNonce,
				"dst_nonce", dstNonce,
			)
		}
	}
	return out
}

func rootsEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
