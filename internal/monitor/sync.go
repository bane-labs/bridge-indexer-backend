package monitor

import (
	"bytes"
	"encoding/hex"
	"log/slog"
	"sort"
	"strings"

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

	if src := state.SourceState; src != nil {
		out.SrcNonce = &src.Nonce
		out.SrcRoot = src.Root
		out.SrcBlockNumber = &src.BlockNumber
		if !src.ObservedAt.IsZero() {
			out.SrcObservedAt = &src.ObservedAt
		}
	}
	if dst := state.DestState; dst != nil {
		out.DstNonce = &dst.Nonce
		out.DstRoot = dst.Root
		// Older mirrored message states predate the persisted synthetic marker.
		src := state.SourceState
		out.DstSynthetic = dst.Synthetic || (state.BridgeType == types.BridgeTypeMessage &&
			state.SrcChain == "neo_x" && src != nil && dst.TxHash != "" &&
			dst.TxHash == src.TxHash && dst.BlockNumber == src.BlockNumber &&
			dst.Nonce == src.Nonce)
		if !out.DstSynthetic {
			out.DstBlockNumber = &dst.BlockNumber
			if !dst.ObservedAt.IsZero() {
				out.DstObservedAt = &dst.ObservedAt
			}
		}
	}
	if state.SourceState == nil || state.DestState == nil {
		out.Status = types.DirectionalSyncStatusUnknown
		out.Reason = types.DirectionalSyncReasonMissingState
		return out
	}

	srcNonce := state.SourceState.Nonce
	dstNonce := state.DestState.Nonce
	delta := int64(srcNonce) - int64(dstNonce)
	out.DeltaNonce = &delta

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
	// Historical records use both prefixed and unprefixed hex. Compare root
	// bytes when possible without rewriting persisted source observations.
	first, firstErr := hex.DecodeString(strings.TrimPrefix(strings.TrimPrefix(*a, "0x"), "0X"))
	second, secondErr := hex.DecodeString(strings.TrimPrefix(strings.TrimPrefix(*b, "0x"), "0X"))
	if firstErr == nil && secondErr == nil {
		return bytes.Equal(first, second)
	}
	return *a == *b
}
