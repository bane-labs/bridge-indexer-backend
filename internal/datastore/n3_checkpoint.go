package datastore

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	badger "github.com/dgraph-io/badger/v4"
)

// SaveN3LifecycleCheckpoint commits source operations, destination receipts,
// legacy completion, directional state and the contiguous checkpoint together.
func (s *Store) SaveN3LifecycleCheckpoint(state *types.IndexerState, ops []*types.BridgeOperation, extra []WriteOp, observations []LifecycleObservation, unresolved ...UnresolvedN3TokenClaim) error {
	if state.Chain != "neo_n3" {
		return fmt.Errorf("unexpected checkpoint chain %q", state.Chain)
	}
	var reconciled []WriteOp
	normalized := make([]*types.BridgeOperation, 0, len(ops))
	for _, op := range ops {
		result, err := s.reconcileDeferredCompletion(op, &reconciled)
		if err != nil {
			return err
		}
		normalized = append(normalized, result)
	}
	return retryLifecycleWrite(s.db, func(txn *badger.Txn) error {
		if err := writeLifecycleObservations(txn, observations); err != nil {
			return err
		}
		if err := writeUnresolvedN3TokenClaims(txn, unresolved); err != nil {
			return err
		}
		for _, op := range normalized {
			key := operationKeyForOp(op)
			var existing types.BridgeOperation
			if err := n3Read(txn, key, &existing); err != nil && err != ErrNotFound {
				return err
			} else if err == nil && (existing.Status == types.StatusCompleted || existing.Status == types.StatusStuck) {
				continue
			}
			if err := n3Set(txn, key, op); err != nil {
				return err
			}
			if err := n3Set(txn, statusIndexKeyForOp(op.Status, op), []byte{}); err != nil {
				return err
			}
		}
		for _, obs := range observations {
			if obs.Identity.Direction != types.DirectionWithdrawal {
				continue
			}
			if obs.Fact == FactSettlement {
				if err := n3ReceivedState(txn, obs); err != nil {
					return err
				}
			}
			if obs.Fact == FactDistribution || obs.Fact == FactClaim || obs.Fact == FactExecution {
				if err := n3LegacyCompletion(txn, obs); err != nil {
					return err
				}
			}
		}
		for _, op := range append(reconciled, extra...) {
			if op.Value == nil {
				if err := txn.Delete([]byte(op.Key)); err != nil {
					return err
				}
			} else if err := n3Set(txn, op.Key, op.Value); err != nil {
				return err
			}
		}
		return n3Set(txn, indexerStateKey(state.Chain), state)
	})
}
func n3Read(txn *badger.Txn, key string, dest any) error {
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return item.Value(func(v []byte) error { return json.Unmarshal(v, dest) })
}
func n3Set(txn *badger.Txn, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return txn.Set([]byte(key), b)
}
func n3Instance(id OperationIdentity) string {
	src, dst := "neo_x", "neo_n3"
	var source, destination *string
	if id.BridgeType == types.BridgeTypeToken {
		source = &id.SourceToken
		destination = &id.DestinationToken
	}
	return BuildInstanceID(string(id.BridgeType), src, dst, source, destination)
}
func n3ReceivedState(txn *badger.Txn, obs LifecycleObservation) error {
	id := obs.Identity
	instance := n3Instance(id)
	var metadata types.DirectionalBridgeState
	if err := n3Read(txn, bridgeStateKey(instance), &metadata); err == ErrNotFound {
		metadata = types.DirectionalBridgeState{InstanceID: instance, BridgeType: id.BridgeType, SrcChain: "neo_x", DstChain: "neo_n3"}
		if id.BridgeType == types.BridgeTypeToken {
			metadata.SrcToken = &id.SourceToken
			metadata.DstToken = &id.DestinationToken
		}
		if err := n3Set(txn, bridgeStateKey(instance), metadata); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	var payload struct {
		Root string `json:"root"`
	}
	if err := json.Unmarshal(obs.Evidence.Payload, &payload); err != nil || payload.Root == "" {
		return fmt.Errorf("invalid received root: %v", err)
	}
	next := BuildDestChainState(id.Nonce, obs.Evidence.BlockHeight, obs.Evidence.TxHash, obs.Evidence.ObservedAt, &payload.Root)
	var current types.ChainState
	if err := n3Read(txn, bridgeDstPrefix+instance, &current); err != nil && err != ErrNotFound {
		return err
	} else if err == nil && !(current.Synthetic && current.Nonce == next.Nonce) && !shouldReplaceChainState(&current, next) {
		return nil
	}
	return n3Set(txn, bridgeDstPrefix+instance, next)
}
func n3LegacyCompletion(txn *badger.Txn, obs LifecycleObservation) error {
	id := obs.Identity
	opKey := operationKey(id.BridgeType, id.Direction, id.Nonce)
	if id.BridgeType == types.BridgeTypeToken {
		opKey += ":" + normalizeTokenKeyPart(id.SourceToken) + ":" + normalizeTokenKeyPart(id.DestinationToken)
	}
	var op types.BridgeOperation
	err := n3Read(txn, opKey, &op)
	if err == ErrNotFound && id.BridgeType == types.BridgeTypeToken {
		// Existing databases can contain the legacy unscoped operation key.
		legacyKey := operationKey(id.BridgeType, id.Direction, id.Nonce)
		if legacyErr := n3Read(txn, legacyKey, &op); legacyErr == nil && normalizeTokenKeyPart(strVal(op.TokenContract)) == normalizeTokenKeyPart(id.SourceToken) && normalizeTokenKeyPart(strVal(op.DestTokenContract)) == normalizeTokenKeyPart(id.DestinationToken) {
			opKey = legacyKey
			err = nil
		} else if legacyErr != ErrNotFound && legacyErr != nil {
			return legacyErr
		}
	}
	if err == ErrNotFound {
		src, dst := id.SourceToken, id.DestinationToken
		var source, destination *string
		if id.BridgeType == types.BridgeTypeToken {
			source = &src
			destination = &dst
		}
		key := deferredCompletionKey(id.BridgeType, id.Direction, id.Nonce, source, destination)
		var previous deferredCompletion
		if oldErr := n3Read(txn, key, &previous); oldErr != nil && oldErr != ErrNotFound {
			return oldErr
		} else if oldErr == nil && previous.DestBlock > obs.Evidence.BlockHeight {
			return nil
		}
		return n3Set(txn, key, deferredCompletion{DestBlock: obs.Evidence.BlockHeight, DestTxHash: obs.Evidence.TxHash, CompletionTime: obs.Evidence.ObservedAt})
	}
	if err != nil {
		return err
	}
	if op.Status == types.StatusCompleted {
		if op.DestTxHash != nil && *op.DestTxHash == obs.Evidence.TxHash {
			return nil
		}
		// Preserve historical completed records; lifecycle evidence remains authoritative.
		return nil
	}
	old := op.Status
	op.Status = types.StatusCompleted
	height := obs.Evidence.BlockHeight
	tx := obs.Evidence.TxHash
	t := obs.Evidence.ObservedAt
	op.DestBlockHeight = &height
	op.DestTxHash = &tx
	op.CompletionTimestamp = &t
	if err := n3Set(txn, opKey, &op); err != nil {
		return err
	}
	if err := txn.Delete([]byte(statusIndexKeyForOp(old, &op))); err != nil {
		return err
	}
	if err := n3Set(txn, statusIndexKeyForOp(types.StatusCompleted, &op), []byte{}); err != nil {
		return err
	}
	return nil
}
