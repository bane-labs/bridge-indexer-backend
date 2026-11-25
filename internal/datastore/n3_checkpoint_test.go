package datastore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestN3CheckpointUpdatesLegacyAndReplacesSynthetic(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	id := OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionWithdrawal, Nonce: 11}
	op := &types.BridgeOperation{BridgeType: id.BridgeType, Direction: id.Direction, Nonce: id.Nonce, SourceChain: "neo_x", DestinationChain: "neo_n3", Status: types.StatusPending}
	if err := os.SaveOperation(op); err != nil {
		t.Fatal(err)
	}
	instance := InstanceIDFromOperation(op)
	if err := s.Set(bridgeDstPrefix+instance, &types.ChainState{Nonce: 11, Synthetic: true, Root: ptrRoot("old")}); err != nil {
		t.Fatal(err)
	}
	root := lifecycleEvidence("neo_n3", "relay", 101)
	root.Kind = "notification"
	root.Payload = json.RawMessage(`{"root":"abc"}`)
	distribution := root
	distribution.Position = "1:2"
	distribution.Payload = json.RawMessage(`{"amount":"1"}`)
	obs := []LifecycleObservation{{Identity: id, Fact: FactSettlement, Evidence: root}, {Identity: id, Fact: FactDistribution, Evidence: distribution}}
	state := &types.IndexerState{Chain: "neo_n3", LastBlockHeight: 101, LastBlockHash: "hash"}
	if err := s.SaveN3LifecycleCheckpoint(state, nil, nil, obs); err != nil {
		t.Fatal(err)
	}
	var received types.ChainState
	if err := s.Get(bridgeDstPrefix+instance, &received); err != nil {
		t.Fatal(err)
	}
	if received.Synthetic || received.TxHash != "relay" || received.Root == nil || *received.Root != "abc" {
		t.Fatalf("expected actual relay: %+v", received)
	}
	got, err := os.GetOperation(id.BridgeType, id.Direction, id.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.StatusCompleted || got.DestTxHash == nil || *got.DestTxHash != "relay" {
		t.Fatalf("legacy completion: %+v", got)
	}
	later := root
	later.TxHash = "other"
	bad := []LifecycleObservation{{Identity: id, Fact: FactSettlement, Evidence: later}}
	next := &types.IndexerState{Chain: "neo_n3", LastBlockHeight: 102, LastBlockHash: "hash2"}
	if err := s.SaveN3LifecycleCheckpoint(next, nil, nil, bad); err == nil {
		t.Fatal("conflicting root accepted")
	}
	persisted, err := s.GetIndexerState("neo_n3")
	if err != nil || persisted.LastBlockHeight != 101 {
		t.Fatalf("checkpoint advanced: %+v %v", persisted, err)
	}
	if err := s.SaveN3LifecycleCheckpoint(state, nil, nil, obs); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, err = os.GetOperation(id.BridgeType, id.Direction, id.Nonce)
	if err != nil || got.Status != types.StatusCompleted {
		t.Fatalf("retry changed legacy: %+v %v", got, err)
	}
	if got.CompletionTimestamp == nil || !got.CompletionTimestamp.Equal(root.ObservedAt) {
		t.Fatalf("timestamp: %+v", got)
	}
}
func ptrRoot(root string) *string { return &root }

func TestN3SourceReconcilesDeferredCompletionAtomically(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	when := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	if err := os.UpdateOperationCompletion(types.BridgeTypeNative, types.DirectionDeposit, 6, 42, "neox-receipt", when); err != nil {
		t.Fatal(err)
	}
	state := &types.IndexerState{Chain: "neo_n3", LastBlockHeight: 100}
	source := &types.BridgeOperation{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 6, SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 100, Status: types.StatusPending}
	if err := s.SaveN3LifecycleCheckpoint(state, []*types.BridgeOperation{source}, nil, nil); err != nil {
		t.Fatal(err)
	}
	op, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 6)
	if err != nil || op.Status != types.StatusCompleted || op.DestTxHash == nil || *op.DestTxHash != "neox-receipt" {
		t.Fatalf("deferred completion lost: %+v %v", op, err)
	}
}
