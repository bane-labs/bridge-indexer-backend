package datastore

import (
	"errors"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestNeoXLifecycleCheckpointRollbackAndRetry(t *testing.T) {
	store := newTestStore(t)
	ops := NewOperationStore(store, nil)
	id := OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 42}
	e := lifecycleEvidence("neo_x", "relay", 12)
	state := &types.IndexerState{Chain: "neo_x", LastBlockHeight: 12, LastBlockHash: e.BlockHash, UpdatedAt: time.Now()}
	bad := e
	bad.BlockHash = ""
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, []LifecycleObservation{{Identity: id, Fact: FactSettlement, Evidence: bad}}); err == nil {
		t.Fatal("invalid evidence accepted")
	}
	if _, err := store.GetIndexerState("neo_x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("checkpoint advanced: %v", err)
	}
	if _, err := ops.GetLifecycle(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("observation leaked: %v", err)
	}
	observations := []LifecycleObservation{{Identity: id, Fact: FactSettlement, Evidence: e}}
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, observations); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, observations); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, err := store.GetIndexerState("neo_x")
	if err != nil || got.LastBlockHeight != 12 {
		t.Fatalf("checkpoint: %v %v", got, err)
	}
	changed := e
	changed.TxHash = "other"
	state.LastBlockHeight = 13
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, []LifecycleObservation{{Identity: id, Fact: FactSettlement, Evidence: changed}}); err == nil {
		t.Fatal("conflicting observation accepted")
	}
	got, err = store.GetIndexerState("neo_x")
	if err != nil || got.LastBlockHeight != 12 {
		t.Fatalf("conflict advanced checkpoint: %v %v", got, err)
	}
}
