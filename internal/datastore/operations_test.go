package datastore

import (
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func sampleOp() *types.BridgeOperation {
	return &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             42,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 1000,
		SourceTxHash:      "0xabc123",
		FromAddress:       "NAddr1",
		ToAddress:         "0xAddr2",
		Timestamp:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Status:            types.StatusPending,
	}
}

func TestOperationStore_SaveAndGet(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	op := sampleOp()
	if err := os.SaveOperation(op); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 42)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Nonce != 42 || got.Status != types.StatusPending {
		t.Fatalf("unexpected: nonce=%d status=%s", got.Nonce, got.Status)
	}
}

func TestOperationStore_GetNotFound(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	_, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 999)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestOperationStore_UpdateCompletion(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	op := sampleOp()
	_ = os.SaveOperation(op)

	completionTime := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	err := os.UpdateOperationCompletion(
		types.BridgeTypeNative, types.DirectionDeposit, 42,
		2000, "0xdef456", completionTime,
	)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	got, _ := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 42)
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
	if got.DestBlockHeight == nil || *got.DestBlockHeight != 2000 {
		t.Fatalf("expected dest block 2000")
	}
	if got.DestTxHash == nil || *got.DestTxHash != "0xdef456" {
		t.Fatalf("expected dest tx hash 0xdef456")
	}
}

func TestOperationStore_ListByStatus(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	op1 := sampleOp()
	op1.Nonce = 1
	_ = os.SaveOperation(op1)

	op2 := sampleOp()
	op2.Nonce = 2
	op2.Status = types.StatusCompleted
	_ = os.SaveOperation(op2)

	op3 := sampleOp()
	op3.Nonce = 3
	_ = os.SaveOperation(op3)

	pending := types.StatusPending
	results, total, err := os.ListOperations(OperationFilter{Status: &pending})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected 2 pending, got %d", total)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestOperationStore_ListByBridgeType(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	op1 := sampleOp()
	op1.Nonce = 1
	op1.BridgeType = types.BridgeTypeNative
	_ = os.SaveOperation(op1)

	op2 := sampleOp()
	op2.Nonce = 2
	op2.BridgeType = types.BridgeTypeToken
	_ = os.SaveOperation(op2)

	native := types.BridgeTypeNative
	results, total, err := os.ListOperations(OperationFilter{BridgeType: &native})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected 1, got %d", total)
	}
	if results[0].BridgeType != types.BridgeTypeNative {
		t.Fatalf("expected native, got %s", results[0].BridgeType)
	}
}

func TestOperationStore_ListByDirection(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	op1 := sampleOp()
	op1.Nonce = 1
	op1.Direction = types.DirectionDeposit
	_ = os.SaveOperation(op1)

	op2 := sampleOp()
	op2.Nonce = 2
	op2.Direction = types.DirectionWithdrawal
	_ = os.SaveOperation(op2)

	deposit := types.DirectionDeposit
	results, total, err := os.ListOperations(OperationFilter{Direction: &deposit})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected 1, got %d", total)
	}
	if results[0].Direction != types.DirectionDeposit {
		t.Fatalf("expected deposit, got %s", results[0].Direction)
	}
}

func TestOperationStore_DeleteOperation(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	op := sampleOp()
	_ = os.SaveOperation(op)

	if err := os.DeleteOperation(types.BridgeTypeNative, types.DirectionDeposit, 42); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 42)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Status index should also be gone
	pending := types.StatusPending
	results, _, _ := os.ListOperations(OperationFilter{Status: &pending})
	if len(results) != 0 {
		t.Fatalf("expected 0 after delete, got %d", len(results))
	}
}

func TestOperationStore_ListWithLimitOffset(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s)

	for i := uint64(1); i <= 5; i++ {
		op := sampleOp()
		op.Nonce = i
		_ = os.SaveOperation(op)
	}

	results, total, err := os.ListOperations(OperationFilter{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 5 {
		t.Fatalf("expected total 5, got %d", total)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestIndexerState_SaveAndGet(t *testing.T) {
	s := newTestStore(t)

	state := &types.IndexerState{
		Chain:           "neo_n3",
		LastBlockHeight: 12345,
		LastBlockHash:   "0xhash",
		IsSyncing:       true,
		UpdatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	if err := s.SaveIndexerState(state); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.GetIndexerState("neo_n3")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LastBlockHeight != 12345 || got.Chain != "neo_n3" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestSaveIndexerStateWithOperations(t *testing.T) {
	s := newTestStore(t)

	state := &types.IndexerState{
		Chain:           "neo_n3",
		LastBlockHeight: 100,
		LastBlockHash:   "0xblock100",
		UpdatedAt:       time.Now(),
	}

	op := sampleOp()
	op.Nonce = 99

	if err := s.SaveIndexerStateWithOperations(state, []*types.BridgeOperation{op}); err != nil {
		t.Fatalf("atomic save: %v", err)
	}

	// Verify both were saved
	got, err := s.GetIndexerState("neo_n3")
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	if got.LastBlockHeight != 100 {
		t.Fatalf("expected height 100, got %d", got.LastBlockHeight)
	}

	os := NewOperationStore(s)
	gotOp, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 99)
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if gotOp.Nonce != 99 {
		t.Fatalf("expected nonce 99, got %d", gotOp.Nonce)
	}
}
