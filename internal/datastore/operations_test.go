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
	os := NewOperationStore(s, nil)

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
	os := NewOperationStore(s, nil)

	_, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 999)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestOperationStore_UpdateCompletion(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)

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
	os := NewOperationStore(s, nil)

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
	os := NewOperationStore(s, nil)

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
	os := NewOperationStore(s, nil)

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
	os := NewOperationStore(s, nil)

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
	os := NewOperationStore(s, nil)

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

	os := NewOperationStore(s, nil)
	gotOp, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 99)
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if gotOp.Nonce != 99 {
		t.Fatalf("expected nonce 99, got %d", gotOp.Nonce)
	}
}

func TestOperationStore_UpdateCompletionWithBridgeState(t *testing.T) {
	s := newTestStore(t)
	bss := NewBridgeStateStore(s)
	os := NewOperationStore(s, bss)

	// Pre-create a bridge state with SourceState set
	instanceID := BuildInstanceID("native", "neo_n3", "neo_x", nil, nil)
	state := &types.DirectionalBridgeState{
		InstanceID: instanceID,
		BridgeType: types.BridgeTypeNative,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
		SourceState: &types.ChainState{
			Nonce:       42,
			BlockNumber: 1000,
			TxHash:      "0xabc123",
			ObservedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			IndexedAt:   time.Now(),
		},
	}
	if err := bss.SaveBridgeState(state); err != nil {
		t.Fatalf("save bridge state: %v", err)
	}

	// Save a pending native operation
	op := sampleOp()
	if err := os.SaveOperation(op); err != nil {
		t.Fatalf("save operation: %v", err)
	}

	// Update operation completion
	completionTime := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	err := os.UpdateOperationCompletion(
		types.BridgeTypeNative, types.DirectionDeposit, 42,
		2000, "0xdest", completionTime,
	)
	if err != nil {
		t.Fatalf("update completion: %v", err)
	}

	// Assert: operation status is completed
	got, _ := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 42)
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
	if got.DestBlockHeight == nil || *got.DestBlockHeight != 2000 {
		t.Fatalf("expected dest block 2000")
	}
	if got.DestTxHash == nil || *got.DestTxHash != "0xdest" {
		t.Fatalf("expected dest tx hash 0xdest")
	}

	// Assert: bridge state has DestState set
	gotState, err := bss.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get bridge state: %v", err)
	}
	if gotState.DestState == nil {
		t.Fatalf("expected DestState to be set")
	}
	if gotState.DestState.BlockNumber != 2000 {
		t.Fatalf("expected dest block 2000, got %d", gotState.DestState.BlockNumber)
	}
	if gotState.DestState.TxHash != "0xdest" {
		t.Fatalf("expected dest tx hash 0xdest, got %s", gotState.DestState.TxHash)
	}

	// Assert: SourceState is still intact
	if gotState.SourceState == nil {
		t.Fatalf("expected SourceState to be preserved")
	}
	if gotState.SourceState.BlockNumber != 1000 {
		t.Fatalf("expected source block 1000, got %d", gotState.SourceState.BlockNumber)
	}
}

func TestOperationStore_UpdateCompletionWithBridgeState_TokenPair(t *testing.T) {
	s := newTestStore(t)
	bss := NewBridgeStateStore(s)
	os := NewOperationStore(s, bss)

	// Create a pending token operation with token contracts
	op := sampleOp()
	op.BridgeType = types.BridgeTypeToken
	op.Nonce = 99
	srcToken := "0xSrc"
	dstToken := "0xDst"
	op.TokenContract = &srcToken
	op.DestTokenContract = &dstToken

	if err := os.SaveOperation(op); err != nil {
		t.Fatalf("save operation: %v", err)
	}

	// Update operation completion
	completionTime := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	err := os.UpdateOperationCompletion(
		types.BridgeTypeToken, types.DirectionDeposit, 99,
		3000, "0xtoken_dest", completionTime,
	)
	if err != nil {
		t.Fatalf("update completion: %v", err)
	}

	// Assert: operation is completed
	got, _ := os.GetOperation(types.BridgeTypeToken, types.DirectionDeposit, 99)
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}

	// Assert: bridge state stored under correct instance ID with token pair
	instanceID := BuildInstanceID("token", "neo_n3", "neo_x", &srcToken, &dstToken)
	gotState, err := bss.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get bridge state: %v", err)
	}
	if gotState.DestState == nil {
		t.Fatalf("expected DestState to be set")
	}
	if gotState.DestState.BlockNumber != 3000 {
		t.Fatalf("expected dest block 3000, got %d", gotState.DestState.BlockNumber)
	}
	if gotState.DestState.TxHash != "0xtoken_dest" {
		t.Fatalf("expected dest tx hash 0xtoken_dest, got %s", gotState.DestState.TxHash)
	}
}

func TestOperationStore_UpdateCompletionWithoutBridgeState(t *testing.T) {
	s := newTestStore(t)
	// Create OperationStore with nil bridgeStateStore
	os := NewOperationStore(s, nil)

	// Save a pending operation
	op := sampleOp()
	op.Nonce = 77
	if err := os.SaveOperation(op); err != nil {
		t.Fatalf("save operation: %v", err)
	}

	// Update operation completion (should not panic)
	completionTime := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	err := os.UpdateOperationCompletion(
		types.BridgeTypeNative, types.DirectionDeposit, 77,
		5000, "0xno_bridge_state", completionTime,
	)
	if err != nil {
		t.Fatalf("update completion: %v", err)
	}

	// Assert: operation is completed normally
	got, _ := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 77)
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
	if got.DestBlockHeight == nil || *got.DestBlockHeight != 5000 {
		t.Fatalf("expected dest block 5000")
	}
	if got.DestTxHash == nil || *got.DestTxHash != "0xno_bridge_state" {
		t.Fatalf("expected dest tx hash 0xno_bridge_state")
	}
}

func TestOperationStore_DeferredCompletionReconciledOnSave_Native(t *testing.T) {
	s := newTestStore(t)
	bss := NewBridgeStateStore(s)
	os := NewOperationStore(s, bss)

	completionTime := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	if err := os.UpdateOperationCompletion(
		types.BridgeTypeNative, types.DirectionDeposit, 42,
		2000, "0xlate", completionTime,
	); err != nil {
		t.Fatalf("deferred completion: %v", err)
	}

	state := &types.IndexerState{
		Chain:           "neo_n3",
		LastBlockHeight: 100,
		LastBlockHash:   "0xblock100",
		UpdatedAt:       time.Now(),
	}
	op := sampleOp()
	root := "0xroot42"
	op.Root = &root

	sourceOps, err := bss.BuildSourceStateWriteOps([]*types.BridgeOperation{op})
	if err != nil {
		t.Fatalf("build source ops: %v", err)
	}
	if err := s.SaveIndexerStateWithOperationsAndExtra(state, []*types.BridgeOperation{op}, sourceOps); err != nil {
		t.Fatalf("save state with reconciled op: %v", err)
	}

	got, err := os.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 42)
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
	if got.DestTxHash == nil || *got.DestTxHash != "0xlate" {
		t.Fatalf("expected reconciled dest tx hash")
	}

	instanceID := BuildInstanceID("native", "neo_n3", "neo_x", nil, nil)
	st, err := bss.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get bridge state: %v", err)
	}
	if st.DestState == nil || st.DestState.Root == nil || *st.DestState.Root != "0xroot42" {
		t.Fatalf("expected destination root to be filled from source operation")
	}
}

func TestOperationStore_DeferredCompletionReconciledOnSave_Token(t *testing.T) {
	s := newTestStore(t)
	bss := NewBridgeStateStore(s)
	os := NewOperationStore(s, bss)

	completionTime := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	if err := os.UpdateOperationCompletion(
		types.BridgeTypeToken, types.DirectionDeposit, 99,
		3000, "0xtokenlate", completionTime,
	); err != nil {
		t.Fatalf("deferred completion: %v", err)
	}

	state := &types.IndexerState{
		Chain:           "neo_n3",
		LastBlockHeight: 101,
		LastBlockHash:   "0xblock101",
		UpdatedAt:       time.Now(),
	}
	op := sampleOp()
	op.BridgeType = types.BridgeTypeToken
	op.Nonce = 99
	srcToken := "0xSrc"
	dstToken := "0xDst"
	root := "0xtokenroot"
	op.TokenContract = &srcToken
	op.DestTokenContract = &dstToken
	op.Root = &root

	sourceOps, err := bss.BuildSourceStateWriteOps([]*types.BridgeOperation{op})
	if err != nil {
		t.Fatalf("build source ops: %v", err)
	}
	if err := s.SaveIndexerStateWithOperationsAndExtra(state, []*types.BridgeOperation{op}, sourceOps); err != nil {
		t.Fatalf("save state with reconciled token op: %v", err)
	}

	got, err := os.GetOperation(types.BridgeTypeToken, types.DirectionDeposit, 99)
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}

	instanceID := BuildInstanceID("token", "neo_n3", "neo_x", &srcToken, &dstToken)
	st, err := bss.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get bridge state: %v", err)
	}
	if st.DestState == nil || st.DestState.Root == nil || *st.DestState.Root != "0xtokenroot" {
		t.Fatalf("expected token destination root to be filled from source operation")
	}
}
