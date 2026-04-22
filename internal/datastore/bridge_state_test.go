package datastore

import (
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestBuildInstanceID_NativeDeposit(t *testing.T) {
	id := BuildInstanceID("native", "neo_n3", "neo_x", nil, nil)
	expected := "native|neo_n3->neo_x"
	if id != expected {
		t.Fatalf("expected %q, got %q", expected, id)
	}
}

func TestBuildInstanceID_MessageWithdrawal(t *testing.T) {
	id := BuildInstanceID("message", "neo_x", "neo_n3", nil, nil)
	expected := "message|neo_x->neo_n3"
	if id != expected {
		t.Fatalf("expected %q, got %q", expected, id)
	}
}

func TestBuildInstanceID_TokenWithTokens(t *testing.T) {
	src := "0xSrcToken"
	dst := "0xDstToken"
	id := BuildInstanceID("token", "neo_n3", "neo_x", &src, &dst)
	expected := "token|neo_n3->neo_x|srctoken|dsttoken"
	if id != expected {
		t.Fatalf("expected %q, got %q", expected, id)
	}
}

func TestBuildInstanceID_TwoTokenPairsDiffer(t *testing.T) {
	srcA, dstA := "0xTokenA_N3", "0xTokenA_X"
	srcB, dstB := "0xTokenB_N3", "0xTokenB_X"

	idA := BuildInstanceID("token", "neo_n3", "neo_x", &srcA, &dstA)
	idB := BuildInstanceID("token", "neo_n3", "neo_x", &srcB, &dstB)
	if idA == idB {
		t.Fatal("two different token pairs should produce different instance IDs")
	}
}

func TestBridgeStateStore_SaveAndGet(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	state := &types.DirectionalBridgeState{
		InstanceID: "native|neo_n3->neo_x",
		BridgeType: types.BridgeTypeNative,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
	}
	if err := bs.SaveBridgeState(state); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := bs.GetBridgeState("native|neo_n3->neo_x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.InstanceID != state.InstanceID || got.BridgeType != state.BridgeType {
		t.Fatalf("unexpected state: %+v", got)
	}
}

func TestBridgeStateStore_GetNotFound(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	_, err := bs.GetBridgeState("nonexistent")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestBridgeStateStore_ListBridgeStates(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	states := []*types.DirectionalBridgeState{
		{InstanceID: "native|neo_n3->neo_x", BridgeType: types.BridgeTypeNative, SrcChain: "neo_n3", DstChain: "neo_x"},
		{InstanceID: "native|neo_x->neo_n3", BridgeType: types.BridgeTypeNative, SrcChain: "neo_x", DstChain: "neo_n3"},
		{InstanceID: "message|neo_n3->neo_x", BridgeType: types.BridgeTypeMessage, SrcChain: "neo_n3", DstChain: "neo_x"},
	}
	for _, st := range states {
		if err := bs.SaveBridgeState(st); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	list, err := bs.ListBridgeStates()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3, got %d", len(list))
	}
}

func TestBridgeStateStore_UpdateSourceState(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	instanceID := "native|neo_n3->neo_x"
	cs := &types.ChainState{
		Nonce:       1,
		BlockNumber: 100,
		TxHash:      "0xabc",
		ObservedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		IndexedAt:   time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
	}

	// Should create if not exists.
	if err := bs.UpdateSourceState(instanceID, cs, nil); err != nil {
		t.Fatalf("update source: %v", err)
	}

	got, err := bs.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.SourceState == nil {
		t.Fatal("expected source state to be set")
	}
	if got.SourceState.Nonce != 1 || got.SourceState.BlockNumber != 100 {
		t.Fatalf("unexpected source state: %+v", got.SourceState)
	}
	if got.DestState != nil {
		t.Fatal("dest state should be nil")
	}

	// Update again with higher nonce.
	cs2 := &types.ChainState{
		Nonce:       2,
		BlockNumber: 200,
		TxHash:      "0xdef",
		ObservedAt:  time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		IndexedAt:   time.Date(2026, 1, 2, 0, 0, 1, 0, time.UTC),
	}
	if err := bs.UpdateSourceState(instanceID, cs2, nil); err != nil {
		t.Fatalf("update source 2: %v", err)
	}

	got, err = bs.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get 2: %v", err)
	}
	if got.SourceState.Nonce != 2 {
		t.Fatalf("expected nonce 2, got %d", got.SourceState.Nonce)
	}
}

func TestBridgeStateStore_UpdateDestState(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	instanceID := "native|neo_x->neo_n3"

	// Pre-create with source state.
	srcCS := &types.ChainState{
		Nonce:       5,
		BlockNumber: 500,
		TxHash:      "0xsrc",
		ObservedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		IndexedAt:   time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
	}
	if err := bs.UpdateSourceState(instanceID, srcCS, nil); err != nil {
		t.Fatalf("update source: %v", err)
	}

	// Now update dest state.
	destCS := &types.ChainState{
		Nonce:       5,
		BlockNumber: 600,
		TxHash:      "0xdst",
		ObservedAt:  time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		IndexedAt:   time.Date(2026, 1, 2, 0, 0, 1, 0, time.UTC),
	}
	if err := bs.UpdateDestState(instanceID, destCS, nil); err != nil {
		t.Fatalf("update dest: %v", err)
	}

	got, err := bs.GetBridgeState(instanceID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.SourceState == nil || got.DestState == nil {
		t.Fatal("expected both states to be set")
	}
	if got.SourceState.Nonce != 5 || got.DestState.Nonce != 5 {
		t.Fatalf("unexpected nonces: src=%d dst=%d", got.SourceState.Nonce, got.DestState.Nonce)
	}
}

func TestBridgeStateStore_TokenPairIsolation(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	srcA, dstA := "0xTokenA_N3", "0xTokenA_X"
	srcB, dstB := "0xTokenB_N3", "0xTokenB_X"

	idA := BuildInstanceID("token", "neo_n3", "neo_x", &srcA, &dstA)
	idB := BuildInstanceID("token", "neo_n3", "neo_x", &srcB, &dstB)

	// Same nonce (42) for both token pairs.
	csA := &types.ChainState{
		Nonce:       42,
		BlockNumber: 100,
		TxHash:      "0xTxA",
		ObservedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		IndexedAt:   time.Now(),
	}
	csB := &types.ChainState{
		Nonce:       42,
		BlockNumber: 200,
		TxHash:      "0xTxB",
		ObservedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		IndexedAt:   time.Now(),
	}

	if err := bs.UpdateSourceState(idA, csA, nil); err != nil {
		t.Fatalf("update A: %v", err)
	}
	if err := bs.UpdateSourceState(idB, csB, nil); err != nil {
		t.Fatalf("update B: %v", err)
	}

	gotA, err := bs.GetBridgeState(idA)
	if err != nil {
		t.Fatalf("get A: %v", err)
	}
	gotB, err := bs.GetBridgeState(idB)
	if err != nil {
		t.Fatalf("get B: %v", err)
	}

	if gotA.SourceState.TxHash != "0xTxA" {
		t.Fatalf("token pair A contaminated: %s", gotA.SourceState.TxHash)
	}
	if gotB.SourceState.TxHash != "0xTxB" {
		t.Fatalf("token pair B contaminated: %s", gotB.SourceState.TxHash)
	}
}

func TestBridgeStateStore_RebuildFromOperations(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)
	opStore := NewOperationStore(s, bs)

	now := time.Now()
	completionTime := now.Add(time.Minute)
	destBlock := uint64(500)
	destTx := "0xdest"

	// Native deposit: nonce 10 pending, nonce 5 completed.
	if err := opStore.SaveOperation(&types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 10,
		SourceChain: "neo_n3", DestinationChain: "neo_x",
		SourceBlockHeight: 100, SourceTxHash: "0xsrc10",
		FromAddress: "a", ToAddress: "b", Timestamp: now, Status: types.StatusRelayed,
	}); err != nil {
		t.Fatal(err)
	}
	if err := opStore.SaveOperation(&types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 5,
		SourceChain: "neo_n3", DestinationChain: "neo_x",
		SourceBlockHeight: 50, SourceTxHash: "0xsrc5",
		DestBlockHeight: &destBlock, DestTxHash: &destTx, CompletionTimestamp: &completionTime,
		FromAddress: "a", ToAddress: "b", Timestamp: now, Status: types.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}

	// Message deposit: nonce 12 completed, both sides should appear.
	msgDestBlock := uint64(600)
	msgDestTx := "0xmsgdest"
	if err := opStore.SaveOperation(&types.BridgeOperation{
		BridgeType: types.BridgeTypeMessage, Direction: types.DirectionDeposit, Nonce: 12,
		SourceChain: "neo_n3", DestinationChain: "neo_x",
		SourceBlockHeight: 200, SourceTxHash: "0xmsgsrc12",
		DestBlockHeight: &msgDestBlock, DestTxHash: &msgDestTx, CompletionTimestamp: &completionTime,
		FromAddress: "a", ToAddress: "b", Timestamp: now, Status: types.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}

	// Token deposit with same nonce as native (nonce 5), different bridge type — must not collide.
	tokenSrc, tokenDst := "0xTokenSrc", "0xTokenDst"
	tokenDestBlock := uint64(700)
	tokenDestTx := "0xtokendest"
	if err := opStore.SaveOperation(&types.BridgeOperation{
		BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 5,
		SourceChain: "neo_n3", DestinationChain: "neo_x",
		SourceBlockHeight: 60, SourceTxHash: "0xtokensrc5",
		TokenContract: &tokenSrc, DestTokenContract: &tokenDst,
		DestBlockHeight: &tokenDestBlock, DestTxHash: &tokenDestTx, CompletionTimestamp: &completionTime,
		FromAddress: "a", ToAddress: "b", Timestamp: now, Status: types.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}

	if err := bs.RebuildFromOperations(nil); err != nil {
		t.Fatalf("RebuildFromOperations: %v", err)
	}

	// Native: src=10 (highest nonce), dst=5 (only completed)
	nativeID := BuildInstanceID("native", "neo_n3", "neo_x", nil, nil)
	nativeState, err := bs.GetBridgeState(nativeID)
	if err != nil {
		t.Fatalf("get native: %v", err)
	}
	if nativeState.SourceState == nil || nativeState.SourceState.Nonce != 10 {
		t.Fatalf("native src nonce: got %v, want 10", nativeState.SourceState)
	}
	if nativeState.DestState == nil || nativeState.DestState.Nonce != 5 {
		t.Fatalf("native dst nonce: got %v, want 5", nativeState.DestState)
	}

	// Message: src=12, dst=12
	messageID := BuildInstanceID("message", "neo_n3", "neo_x", nil, nil)
	messageState, err := bs.GetBridgeState(messageID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	if messageState.SourceState == nil || messageState.SourceState.Nonce != 12 {
		t.Fatalf("message src nonce: got %v, want 12", messageState.SourceState)
	}
	if messageState.DestState == nil || messageState.DestState.Nonce != 12 {
		t.Fatalf("message dst nonce: got %v, want 12", messageState.DestState)
	}

	// Token: src=5, dst=5 (isolated from native nonce 5)
	tokenID := BuildInstanceID("token", "neo_n3", "neo_x", &tokenSrc, &tokenDst)
	tokenState, err := bs.GetBridgeState(tokenID)
	if err != nil {
		t.Fatalf("get token: %v", err)
	}
	if tokenState.SourceState == nil || tokenState.SourceState.Nonce != 5 {
		t.Fatalf("token src nonce: got %v, want 5", tokenState.SourceState)
	}
	if tokenState.DestState == nil || tokenState.DestState.Nonce != 5 {
		t.Fatalf("token dst nonce: got %v, want 5", tokenState.DestState)
	}
}

func TestBridgeStateStore_RebuildPreservesHigherExistingState(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)

	// Pre-set a bridge state with src nonce=20 (higher than any operation).
	root := "0xroot"
	if err := bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: "native|neo_n3->neo_x",
		BridgeType: types.BridgeTypeNative,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
		SourceState: &types.ChainState{
			Nonce:       20,
			Root:        &root,
			BlockNumber: 999,
			TxHash:      "0xhigh",
			ObservedAt:  time.Now(),
			IndexedAt:   time.Now(),
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Operation with lower nonce.
	opStore := NewOperationStore(s, bs)
	if err := opStore.SaveOperation(&types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 10,
		SourceChain: "neo_n3", DestinationChain: "neo_x",
		SourceBlockHeight: 100, SourceTxHash: "0xsrc10",
		FromAddress: "a", ToAddress: "b", Timestamp: time.Now(), Status: types.StatusRelayed,
	}); err != nil {
		t.Fatal(err)
	}

	if err := bs.RebuildFromOperations(nil); err != nil {
		t.Fatalf("RebuildFromOperations: %v", err)
	}

	state, err := bs.GetBridgeState("native|neo_n3->neo_x")
	if err != nil {
		t.Fatal(err)
	}
	if state.SourceState.Nonce != 20 {
		t.Fatalf("expected nonce 20 to be preserved, got %d", state.SourceState.Nonce)
	}
}

func TestBridgeStateStore_InstanceIDFromOperation(t *testing.T) {
	// Native operation — no tokens.
	op := &types.BridgeOperation{
		BridgeType:       types.BridgeTypeNative,
		Direction:        types.DirectionDeposit,
		SourceChain:      "neo_n3",
		DestinationChain: "neo_x",
	}
	id := InstanceIDFromOperation(op)
	if id != "native|neo_n3->neo_x" {
		t.Fatalf("unexpected: %q", id)
	}

	// Token operation.
	src := "0xSrc"
	dst := "0xDst"
	op2 := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeToken,
		Direction:         types.DirectionDeposit,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		TokenContract:     &src,
		DestTokenContract: &dst,
	}
	id2 := InstanceIDFromOperation(op2)
	expected := "token|neo_n3->neo_x|src|dst"
	if id2 != expected {
		t.Fatalf("expected %q, got %q", expected, id2)
	}
}
