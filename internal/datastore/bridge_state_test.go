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
	expected := "token|neo_n3->neo_x|0xSrcToken|0xDstToken"
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
	if err := bs.UpdateSourceState(instanceID, cs); err != nil {
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
	if err := bs.UpdateSourceState(instanceID, cs2); err != nil {
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
	if err := bs.UpdateSourceState(instanceID, srcCS); err != nil {
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
	if err := bs.UpdateDestState(instanceID, destCS); err != nil {
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

	if err := bs.UpdateSourceState(idA, csA); err != nil {
		t.Fatalf("update A: %v", err)
	}
	if err := bs.UpdateSourceState(idB, csB); err != nil {
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
	expected := "token|neo_n3->neo_x|0xSrc|0xDst"
	if id2 != expected {
		t.Fatalf("expected %q, got %q", expected, id2)
	}
}
