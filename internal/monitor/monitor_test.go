package monitor

import (
	"context"
	"log/slog"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func newTestStore(t *testing.T) (*datastore.Store, *datastore.OperationStore) {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	return store, datastore.NewOperationStore(store, nil)
}

func TestStuckDetector_MarksStuck(t *testing.T) {
	_, opStore := newTestStore(t)

	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             1,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 100,
		SourceTxHash:      "0xtx",
		FromAddress:       "from",
		ToAddress:         "to",
		Timestamp:         time.Now().Add(-10 * time.Minute),
		Status:            types.StatusPending,
	}
	_ = opStore.SaveOperation(op)

	detector := NewStuckDetector(opStore, &alerter.NoopAlerter{}, StuckThresholds{Default: 5 * time.Minute}, slog.Default())
	if err := detector.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}

	got, _ := opStore.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 1)
	if got.Status != types.StatusStuck {
		t.Fatalf("expected stuck, got %s", got.Status)
	}
}

func TestStuckDetector_KeepsPending(t *testing.T) {
	_, opStore := newTestStore(t)

	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             2,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 100,
		SourceTxHash:      "0xtx",
		FromAddress:       "from",
		ToAddress:         "to",
		Timestamp:         time.Now().Add(-1 * time.Minute),
		Status:            types.StatusPending,
	}
	_ = opStore.SaveOperation(op)

	detector := NewStuckDetector(opStore, &alerter.NoopAlerter{}, StuckThresholds{Default: 5 * time.Minute}, slog.Default())
	if err := detector.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}

	got, _ := opStore.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 2)
	if got.Status != types.StatusPending {
		t.Fatalf("expected pending, got %s", got.Status)
	}
}

func TestStuckDetector_MessageExecutableUsesLongerThreshold(t *testing.T) {
	_, opStore := newTestStore(t)
	ex := types.AMBMessageTypeExecutable
	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionWithdrawal,
		Nonce:             5,
		SourceChain:       "neo_x",
		DestinationChain:  "neo_n3",
		SourceBlockHeight: 100,
		SourceTxHash:      "0xtx",
		FromAddress:       "0xs",
		ToAddress:         "",
		Timestamp:         time.Now().Add(-6 * time.Minute),
		Status:            types.StatusPending,
		AMBMessageType:    &ex,
	}
	_ = opStore.SaveOperation(op)

	detector := NewStuckDetector(opStore, &alerter.NoopAlerter{}, StuckThresholds{Default: 5 * time.Minute, Message: 24 * time.Hour}, slog.Default())
	if err := detector.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	got, _ := opStore.GetOperation(types.BridgeTypeMessage, types.DirectionWithdrawal, 5)
	if got.Status != types.StatusPending {
		t.Fatalf("expected still pending (message threshold 24h), got %s", got.Status)
	}
}

func TestStuckDetector_NativeUsesCustomThreshold(t *testing.T) {
	_, opStore := newTestStore(t)
	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             3,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 100,
		SourceTxHash:      "0xtx",
		FromAddress:       "from",
		ToAddress:         "to",
		Timestamp:         time.Now().Add(-15 * time.Minute),
		Status:            types.StatusPending,
	}
	_ = opStore.SaveOperation(op)

	detector := NewStuckDetector(opStore, &alerter.NoopAlerter{}, StuckThresholds{
		Default: 5 * time.Minute,
		Native:  30 * time.Minute,
	}, slog.Default())
	if err := detector.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	got, _ := opStore.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 3)
	if got.Status != types.StatusPending {
		t.Fatalf("expected pending (native threshold 30m), got %s", got.Status)
	}
}

func TestStuckDetector_SkipsMessageStoreOnlyPending(t *testing.T) {
	_, opStore := newTestStore(t)
	so := types.AMBMessageTypeStoreOnly
	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionWithdrawal,
		Nonce:             1,
		SourceChain:       "neo_x",
		DestinationChain:  "neo_n3",
		SourceBlockHeight: 100,
		SourceTxHash:      "0xtx",
		FromAddress:       "0xs",
		ToAddress:         "",
		Timestamp:         time.Now().Add(-48 * time.Hour),
		Status:            types.StatusPending,
		AMBMessageType:    &so,
	}
	_ = opStore.SaveOperation(op)

	detector := NewStuckDetector(opStore, &alerter.NoopAlerter{}, StuckThresholds{Default: 5 * time.Minute, Message: 24 * time.Hour}, slog.Default())
	if err := detector.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	got, _ := opStore.GetOperation(types.BridgeTypeMessage, types.DirectionWithdrawal, 1)
	if got.Status != types.StatusPending {
		t.Fatalf("store_only should not be marked stuck, got %s", got.Status)
	}
}

func TestPauseDetector_UpdateAndQuery(t *testing.T) {
	store, _ := newTestStore(t)
	pd := NewPauseDetector(store, slog.Default())

	pd.UpdatePause(types.BridgeTypeNative, true, "all")
	if !pd.IsDepositsPaused(types.BridgeTypeNative) {
		t.Fatal("expected native deposits paused")
	}
	if !pd.IsWithdrawalsPaused(types.BridgeTypeNative) {
		t.Fatal("expected native withdrawals paused")
	}
	if pd.IsDepositsPaused(types.BridgeTypeToken) {
		t.Fatal("token should not be paused")
	}

	pd.UpdatePause(types.BridgeTypeNative, false, "deposits")
	if pd.IsDepositsPaused(types.BridgeTypeNative) {
		t.Fatal("expected native deposits unpaused")
	}
	if !pd.IsWithdrawalsPaused(types.BridgeTypeNative) {
		t.Fatal("expected native withdrawals still paused")
	}
}

func TestHealthAggregator_Healthy(t *testing.T) {
	store, opStore := newTestStore(t)
	pd := NewPauseDetector(store, slog.Default())
	ha := NewHealthAggregator(opStore, pd, slog.Default())

	health, err := ha.GetOverallHealth()
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != types.HealthStatusHealthy {
		t.Fatalf("expected healthy, got %s", health.Status)
	}
}

func TestHealthAggregator_StuckDoesNotAffectStatus(t *testing.T) {
	store, opStore := newTestStore(t)
	pd := NewPauseDetector(store, slog.Default())
	ha := NewHealthAggregator(opStore, pd, slog.Default())

	completionTime := time.Now()
	destBlock := uint64(200)
	destTx := "0xdest"

	completed := &types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 1,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 100,
		SourceTxHash: "0xtx1", FromAddress: "f", ToAddress: "t",
		Timestamp: time.Now().Add(-10 * time.Minute), Status: types.StatusCompleted,
		DestBlockHeight: &destBlock, DestTxHash: &destTx, CompletionTimestamp: &completionTime,
	}
	_ = opStore.SaveOperation(completed)

	stuck := &types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 2,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 101,
		SourceTxHash: "0xtx2", FromAddress: "f", ToAddress: "t",
		Timestamp: time.Now().Add(-10 * time.Minute), Status: types.StatusStuck,
	}
	_ = opStore.SaveOperation(stuck)

	health, err := ha.GetOverallHealth()
	if err != nil {
		t.Fatalf("health: %v", err)
	}

	found := false
	for _, b := range health.Bridges {
		if b.BridgeType == types.BridgeTypeNative {
			found = true
			if b.Status != types.HealthStatusHealthy {
				t.Fatalf("expected healthy (stuck ops do not degrade health), got %s", b.Status)
			}
			if b.StuckOperations != 1 {
				t.Fatalf("expected 1 stuck, got %d", b.StuckOperations)
			}
		}
	}
	if !found {
		t.Fatal("native bridge not found in health")
	}
}

func TestHealthAggregator_Delays(t *testing.T) {
	_, opStore := newTestStore(t)
	store2, _ := newTestStore(t)
	pd := NewPauseDetector(store2, slog.Default())
	ha := NewHealthAggregator(opStore, pd, slog.Default())

	now := time.Now()
	completionTime := now.Add(-30 * time.Minute)
	destBlock := uint64(200)
	destTx := "0xdest"

	op := &types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 10,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 100,
		SourceTxHash: "0xtx", FromAddress: "f", ToAddress: "t",
		Timestamp: now.Add(-35 * time.Minute), Status: types.StatusCompleted,
		DestBlockHeight: &destBlock, DestTxHash: &destTx, CompletionTimestamp: &completionTime,
	}
	_ = opStore.SaveOperation(op)

	delays, err := ha.GetDelays()
	if err != nil {
		t.Fatalf("delays: %v", err)
	}

	avg, ok := delays.Avg24h[types.BridgeTypeNative]
	if !ok {
		t.Fatal("expected native avg24h")
	}
	if avg < 200 || avg > 400 {
		t.Fatalf("expected ~300s avg, got %f", avg)
	}
}
