//go:build integration

package monitor

import (
	"log/slog"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// TestStuckDetector_WithRealOperations inserts a mix of pending operations —
// some old enough to exceed the stuck threshold, some recent — then runs
// detector.Check and verifies old ones become Stuck while recent ones stay Pending.
func TestStuckDetector_WithRealOperations(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	_, opStore := testutil.NewTestStore(t)

	threshold := 5 * time.Minute

	// Old operation: 10 minutes ago → should become stuck
	oldOp := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             100,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 5000,
		SourceTxHash:      "0xold_stuck_tx",
		FromAddress:       "NAddr1",
		ToAddress:         "0xAddr1",
		Timestamp:         time.Now().Add(-10 * time.Minute),
		Status:            types.StatusPending,
	}
	if err := opStore.SaveOperation(oldOp); err != nil {
		t.Fatalf("save old op: %v", err)
	}

	// Recent operation: 1 minute ago → should stay pending
	recentOp := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeToken,
		Direction:         types.DirectionWithdrawal,
		Nonce:             200,
		SourceChain:       "neo_x",
		DestinationChain:  "neo_n3",
		SourceBlockHeight: 6000,
		SourceTxHash:      "0xrecent_pending_tx",
		FromAddress:       "0xAddr2",
		ToAddress:         "NAddr2",
		Timestamp:         time.Now().Add(-1 * time.Minute),
		Status:            types.StatusPending,
	}
	if err := opStore.SaveOperation(recentOp); err != nil {
		t.Fatalf("save recent op: %v", err)
	}

	// Another old operation on a different bridge type
	oldOp2 := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionDeposit,
		Nonce:             300,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 7000,
		SourceTxHash:      "0xold_msg_tx",
		FromAddress:       "NAddr3",
		ToAddress:         "0xAddr3",
		Timestamp:         time.Now().Add(-15 * time.Minute),
		Status:            types.StatusPending,
	}
	if err := opStore.SaveOperation(oldOp2); err != nil {
		t.Fatalf("save old op2: %v", err)
	}

	detector := NewStuckDetector(opStore, &alerter.NoopAlerter{}, StuckThresholds{Default: threshold}, slog.Default())
	if err := detector.Check(ctx); err != nil {
		t.Fatalf("check: %v", err)
	}

	// Verify old native deposit is now stuck
	got1, err := opStore.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 100)
	if err != nil {
		t.Fatalf("get old op: %v", err)
	}
	if got1.Status != types.StatusStuck {
		t.Errorf("expected old native op stuck, got %s", got1.Status)
	}

	// Verify recent token withdrawal is still pending
	got2, err := opStore.GetOperation(types.BridgeTypeToken, types.DirectionWithdrawal, 200)
	if err != nil {
		t.Fatalf("get recent op: %v", err)
	}
	if got2.Status != types.StatusPending {
		t.Errorf("expected recent token op pending, got %s", got2.Status)
	}

	// Verify old message deposit is now stuck
	got3, err := opStore.GetOperation(types.BridgeTypeMessage, types.DirectionDeposit, 300)
	if err != nil {
		t.Fatalf("get old msg op: %v", err)
	}
	if got3.Status != types.StatusStuck {
		t.Errorf("expected old message op stuck, got %s", got3.Status)
	}
}

// TestHealthAggregator_WithRealOperations inserts completed and stuck operations
// across bridge types, then verifies GetOverallHealth returns a valid structure
// with all 3 bridge types and correct status.
func TestHealthAggregator_WithRealOperations(t *testing.T) {
	testutil.SkipIfNoNetwork(t)

	store, opStore := testutil.NewTestStore(t)
	pd := NewPauseDetector(store, slog.Default())
	ha := NewHealthAggregator(opStore, pd, slog.Default())

	completionTime := time.Now()
	destBlock := uint64(500)
	destTx := "0xdest_completed"

	// Completed native deposit
	completed := &types.BridgeOperation{
		BridgeType:          types.BridgeTypeNative,
		Direction:           types.DirectionDeposit,
		Nonce:               1,
		SourceChain:         "neo_n3",
		DestinationChain:    "neo_x",
		SourceBlockHeight:   100,
		SourceTxHash:        "0xtx_completed",
		FromAddress:         "NAddr",
		ToAddress:           "0xAddr",
		Timestamp:           time.Now().Add(-10 * time.Minute),
		Status:              types.StatusCompleted,
		DestBlockHeight:     &destBlock,
		DestTxHash:          &destTx,
		CompletionTimestamp: &completionTime,
	}
	if err := opStore.SaveOperation(completed); err != nil {
		t.Fatalf("save completed: %v", err)
	}

	// Stuck native deposit
	stuck := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             2,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 101,
		SourceTxHash:      "0xtx_stuck",
		FromAddress:       "NAddr",
		ToAddress:         "0xAddr",
		Timestamp:         time.Now().Add(-20 * time.Minute),
		Status:            types.StatusStuck,
	}
	if err := opStore.SaveOperation(stuck); err != nil {
		t.Fatalf("save stuck: %v", err)
	}

	// Completed token withdrawal
	completionTime2 := time.Now().Add(-5 * time.Minute)
	destBlock2 := uint64(600)
	destTx2 := "0xdest_token"
	tokenCompleted := &types.BridgeOperation{
		BridgeType:          types.BridgeTypeToken,
		Direction:           types.DirectionWithdrawal,
		Nonce:               1,
		SourceChain:         "neo_x",
		DestinationChain:    "neo_n3",
		SourceBlockHeight:   200,
		SourceTxHash:        "0xtx_token",
		FromAddress:         "0xAddr",
		ToAddress:           "NAddr",
		Timestamp:           time.Now().Add(-15 * time.Minute),
		Status:              types.StatusCompleted,
		DestBlockHeight:     &destBlock2,
		DestTxHash:          &destTx2,
		CompletionTimestamp: &completionTime2,
	}
	if err := opStore.SaveOperation(tokenCompleted); err != nil {
		t.Fatalf("save token completed: %v", err)
	}

	health, err := ha.GetOverallHealth()
	if err != nil {
		t.Fatalf("GetOverallHealth: %v", err)
	}

	// Verify structure: must have all 3 bridge types
	if len(health.Bridges) != 3 {
		t.Fatalf("expected 3 bridges, got %d", len(health.Bridges))
	}

	bridgeMap := make(map[types.BridgeType]types.BridgeHealth)
	for _, b := range health.Bridges {
		bridgeMap[b.BridgeType] = b
	}

	// Native has 1 completed + 1 stuck; status stays healthy (stuck is informational).
	native, ok := bridgeMap[types.BridgeTypeNative]
	if !ok {
		t.Fatal("native bridge not found in health")
	}
	if native.Status != types.HealthStatusHealthy {
		t.Errorf("expected native healthy, got %s", native.Status)
	}

	// Token has 1 completed, 0 stuck → healthy
	token, ok := bridgeMap[types.BridgeTypeToken]
	if !ok {
		t.Fatal("token bridge not found in health")
	}
	if token.Status != types.HealthStatusHealthy {
		t.Errorf("expected token healthy, got %s", token.Status)
	}

	// Message has no operations → healthy
	msg, ok := bridgeMap[types.BridgeTypeMessage]
	if !ok {
		t.Fatal("message bridge not found in health")
	}
	if msg.Status != types.HealthStatusHealthy {
		t.Errorf("expected message healthy, got %s", msg.Status)
	}

	if health.Status != types.HealthStatusHealthy {
		t.Errorf("expected overall healthy, got %s", health.Status)
	}
}

// TestHealthAggregator_DelaysWithRealData inserts completed operations with known
// timestamps and verifies GetDelays returns expected average delay values.
func TestHealthAggregator_DelaysWithRealData(t *testing.T) {
	testutil.SkipIfNoNetwork(t)

	store, opStore := testutil.NewTestStore(t)
	pd := NewPauseDetector(store, slog.Default())
	ha := NewHealthAggregator(opStore, pd, slog.Default())

	now := time.Now()
	destBlock := uint64(500)
	destTx := "0xdest"

	// Operation 1: started 20min ago, completed 15min ago → 5min = 300s delay
	completionTime1 := now.Add(-15 * time.Minute)
	op1 := &types.BridgeOperation{
		BridgeType:          types.BridgeTypeNative,
		Direction:           types.DirectionDeposit,
		Nonce:               10,
		SourceChain:         "neo_n3",
		DestinationChain:    "neo_x",
		SourceBlockHeight:   100,
		SourceTxHash:        "0xtx_delay1",
		FromAddress:         "NAddr",
		ToAddress:           "0xAddr",
		Timestamp:           now.Add(-20 * time.Minute),
		Status:              types.StatusCompleted,
		DestBlockHeight:     &destBlock,
		DestTxHash:          &destTx,
		CompletionTimestamp: &completionTime1,
	}
	if err := opStore.SaveOperation(op1); err != nil {
		t.Fatalf("save op1: %v", err)
	}

	// Operation 2: started 12min ago, completed 2min ago → 10min = 600s delay
	completionTime2 := now.Add(-2 * time.Minute)
	destTx2 := "0xdest2"
	op2 := &types.BridgeOperation{
		BridgeType:          types.BridgeTypeNative,
		Direction:           types.DirectionDeposit,
		Nonce:               11,
		SourceChain:         "neo_n3",
		DestinationChain:    "neo_x",
		SourceBlockHeight:   101,
		SourceTxHash:        "0xtx_delay2",
		FromAddress:         "NAddr",
		ToAddress:           "0xAddr",
		Timestamp:           now.Add(-12 * time.Minute),
		Status:              types.StatusCompleted,
		DestBlockHeight:     &destBlock,
		DestTxHash:          &destTx2,
		CompletionTimestamp: &completionTime2,
	}
	if err := opStore.SaveOperation(op2); err != nil {
		t.Fatalf("save op2: %v", err)
	}

	delays, err := ha.GetDelays()
	if err != nil {
		t.Fatalf("GetDelays: %v", err)
	}

	// Expected avg: (300 + 600) / 2 = 450s
	avg24h, ok := delays.Avg24h[types.BridgeTypeNative]
	if !ok {
		t.Fatal("expected native avg24h entry")
	}
	// Allow ±30s tolerance for timing drift
	if avg24h < 420 || avg24h > 480 {
		t.Errorf("expected ~450s avg24h, got %f", avg24h)
	}

	avg7d, ok := delays.Avg7d[types.BridgeTypeNative]
	if !ok {
		t.Fatal("expected native avg7d entry")
	}
	if avg7d < 420 || avg7d > 480 {
		t.Errorf("expected ~450s avg7d, got %f", avg7d)
	}

	// Token and message should have no entries (no completed ops)
	if _, ok := delays.Avg24h[types.BridgeTypeToken]; ok {
		t.Error("expected no token avg24h entry")
	}
	if _, ok := delays.Avg24h[types.BridgeTypeMessage]; ok {
		t.Error("expected no message avg24h entry")
	}
}

// TestPauseDetector_WithRealState tests UpdatePause across different bridge types
// and scopes, verifying IsDepositsPaused and IsWithdrawalsPaused return correct values.
func TestPauseDetector_WithRealState(t *testing.T) {
	testutil.SkipIfNoNetwork(t)

	store, _ := testutil.NewTestStore(t)
	pd := NewPauseDetector(store, slog.Default())

	// Initially nothing is paused
	for _, bt := range []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage} {
		if pd.IsDepositsPaused(bt) {
			t.Errorf("expected %s deposits not paused initially", bt)
		}
		if pd.IsWithdrawalsPaused(bt) {
			t.Errorf("expected %s withdrawals not paused initially", bt)
		}
	}

	// Pause all for native
	pd.UpdatePause(types.BridgeTypeNative, true, "all")
	if !pd.IsDepositsPaused(types.BridgeTypeNative) {
		t.Error("expected native deposits paused after 'all'")
	}
	if !pd.IsWithdrawalsPaused(types.BridgeTypeNative) {
		t.Error("expected native withdrawals paused after 'all'")
	}
	// Token and message should be unaffected
	if pd.IsDepositsPaused(types.BridgeTypeToken) {
		t.Error("token deposits should not be paused")
	}
	if pd.IsWithdrawalsPaused(types.BridgeTypeMessage) {
		t.Error("message withdrawals should not be paused")
	}

	// Unpause only deposits for native
	pd.UpdatePause(types.BridgeTypeNative, false, "deposits")
	if pd.IsDepositsPaused(types.BridgeTypeNative) {
		t.Error("expected native deposits unpaused")
	}
	if !pd.IsWithdrawalsPaused(types.BridgeTypeNative) {
		t.Error("expected native withdrawals still paused")
	}

	// Pause deposits only for token
	pd.UpdatePause(types.BridgeTypeToken, true, "deposits")
	if !pd.IsDepositsPaused(types.BridgeTypeToken) {
		t.Error("expected token deposits paused")
	}
	if pd.IsWithdrawalsPaused(types.BridgeTypeToken) {
		t.Error("expected token withdrawals not paused")
	}

	// Pause withdrawals only for message
	pd.UpdatePause(types.BridgeTypeMessage, true, "withdrawals")
	if pd.IsDepositsPaused(types.BridgeTypeMessage) {
		t.Error("expected message deposits not paused")
	}
	if !pd.IsWithdrawalsPaused(types.BridgeTypeMessage) {
		t.Error("expected message withdrawals paused")
	}

	// Verify health aggregator reflects pause state
	_, opStore := testutil.NewTestStore(t)
	ha := NewHealthAggregator(opStore, pd, slog.Default())
	health, err := ha.GetOverallHealth()
	if err != nil {
		t.Fatalf("GetOverallHealth: %v", err)
	}

	bridgeMap := make(map[types.BridgeType]types.BridgeHealth)
	for _, b := range health.Bridges {
		bridgeMap[b.BridgeType] = b
	}

	// Native: withdrawals paused → degraded
	if bridgeMap[types.BridgeTypeNative].Status != types.HealthStatusDegraded {
		t.Errorf("expected native degraded (withdrawals paused), got %s", bridgeMap[types.BridgeTypeNative].Status)
	}
	// Token: deposits paused → degraded
	if bridgeMap[types.BridgeTypeToken].Status != types.HealthStatusDegraded {
		t.Errorf("expected token degraded (deposits paused), got %s", bridgeMap[types.BridgeTypeToken].Status)
	}
	// Message: withdrawals paused → degraded
	if bridgeMap[types.BridgeTypeMessage].Status != types.HealthStatusDegraded {
		t.Errorf("expected message degraded (withdrawals paused), got %s", bridgeMap[types.BridgeTypeMessage].Status)
	}
}
