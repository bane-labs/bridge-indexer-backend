//go:build integration

package indexer

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// setupNeoXIndexer creates a NeoX indexer wired to an in-memory store and real RPC.
// Returns the indexer plus a cleanup-registered client. Skips the test on connectivity failure.
func setupNeoXIndexer(t *testing.T, ctx context.Context) *NeoXIndexer {
	t.Helper()

	client, err := neox.NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to NeoX RPC: %v", err)
	}
	t.Cleanup(client.Close)

	store, opStore := testutil.NewTestStore(t)

	proc := NewNeoXEventProcessor(
		opStore, store,
		testutil.NeoXTokenBridgeAddr,
		testutil.NeoXMessageBridgeAddr,
		slog.Default(),
	)

	contractAddrs := []string{testutil.NeoXTokenBridgeAddr, testutil.NeoXMessageBridgeAddr}
	return NewNeoXIndexer(client, store, opStore, proc, contractAddrs, 0, 1, slog.Default())
}

func TestNeoXIndexer_ProcessBlockRange_WithEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	t.Cleanup(cancel)

	idx := setupNeoXIndexer(t, ctx)

	// Process the known range with bridge events (block 3244 has a confirmed event).
	from := testutil.NeoXMainnetEventBlockFrom // 3200
	to := testutil.NeoXMainnetEventBlockTo     // 3700
	if err := idx.processBlockRange(ctx, from, to); err != nil {
		t.Fatalf("processBlockRange(%d, %d) failed: %v", from, to, err)
	}

	// Verify IndexerState was persisted.
	state, err := idx.store.GetIndexerState("neo_x")
	if err != nil {
		t.Fatalf("GetIndexerState: %v", err)
	}
	if state.LastBlockHeight != to {
		t.Errorf("expected LastBlockHeight=%d, got %d", to, state.LastBlockHeight)
	}
	t.Logf("IndexerState: height=%d hash=%q", state.LastBlockHeight, state.LastBlockHash)

	// Check if any operations were saved.
	ops, total, err := idx.opStore.ListOperations(datastore.OperationFilter{Limit: 100})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	t.Logf("Operations found: %d (total=%d) in block range %d-%d", len(ops), total, from, to)
	for i, op := range ops {
		t.Logf("  op[%d]: bridge=%s dir=%s nonce=%d status=%s tx=%s",
			i, op.BridgeType, op.Direction, op.Nonce, op.Status, op.SourceTxHash)
	}
}

func TestNeoXIndexer_ProcessBlockRange_NoEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	t.Cleanup(cancel)

	idx := setupNeoXIndexer(t, ctx)

	// Process a range with no bridge events (very early blocks, no bridge activity).
	from := uint64(100)
	to := uint64(200)
	if err := idx.processBlockRange(ctx, from, to); err != nil {
		t.Fatalf("processBlockRange(%d, %d) failed: %v", from, to, err)
	}

	// State should still be saved.
	state, err := idx.store.GetIndexerState("neo_x")
	if err != nil {
		t.Fatalf("GetIndexerState: %v", err)
	}
	if state.LastBlockHeight != to {
		t.Errorf("expected LastBlockHeight=%d, got %d", to, state.LastBlockHeight)
	}
	t.Logf("Correctly processed no-event range %d-%d, state height=%d", from, to, state.LastBlockHeight)

	// Verify no operations were created.
	ops, total, err := idx.opStore.ListOperations(datastore.OperationFilter{Limit: 100})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if total != 0 {
		t.Errorf("expected 0 operations for no-event range, got %d", total)
	}
	t.Logf("Operations found: %d (expected 0)", len(ops))
}
