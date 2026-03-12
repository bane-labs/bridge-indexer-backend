//go:build integration

package indexer

import (
	"log/slog"
	"testing"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
)

// TestN3Indexer_ProcessBlock_WithEvents processes a known mainnet block that
// contains bridge events and verifies that IndexerState and BridgeOperations
// are persisted correctly.
func TestN3Indexer_ProcessBlock_WithEvents(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := n3.NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to N3 RPC: %v", err)
	}
	t.Cleanup(client.Close)

	store, opStore := testutil.NewTestStore(t)

	proc, err := NewN3EventProcessor(opStore, testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash(), slog.Default())
	if err != nil {
		t.Fatalf("NewN3EventProcessor: %v", err)
	}

	contractHashes := []string{testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash()}
	idx, err := NewN3Indexer(client, store, opStore, nil, proc, contractHashes, 0, 1, slog.Default())
	if err != nil {
		t.Fatalf("NewN3Indexer: %v", err)
	}

	// Process the known block with bridge events
	if err := idx.processBlock(ctx, testutil.N3MainnetEventBlock); err != nil {
		t.Fatalf("processBlock(%d) failed: %v", testutil.N3MainnetEventBlock, err)
	}

	// Verify IndexerState was saved
	state, err := store.GetIndexerState("neo_n3")
	if err != nil {
		t.Fatalf("GetIndexerState: %v", err)
	}
	if state.LastBlockHeight != uint64(testutil.N3MainnetEventBlock) {
		t.Errorf("expected LastBlockHeight=%d, got %d", testutil.N3MainnetEventBlock, state.LastBlockHeight)
	}
	t.Logf("IndexerState: height=%d hash=%s", state.LastBlockHeight, state.LastBlockHash)

	// Verify at least one BridgeOperation was saved
	ops, total, err := opStore.ListOperations(datastore.OperationFilter{})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	t.Logf("Bridge operations found: %d (total=%d)", len(ops), total)
	for _, op := range ops {
		t.Logf("  op: type=%s dir=%s nonce=%d from=%s to=%s", op.BridgeType, op.Direction, op.Nonce, op.FromAddress, op.ToAddress)
	}
}

// TestN3Indexer_ProcessBlock_NoEvents processes a block known to have no bridge
// events and verifies that state is saved but no operations are created.
func TestN3Indexer_ProcessBlock_NoEvents(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := n3.NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to N3 RPC: %v", err)
	}
	t.Cleanup(client.Close)

	store, opStore := testutil.NewTestStore(t)

	proc, err := NewN3EventProcessor(opStore, testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash(), slog.Default())
	if err != nil {
		t.Fatalf("NewN3EventProcessor: %v", err)
	}

	contractHashes := []string{testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash()}
	idx, err := NewN3Indexer(client, store, opStore, nil, proc, contractHashes, 0, 1, slog.Default())
	if err != nil {
		t.Fatalf("NewN3Indexer: %v", err)
	}

	// Process a block known to have no bridge events (5 blocks before the event block)
	noEventBlock := testutil.N3MainnetEventBlock - 5
	if err := idx.processBlock(ctx, noEventBlock); err != nil {
		t.Fatalf("processBlock(%d) failed: %v", noEventBlock, err)
	}

	// State should be saved
	state, err := store.GetIndexerState("neo_n3")
	if err != nil {
		t.Fatalf("GetIndexerState: %v", err)
	}
	if state.LastBlockHeight != uint64(noEventBlock) {
		t.Errorf("expected LastBlockHeight=%d, got %d", noEventBlock, state.LastBlockHeight)
	}

	// No operations should be saved
	ops, _, err := opStore.ListOperations(datastore.OperationFilter{})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("expected 0 operations for no-event block, got %d", len(ops))
	}
	t.Logf("Correctly found 0 operations for block %d", noEventBlock)
}

// TestN3Indexer_ProcessBlock_Sequential processes 3 consecutive blocks and
// verifies that IndexerState is updated after each one.
func TestN3Indexer_ProcessBlock_Sequential(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := n3.NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to N3 RPC: %v", err)
	}
	t.Cleanup(client.Close)

	store, opStore := testutil.NewTestStore(t)

	proc, err := NewN3EventProcessor(opStore, testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash(), slog.Default())
	if err != nil {
		t.Fatalf("NewN3EventProcessor: %v", err)
	}

	contractHashes := []string{testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash()}
	idx, err := NewN3Indexer(client, store, opStore, nil, proc, contractHashes, 0, 1, slog.Default())
	if err != nil {
		t.Fatalf("NewN3Indexer: %v", err)
	}

	// Process 3 consecutive blocks ending at the event block
	startBlock := testutil.N3MainnetEventBlock - 2
	for i := uint32(0); i < 3; i++ {
		height := startBlock + i
		if err := idx.processBlock(ctx, height); err != nil {
			t.Fatalf("processBlock(%d) failed: %v", height, err)
		}
		state, err := store.GetIndexerState("neo_n3")
		if err != nil {
			t.Fatalf("GetIndexerState after block %d: %v", height, err)
		}
		if state.LastBlockHeight != uint64(height) {
			t.Errorf("block %d: expected LastBlockHeight=%d, got %d", height, height, state.LastBlockHeight)
		}
		t.Logf("Block %d processed: hash=%s", height, state.LastBlockHash)
	}
}
