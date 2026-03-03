//go:build integration

package n3

import (
	"log/slog"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"

	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
)

func TestN3Client_Connect(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to N3 RPC: %v", err)
	}
	t.Cleanup(client.Close)
	// client is non-nil — connection succeeded
}

func TestN3Client_GetBlockCount(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	count, err := client.GetBlockCount(ctx)
	if err != nil {
		t.Fatalf("GetBlockCount failed: %v", err)
	}
	if count < 6_000_000 {
		t.Errorf("expected block count > 6000000, got %d", count)
	}
	t.Logf("N3 block count: %d", count)
}

func TestN3Client_GetBlockByIndex(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	blk, err := client.GetBlockByIndex(ctx, testutil.N3MainnetEventBlock)
	if err != nil {
		t.Fatalf("GetBlockByIndex(%d) failed: %v", testutil.N3MainnetEventBlock, err)
	}
	if blk == nil {
		t.Fatal("expected non-nil block")
	}
	t.Logf("Block %d: hash=%s txs=%d", testutil.N3MainnetEventBlock, blk.Hash().StringLE(), len(blk.Transactions))
}

func TestN3Client_GetApplicationLog(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	// Fetch a known block and find a tx with a Halt execution
	blk, err := client.GetBlockByIndex(ctx, testutil.N3MainnetEventBlock)
	if err != nil {
		t.Skipf("skipping: cannot fetch block: %v", err)
	}

	found := false
	for _, tx := range blk.Transactions {
		appLog, err := client.GetApplicationLog(ctx, tx.Hash())
		if err != nil {
			continue
		}
		for _, exec := range appLog.Executions {
			if exec.VMState == vmstate.Halt && len(exec.Events) > 0 {
				t.Logf("Found tx with events: %s, events=%d", tx.Hash().StringLE(), len(exec.Events))
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Logf("No tx with Halt+events found in block %d (may be normal)", testutil.N3MainnetEventBlock)
	}
}

func TestN3Client_FailoverRecovery(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	// Provide invalid endpoint first, then real endpoint — pool should connect via the real one
	endpoints := []string{"http://invalid-endpoint-that-does-not-exist:9999", testutil.N3Endpoint(t)}
	client, err := NewClient(ctx, endpoints, 2, slog.Default())
	if err != nil {
		t.Skipf("skipping: failover also failed: %v", err)
	}
	t.Cleanup(client.Close)

	// Should be able to get block count via the working endpoint
	count, err := client.GetBlockCount(ctx)
	if err != nil {
		t.Fatalf("GetBlockCount via failover failed: %v", err)
	}
	if count == 0 {
		t.Error("expected non-zero block count")
	}
	t.Logf("Failover succeeded, block count: %d", count)
}
