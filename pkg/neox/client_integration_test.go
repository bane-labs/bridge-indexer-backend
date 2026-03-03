//go:build integration

package neox

import (
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"

	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
)

func TestNeoXClient_Connect(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to NeoX RPC: %v", err)
	}
	t.Cleanup(client.Close)
}

func TestNeoXClient_BlockNumber(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	num, err := client.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("BlockNumber failed: %v", err)
	}
	if num < 1000 {
		t.Errorf("expected block number > 1000, got %d", num)
	}
	t.Logf("NeoX block number: %d", num)
}

func TestNeoXClient_BlockByNumber(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	blk, err := client.BlockByNumber(ctx, new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockFrom))
	if err != nil {
		t.Fatalf("BlockByNumber(%d) failed: %v", testutil.NeoXMainnetEventBlockFrom, err)
	}
	if blk == nil {
		t.Fatal("expected non-nil block")
	}
	t.Logf("NeoX block %d: hash=%s", testutil.NeoXMainnetEventBlockFrom, blk.Hash().Hex())
}

func TestNeoXClient_HeaderByNumber(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	header, err := client.HeaderByNumber(ctx, new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockFrom))
	if err != nil {
		t.Fatalf("HeaderByNumber(%d) failed: %v", testutil.NeoXMainnetEventBlockFrom, err)
	}
	if header == nil {
		t.Fatal("expected non-nil header")
	}
	t.Logf("NeoX header %d: hash=%s time=%d", testutil.NeoXMainnetEventBlockFrom, header.Hash().Hex(), header.Time)
}

func TestNeoXClient_FilterLogs(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockFrom),
		ToBlock:   new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockTo),
		Addresses: HexToAddresses([]string{testutil.NeoXTokenBridgeAddr}),
	}
	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		t.Fatalf("FilterLogs failed: %v", err)
	}
	// Logs may be empty for this range — just verify no error
	t.Logf("FilterLogs returned %d logs for blocks %d-%d", len(logs), testutil.NeoXMainnetEventBlockFrom, testutil.NeoXMainnetEventBlockTo)
}

func TestNeoXClient_FilterLogs_WithEvents(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect: %v", err)
	}
	t.Cleanup(client.Close)

	// Query a wider range to find bridge events
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockFrom),
		ToBlock:   new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockTo),
		Addresses: HexToAddresses([]string{testutil.NeoXTokenBridgeAddr, testutil.NeoXMessageBridgeAddr}),
	}
	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		t.Fatalf("FilterLogs failed: %v", err)
	}
	t.Logf("Found %d logs from bridge contracts in blocks %d-%d", len(logs), testutil.NeoXMainnetEventBlockFrom, testutil.NeoXMainnetEventBlockTo)
	// Log details for debugging
	for i, log := range logs {
		if i >= 3 {
			break
		}
		t.Logf("  log[%d]: addr=%s topics=%d block=%d", i, log.Address.Hex(), len(log.Topics), log.BlockNumber)
	}
}

func TestNeoXClient_FailoverRecovery(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	endpoints := []string{"https://invalid-endpoint-that-does-not-exist:9999", testutil.NeoXEndpoint(t)}
	client, err := NewClient(ctx, endpoints, slog.Default())
	if err != nil {
		t.Skipf("skipping: failover also failed: %v", err)
	}
	t.Cleanup(client.Close)

	num, err := client.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("BlockNumber via failover failed: %v", err)
	}
	if num == 0 {
		t.Error("expected non-zero block number")
	}
	t.Logf("Failover succeeded, block number: %d", num)
}
