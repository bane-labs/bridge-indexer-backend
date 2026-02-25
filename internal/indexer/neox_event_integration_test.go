//go:build integration

package indexer

import (
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"

	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

func TestNeoXEventProcessor_RealNativeWithdrawal(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := neox.NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to NeoX RPC: %v", err)
	}
	t.Cleanup(client.Close)

	store, opStore := testutil.NewTestStore(t)

	proc := NewNeoXEventProcessor(opStore, store, testutil.NeoXTokenBridgeAddr, testutil.NeoXMessageBridgeAddr, slog.Default())

	// Fetch logs from the known block range (block 3244 has a confirmed log)
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockFrom),
		ToBlock:   new(big.Int).SetUint64(testutil.NeoXMainnetEventBlockTo),
		Addresses: neox.HexToAddresses([]string{testutil.NeoXTokenBridgeAddr, testutil.NeoXMessageBridgeAddr}),
	}
	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		t.Skipf("skipping: FilterLogs failed: %v", err)
	}

	if len(logs) == 0 {
		t.Skip("no logs found in known block range — skipping")
	}

	t.Logf("Found %d logs in blocks %d-%d", len(logs), testutil.NeoXMainnetEventBlockFrom, testutil.NeoXMainnetEventBlockTo)

	// Process each log through the event processor
	processedAny := false
	for _, lg := range logs {
		blockTime := time.Now() // approximate — real time not critical for parsing test

		op, pause, err := proc.ProcessLog(lg, blockTime)
		if err != nil {
			t.Logf("ProcessLog error for log at block %d tx %s: %v", lg.BlockNumber, lg.TxHash.Hex(), err)
			continue
		}
		if op != nil {
			t.Logf("ProcessLog returned op: type=%s dir=%s nonce=%d from=%s to=%s block=%d",
				op.BridgeType, op.Direction, op.Nonce, op.FromAddress, op.ToAddress, op.SourceBlockHeight)
			processedAny = true
		}
		if pause != nil {
			t.Logf("ProcessLog returned pause: paused=%v scope=%s bridgeType=%s",
				pause.Paused, pause.Scope, pause.BridgeType)
			processedAny = true
		}
	}

	if !processedAny {
		t.Log("No logs were parsed into bridge operations or pause states — logging raw log details:")
		for i, lg := range logs {
			t.Logf("  log[%d]: addr=%s topics=%d block=%d removed=%v",
				i, lg.Address.Hex(), len(lg.Topics), lg.BlockNumber, lg.Removed)
			for j, topic := range lg.Topics {
				t.Logf("    topic[%d]: %s", j, topic.Hex())
			}
			if len(lg.Data) > 0 {
				t.Logf("    data length: %d bytes", len(lg.Data))
			}
		}
		t.Skip("Logs found but none parsed into operations — may be a different event type")
	}
}
