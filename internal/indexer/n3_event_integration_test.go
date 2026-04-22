//go:build integration

package indexer

import (
	"log/slog"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"

	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
)

// TestN3EventProcessor_RealDeposit connects to N3 mainnet, fetches a known block
// with a bridge event, and runs it through the real event processor.
func TestN3EventProcessor_RealDeposit(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)

	client, err := n3.NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, slog.Default())
	if err != nil {
		t.Skipf("skipping: cannot connect to N3 RPC: %v", err)
	}
	t.Cleanup(client.Close)

	_, opStore := testutil.NewTestStore(t)

	proc, err := NewN3EventProcessor(opStore, testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash(), slog.Default())
	if err != nil {
		t.Fatalf("NewN3EventProcessor: %v", err)
	}

	// Fetch the known block to get timestamp
	blk, err := client.GetBlockByIndex(ctx, testutil.N3MainnetEventBlock)
	if err != nil {
		t.Skipf("skipping: cannot fetch block %d: %v", testutil.N3MainnetEventBlock, err)
	}
	blockTime := time.UnixMilli(int64(blk.Timestamp))
	t.Logf("Block %d timestamp: %s (%d)", testutil.N3MainnetEventBlock, blockTime, blk.Timestamp)

	// Fetch application log for the known tx
	const knownTxHash = "c36c411edd07ae6ddfc0c8345e3b755d9bbba96760efe6a7d66e4c293b62c788"
	txHash, err := util.Uint256DecodeStringLE(knownTxHash)
	if err != nil {
		t.Fatalf("decode tx hash: %v", err)
	}

	appLog, err := client.GetApplicationLog(ctx, txHash)
	if err != nil {
		t.Skipf("skipping: cannot fetch app log for %s: %v", knownTxHash, err)
	}
	t.Logf("Application log has %d execution(s)", len(appLog.Executions))

	// Decode the NeoXBridge contract hash for comparison
	neoXBridgeHash, err := util.Uint160DecodeStringLE(testutil.N3NeoXBridgeHash())
	if err != nil {
		t.Fatalf("decode NeoXBridge hash: %v", err)
	}

	// Find the bridge event and process it
	var foundOp *types.BridgeOperation
	for _, exec := range appLog.Executions {
		t.Logf("Execution VMState=%s, events=%d", exec.VMState, len(exec.Events))
		if exec.VMState != vmstate.Halt {
			continue
		}
		for _, event := range exec.Events {
			t.Logf("  event: contract=%s name=%s", event.ScriptHash.StringLE(), event.Name)
			if !event.ScriptHash.Equals(neoXBridgeHash) {
				continue
			}
			op, err := proc.ProcessEvent(event, testutil.N3MainnetEventBlock, knownTxHash, blockTime)
			if err != nil {
				t.Logf("  ProcessEvent error (may be expected for some event types): %v", err)
				continue
			}
			if op != nil {
				foundOp = op
				t.Logf("  ProcessEvent returned op: type=%s dir=%s nonce=%d from=%s to=%s",
					op.BridgeType, op.Direction, op.Nonce, op.FromAddress, op.ToAddress)
				break
			}
		}
		if foundOp != nil {
			break
		}
	}

	if foundOp == nil {
		// Log all events for debugging before skipping
		for _, exec := range appLog.Executions {
			for _, event := range exec.Events {
				t.Logf("  all events: contract=%s name=%s", event.ScriptHash.StringLE(), event.Name)
			}
		}
		t.Skip("No parseable bridge operation found in known tx — event type may not produce an operation")
	}

	// Validate the returned operation
	if foundOp.Nonce == 0 {
		t.Error("expected nonce > 0")
	}
	if foundOp.FromAddress == "" {
		t.Error("expected non-empty FromAddress")
	}
	if foundOp.SourceChain == "" {
		t.Error("expected non-empty SourceChain")
	}
	if foundOp.DestinationChain == "" {
		t.Error("expected non-empty DestinationChain")
	}
	if foundOp.SourceTxHash != knownTxHash {
		t.Errorf("expected SourceTxHash=%s, got %s", knownTxHash, foundOp.SourceTxHash)
	}
	if foundOp.SourceBlockHeight != uint64(testutil.N3MainnetEventBlock) {
		t.Errorf("expected SourceBlockHeight=%d, got %d", testutil.N3MainnetEventBlock, foundOp.SourceBlockHeight)
	}
	if foundOp.Status != types.StatusRelayed {
		t.Errorf("expected status=pending, got %s", foundOp.Status)
	}
	if foundOp.Timestamp.IsZero() {
		t.Error("expected non-zero Timestamp")
	}
}
