package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// eventOnlyFixture: blocks 0-103 (0-99 empty). Block 100 makes token
// nonces 7 and 8 claimable; block 101 claims 7 directly and 8 through a
// router; no claim emits a bridge log. The contract's claimable records are
// served from storage.
func eventOnlyFixture(t *testing.T, traceMode string, nonce8Claimed bool) (*optionalTraceFixture, *Recoverer) {
	t.Helper()
	f := newOptionalTraceFixture(t, traceMode)
	dst := common.HexToAddress(f.direct.DestinationToken)
	held := func(from, until uint64) func(uint64) common.Hash {
		return func(h uint64) common.Hash {
			if h >= from && h < until {
				return common.HexToHash("0xce0c7fbe9c64bc4b1bdc1436f2fe8e795dd01e90")
			}
			return common.Hash{}
		}
	}
	f.rpc.storage[tokenClaimableSlot(dst, 7)] = held(100, 101)
	if nonce8Claimed {
		f.rpc.storage[tokenClaimableSlot(dst, 8)] = held(100, 101)
	} else {
		f.rpc.storage[tokenClaimableSlot(dst, 8)] = held(100, 1<<62)
	}
	client, err := neox.NewClient(context.Background(), []string{f.url}, f.idx.logger, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f, NewNeoXRecoverer(client, f.store, f.opStore, testTokenBridgeAddr, testMessageBridgeAddr, 1, f.idx.logger)
}

// Event-only recovery never inspects transactions for silent claims: it
// recovers every bridge event, leaves claims unknown, and certifies nothing.
func TestEventOnlyRecoveryDefersClaims(t *testing.T) {
	ctx := context.Background()
	f, rec := eventOnlyFixture(t, "unsupported", true)
	if err := f.store.SaveIndexerState(&types.IndexerState{Chain: chainNeoX, LastBlockHeight: 103}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Begin(ctx, 0, 103, false); err != nil {
		t.Fatal(err)
	}
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 100000}, nil); err != nil {
		t.Fatal(err)
	}
	// Target hash (begin, run), range end, range parent and the one log block.
	if n := f.rpc.count("eth_getBlockByNumber"); n > 5 {
		t.Fatalf("event-only recovery fetched %d blocks/headers", n)
	}
	if n := f.rpc.count("debug_traceBlockByNumber"); n != 0 {
		t.Fatalf("event-only recovery traced %d blocks", n)
	}
	direct, indirect := f.lifecycle(t, f.direct), f.lifecycle(t, f.indirect)
	if direct.Settlement == nil || direct.Claimable == nil || direct.Claim != nil || indirect.Claim != nil {
		t.Fatalf("events or claims wrong: %+v / %+v", direct, indirect)
	}
	st := mustStatus(t, f.opStore, "neo_x")
	if !st.Contiguous || st.Complete || st.HandoffReady || st.ClaimsDeferredBlocks != 104 {
		t.Fatalf("missing claim coverage certified: %+v", st)
	}
}

// Claim reconciliation identifies each consumed claimable's claim from the
// one block that consumed it, and clears claim gaps only when every
// claimable is accounted for in a replay that started at the deployment.
func TestClaimReconciliationFromContractState(t *testing.T) {
	ctx := context.Background()
	run := func(f *optionalTraceFixture, rec *Recoverer, start uint64) {
		t.Helper()
		if err := f.store.SaveIndexerState(&types.IndexerState{Chain: chainNeoX, LastBlockHeight: 103}); err != nil {
			t.Fatal(err)
		}
		if err := rec.Begin(ctx, start, 103, false); err != nil {
			t.Fatal(err)
		}
		if err := rec.Run(ctx, RecoveryRunOptions{Batch: 100000}, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("indirect claim without trace stays unresolved", func(t *testing.T) {
		f, rec := eventOnlyFixture(t, "unsupported", true)
		run(f, rec, 0)
		sum, err := rec.ReconcileClaims(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if sum.Claims != 1 || sum.Unresolved != 1 || sum.GapsCleared {
			t.Fatalf("summary %+v", sum)
		}
		claim := f.lifecycle(t, f.direct).Claim
		if claim == nil || claim.Position != "top-level" || claim.BlockHeight != 101 {
			t.Fatalf("direct claim: %+v", claim)
		}
		if f.lifecycle(t, f.indirect).Claim != nil {
			t.Fatal("claim inferred for the untraced indirect claim")
		}
		unresolved, _ := f.store.ListUnresolvedNeoXTokenClaims()
		if len(unresolved) != 1 || unresolved[0].ConsumedAt != 101 || !strings.HasPrefix(unresolved[0].Reason, "indirect_claim_trace") {
			t.Fatalf("unresolved: %+v", unresolved)
		}
		if st := mustStatus(t, f.opStore, "neo_x"); st.Complete || st.UnresolvedClaims != 1 || st.ClaimsDeferredBlocks != 104 {
			t.Fatalf("status: %+v", st)
		}
		// Retried with a tracing endpoint, the same reconciliation resolves it.
		f.rpc.setTraceMode("ok")
		client, _ := neox.NewClient(ctx, []string{f.url}, f.idx.logger, 0)
		again := NewNeoXRecoverer(client, f.store, f.opStore, testTokenBridgeAddr, testMessageBridgeAddr, 1, f.idx.logger)
		if sum, err = again.ReconcileClaims(ctx); err != nil || sum.Unresolved != 0 || !sum.GapsCleared {
			t.Fatalf("retry: %+v %v", sum, err)
		}
		if c := f.lifecycle(t, f.indirect).Claim; c == nil || c.Position != "top-level/0" {
			t.Fatalf("indirect claim: %+v", c)
		}
		if st := mustStatus(t, f.opStore, "neo_x"); !st.Complete || !st.HandoffReady || st.UnresolvedClaims != 0 {
			t.Fatalf("status after reconciliation: %+v", st)
		}
		if sum, err = again.ReconcileClaims(ctx); err != nil || sum.Claims != 0 || !sum.GapsCleared {
			t.Fatalf("idempotent reconciliation: %+v %v", sum, err)
		}
	})
	t.Run("a claimable still held is outstanding, not claimed", func(t *testing.T) {
		f, rec := eventOnlyFixture(t, "unsupported", false)
		run(f, rec, 0)
		sum, err := rec.ReconcileClaims(ctx)
		if err != nil || sum.Claims != 1 || sum.Outstanding != 1 || !sum.GapsCleared {
			t.Fatalf("summary %+v %v", sum, err)
		}
		if f.lifecycle(t, f.indirect).Claim != nil {
			t.Fatal("outstanding claimable claimed")
		}
	})
	t.Run("a replay after the deployment cannot clear claim gaps", func(t *testing.T) {
		f, rec := eventOnlyFixture(t, "ok", true)
		run(f, rec, 50)
		sum, err := rec.ReconcileClaims(ctx)
		if err != nil || sum.Claims != 2 || sum.GapsCleared {
			t.Fatalf("summary %+v %v", sum, err)
		}
		if st := mustStatus(t, f.opStore, "neo_x"); st.Complete {
			t.Fatalf("claims before the replay start certified: %+v", st)
		}
	})
}

// Event-only ranges are bounded, interruptible and adapt to eth_getLogs
// limits; each committed range is atomic with its watermark.
func TestEventOnlyRecoveryRangesAdaptAndResume(t *testing.T) {
	ctx := context.Background()
	f, rec := eventOnlyFixture(t, "unsupported", true)
	f.rpc.maxLogsRange = 40
	if err := rec.Begin(ctx, 0, 103, false); err != nil {
		t.Fatal(err)
	}
	var spans []uint64
	report := func(p RecoveryProgress) { spans = append(spans, p.To-p.From+1) }
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 80, MaxBlocks: 60, MinLogsSpan: 10}, report); err != nil {
		t.Fatal(err)
	}
	m, _ := f.opStore.GetRecoveryManifest("neo_x")
	if m.NextHeight != 60 || len(spans) == 0 || spans[0] > 40 {
		t.Fatalf("bounded adaptive run: next %d spans %v", m.NextHeight, spans)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := rec.Run(cancelled, RecoveryRunOptions{Batch: 80, MinLogsSpan: 10}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 40, MinLogsSpan: 10}, nil); err != nil {
		t.Fatal(err)
	}
	st := mustStatus(t, f.opStore, "neo_x")
	if !st.Contiguous || st.ClaimsDeferredBlocks != 104 {
		t.Fatalf("resumed: %+v", st)
	}
	if l := f.lifecycle(t, f.direct); l.Settlement == nil || l.Claimable == nil {
		t.Fatalf("events lost across interruption: %+v", l)
	}
	if _, err := f.opStore.GetLifecycle(datastore.OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 1}); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("unexpected native lifecycle: %v", err)
	}
}

func nativeID(dir types.Direction, nonce uint64) datastore.OperationIdentity {
	return datastore.OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: dir, Nonce: nonce}
}

func payloadField(t *testing.T, e *datastore.LifecycleEvidence, field string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(e.Payload, &m); err != nil {
		t.Fatalf("payload %s: %v", e.Payload, err)
	}
	s, _ := m[field].(string)
	return s
}

// The native (GAS) bridge on mainnet, decoded from recorded receipts and
// application logs on both sides of each chain's 3.0.0 activation: Gas*
// before, Native* after, the same native operations, roots and units.
func TestNativeBridgeAcrossBridgeVersions(t *testing.T) {
	f := loadRecoveryFixture(t)
	for _, tc := range []struct {
		name                string
		neox, n3Dep, n3With string
		deposit, withdrawal uint64
		neoxBlock, n3DepBlk uint64
		n3WithBlk           uint64
		neoxDepTx, n3WithTx string
	}{
		{"before 3.0.0 (Gas*)", "neox_gas_pre_3_0", "n3_gas_deposit_1", "n3_gas_withdrawal_1", 1, 1, 5296, 5752006, 5752312, "", "0x1e5375c212843cb624fcdc63004bc4215e00e35fe9b528a470785e3ea19e5010"},
		{"from 3.0.0 (Native*)", "neox_native_post_3_0", "n3_native_deposit_1317", "n3_native_withdrawal_441", 1317, 441, 1779484, 6881592, 6881599, "", "0x4ca2086bbe0b569b4e2d7d7096bba9b612a9b8b89696fcf1302680e2ba313024"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, opStore := openRecoveryStore(t)
			replayRecoveryCase(t, f, tc.neox, store, opStore)
			dep := mustLifecycle(t, opStore, nativeID(types.DirectionDeposit, tc.deposit))
			if dep.Settlement == nil || dep.Distribution == nil || dep.Settlement.BlockHeight != tc.neoxBlock || dep.Distribution.TxHash != dep.Settlement.TxHash || dep.Claim != nil {
				t.Fatalf("Neo X native deposit %d: %+v", tc.deposit, dep)
			}
			receivedRoot := payloadField(t, dep.Settlement, "root")
			// Neo X source: the native withdrawal is a recovered operation.
			wops, _ := opStore.GetOperationsByNonce(tc.withdrawal)
			var src *types.BridgeOperation
			for i := range wops {
				if wops[i].BridgeType == types.BridgeTypeNative && wops[i].Direction == types.DirectionWithdrawal {
					src = &wops[i]
				}
			}
			if src == nil || src.SourceChain != "neo_x" || src.Amount == nil || src.Root == nil {
				t.Fatalf("Neo X native withdrawal %d not recovered: %+v", tc.withdrawal, wops)
			}

			replayRecoveryCase(t, f, tc.n3Dep, store, opStore)
			dops, _ := opStore.GetOperationsByNonce(tc.deposit)
			var deposit *types.BridgeOperation
			for i := range dops {
				if dops[i].BridgeType == types.BridgeTypeNative && dops[i].Direction == types.DirectionDeposit {
					deposit = &dops[i]
				}
			}
			if deposit == nil || deposit.SourceBlockHeight != tc.n3DepBlk || deposit.Status != types.StatusCompleted || !sameHash(*deposit.DestTxHash, dep.Distribution.TxHash) {
				t.Fatalf("N3 native deposit %d: %+v", tc.deposit, deposit)
			}
			if !sameHash(*deposit.Root, receivedRoot) {
				t.Fatalf("N3 deposit root %s, Neo X received root %s", *deposit.Root, receivedRoot)
			}

			// A second, separate N3 recovery campaign over the same database.
			if err := store.Delete("lifecycle_recovery:v1:neo_n3"); err != nil {
				t.Fatal(err)
			}
			replayRecoveryCase(t, f, tc.n3With, store, opStore)
			wd := mustLifecycle(t, opStore, nativeID(types.DirectionWithdrawal, tc.withdrawal))
			if wd.Settlement == nil || wd.Distribution == nil || !sameHash(wd.Distribution.TxHash, tc.n3WithTx) || wd.Distribution.BlockHeight != tc.n3WithBlk {
				t.Fatalf("N3 native withdrawal %d: %+v", tc.withdrawal, wd)
			}
			// Units did not change: the N3 distribution pays the amount the
			// Neo X withdrawal recorded (8 decimals on both sides).
			if got := payloadField(t, wd.Distribution, "amount"); got != *src.Amount {
				t.Fatalf("withdrawal %d amount: Neo X %s, N3 %s", tc.withdrawal, *src.Amount, got)
			}
			if !sameHash(payloadField(t, wd.Settlement, "root"), *src.Root) {
				t.Fatalf("withdrawal %d root: Neo X %s, N3 %s", tc.withdrawal, *src.Root, payloadField(t, wd.Settlement, "root"))
			}
			ops, _ := opStore.GetOperationsByNonce(tc.withdrawal)
			for _, op := range ops {
				if op.BridgeType == types.BridgeTypeNative && op.Direction == types.DirectionWithdrawal && (op.Status != types.StatusCompleted || !sameHash(*op.DestTxHash, tc.n3WithTx)) {
					t.Fatalf("legacy withdrawal %d not completed: %+v", tc.withdrawal, op)
				}
			}
		})
	}
}

// GasClaimable 958 (2.0.0 era): the outcome is claimable; its later claim,
// which would emit GasClaim, is not in the replayed interval.
func TestGasClaimableBefore3_0_0(t *testing.T) {
	f := loadRecoveryFixture(t)
	store, opStore := openRecoveryStore(t)
	replayRecoveryCase(t, f, "neox_gas_claimable_958", store, opStore)
	l := mustLifecycle(t, opStore, nativeID(types.DirectionDeposit, 958))
	if l.Settlement == nil || l.Claimable == nil || l.Distribution != nil || l.Claim != nil {
		t.Fatalf("claimable 958: %+v", l)
	}
	if payloadField(t, l.Claimable, "amount") != "6990000000" || !strings.EqualFold(payloadField(t, l.Claimable, "recipient"), mainnetNeoXBridgeAddress) {
		t.Fatalf("claimable payload: %s", l.Claimable.Payload)
	}
}

// The only Neo X token claimable (nonce 108) and its silent claim: the
// event-only replay leaves the claim unknown, and reconciliation finds it
// from contract state and one block, without clearing claim coverage for a
// replay that did not start at the deployment.
func TestSilentTokenClaimReconciledFromState(t *testing.T) {
	f := loadRecoveryFixture(t)
	store, opStore := openRecoveryStore(t)
	replayRecoveryCase(t, f, "neox_token_claim_108", store, opStore)
	id := datastore.OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 108,
		SourceToken: "0xef4073a0f2b305a38ec4050e4d3d28bc40ea63f5", DestinationToken: "0xc28736dc83f4fd43d6fb832fd93c3ee7bb26828f"}
	l := mustLifecycle(t, opStore, id)
	if l.Claimable == nil || l.Claim == nil || !sameHash(l.Claim.TxHash, "0xeba5f8409858a106934f567f9562e5ab9269835d938a6f64d0031a3ccfb04ab4") || l.Claim.BlockHeight != 6757004 || l.Claim.Position != "top-level" {
		t.Fatalf("claim 108: %+v", l)
	}
	if !sameHash(l.Settlement.TxHash, "0xf81aeb05ae05142e69eb61414cfdfcc8b947ac0019357ce9dddb3d1fb64c29f3") {
		t.Fatalf("settlement replaced by claim: %+v", l.Settlement)
	}
	st := mustStatus(t, opStore, "neo_x")
	if c := st.Manifest.Claims; c == nil || c.Claims != 1 || c.Unresolved != 0 || c.GapsCleared || st.Complete || st.ClaimsDeferredBlocks == 0 {
		t.Fatalf("status: %+v", st)
	}
}

// Neo X testnet uses the mainnet native contract addresses with its own
// upgrade history. A real testnet Native* event below mainnet's 3.0.0
// height is indexed live, and historical recovery refuses the network.
func TestNeoXTestnetIsNotHeldToMainnetVersions(t *testing.T) {
	f := loadRecoveryFixture(t)
	srv, _ := rpcServer(t, "", f.Exchanges[neoxTestnet])
	opStore, beginErr := captureNeoXTestnet(t, srv.URL)
	l := mustLifecycle(t, opStore, nativeID(types.DirectionDeposit, 938))
	if l.Settlement == nil || l.Settlement.BlockHeight != testnetNativeRootBlock {
		t.Fatalf("testnet root update: %+v", l)
	}
	if beginErr == nil || !strings.Contains(beginErr.Error(), "mainnet only") {
		t.Fatalf("recovery accepted Neo X testnet: %v", beginErr)
	}
	if _, err := opStore.GetRecoveryManifest("neo_x"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("manifest created on testnet: %v", err)
	}
}

// A manifest created before event-only recovery (claims required) cannot
// silently switch to event-only ranges; it continues with --claim-scan.
func TestOldManifestRequiresClaimScan(t *testing.T) {
	ctx := context.Background()
	f, rec := eventOnlyFixture(t, "unsupported", true)
	hash, _, err := rec.blockHash(ctx, 103)
	if err != nil {
		t.Fatal(err)
	}
	old := datastore.RecoveryManifest{Chain: "neo_x", WatchedContracts: rec.contracts, ImplementationRanges: []string{"#39"},
		RequiredPaths: append(datastore.BaseRecoveryPaths("neo_x"), "transactions", "claims"), StartHeight: 100, TargetHeight: 103, TargetHash: hash, NextHeight: 100}
	if err := f.opStore.BeginRecovery(old); err != nil {
		t.Fatal(err)
	}
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 100000}, nil); err == nil || !strings.Contains(err.Error(), "--claim-scan") {
		t.Fatalf("event-only run on an old manifest: %v", err)
	}
	if m, _ := f.opStore.GetRecoveryManifest("neo_x"); m.NextHeight != 100 {
		t.Fatalf("watermark moved: %+v", m)
	}
	rec.ScanClaims(true)
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 250}, nil); err != nil {
		t.Fatalf("claim-scan continuation: %v", err)
	}
	if m, _ := f.opStore.GetRecoveryManifest("neo_x"); !m.Contiguous() {
		t.Fatalf("not continued: %+v", m)
	}
}

// N3 testnet (magic 894710606) is not held to mainnet's upgrade heights,
// and N3 recovery refuses it.
func TestN3TestnetIsNotHeldToMainnetVersions(t *testing.T) {
	proc, opStore := newTestProcessor(t)
	deposit := n3Event(proc.neoXBridge, "GasDeposit", n3Int(1), n3Bytes(2, 20), n3Int(5), n3Bytes(3, 20), n3Bytes(4, 32), n3Bytes(5, 32)).event
	halt := []state.Execution{{VMState: vmstate.Halt, Events: []state.NotificationEvent{deposit}}}
	after := uint32(n3BridgeNativeNamesFrom + 10)
	for _, tc := range []struct {
		network uint32
		ok      bool
	}{{894710606, true}, {mainnetN3Network, false}} {
		idx := &N3Indexer{processor: proc, contracts: []util.Uint160{proc.neoXBridge, proc.messageBridge}, network: tc.network}
		_, _, err := idx.decodeN3Executions(halt, after, "block", "tx", time.Unix(100, 0))
		if (err == nil) != tc.ok {
			t.Fatalf("network %d: GasDeposit after mainnet 3.0.0: %v", tc.network, err)
		}
	}
	rec := &Recoverer{chain: chainN3, opStore: opStore, n3: &N3Indexer{network: 894710606}}
	if err := rec.Begin(context.Background(), 1, 2, false); err == nil || !strings.Contains(err.Error(), "mainnet only") {
		t.Fatalf("N3 testnet recovery: %v", err)
	}
}
