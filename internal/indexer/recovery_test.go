package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// Identities from the captured mainnet evidence. Token identities use the
// decoders' own spellings; the store normalizes prefix and case.
var (
	ndmeme27 = datastore.OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 27,
		SourceToken: "0x57d1761945697a2257be76b756dcc9c19b512ff1", DestinationToken: "0xe816dee05cf6d0f2a57eb4c489241d8326b5d106"}
	neoWithdrawal11 = datastore.OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionWithdrawal, Nonce: 11,
		SourceToken: "c28736dc83f4fd43d6fb832fd93c3ee7bb26828f", DestinationToken: "ef4073a0f2b305a38ec4050e4d3d28bc40ea63f5"}
)

const (
	ndmemeDestTx   = "0xc34ba5e2efcc32a5c6b317c87e8ad7b4e5e2113e9a0eb333e6f79cc8a4f6cdb2"
	ndmemeDestBlk  = 0x74acee
	ndmemeSourceTx = "0x7f3a35997b347304c5f3de909e20c4c8ddc4fb17b488646ab36dbf820f2cc4a0"
	neo11Tx        = "0x1c4c374824b8059230e3bd1dc8c780294531bed4c591de72032119372a79c1f8"
)

func sameHash(a, b string) bool {
	return strings.EqualFold(strings.TrimPrefix(a, "0x"), strings.TrimPrefix(b, "0x"))
}

func mustLifecycle(t *testing.T, opStore *datastore.OperationStore, id datastore.OperationIdentity) *datastore.OperationLifecycle {
	t.Helper()
	l, err := opStore.GetLifecycle(id)
	if err != nil {
		t.Fatalf("lifecycle %+v: %v", id, err)
	}
	return l
}

func mustStatus(t *testing.T, opStore *datastore.OperationStore, chain string) *datastore.RecoveryStatus {
	t.Helper()
	st, err := opStore.RecoveryStatus(chain)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// Regression 1: NDMEME deposit nonce 27, recovered from mainnet evidence in
// each chain's own heights, destination first and then the missing source.
func TestRecoveryNDMEMEDeposit27(t *testing.T) {
	f := loadRecoveryFixture(t)
	store, opStore := openRecoveryStore(t)
	replayRecoveryCase(t, f, "ndmeme_27_neox_dest", store, opStore)
	l := mustLifecycle(t, opStore, ndmeme27)
	if l.Settlement == nil || !sameHash(l.Settlement.TxHash, ndmemeDestTx) || l.Settlement.BlockHeight != ndmemeDestBlk || l.Settlement.Chain != "neo_x" || l.Settlement.Position != "0" {
		t.Fatalf("settlement: %+v", l.Settlement)
	}
	var root struct{ Root string }
	_ = json.Unmarshal(l.Settlement.Payload, &root)
	if !sameHash(root.Root, "5c744169481efcfcee2650c2f62a8352284f0c78d9c8af0732ae00f100d6bc83") {
		t.Fatalf("received root: %s", l.Settlement.Payload)
	}
	if l.Distribution == nil || !sameHash(l.Distribution.TxHash, ndmemeDestTx) || l.Distribution.Position != "2" || l.Claimable != nil || l.Claim != nil {
		t.Fatalf("distribution: %+v", l)
	}
	// The public Neo X node has no tracing, so indirect claim coverage of the
	// block stays explicitly incomplete even though events were recovered.
	st := mustStatus(t, opStore, "neo_x")
	// Event-only recovery inspected no transaction for silent claims, so the
	// block's claim coverage is explicitly deferred.
	if !st.Contiguous || st.Complete || st.ClaimsDeferredBlocks != 1 || st.ClaimTraceGaps[0].Reason != datastore.GapClaimsDeferred {
		t.Fatalf("Neo X status: %+v", st)
	}

	// The source operation is missing from this database (frontend #19).
	replayRecoveryCase(t, f, "ndmeme_27_n3_source", store, opStore)
	ops, err := opStore.GetOperationsByNonce(27)
	if err != nil || len(ops) != 1 {
		t.Fatalf("recovered source operations: %+v %v", ops, err)
	}
	op := ops[0]
	if op.SourceChain != "neo_n3" || op.SourceBlockHeight != 13051516 || !sameHash(op.SourceTxHash, ndmemeSourceTx) ||
		!sameHash(*op.TokenContract, ndmeme27.SourceToken) || !sameHash(*op.DestTokenContract, ndmeme27.DestinationToken) {
		t.Fatalf("source operation: %+v", op)
	}
	if op.Status != types.StatusCompleted || !sameHash(*op.DestTxHash, ndmemeDestTx) || *op.DestBlockHeight != ndmemeDestBlk {
		t.Fatalf("legacy projection of recovered distribution: %+v", op)
	}
	if !sameHash(*op.Root, root.Root) {
		t.Fatalf("source root %s does not match received root %s", *op.Root, root.Root)
	}
	if st := mustStatus(t, opStore, "neo_n3"); !st.Complete {
		t.Fatalf("N3 status: %+v", st)
	}
}

// Starting a Neo X replay at the N3 source height must fail: Neo X has not
// reached it, and the destination block lies far below it.
func TestRecoveryRejectsOtherChainHeights(t *testing.T) {
	f := loadRecoveryFixture(t)
	store, opStore := openRecoveryStore(t)
	srv, _ := rpcServer(t, "", f.Exchanges[chainNeoX])
	r := newFixtureRecoverer(t, chainNeoX, srv.URL, store, opStore)
	err := r.Begin(context.Background(), 13051516, 13051516, false)
	if err == nil || !strings.Contains(err.Error(), "beyond the chain head") {
		t.Fatalf("N3 height accepted as a Neo X bound: %v", err)
	}
	if _, err := opStore.GetRecoveryManifest("neo_x"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("manifest created: %v", err)
	}
}

// Regression 3: the actual N3 application log of the NEO withdrawal nonce 11
// relay contains its received root and its distribution.
func TestRecoveryNEOWithdrawal11(t *testing.T) {
	f := loadRecoveryFixture(t)
	store, opStore := openRecoveryStore(t)
	stuck := &types.BridgeOperation{BridgeType: types.BridgeTypeToken, Direction: types.DirectionWithdrawal, Nonce: 11,
		SourceChain: "neo_x", DestinationChain: "neo_n3", TokenContract: &neoWithdrawal11.SourceToken, DestTokenContract: &neoWithdrawal11.DestinationToken, Status: types.StatusStuck}
	if err := opStore.SaveOperation(stuck); err != nil {
		t.Fatal(err)
	}
	replayRecoveryCase(t, f, "neo_withdrawal_11_n3", store, opStore)
	l := mustLifecycle(t, opStore, neoWithdrawal11)
	if l.Settlement == nil || l.Distribution == nil || !sameHash(l.Distribution.TxHash, neo11Tx) || l.Distribution.BlockHeight != 6818342 || l.Distribution.Position != "0:4" || l.Settlement.Position != "0:0" {
		t.Fatalf("NEO nonce 11 lifecycle: %+v", l)
	}
	var payload struct{ Recipient, Amount string }
	_ = json.Unmarshal(l.Distribution.Payload, &payload)
	if payload.Amount != "1" {
		t.Fatalf("distribution payload: %s", l.Distribution.Payload)
	}
	ops, _ := opStore.GetOperationsByNonce(11)
	if len(ops) != 1 || ops[0].Status != types.StatusCompleted || !sameHash(*ops[0].DestTxHash, neo11Tx) {
		t.Fatalf("stuck legacy operation not completed: %+v", ops)
	}
	if st := mustStatus(t, opStore, "neo_n3"); !st.Complete {
		t.Fatalf("status: %+v", st)
	}
}

// Regression 2: Neo X to N3 messages 14 and 15. The relay stores each message
// with its received root and replaces the synthetic destination; it records
// no execution, so arrival is not reported as execution.
func TestRecoveryMessageWithdrawalReceivedRoot(t *testing.T) {
	f := loadRecoveryFixture(t)
	for _, tc := range []struct {
		name  string
		nonce uint64
		tx    string
		root  string
	}{
		{"message_withdrawal_14_n3", 14, "757635d8b27c8ced0a6b667e558a7d9c1696d8287dc5471900efb9349f956761", "37ac577f0937f5c2f0c0acc6b352b3972868c5071b0b3e8915080658a87ef080"},
		{"message_withdrawal_15_n3", 15, "def33b868178c301b2f2ebd874e45f12e89a911b0aca4fc29828d3808528924e", "bd8a3be21d32d769c8f80e7f803166aa7c29bdfb72a315aabb3678869eb324f6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, opStore := openRecoveryStore(t)
			bs := datastore.NewBridgeStateStore(store)
			instance := datastore.BuildInstanceID("message", "neo_x", "neo_n3", nil, nil)
			// The incident: synthetic destination bookkeeping at nonce 13.
			if err := bs.SaveBridgeState(&types.DirectionalBridgeState{InstanceID: instance, BridgeType: types.BridgeTypeMessage, SrcChain: "neo_x", DstChain: "neo_n3",
				DestState: &types.ChainState{Synthetic: true, Nonce: 13, TxHash: "source-tx-13"}}); err != nil {
				t.Fatal(err)
			}
			pending := &types.BridgeOperation{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionWithdrawal, Nonce: tc.nonce, SourceChain: "neo_x", DestinationChain: "neo_n3", Status: types.StatusPending}
			if err := opStore.SaveOperation(pending); err != nil {
				t.Fatal(err)
			}
			replayRecoveryCase(t, f, tc.name, store, opStore)
			id := datastore.OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionWithdrawal, Nonce: tc.nonce}
			l := mustLifecycle(t, opStore, id)
			var payload struct {
				Root  string
				Store map[string]string
			}
			_ = json.Unmarshal(l.Settlement.Payload, &payload)
			if !sameHash(l.Settlement.TxHash, tc.tx) || payload.Root != tc.root || payload.Store["metadata"] == "" {
				t.Fatalf("message settlement: %+v %s", l.Settlement, l.Settlement.Payload)
			}
			if l.Execution != nil {
				t.Fatalf("arrival recorded as execution: %+v", l.Execution)
			}
			state, err := bs.GetBridgeState(instance)
			if err != nil || state.DestState == nil || state.DestState.Synthetic || state.DestState.Nonce != tc.nonce || *state.DestState.Root != tc.root || !sameHash(state.DestState.TxHash, tc.tx) {
				t.Fatalf("received state: %+v %v", state.DestState, err)
			}
			op, _ := opStore.GetOperation(types.BridgeTypeMessage, types.DirectionWithdrawal, tc.nonce)
			if op.Status != types.StatusPending {
				t.Fatalf("arrival completed an executable message in the legacy projection: %+v", op)
			}
		})
	}
}

// Recovery replays through the live decoders: the same block produces the
// same lifecycle whether live indexing or recovery processed it, and either
// order of the two leaves the other able to proceed (no conflict).
func TestRecoveryMatchesLiveIndexingAndCoexists(t *testing.T) {
	f := loadRecoveryFixture(t)
	t.Run("neo_n3", func(t *testing.T) {
		srv, _ := rpcServer(t, "", f.Exchanges[chainN3])
		liveStore, liveOps := openRecoveryStore(t)
		live := newFixtureRecoverer(t, chainN3, srv.URL, liveStore, liveOps).n3
		live.bridgeStateStore = datastore.NewBridgeStateStore(liveStore)
		if err := live.processBlock(context.Background(), 6818342); err != nil {
			t.Fatal(err)
		}
		recStore, recOps := openRecoveryStore(t)
		replayRecoveryCase(t, f, "neo_withdrawal_11_n3", recStore, recOps)
		if a, b := mustLifecycle(t, liveOps, neoWithdrawal11), mustLifecycle(t, recOps, neoWithdrawal11); jsonOf(a) != jsonOf(b) {
			t.Fatalf("live and replay differ:\n%s\n%s", jsonOf(a), jsonOf(b))
		}
		// Recovery after live on the same database, then live again.
		replayRecoveryCase(t, f, "neo_withdrawal_11_n3", liveStore, liveOps)
		if err := live.processBlock(context.Background(), 6818342); err != nil {
			t.Fatalf("live after recovery: %v", err)
		}
	})
	t.Run("neo_x", func(t *testing.T) {
		srv, _ := rpcServer(t, "", f.Exchanges[chainNeoX])
		liveStore, liveOps := openRecoveryStore(t)
		live := newFixtureRecoverer(t, chainNeoX, srv.URL, liveStore, liveOps).neox
		live.processor = NewNeoXEventProcessor(liveOps, mainnetNeoXTokenBridge, mainnetNeoXMessageBridge, live.logger)
		if err := live.processBlockRange(context.Background(), ndmemeDestBlk, ndmemeDestBlk); err != nil {
			t.Fatal(err)
		}
		recStore, recOps := openRecoveryStore(t)
		replayRecoveryCase(t, f, "ndmeme_27_neox_dest", recStore, recOps)
		if a, b := mustLifecycle(t, liveOps, ndmeme27), mustLifecycle(t, recOps, ndmeme27); jsonOf(a) != jsonOf(b) {
			t.Fatalf("live and replay differ:\n%s\n%s", jsonOf(a), jsonOf(b))
		}
		replayRecoveryCase(t, f, "ndmeme_27_neox_dest", liveStore, liveOps)
		if err := live.processBlockRange(context.Background(), ndmemeDestBlk, ndmemeDestBlk); err != nil {
			t.Fatalf("live after recovery: %v", err)
		}
		if st, _ := liveOps.RecoveryStatus("neo_x"); !st.Contiguous || st.ClaimTraceGapBlocks != 1 {
			t.Fatalf("status after live reprocessing: %+v", st)
		}
	})
}

func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Bounded work, interruption and restart: an interrupted run keeps its last
// committed range, a new process resumes from it, and the result equals an
// uninterrupted run. A traced replay also resolves live trace gaps and finds
// the indirect claim the live indexer could not see.
func TestRecoveryInterruptResumeAndTraceEnrichment(t *testing.T) {
	ctx := context.Background()
	live := newOptionalTraceFixture(t, "unsupported")
	if err := live.idx.processBlockRange(ctx, 100, 103); err != nil {
		t.Fatal(err)
	}
	if gaps, _ := live.store.NeoXClaimCoverageGaps(100, 103); len(gaps) != 1 {
		t.Fatalf("live gap: %+v", gaps)
	}
	live.rpc.setTraceMode("ok")
	// Recovery runs as its own process with its own client, so the live
	// client's cached -32601 for this endpoint does not apply to it.
	client, err := neox.NewClient(ctx, []string{live.url}, live.idx.logger, 0)
	if err != nil {
		t.Fatal(err)
	}
	rec := NewNeoXRecoverer(client, live.store, live.opStore, testTokenBridgeAddr, testMessageBridgeAddr, 1, live.idx.logger)
	rec.ScanClaims(true)
	if err := rec.Begin(ctx, 100, 103, false); err != nil {
		t.Fatal(err)
	}
	var reports []RecoveryProgress
	report := func(p RecoveryProgress) { reports = append(reports, p) }
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 1, MaxBlocks: 2}, report); err != nil {
		t.Fatal(err)
	}
	m, _ := live.opStore.GetRecoveryManifest("neo_x")
	if m.NextHeight != 102 || len(reports) != 2 || reports[1].NextHeight != 102 {
		t.Fatalf("bounded run: %+v %+v", m, reports)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := rec.Run(cancelled, RecoveryRunOptions{Batch: 1}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run: %v", err)
	}
	if m, _ := live.opStore.GetRecoveryManifest("neo_x"); m.NextHeight != 102 {
		t.Fatalf("cancelled run moved the watermark: %+v", m)
	}
	// A restarted process resumes from the watermark.
	restarted := NewNeoXRecoverer(client, live.store, live.opStore, testTokenBridgeAddr, testMessageBridgeAddr, 1, live.idx.logger)
	restarted.ScanClaims(true)
	if err := restarted.Begin(ctx, 100, 103, false); err != nil {
		t.Fatalf("begin on restart: %v", err)
	}
	if err := restarted.Run(ctx, RecoveryRunOptions{Batch: 10}, nil); err != nil {
		t.Fatal(err)
	}
	claim := live.lifecycle(t, live.indirect).Claim
	if claim == nil || claim.TxHash != live.router.Hex() {
		t.Fatalf("traced replay did not recover the indirect claim: %+v", claim)
	}
	st := mustStatus(t, live.opStore, "neo_x")
	if !st.Complete || !st.HandoffReady || st.ClaimTraceGapBlocks != 0 {
		t.Fatalf("status: %+v", st)
	}
	if live.checkpoint(t) != 103 {
		t.Fatal("recovery moved the live checkpoint")
	}

	// The same interval replayed in one uninterrupted run gives the same result.
	fresh := newOptionalTraceFixture(t, "ok")
	once := NewNeoXRecoverer(fresh.idx.client, fresh.store, fresh.opStore, testTokenBridgeAddr, testMessageBridgeAddr, 4, fresh.idx.logger)
	once.ScanClaims(true)
	if err := once.Begin(ctx, 100, 103, false); err != nil {
		t.Fatal(err)
	}
	if err := once.Run(ctx, RecoveryRunOptions{Batch: 250}, nil); err != nil {
		t.Fatal(err)
	}
	for _, id := range []datastore.OperationIdentity{live.direct, live.indirect} {
		if a, b := jsonOf(live.lifecycle(t, id)), jsonOf(fresh.lifecycle(t, id)); a != b {
			t.Fatalf("interrupted and uninterrupted replay differ:\n%s\n%s", a, b)
		}
	}
}

// A required RPC failure stops the range without committing anything.
func TestRecoveryRequiredFailureCommitsNothing(t *testing.T) {
	ctx := context.Background()
	f := newOptionalTraceFixture(t, "unsupported")
	f.rpc.receipts[f.rootTx].Logs[1].Topics = f.rpc.receipts[f.rootTx].Logs[1].Topics[:3] // malformed watched event
	rec := NewNeoXRecoverer(f.idx.client, f.store, f.opStore, testTokenBridgeAddr, testMessageBridgeAddr, 1, f.idx.logger)
	if err := rec.Begin(ctx, 100, 103, false); err != nil {
		t.Fatal(err)
	}
	if err := rec.Run(ctx, RecoveryRunOptions{Batch: 250}, nil); err == nil {
		t.Fatal("malformed event recovered")
	}
	if m, _ := f.opStore.GetRecoveryManifest("neo_x"); m.NextHeight != 100 || m.Batches != 0 {
		t.Fatalf("watermark moved: %+v", m)
	}
	if _, err := f.opStore.GetLifecycle(f.direct); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("partial evidence committed: %v", err)
	}
}

// The event filter only locates blocks inside its live-indexed window; the
// evidence still comes from the real blocks and application logs.
func TestRecoveryN3EventFilterLocator(t *testing.T) {
	f := loadRecoveryFixture(t)
	ctx := context.Background()
	t.Run("located range fetches only listed blocks", func(t *testing.T) {
		store, opStore := openRecoveryStore(t)
		replayRecoveryCase(t, f, "ndmeme_27_n3_located", store, opStore)
		ops, _ := opStore.GetOperationsByNonce(27)
		if len(ops) != 1 || ops[0].SourceBlockHeight != 13051516 || !sameHash(ops[0].SourceTxHash, ndmemeSourceTx) {
			t.Fatalf("located source operation: %+v", ops)
		}
		st := mustStatus(t, opStore, "neo_n3")
		if !st.Complete || st.Manifest.TrustedIndex != EventFilterIndex || st.Manifest.LocatedBlocks != 201 {
			t.Fatalf("status: %+v", st)
		}
	})
	t.Run("outside the live window every block is scanned", func(t *testing.T) {
		store, opStore := openRecoveryStore(t)
		replayRecoveryCase(t, f, "ndmeme_27_n3_replayed", store, opStore)
		m, _ := opStore.GetRecoveryManifest("neo_n3")
		if ops, _ := opStore.GetOperationsByNonce(27); len(ops) != 1 || m.LocatedBlocks != 0 {
			t.Fatalf("full scan expected: ops %+v, located %d", ops, m.LocatedBlocks)
		}
		if st := mustStatus(t, opStore, "neo_n3"); !st.Complete || st.IndexTrusted {
			t.Fatalf("status: %+v", st)
		}
	})
	t.Run("a plugin failing its positive control is not used", func(t *testing.T) {
		store, opStore := openRecoveryStore(t)
		srv, _ := rpcServer(t, "", f.Exchanges[chainN3])
		r := newFixtureRecoverer(t, chainN3, srv.URL, store, opStore)
		filter := eventFilterFor("live")
		filter.upstream = srv.URL
		filter.pageLimit = -1 // a plugin capped at zero results answers [] to everything
		useFakeEventFilter(t, r, filter)
		if err := r.Begin(ctx, 13051516, 13051516, true); err != nil {
			t.Fatal(err)
		}
		if err := r.Run(ctx, RecoveryRunOptions{Batch: 100}, nil); err != nil {
			t.Fatal(err)
		}
		m, _ := opStore.GetRecoveryManifest("neo_n3")
		if ops, _ := opStore.GetOperationsByNonce(27); len(ops) != 1 || m.LocatedBlocks != 0 {
			t.Fatalf("an empty-answering plugin was trusted: ops %+v, located %d", ops, m.LocatedBlocks)
		}
	})
	t.Run("a plugin node on another chain is not used", func(t *testing.T) {
		store, opStore := openRecoveryStore(t)
		srv, _ := rpcServer(t, "", f.Exchanges[chainN3])
		r := newFixtureRecoverer(t, chainN3, srv.URL, store, opStore)
		filter := eventFilterFor("live")
		filter.upstream = srv.URL
		filter.wrongHash = true
		useFakeEventFilter(t, r, filter)
		if err := r.Begin(ctx, 13051516, 13051516, true); err != nil {
			t.Fatal(err)
		}
		if err := r.Run(ctx, RecoveryRunOptions{Batch: 100}, nil); err != nil {
			t.Fatal(err)
		}
		m, _ := opStore.GetRecoveryManifest("neo_n3")
		if ops, _ := opStore.GetOperationsByNonce(27); len(ops) != 1 || m.LocatedBlocks != 0 || filter.queries == 0 {
			t.Fatalf("mismatched node trusted: ops %+v, located %d, queries %d", ops, m.LocatedBlocks, filter.queries)
		}
	})
	t.Run("a manifest that does not trust the index never uses it", func(t *testing.T) {
		store, opStore := openRecoveryStore(t)
		srv, _ := rpcServer(t, "", f.Exchanges[chainN3])
		r := newFixtureRecoverer(t, chainN3, srv.URL, store, opStore)
		filter := eventFilterFor("live")
		filter.upstream = srv.URL
		useFakeEventFilter(t, r, filter)
		if err := r.Begin(ctx, 13051516, 13051516, false); err != nil {
			t.Fatal(err)
		}
		if err := r.Run(ctx, RecoveryRunOptions{Batch: 100}, nil); err != nil {
			t.Fatal(err)
		}
		if filter.queries != 0 {
			t.Fatalf("untrusted index queried %d times", filter.queries)
		}
		if err := r.Begin(ctx, 13051516, 13051516, true); err == nil {
			t.Fatal("trust changed on an existing manifest")
		}
	})
	t.Run("Neo X cannot trust the N3 index", func(t *testing.T) {
		store, opStore := openRecoveryStore(t)
		srv, _ := rpcServer(t, "", f.Exchanges[chainNeoX])
		r := newFixtureRecoverer(t, chainNeoX, srv.URL, store, opStore)
		if err := r.Begin(ctx, ndmemeDestBlk, ndmemeDestBlk, true); err == nil {
			t.Fatal("event filter accepted for Neo X")
		}
	})
}
