package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func recoveryManifest(chain string, start, target uint64) RecoveryManifest {
	return RecoveryManifest{Chain: chain, WatchedContracts: []string{"bridge@v1"}, ImplementationRanges: []string{"v1:0-100"},
		RequiredPaths: BaseRecoveryPaths(chain), StartHeight: start, TargetHeight: target,
		TargetHash: fmt.Sprintf("block-%d", target), NextHeight: start}
}

func recoveryRange(chain string, from, to uint64) RecoveryRange {
	parent := ""
	if from > 0 {
		parent = fmt.Sprintf("block-%d", from-1)
	}
	return RecoveryRange{Chain: chain, From: from, To: to, FromParentHash: parent, ToHash: fmt.Sprintf("block-%d", to), VerifiedPaths: BaseRecoveryPaths(chain)}
}

func tokenID(dir types.Direction, nonce uint64, src, dst string) OperationIdentity {
	return OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: dir, Nonce: nonce, SourceToken: src, DestinationToken: dst}
}

func rootEvidence(chain, tx string, height uint64, root string) LifecycleEvidence {
	e := lifecycleEvidence(chain, tx, height)
	e.Payload = json.RawMessage(fmt.Sprintf(`{"root":%q}`, root))
	return e
}

func tokenDeposit(nonce uint64, src, dst string, status types.OperationStatus) *types.BridgeOperation {
	return &types.BridgeOperation{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: nonce,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 1200, SourceTxHash: fmt.Sprintf("src-%d", nonce),
		TokenContract: &src, DestTokenContract: &dst, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Status: status}
}

func mustCommit(t *testing.T, os *OperationStore, r RecoveryRange, b RecoveryBatch) *RecoveryCommitResult {
	t.Helper()
	res, err := os.CommitRecoveryRange(r, b)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestRecoveryBoundsAreChainSpecificAndContiguous(t *testing.T) {
	os := NewOperationStore(newTestStore(t), nil)
	x := recoveryManifest("neo_x", 20, 29)
	n3 := recoveryManifest("neo_n3", 1200, 1209)
	incomplete := x
	incomplete.RequiredPaths = []string{"blocks", "logs"}
	if err := os.BeginRecovery(incomplete); err == nil {
		t.Fatal("manifest omitted claim evidence paths")
	}
	for _, m := range []RecoveryManifest{x, n3} {
		if err := os.BeginRecovery(m); err != nil {
			t.Fatal(err)
		}
		if err := os.BeginRecovery(m); err != nil {
			t.Fatalf("resume: %v", err)
		}
	}
	changed := x
	changed.StartHeight = 10
	changed.NextHeight = 10
	if err := os.BeginRecovery(changed); err == nil {
		t.Fatal("changed bounds accepted")
	}

	// An N3 height can never certify a Neo X interval, and vice versa.
	n3Fact := []LifecycleObservation{{tokenID(types.DirectionWithdrawal, 1, "a", "b"), FactSettlement, rootEvidence("neo_n3", "relay", 1200, "r"), ""}}
	if _, err := os.CommitRecoveryRange(recoveryRange("neo_x", 20, 24), RecoveryBatch{Observations: n3Fact}); err == nil {
		t.Fatal("N3 evidence committed to Neo X recovery")
	}
	outside := []LifecycleObservation{{tokenID(types.DirectionDeposit, 1, "a", "b"), FactSettlement, rootEvidence("neo_x", "relay", 30, "r"), ""}}
	if _, err := os.CommitRecoveryRange(recoveryRange("neo_x", 20, 24), RecoveryBatch{Observations: outside}); err == nil {
		t.Fatal("evidence outside the range accepted")
	}
	if _, err := os.CommitRecoveryRange(recoveryRange("neo_x", 21, 24), RecoveryBatch{}); err == nil {
		t.Fatal("range skipping the start accepted")
	}
	if _, err := os.CommitRecoveryRange(recoveryRange("neo_x", 20, 30), RecoveryBatch{}); err == nil {
		t.Fatal("range beyond target accepted")
	}
	missing := recoveryRange("neo_x", 20, 24)
	missing.VerifiedPaths = []string{"blocks", "logs"}
	if _, err := os.CommitRecoveryRange(missing, RecoveryBatch{}); err == nil {
		t.Fatal("unverified claim path accepted")
	}
	mustCommit(t, os, recoveryRange("neo_x", 20, 24), RecoveryBatch{})
	if _, err := os.CommitRecoveryRange(recoveryRange("neo_x", 26, 29), RecoveryBatch{}); err == nil {
		t.Fatal("gap between ranges accepted")
	}
	disconnected := recoveryRange("neo_x", 25, 29)
	disconnected.FromParentHash = "fork"
	if _, err := os.CommitRecoveryRange(disconnected, RecoveryBatch{}); err == nil {
		t.Fatal("disconnected range accepted")
	}
	wrongTarget := recoveryRange("neo_x", 25, 29)
	wrongTarget.ToHash = "other"
	if _, err := os.CommitRecoveryRange(wrongTarget, RecoveryBatch{}); err == nil {
		t.Fatal("target hash mismatch accepted")
	}
	mustCommit(t, os, recoveryRange("neo_x", 25, 29), RecoveryBatch{})
	m, _ := os.GetRecoveryManifest("neo_x")
	if !m.Contiguous() || m.Batches != 2 {
		t.Fatalf("contiguous coverage: %+v", m)
	}
	if n, _ := os.GetRecoveryManifest("neo_n3"); n.NextHeight != 1200 || n.Contiguous() {
		t.Fatalf("N3 coverage leaked from Neo X: %+v", n)
	}
}

func TestRecoveryRetryIsIdempotentAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	os := NewOperationStore(s, nil)
	if err := os.BeginRecovery(recoveryManifest("neo_x", 50, 59)); err != nil {
		t.Fatal(err)
	}
	id := tokenID(types.DirectionDeposit, 4, "aa", "bb")
	batch := RecoveryBatch{Observations: []LifecycleObservation{{id, FactSettlement, rootEvidence("neo_x", "relay", 51, "r4"), ""}}}
	r := recoveryRange("neo_x", 50, 54)
	mustCommit(t, os, r, batch)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	os = NewOperationStore(s, nil)
	if res := mustCommit(t, os, r, batch); !res.Retry {
		t.Fatal("retry after restart was not recognized")
	}
	changed := batch
	changed.Observations = []LifecycleObservation{{id, FactSettlement, rootEvidence("neo_x", "other-relay", 51, "r4"), ""}}
	if _, err := os.CommitRecoveryRange(r, changed); err == nil {
		t.Fatal("retry with different evidence accepted")
	}
	m, _ := os.GetRecoveryManifest("neo_x")
	if m.NextHeight != 55 || m.Batches != 1 || m.Observations != 1 {
		t.Fatalf("retry changed progress: %+v", m)
	}
	// Concurrent identical retries commit at most once.
	var wg sync.WaitGroup
	next := recoveryRange("neo_x", 55, 59)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := os.CommitRecoveryRange(next, RecoveryBatch{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent retry: %v", err)
		}
	}
	if m, _ := os.GetRecoveryManifest("neo_x"); m.NextHeight != 60 || m.Batches != 2 {
		t.Fatalf("concurrent retry advanced twice: %+v", m)
	}
}

func TestRecoveryRangeRollsBackAtomically(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	if err := os.BeginRecovery(recoveryManifest("neo_x", 100, 199)); err != nil {
		t.Fatal(err)
	}
	id := tokenID(types.DirectionDeposit, 7, "aa", "bb")
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactSettlement, rootEvidence("neo_x", "live-relay", 120, "r7"), ""}}); err != nil {
		t.Fatal(err)
	}
	op := tokenDeposit(8, "aa", "bb", types.StatusPending)
	op.SourceChain, op.DestinationChain, op.Direction, op.SourceBlockHeight = "neo_x", "neo_n3", types.DirectionWithdrawal, 110
	conflicting := RecoveryBatch{
		Operations:   []*types.BridgeOperation{op},
		TraceGaps:    []NeoXClaimTraceGap{{FromHeight: 101, ToHeight: 101, Reason: "trace_unsupported"}},
		Observations: []LifecycleObservation{{id, FactSettlement, rootEvidence("neo_x", "different-relay", 120, "r7"), ""}},
	}
	if _, err := os.CommitRecoveryRange(recoveryRange("neo_x", 100, 149), conflicting); err == nil {
		t.Fatal("conflicting evidence accepted")
	}
	m, _ := os.GetRecoveryManifest("neo_x")
	if m.NextHeight != 100 || m.Batches != 0 {
		t.Fatalf("watermark moved on failure: %+v", m)
	}
	if _, err := os.GetOperation(types.BridgeTypeToken, types.DirectionWithdrawal, 8); !errors.Is(err, ErrNotFound) {
		t.Fatalf("operation leaked from failed range: %v", err)
	}
	if gaps, _ := s.ListNeoXClaimTraceGaps(); len(gaps) != 0 {
		t.Fatalf("gap leaked from failed range: %+v", gaps)
	}
	if l, _ := os.GetLifecycle(id); l.Settlement.TxHash != "live-relay" {
		t.Fatalf("live evidence replaced: %+v", l.Settlement)
	}
}

// Missing traces keep the interval incomplete without stopping event
// recovery; a traced replay resolves earlier gaps inside its range only.
func TestRecoveryTraceGapsKeepCoverageIncomplete(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	if err := os.BeginRecovery(recoveryManifest("neo_x", 100, 199)); err != nil {
		t.Fatal(err)
	}
	// Live indexing earlier left a gap across the boundary of two ranges.
	live := &types.IndexerState{Chain: "neo_x", LastBlockHeight: 180, LastBlockHash: "block-180"}
	if err := s.SaveNeoXLifecycleCheckpoint(live, nil, nil, nil, NeoXClaimTraceGap{FromHeight: 140, ToHeight: 160, Reason: "trace_unsupported"}); err != nil {
		t.Fatal(err)
	}
	id := tokenID(types.DirectionDeposit, 9, "aa", "bb")
	untraced := RecoveryBatch{
		Observations: []LifecycleObservation{{id, FactSettlement, rootEvidence("neo_x", "relay", 120, "r9"), ""}},
		TraceGaps:    []NeoXClaimTraceGap{{FromHeight: 110, ToHeight: 112, Reason: "trace_unsupported"}},
	}
	mustCommit(t, os, recoveryRange("neo_x", 100, 149), untraced)
	if l, err := os.GetLifecycle(id); err != nil || l.Settlement == nil || l.Claim != nil {
		t.Fatalf("event recovery blocked or claim inferred: %+v %v", l, err)
	}
	gaps, _ := s.ListNeoXClaimTraceGaps()
	// 140-149 were traced by this replay; 150-160 lie outside the range.
	want := []NeoXClaimTraceGap{{110, 112, "trace_unsupported"}, {150, 160, "trace_unsupported"}}
	if fmt.Sprint(gaps) != fmt.Sprint(want) {
		t.Fatalf("gaps %+v, want %+v", gaps, want)
	}
	mustCommit(t, os, recoveryRange("neo_x", 150, 199), RecoveryBatch{})
	st, err := os.RecoveryStatus("neo_x")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Contiguous || st.Complete || st.HandoffReady || st.ClaimTraceGapBlocks != 3 {
		t.Fatalf("incomplete claim coverage certified: %+v", st)
	}
	if got, _ := s.NeoXClaimCoverageGaps(150, 160); len(got) != 0 {
		t.Fatalf("traced replay did not resolve the live gap: %+v", got)
	}

	strict := recoveryManifest("neo_x", 0, 9)
	strict.RequiredPaths = append(strict.RequiredPaths, "traces")
	other := NewOperationStore(newTestStore(t), nil)
	if err := other.BeginRecovery(strict); err != nil {
		t.Fatal(err)
	}
	r := recoveryRange("neo_x", 0, 9)
	r.VerifiedPaths = strict.RequiredPaths
	if _, err := other.CommitRecoveryRange(r, RecoveryBatch{TraceGaps: []NeoXClaimTraceGap{{1, 1, "trace_unavailable"}}}); err == nil {
		t.Fatal("strict trace manifest accepted a gap")
	}
}

func TestRecoveryN3UnresolvedClaimsBlockCompletion(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	if err := os.BeginRecovery(recoveryManifest("neo_n3", 1000, 1099)); err != nil {
		t.Fatal(err)
	}
	claimEvidence := lifecycleEvidence("neo_n3", "claim-tx", 1050)
	claimEvidence.Kind = "notification"
	claimEvidence.Position = "0:1"
	// Live indexing saw the claim before its claimable was known.
	live := &types.IndexerState{Chain: "neo_n3", LastBlockHeight: 1099, LastBlockHash: "block-1099"}
	if err := s.SaveN3LifecycleCheckpoint(live, nil, nil, nil, UnresolvedN3TokenClaim{Nonce: 3, DestinationToken: "bb", Evidence: claimEvidence, Reason: "missing_claimable_pair"}); err != nil {
		t.Fatal(err)
	}
	mustCommit(t, os, recoveryRange("neo_n3", 1000, 1049), RecoveryBatch{})
	mustCommit(t, os, recoveryRange("neo_n3", 1050, 1099), RecoveryBatch{UnresolvedClaims: []UnresolvedN3TokenClaim{{Nonce: 3, DestinationToken: "bb", Evidence: claimEvidence, Reason: "missing_claimable_pair"}}})
	if st, _ := os.RecoveryStatus("neo_n3"); !st.Contiguous || st.Complete || st.UnresolvedClaims != 1 {
		t.Fatalf("unresolved claim certified: %+v", st)
	}

	// A replay that finds the claimable first resolves the claim.
	retry := NewOperationStore(newTestStore(t), nil)
	if err := retry.BeginRecovery(recoveryManifest("neo_n3", 1000, 1099)); err != nil {
		t.Fatal(err)
	}
	if err := retry.store.SaveN3LifecycleCheckpoint(live, nil, nil, nil, UnresolvedN3TokenClaim{Nonce: 3, DestinationToken: "bb", Evidence: claimEvidence, Reason: "missing_claimable_pair"}); err != nil {
		t.Fatal(err)
	}
	id := tokenID(types.DirectionWithdrawal, 3, "aa", "bb")
	claimable := lifecycleEvidence("neo_n3", "relay", 1010)
	mustCommit(t, retry, recoveryRange("neo_n3", 1000, 1099), RecoveryBatch{Observations: []LifecycleObservation{
		{id, FactClaimable, claimable, ""}, {id, FactClaim, claimEvidence, ""},
	}})
	if st, _ := retry.RecoveryStatus("neo_n3"); !st.Complete || !st.HandoffReady {
		t.Fatalf("resolved claim still blocks completion: %+v", st)
	}
}

// Replay never regresses live state: existing operations are not replaced,
// completed legacy records keep their destination, and directional states
// only rise. Token pairs with equal nonces stay separate.
func TestRecoveryNeverRegressesLiveStateAndIsolatesTokenPairs(t *testing.T) {
	s := newTestStore(t)
	bs := NewBridgeStateStore(s)
	os := NewOperationStore(s, bs)
	if err := os.BeginRecovery(recoveryManifest("neo_x", 100, 199)); err != nil {
		t.Fatal(err)
	}
	// Live state: pair A nonce 5 completed by a later claim, pair B nonce 5
	// stuck, and a newer received state for pair A.
	completed := tokenDeposit(5, "aa", "a1", types.StatusCompleted)
	claimTx, claimHeight := "live-claim", uint64(190)
	completed.DestTxHash, completed.DestBlockHeight = &claimTx, &claimHeight
	stuck := tokenDeposit(5, "bb", "b1", types.StatusStuck)
	for _, op := range []*types.BridgeOperation{completed, stuck} {
		if err := os.SaveOperation(op); err != nil {
			t.Fatal(err)
		}
	}
	instA := receivedInstance(tokenID(types.DirectionDeposit, 5, "aa", "a1"))
	newer := "r9"
	if err := s.Set(bridgeDstPrefix+instA, &types.ChainState{Nonce: 9, Root: &newer, TxHash: "live-relay-9"}); err != nil {
		t.Fatal(err)
	}
	a := tokenID(types.DirectionDeposit, 5, "0xAA", "0xA1")
	b := tokenID(types.DirectionDeposit, 5, "0xBB", "0xB1")
	replayedOp := tokenDeposit(5, "aa", "a1", types.StatusPending)
	replayedOp.SourceChain, replayedOp.SourceBlockHeight = "neo_x", 110 // stands in for a re-decoded source record
	batch := RecoveryBatch{Observations: []LifecycleObservation{
		{a, FactSettlement, rootEvidence("neo_x", "relay-a", 120, "r5a"), ""},
		{a, FactClaimable, lifecycleEvidence("neo_x", "relay-a", 120), ""},
		{b, FactSettlement, rootEvidence("neo_x", "relay-b", 121, "r5b"), ""},
		{b, FactDistribution, lifecycleEvidence("neo_x", "relay-b", 121), ""},
	}, Operations: []*types.BridgeOperation{replayedOp}}
	res := mustCommit(t, os, recoveryRange("neo_x", 100, 149), batch)
	if res.InsertedOperations != 0 || res.LegacyCompleted != 1 {
		t.Fatalf("result: %+v", res)
	}
	gotA, _ := s.getRaw(operationKeyForOp(completed))
	if gotA.Status != types.StatusCompleted || *gotA.DestTxHash != "live-claim" {
		t.Fatalf("completed record regressed: %+v", gotA)
	}
	gotB, _ := s.getRaw(operationKeyForOp(stuck))
	if gotB.Status != types.StatusCompleted || *gotB.DestTxHash != "relay-b" || *gotB.DestBlockHeight != 121 {
		t.Fatalf("stuck pair B not completed from its own evidence: %+v", gotB)
	}
	var dstA types.ChainState
	if err := s.Get(bridgeDstPrefix+instA, &dstA); err != nil || dstA.Nonce != 9 {
		t.Fatalf("received state regressed: %+v %v", dstA, err)
	}
	var dstB types.ChainState
	if err := s.Get(bridgeDstPrefix+receivedInstance(b), &dstB); err != nil || dstB.Nonce != 5 || *dstB.Root != "r5b" {
		t.Fatalf("pair B received state: %+v %v", dstB, err)
	}
	la, _ := os.GetLifecycle(a)
	lb, _ := os.GetLifecycle(b)
	if la.Distribution != nil || lb.Claimable != nil || la.Settlement.TxHash != "relay-a" || lb.Settlement.TxHash != "relay-b" {
		t.Fatalf("token pairs mixed: %+v / %+v", la, lb)
	}
}

// A missing source operation is inserted once and picks up destination
// evidence that was recorded before it, whichever chain replayed first.
func TestRecoveryInsertsMissingSourceHistory(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, NewBridgeStateStore(s))
	if err := os.BeginRecovery(recoveryManifest("neo_n3", 1000, 1099)); err != nil {
		t.Fatal(err)
	}
	id := tokenID(types.DirectionDeposit, 27, "0x57d1", "0xe816")
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{
		{id, FactSettlement, rootEvidence("neo_x", "dest", 7646446, "5c74"), ""},
		{id, FactDistribution, lifecycleEvidence("neo_x", "dest", 7646446), ""},
	}); err != nil {
		t.Fatal(err)
	}
	src := tokenDeposit(27, "57d1", "e816", types.StatusPending)
	src.SourceBlockHeight = 1016
	root := "5c74"
	src.Root = &root
	pending := tokenDeposit(28, "57d1", "e816", types.StatusPending)
	pending.SourceBlockHeight = 1017
	res := mustCommit(t, os, recoveryRange("neo_n3", 1000, 1099), RecoveryBatch{Operations: []*types.BridgeOperation{src, pending}})
	if res.InsertedOperations != 2 || res.LegacyCompleted != 1 {
		t.Fatalf("result %+v", res)
	}
	got, _ := s.getRaw(operationKeyForOp(src))
	if got.Status != types.StatusCompleted || *got.DestTxHash != "dest" || *got.DestBlockHeight != 7646446 {
		t.Fatalf("recovered source not completed from destination evidence: %+v", got)
	}
	if got, _ := s.getRaw(operationKeyForOp(pending)); got.Status != types.StatusPending || got.DestTxHash != nil {
		t.Fatalf("operation without destination evidence completed: %+v", got)
	}
	completed := types.StatusCompleted
	if n, _ := os.CountOperations(OperationFilter{Status: &completed}); n != 1 {
		t.Fatalf("status index: %d completed", n)
	}
	var srcState types.ChainState
	if err := s.Get(bridgeSrcPrefix+InstanceIDFromOperation(src), &srcState); err != nil || srcState.Nonce != 28 {
		t.Fatalf("source state: %+v %v", srcState, err)
	}
}

func TestRecoveredReceivedRootReplacesSyntheticDestination(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, NewBridgeStateStore(s))
	if err := os.BeginRecovery(recoveryManifest("neo_n3", 1000, 1099)); err != nil {
		t.Fatal(err)
	}
	id := OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionWithdrawal, Nonce: 15}
	instance := receivedInstance(id)
	if err := s.Set(bridgeDstPrefix+instance, &types.ChainState{Synthetic: true, Nonce: 15, TxHash: "source-tx"}); err != nil {
		t.Fatal(err)
	}
	e := rootEvidence("neo_n3", "n3-relay", 1050, "bd8a3be2")
	lower := OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionWithdrawal, Nonce: 14}
	mustCommit(t, os, recoveryRange("neo_n3", 1000, 1099), RecoveryBatch{Observations: []LifecycleObservation{
		{lower, FactSettlement, e, ""}, {id, FactSettlement, e, ""},
	}})
	var dst types.ChainState
	if err := s.Get(bridgeDstPrefix+instance, &dst); err != nil || dst.Synthetic || dst.Nonce != 15 || dst.TxHash != "n3-relay" || *dst.Root != "bd8a3be2" {
		t.Fatalf("verified received root did not replace synthetic state: %+v %v", dst, err)
	}
	l, _ := os.GetLifecycle(id)
	if l.Settlement == nil || l.Execution != nil {
		t.Fatalf("arrival conflated with execution: %+v", l)
	}
}

// Cutover: the first target was captured on a database copy; live indexing
// then advanced. Recovery must be extended through the live checkpoint before
// handoff, so no interval between replay and live indexing is uncovered.
func TestRecoveryCutoverCatchUp(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	if err := os.BeginRecovery(recoveryManifest("neo_x", 100, 149)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveIndexerState(&types.IndexerState{Chain: "neo_x", LastBlockHeight: 140}); err != nil {
		t.Fatal(err)
	}
	mustCommit(t, os, recoveryRange("neo_x", 100, 149), RecoveryBatch{})
	if st, _ := os.RecoveryStatus("neo_x"); !st.Complete || !st.HandoffReady {
		t.Fatalf("copy rehearsal: %+v", st)
	}
	// Production kept indexing after the copy.
	if err := s.SaveIndexerState(&types.IndexerState{Chain: "neo_x", LastBlockHeight: 175}); err != nil {
		t.Fatal(err)
	}
	st, _ := os.RecoveryStatus("neo_x")
	if !st.Complete || st.HandoffReady {
		t.Fatalf("handoff allowed with live checkpoint past the target: %+v", st)
	}
	if err := os.ExtendRecoveryTarget("neo_x", 140, "block-140"); err == nil {
		t.Fatal("target lowered")
	}
	if err := os.ExtendRecoveryTarget("neo_x", 180, "block-180"); err != nil {
		t.Fatal(err)
	}
	if err := os.ExtendRecoveryTarget("neo_x", 180, "block-180"); err != nil {
		t.Fatalf("idempotent extend: %v", err)
	}
	if err := os.ExtendRecoveryTarget("neo_x", 180, "other"); err == nil {
		t.Fatal("target re-hashed")
	}
	if st, _ := os.RecoveryStatus("neo_x"); st.Complete || st.HandoffReady {
		t.Fatalf("extension not replayed yet but complete: %+v", st)
	}
	mustCommit(t, os, recoveryRange("neo_x", 150, 180), RecoveryBatch{})
	st, _ = os.RecoveryStatus("neo_x")
	m := st.Manifest
	if !st.HandoffReady || m.StartHeight != 100 || len(m.Targets) != 2 || m.Targets[0].Height != 149 {
		t.Fatalf("catch-up handoff: %+v", st)
	}
	if live, _ := s.GetIndexerState("neo_x"); live.LastBlockHeight != 175 {
		t.Fatalf("recovery changed the live checkpoint: %+v", live)
	}
}

func (s *Store) getRaw(key string) (*types.BridgeOperation, error) {
	var op types.BridgeOperation
	if err := s.Get(key, &op); err != nil {
		return nil, err
	}
	return &op, nil
}
