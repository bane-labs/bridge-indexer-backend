package datastore

import (
	"errors"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestNeoXClaimTraceGapCheckpointAndEnrichment(t *testing.T) {
	store := newTestStore(t)
	ops := NewOperationStore(store, nil)
	state := &types.IndexerState{Chain: "neo_x", LastBlockHeight: 20, LastBlockHash: "block-20", UpdatedAt: time.Now()}
	gap := NeoXClaimTraceGap{FromHeight: 11, ToHeight: 12, Reason: "trace_unavailable"}

	// An invalid core observation rolls back the gap with the checkpoint.
	bad := lifecycleEvidence("neo_x", "relay", 11)
	bad.BlockHash = ""
	id := OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 1}
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, []LifecycleObservation{{Identity: id, Fact: FactSettlement, Evidence: bad}}, gap); err == nil {
		t.Fatal("invalid evidence accepted")
	}
	if gaps, _ := store.ListNeoXClaimTraceGaps(); len(gaps) != 0 {
		t.Fatalf("gap leaked from failed checkpoint: %+v", gaps)
	}
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, nil, NeoXClaimTraceGap{FromHeight: 21, ToHeight: 21, Reason: "trace_unavailable"}); err == nil {
		t.Fatal("gap beyond checkpoint accepted")
	}

	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, nil, gap); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, nil, gap); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	changed := gap
	changed.Reason = "trace_unsupported"
	if err := store.SaveNeoXLifecycleCheckpoint(state, nil, nil, nil, changed); err == nil {
		t.Fatal("conflicting gap accepted")
	}

	// A checkpoint past the gap does not certify claim coverage there.
	if got, err := store.NeoXClaimCoverageGaps(1, 20); err != nil || len(got) != 1 {
		t.Fatalf("incomplete coverage hidden: %+v %v", got, err)
	}
	if got, _ := store.NeoXClaimCoverageGaps(13, 20); len(got) != 0 {
		t.Fatalf("non-overlapping interval reported: %+v", got)
	}

	claimID := OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 7, SourceToken: "0x01", DestinationToken: "0x02"}
	claim := lifecycleEvidence("neo_x", "claim", 11)
	claim.Kind, claim.Position = "call", "top-level/0"
	obs := []LifecycleObservation{{Identity: claimID, Fact: FactClaim, Evidence: claim}}
	if err := store.ResolveNeoXClaimTraceGap(12, "block-12", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("out-of-order enrichment: %v", err)
	}
	if err := store.ResolveNeoXClaimTraceGap(11, "other", obs); err == nil {
		t.Fatal("observation from another block accepted")
	}
	if err := store.ResolveNeoXClaimTraceGap(11, "block-11", obs); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveNeoXClaimTraceGap(11, "block-11", obs); !errors.Is(err, ErrNotFound) {
		t.Fatalf("repeated enrichment: %v", err)
	}
	if l, err := ops.GetLifecycle(claimID); err != nil || l.Claim == nil || l.Claim.TxHash != "claim" {
		t.Fatalf("enriched claim: %+v %v", l, err)
	}
	if got, _ := store.ListNeoXClaimTraceGaps(); len(got) != 1 || got[0].FromHeight != 12 || got[0].ToHeight != 12 {
		t.Fatalf("gap not narrowed: %+v", got)
	}

	// Conflicting enrichment leaves the gap in place.
	conflict := claim
	conflict.BlockHeight, conflict.BlockHash, conflict.TxHash = 12, "block-12", "different"
	if err := store.ResolveNeoXClaimTraceGap(12, "block-12", []LifecycleObservation{{Identity: claimID, Fact: FactClaim, Evidence: conflict}}); err == nil {
		t.Fatal("conflicting claim accepted")
	}
	if got, _ := store.NeoXClaimCoverageGaps(12, 12); len(got) != 1 {
		t.Fatal("gap lost after conflicting enrichment")
	}
	if err := store.ResolveNeoXClaimTraceGap(12, "block-12", nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.NeoXClaimCoverageGaps(1, 20); len(got) != 0 {
		t.Fatalf("resolved gap remains: %+v", got)
	}
	if s, err := store.GetIndexerState("neo_x"); err != nil || s.LastBlockHeight != 20 {
		t.Fatalf("enrichment moved the live checkpoint: %+v %v", s, err)
	}
}
