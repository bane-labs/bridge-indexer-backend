package monitor

import (
	"log/slog"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestSyncComputer_StatusDerivation(t *testing.T) {
	store := newMonitorTestStore(t)
	bs := datastore.NewBridgeStateStore(store)
	sc := NewSyncComputer(bs, slog.Default())

	rootA := "0xaaa"
	rootB := "0xbbb"
	now := time.Now()

	missingID := datastore.BuildInstanceID("native", "neo_n3", "neo_x", nil, nil)
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: missingID,
		BridgeType: types.BridgeTypeNative,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
	})

	syncedID := datastore.BuildInstanceID("message", "neo_n3", "neo_x", nil, nil)
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: syncedID,
		BridgeType: types.BridgeTypeMessage,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
		SourceState: &types.ChainState{
			Nonce:      10,
			Root:       &rootA,
			ObservedAt: now,
		},
		DestState: &types.ChainState{
			Nonce:      10,
			Root:       &rootA,
			ObservedAt: now,
		},
	})

	sourceAheadID := datastore.BuildInstanceID("native", "neo_x", "neo_n3", nil, nil)
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: sourceAheadID,
		BridgeType: types.BridgeTypeNative,
		SrcChain:   "neo_x",
		DstChain:   "neo_n3",
		SourceState: &types.ChainState{
			Nonce:      20,
			Root:       &rootA,
			ObservedAt: now,
		},
		DestState: &types.ChainState{
			Nonce:      15,
			Root:       &rootA,
			ObservedAt: now,
		},
	})

	rootMismatchID := datastore.BuildInstanceID("message", "neo_x", "neo_n3", nil, nil)
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: rootMismatchID,
		BridgeType: types.BridgeTypeMessage,
		SrcChain:   "neo_x",
		DstChain:   "neo_n3",
		SourceState: &types.ChainState{
			Nonce:      30,
			Root:       &rootA,
			ObservedAt: now,
		},
		DestState: &types.ChainState{
			Nonce:      30,
			Root:       &rootB,
			ObservedAt: now,
		},
	})

	invariantID := datastore.BuildInstanceID("native", "neo_n3", "neo_x", nil, nil) + "|invariant"
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: invariantID,
		BridgeType: types.BridgeTypeNative,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
		SourceState: &types.ChainState{
			Nonce:      40,
			Root:       &rootA,
			ObservedAt: now,
		},
		DestState: &types.ChainState{
			Nonce:      41,
			Root:       &rootA,
			ObservedAt: now,
		},
	})

	syncs, err := sc.ComputeAll()
	if err != nil {
		t.Fatalf("ComputeAll() error: %v", err)
	}
	got := mapSyncByID(syncs)

	assertSync(t, got[missingID], types.DirectionalSyncStatusUnknown, nil, types.DirectionalSyncReasonMissingState)
	assertSync(t, got[syncedID], types.DirectionalSyncStatusSynced, ptrInt64(0), types.DirectionalSyncReasonSynced)
	assertSync(t, got[sourceAheadID], types.DirectionalSyncStatusSourceAhead, ptrInt64(5), types.DirectionalSyncReasonSourceNonceGreater)
	assertSync(t, got[rootMismatchID], types.DirectionalSyncStatusRootMismatch, ptrInt64(0), types.DirectionalSyncReasonRootMismatchAtEqualNonce)
	assertSync(t, got[invariantID], types.DirectionalSyncStatusUnknown, ptrInt64(-1), types.DirectionalSyncReasonInvariantViolation)

	// Verify nonce/root fields are populated
	assertNonceRoot(t, got[missingID], nil, nil, nil, nil)
	assertNonceRoot(t, got[syncedID], ptrUint64(10), ptrUint64(10), &rootA, &rootA)
	assertNonceRoot(t, got[sourceAheadID], ptrUint64(20), ptrUint64(15), &rootA, &rootA)
	assertNonceRoot(t, got[rootMismatchID], ptrUint64(30), ptrUint64(30), &rootA, &rootB)
}

func TestSyncComputer_TokenNonceCollisionIsolation(t *testing.T) {
	store := newMonitorTestStore(t)
	bs := datastore.NewBridgeStateStore(store)
	sc := NewSyncComputer(bs, slog.Default())

	srcA, dstA := "0xTokenA_N3", "0xTokenA_X"
	srcB, dstB := "0xTokenB_N3", "0xTokenB_X"
	idA := datastore.BuildInstanceID("token", "neo_n3", "neo_x", &srcA, &dstA)
	idB := datastore.BuildInstanceID("token", "neo_n3", "neo_x", &srcB, &dstB)

	root := "0xroot"
	now := time.Now()
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: idA,
		BridgeType: types.BridgeTypeToken,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
		SrcToken:   &srcA,
		DstToken:   &dstA,
		SourceState: &types.ChainState{
			Nonce:      50,
			Root:       &root,
			ObservedAt: now,
		},
		DestState: &types.ChainState{
			Nonce:      49,
			Root:       &root,
			ObservedAt: now,
		},
	})
	_ = bs.SaveBridgeState(&types.DirectionalBridgeState{
		InstanceID: idB,
		BridgeType: types.BridgeTypeToken,
		SrcChain:   "neo_n3",
		DstChain:   "neo_x",
		SrcToken:   &srcB,
		DstToken:   &dstB,
		SourceState: &types.ChainState{
			Nonce:      50,
			Root:       &root,
			ObservedAt: now,
		},
		DestState: &types.ChainState{
			Nonce:      50,
			Root:       &root,
			ObservedAt: now,
		},
	})

	syncs, err := sc.ComputeAll()
	if err != nil {
		t.Fatalf("ComputeAll() error: %v", err)
	}
	got := mapSyncByID(syncs)

	assertSync(t, got[idA], types.DirectionalSyncStatusSourceAhead, ptrInt64(1), types.DirectionalSyncReasonSourceNonceGreater)
	assertSync(t, got[idB], types.DirectionalSyncStatusSynced, ptrInt64(0), types.DirectionalSyncReasonSynced)
}

func newMonitorTestStore(t *testing.T) *datastore.Store {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return datastore.NewStoreFromDB(db, slog.Default())
}

func mapSyncByID(syncs []types.DirectionalInstanceSync) map[string]types.DirectionalInstanceSync {
	out := make(map[string]types.DirectionalInstanceSync, len(syncs))
	for _, s := range syncs {
		out[s.ID] = s
	}
	return out
}

func assertSync(t *testing.T, got types.DirectionalInstanceSync, wantStatus types.DirectionalSyncStatus, wantDelta *int64, wantReason types.DirectionalSyncReason) {
	t.Helper()
	if got.Status != wantStatus {
		t.Fatalf("status = %s, want %s", got.Status, wantStatus)
	}
	if got.Reason != wantReason {
		t.Fatalf("reason = %s, want %s", got.Reason, wantReason)
	}
	if (got.DeltaNonce == nil) != (wantDelta == nil) {
		t.Fatalf("delta presence mismatch: got=%v want=%v", got.DeltaNonce, wantDelta)
	}
	if got.DeltaNonce != nil && wantDelta != nil && *got.DeltaNonce != *wantDelta {
		t.Fatalf("delta = %d, want %d", *got.DeltaNonce, *wantDelta)
	}
}

func ptrInt64(v int64) *int64    { return &v }
func ptrUint64(v uint64) *uint64 { return &v }

func assertNonceRoot(t *testing.T, got types.DirectionalInstanceSync, wantSrcNonce, wantDstNonce *uint64, wantSrcRoot, wantDstRoot *string) {
	t.Helper()
	assertUint64Ptr(t, "src_nonce", got.SrcNonce, wantSrcNonce)
	assertUint64Ptr(t, "dst_nonce", got.DstNonce, wantDstNonce)
	assertStringPtr(t, "src_root", got.SrcRoot, wantSrcRoot)
	assertStringPtr(t, "dst_root", got.DstRoot, wantDstRoot)
}

func assertUint64Ptr(t *testing.T, field string, got, want *uint64) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Fatalf("%s presence mismatch: got=%v want=%v", field, got, want)
	}
	if got != nil && want != nil && *got != *want {
		t.Fatalf("%s = %d, want %d", field, *got, *want)
	}
}

func assertStringPtr(t *testing.T, field string, got, want *string) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Fatalf("%s presence mismatch: got=%v want=%v", field, got, want)
	}
	if got != nil && want != nil && *got != *want {
		t.Fatalf("%s = %q, want %q", field, *got, *want)
	}
}
