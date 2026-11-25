package datastore

import (
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func recoveryManifest(chain string, start, target uint64) RecoveryManifest {
	paths := []string{"blocks", "implementations", "notifications", "application_logs"}
	if chain == "neo_x" {
		paths = []string{"blocks", "implementations", "logs", "transactions", "receipts", "claims", "traces"}
	}
	return RecoveryManifest{Chain: chain, WatchedContracts: []string{"bridge@v1"}, ImplementationRanges: []string{"v1:0-100"},
		RequiredPaths: paths, StartHeight: start, TargetHeight: target,
		TargetHash: "target", NextHeight: start}
}

func TestRecoveryContiguousAtomicAndSeparateChainBounds(t *testing.T) {
	os := NewOperationStore(newTestStore(t), nil)
	x := recoveryManifest("neo_x", 20, 21)
	n3 := recoveryManifest("neo_n3", 1200, 1200)
	incomplete := x
	incomplete.RequiredPaths = []string{"blocks", "logs"}
	if err := os.BeginRecovery(incomplete); err == nil {
		t.Fatal("manifest omitted silent claim evidence")
	}
	for _, m := range []RecoveryManifest{x, n3} {
		if err := os.BeginRecovery(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.BeginRecovery(x); err != nil {
		t.Fatalf("resume: %v", err)
	}
	x.TargetHeight = 22
	if err := os.BeginRecovery(x); err == nil {
		t.Fatal("changed bounds accepted")
	}
	id := OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 27, SourceToken: "A", DestinationToken: "B"}
	e := lifecycleEvidence("neo_x", "relay", 20)
	obs := []LifecycleObservation{{id, FactSettlement, e, ""}}
	block := RecoveryBlock{Chain: "neo_x", Height: 20, Hash: e.BlockHash, VerifiedPaths: recoveryManifest("neo_x", 20, 21).RequiredPaths}
	if err := os.CommitRecoveryBlock(RecoveryBlock{Chain: "neo_x", Height: 21, Hash: "target", VerifiedPaths: block.VerifiedPaths}, obs); err == nil {
		t.Fatal("skipped block accepted")
	}
	missingTrace := block
	missingTrace.VerifiedPaths = block.VerifiedPaths[:len(block.VerifiedPaths)-1]
	if err := os.CommitRecoveryBlock(missingTrace, obs); err == nil {
		t.Fatal("incomplete claim-trace coverage accepted")
	}
	unresolved := block
	unresolved.Unresolved = 1
	if err := os.CommitRecoveryBlock(unresolved, obs); err == nil {
		t.Fatal("unresolved claim accepted")
	}
	if err := os.RecordRecoveryUnresolved("neo_x", 20, 1); err != nil {
		t.Fatal(err)
	}
	progress, err := os.GetRecoveryManifest("neo_x")
	if err != nil || progress.NextHeight != 20 || progress.Unresolved != 1 || progress.VerifiedThroughTarget() {
		t.Fatalf("unresolved progress was not persisted: %+v, %v", progress, err)
	}
	if _, err := os.GetLifecycle(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed checkpoint persisted observation: %v", err)
	}
	if err := os.CommitRecoveryBlock(block, obs); err != nil {
		t.Fatal(err)
	}
	m, _ := os.GetRecoveryManifest("neo_x")
	if m.NextHeight != 21 || m.Unresolved != 0 || m.VerifiedThroughTarget() {
		t.Fatalf("premature coverage: %+v", m)
	}
	if err := os.CommitRecoveryBlock(block, obs); err != nil {
		t.Fatalf("retry of committed block: %v", err)
	}
	progress, err = os.GetRecoveryManifest("neo_x")
	if err != nil || progress.NextHeight != 21 || progress.ContiguousHash != e.BlockHash {
		t.Fatalf("retry advanced watermark: %+v, %v", progress, err)
	}
	changed := e
	changed.TxHash = "different-relay"
	if err := os.CommitRecoveryBlock(block, []LifecycleObservation{{id, FactSettlement, changed, ""}}); err == nil {
		t.Fatal("retry with different observations accepted")
	}
	conflictingBlock := block
	conflictingBlock.Hash = "other-block"
	if err := os.CommitRecoveryBlock(conflictingBlock, obs); err == nil {
		t.Fatal("retry with different block hash accepted")
	}
	if err := os.CommitRecoveryBlock(missingTrace, obs); err == nil {
		t.Fatal("retry with missing verified path accepted")
	}
	if err := os.CommitRecoveryBlock(RecoveryBlock{Chain: "neo_x", Height: 21, Hash: "wrong", VerifiedPaths: block.VerifiedPaths}, nil); err == nil {
		t.Fatal("target hash mismatch accepted")
	}
	if err := os.CommitRecoveryBlock(RecoveryBlock{Chain: "neo_x", Height: 21, Hash: "target", ParentHash: "wrong", VerifiedPaths: block.VerifiedPaths}, nil); err == nil {
		t.Fatal("disconnected block accepted")
	}
	if err := os.CommitRecoveryBlock(RecoveryBlock{Chain: "neo_x", Height: 21, Hash: "target", ParentHash: e.BlockHash, VerifiedPaths: block.VerifiedPaths}, nil); err != nil {
		t.Fatal(err)
	}
	m, _ = os.GetRecoveryManifest("neo_x")
	if !m.VerifiedThroughTarget() {
		t.Fatalf("verified interval incomplete: %+v", m)
	}
	last := RecoveryBlock{Chain: "neo_x", Height: 21, Hash: "target", ParentHash: e.BlockHash, VerifiedPaths: block.VerifiedPaths}
	if err := os.CommitRecoveryBlock(last, nil); err != nil {
		t.Fatalf("retry at target: %v", err)
	}
	if err := os.CommitRecoveryBlock(block, obs); err == nil {
		t.Fatal("retry older than last committed block accepted")
	}
	n, _ := os.GetRecoveryManifest("neo_n3")
	if n.NextHeight != 1200 || n.VerifiedThroughTarget() {
		t.Fatalf("N3 coverage leaked from Neo X: %+v", n)
	}
}

func TestRecoveryConcurrentIdenticalBlockRetry(t *testing.T) {
	os := NewOperationStore(newTestStore(t), nil)
	m := recoveryManifest("neo_n3", 1200, 1200)
	if err := os.BeginRecovery(m); err != nil {
		t.Fatal(err)
	}
	block := RecoveryBlock{Chain: "neo_n3", Height: 1200, Hash: "target", VerifiedPaths: m.RequiredPaths}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- os.CommitRecoveryBlock(block, nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent retry: %v", err)
		}
	}
	progress, err := os.GetRecoveryManifest("neo_n3")
	if err != nil || progress.NextHeight != 1201 || !progress.VerifiedThroughTarget() {
		t.Fatalf("concurrent retry advanced twice: %+v, %v", progress, err)
	}
}

func TestRecoveryRetryAfterReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	os := NewOperationStore(s, nil)
	m := recoveryManifest("neo_x", 50, 50)
	if err := os.BeginRecovery(m); err != nil {
		t.Fatal(err)
	}
	block := RecoveryBlock{Chain: "neo_x", Height: 50, Hash: "target", VerifiedPaths: m.RequiredPaths}
	id := OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 4}
	e := lifecycleEvidence("neo_x", "relay", 50)
	e.BlockHash = block.Hash
	observations := []LifecycleObservation{{id, FactSettlement, e, ""}}
	if err := os.CommitRecoveryBlock(block, observations); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	os = NewOperationStore(s, nil)
	if err := os.CommitRecoveryBlock(block, observations); err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	progress, err := os.GetRecoveryManifest("neo_x")
	if err != nil || progress.NextHeight != 51 || !progress.VerifiedThroughTarget() {
		t.Fatalf("retry changed recovered coverage: %+v, %v", progress, err)
	}
	got, err := os.GetLifecycle(id)
	if err != nil || got.Settlement.TxHash != e.TxHash {
		t.Fatalf("retry lost observation: %+v, %v", got, err)
	}
}

func TestLifecycleAdditiveMigrationOnExistingDatabase(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	op := sampleOp()
	if err := NewOperationStore(s, nil).SaveOperation(op); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	os := NewOperationStore(s, nil)
	id := OperationIdentity{BridgeType: op.BridgeType, Direction: op.Direction, Nonce: op.Nonce}
	if _, err := os.GetLifecycle(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old record inferred lifecycle: %v", err)
	}
	if _, err := os.GetRecoveryManifest("neo_x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old checkpoint inferred coverage: %v", err)
	}
	e := lifecycleEvidence("neo_x", "relay", 2000)
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactSettlement, e, ""}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	os = NewOperationStore(s, nil)
	got, err := os.GetLifecycle(id)
	if err != nil || got.Settlement.TxHash != "relay" {
		t.Fatalf("sidecar not durable: %+v, %v", got, err)
	}
	legacy, err := os.GetOperation(op.BridgeType, op.Direction, op.Nonce)
	if err != nil || legacy.Status != types.StatusPending || legacy.DestTxHash != nil {
		t.Fatalf("legacy record modified: %+v, %v", legacy, err)
	}
}
