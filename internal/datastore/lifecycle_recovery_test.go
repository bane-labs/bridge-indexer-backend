package datastore

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

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
