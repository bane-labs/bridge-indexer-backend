package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func lifecycleEvidence(chain, tx string, height uint64) LifecycleEvidence {
	return LifecycleEvidence{Kind: "event", Chain: chain, Contract: "bridge", BlockHeight: height,
		BlockHash: fmt.Sprintf("block-%d", height), TxHash: tx,
		ObservedAt: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC), Position: "log:0"}
}

func TestLifecycleOutOfOrderAndLegacyCompatibility(t *testing.T) {
	s := newTestStore(t)
	os := NewOperationStore(s, nil)
	id := OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 108,
		SourceToken: "0xABC", DestinationToken: "0xDEF"}
	settlement := lifecycleEvidence("neo_x", "relay", 40)
	claimable := lifecycleEvidence("neo_x", "relay", 40)
	claimable.Position = "log:1"
	claim := lifecycleEvidence("neo_x", "claim", 45)
	claim.Kind, claim.Position = "call", "direct:0"
	claim.Payload = json.RawMessage(`{"implementation":"silent-claim","transfer":true}`)
	// A claim may be observed before the source request or relay receipt.
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactClaim, claim, ""}, {id, FactExecution, claim, ""}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.GetOperation(types.BridgeTypeToken, types.DirectionDeposit, id.Nonce); !errors.Is(err, ErrNotFound) {
		t.Fatalf("source operation unexpectedly created: %v", err)
	}
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactSettlement, settlement, ""}, {id, FactClaimable, claimable, ""}}); err != nil {
		t.Fatal(err)
	}
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactExecution, claim, ""}}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, err := os.GetLifecycle(OperationIdentity{BridgeType: id.BridgeType, Direction: id.Direction, Nonce: id.Nonce, SourceToken: "abc", DestinationToken: "DEF"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Settlement.TxHash != "relay" || got.Execution.TxHash != "claim" || got.Claim == nil || got.Claimable == nil || got.Distribution != nil {
		t.Fatalf("independent facts lost: %+v", got)
	}
	if err := os.SaveOperation(&types.BridgeOperation{BridgeType: id.BridgeType, Direction: id.Direction, Nonce: id.Nonce,
		TokenContract: &id.SourceToken, DestTokenContract: &id.DestinationToken, Status: types.StatusPending}); err != nil {
		t.Fatal(err)
	}
	legacy, err := os.GetOperation(types.BridgeTypeToken, types.DirectionDeposit, id.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Status != types.StatusPending || legacy.DestTxHash != nil || legacy.CompletionTimestamp != nil || json.Valid(encoded) == false {
		t.Fatalf("legacy projection changed: %s", encoded)
	}
	if err := os.UpdateTokenOperationCompletionByDestToken(id.Direction, id.Nonce, id.DestinationToken, 45, "claim", claim.ObservedAt); err != nil {
		t.Fatal(err)
	}
	got, err = os.GetLifecycle(id)
	if err != nil || got.Settlement.TxHash != "relay" {
		t.Fatalf("legacy completion replaced arrival: %+v, %v", got, err)
	}
}

func TestLifecycleAtomicConflictsAndTokenPairs(t *testing.T) {
	os := NewOperationStore(newTestStore(t), nil)
	id := OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionWithdrawal, Nonce: 7, SourceToken: "A", DestinationToken: "B"}
	other := id
	other.SourceToken = "C"
	e := lifecycleEvidence("neo_n3", "arrival", 20)
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactSettlement, e, ""}, {other, FactDistribution, e, ""}}); err != nil {
		t.Fatal(err)
	}
	bad := e
	bad.TxHash = "later-checkpoint"
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactSettlement, bad, ""}, {other, FactExecution, e, ""}}); err == nil {
		t.Fatal("conflicting original settlement accepted")
	}
	got, _ := os.GetLifecycle(other)
	if got.Execution != nil || got.Settlement != nil {
		t.Fatalf("failed batch partially committed: %+v", got)
	}
	if _, err := os.GetLifecycle(OperationIdentity{BridgeType: id.BridgeType, Direction: id.Direction, Nonce: id.Nonce, SourceToken: "A"}); err == nil {
		t.Fatal("ambiguous token identity accepted")
	}
	malformed := id
	malformed.SourceToken = "A:B"
	if _, err := os.GetLifecycle(malformed); err == nil {
		t.Fatal("token identity with key separator accepted")
	}
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactExecution, lifecycleEvidence("neo_x", "wrong-chain", 21), ""}}); err == nil {
		t.Fatal("wrong destination chain accepted")
	}
}

func TestLifecycleMessageTypeAndUnknownHistory(t *testing.T) {
	os := NewOperationStore(newTestStore(t), nil)
	id := OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionWithdrawal, Nonce: 15}
	if _, err := os.GetLifecycle(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing record must be unknown: %v", err)
	}
	source := lifecycleEvidence("neo_x", "send", 10)
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactMessageType, lifecycleEvidence("neo_n3", "wrong-source", 10), "execute"}}); err == nil {
		t.Fatal("destination evidence accepted as message source type")
	}
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactMessageType, source, "execute"}}); err != nil {
		t.Fatal(err)
	}
	arrival := lifecycleEvidence("neo_n3", "relay", 12)
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactSettlement, arrival, ""}}); err != nil {
		t.Fatal(err)
	}
	got, err := os.GetLifecycle(id)
	if err != nil || got.Execution != nil || got.MessageType == nil || *got.MessageType != "execute" {
		t.Fatalf("arrival guessed execution: %+v, %v", got, err)
	}
	execution := lifecycleEvidence("neo_n3", "execute", 13)
	execution.Kind, execution.Payload = "notification", json.RawMessage(`{"success":false,"result":[1,true,null]}`)
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactExecution, execution, ""}}); err != nil {
		t.Fatal(err)
	}
	got, _ = os.GetLifecycle(id)
	if !reflect.DeepEqual(got.Execution.Payload, execution.Payload) || got.Settlement.TxHash != "relay" {
		t.Fatalf("execution result or settlement changed: %+v", got)
	}
	if err := os.ApplyLifecycleObservations([]LifecycleObservation{{id, FactMessageType, source, "store"}}); err == nil {
		t.Fatal("message type changed")
	}
}

func TestLifecycleConcurrentIndependentFacts(t *testing.T) {
	os := NewOperationStore(newTestStore(t), nil)
	id := OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 1}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, fact := range []LifecycleFact{FactSettlement, FactExecution} {
		wg.Add(1)
		go func(f LifecycleFact) {
			defer wg.Done()
			errs <- os.ApplyLifecycleObservations([]LifecycleObservation{{id, f, lifecycleEvidence("neo_x", string(f), 1), ""}})
		}(fact)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.GetLifecycle(id)
	if err != nil || got.Settlement == nil || got.Execution == nil {
		t.Fatalf("concurrent facts lost: %+v, %v", got, err)
	}
}
