package indexer

import (
	"context"
	"log/slog"
	"math/big"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// newTestBackfillStore creates an in-memory BadgerDB-backed OperationStore for
// backfill tests, following the same pattern as neox_event_processor_test.go.
func newTestBackfillStore(t *testing.T) *datastore.OperationStore {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	return datastore.NewOperationStore(store, nil)
}

// ---------------------------------------------------------------------------
// TestBackfill_NoStuckOperations
// ---------------------------------------------------------------------------

func TestBackfill_NoStuckOperations(t *testing.T) {
	opStore := newTestBackfillStore(t)

	// Insert a completed operation — should NOT be picked up.
	completedOp := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             1,
		SourceChain:       "neo_n3",
		DestinationChain:  "neo_x",
		SourceBlockHeight: 100,
		SourceTxHash:      "0xabc",
		FromAddress:       "NFrom",
		ToAddress:         "0xTo",
		Timestamp:         time.Now().Add(-10 * time.Minute),
		Status:            types.StatusCompleted,
	}
	if err := opStore.SaveOperation(completedOp); err != nil {
		t.Fatalf("save completed op: %v", err)
	}

	bf := NewBackfiller(opStore, nil, nil, nil, nil, slog.Default())
	resolved, err := bf.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if resolved != 0 {
		t.Fatalf("expected 0 resolved, got %d", resolved)
	}
}

// ---------------------------------------------------------------------------
// TestExtractNeoXNonce — Native events (nonce in Topics[1])
// ---------------------------------------------------------------------------

func TestExtractNeoXNonce_NativeEvents(t *testing.T) {
	tests := []struct {
		name  string
		sig   common.Hash
		nonce uint64
	}{
		{"NativeClaim", sigNativeClaim, 1},
		{"NativeClaimable", sigNativeClaimable, 42},
		{"NativeDeposit", sigNativeDeposit, 999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nonceTopic := common.BigToHash(new(big.Int).SetUint64(tt.nonce))
			log := ethtypes.Log{
				Topics: []common.Hash{tt.sig, nonceTopic},
			}
			got, ok := extractNeoXNonce(log)
			if !ok {
				t.Fatal("expected ok=true")
			}
			if got != tt.nonce {
				t.Fatalf("expected nonce %d, got %d", tt.nonce, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestExtractNeoXNonce — Token events (nonce in Topics[2])
// ---------------------------------------------------------------------------

func TestExtractNeoXNonce_TokenEvents(t *testing.T) {
	tests := []struct {
		name  string
		sig   common.Hash
		nonce uint64
	}{
		{"TokenClaim", sigTokenClaim, 7},
		{"TokenClaimable", sigTokenClaimable, 100},
		{"TokenDeposit", sigTokenDeposit, 55},
	}

	dummyTopic := common.BigToHash(big.NewInt(0)) // placeholder for Topics[1]

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nonceTopic := common.BigToHash(new(big.Int).SetUint64(tt.nonce))
			log := ethtypes.Log{
				Topics: []common.Hash{tt.sig, dummyTopic, nonceTopic},
			}
			got, ok := extractNeoXNonce(log)
			if !ok {
				t.Fatal("expected ok=true")
			}
			if got != tt.nonce {
				t.Fatalf("expected nonce %d, got %d", tt.nonce, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestExtractNeoXNonce — Execution event (nonce in Topics[1])
// ---------------------------------------------------------------------------

func TestExtractNeoXNonce_Execution(t *testing.T) {
	nonce := uint64(123)
	nonceTopic := common.BigToHash(new(big.Int).SetUint64(nonce))
	log := ethtypes.Log{
		Topics: []common.Hash{sigExecution, nonceTopic},
	}
	got, ok := extractNeoXNonce(log)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != nonce {
		t.Fatalf("expected nonce %d, got %d", nonce, got)
	}
}

// ---------------------------------------------------------------------------
// TestExtractNeoXNonce — Unknown topic
// ---------------------------------------------------------------------------

func TestExtractNeoXNonce_Unknown(t *testing.T) {
	unknownSig := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	log := ethtypes.Log{
		Topics: []common.Hash{unknownSig, common.BigToHash(big.NewInt(1))},
	}
	_, ok := extractNeoXNonce(log)
	if ok {
		t.Fatal("expected ok=false for unknown topic")
	}
}

// ---------------------------------------------------------------------------
// TestExtractN3WithdrawalKey — NativeWithdrawal
// ---------------------------------------------------------------------------

func TestExtractN3WithdrawalKey_NativeWithdrawal(t *testing.T) {
	nonce := int64(42)
	event := state.NotificationEvent{
		Name: "NativeWithdrawal",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(nonce), // Nonce at index 0
		}),
	}

	key, ok := extractN3WithdrawalKey(event)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if key.bridgeType != types.BridgeTypeNative {
		t.Fatalf("expected bridge type %s, got %s", types.BridgeTypeNative, key.bridgeType)
	}
	if key.nonce != uint64(nonce) {
		t.Fatalf("expected nonce %d, got %d", nonce, key.nonce)
	}
}

// ---------------------------------------------------------------------------
// TestExtractN3WithdrawalKey — TokenWithdrawal
// ---------------------------------------------------------------------------

func TestExtractN3WithdrawalKey_TokenWithdrawal(t *testing.T) {
	nonce := int64(99)
	tokenAddr := make([]byte, 20)
	tokenAddr[0] = 0xCC

	event := state.NotificationEvent{
		Name: "TokenWithdrawal",
		Item: stackitem.NewArray([]stackitem.Item{
			stackitem.NewByteArray(tokenAddr), // NeoN3Token at index 0
			intItem(nonce),                    // Nonce at index 1
		}),
	}

	key, ok := extractN3WithdrawalKey(event)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if key.bridgeType != types.BridgeTypeToken {
		t.Fatalf("expected bridge type %s, got %s", types.BridgeTypeToken, key.bridgeType)
	}
	if key.nonce != uint64(nonce) {
		t.Fatalf("expected nonce %d, got %d", nonce, key.nonce)
	}
}

// ---------------------------------------------------------------------------
// TestExtractN3WithdrawalKey — Execution
// ---------------------------------------------------------------------------

func TestExtractN3WithdrawalKey_Execution(t *testing.T) {
	nonce := int64(15)
	event := state.NotificationEvent{
		Name: "Execution",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(nonce), // Nonce at index 0
		}),
	}

	key, ok := extractN3WithdrawalKey(event)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if key.bridgeType != types.BridgeTypeMessage {
		t.Fatalf("expected bridge type %s, got %s", types.BridgeTypeMessage, key.bridgeType)
	}
	if key.nonce != uint64(nonce) {
		t.Fatalf("expected nonce %d, got %d", nonce, key.nonce)
	}
}

// ---------------------------------------------------------------------------
// TestExtractN3WithdrawalKey — Unknown event name
// ---------------------------------------------------------------------------

func TestExtractN3WithdrawalKey_Unknown(t *testing.T) {
	event := state.NotificationEvent{
		Name: "SomeUnknownEvent",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(1),
		}),
	}

	_, ok := extractN3WithdrawalKey(event)
	if ok {
		t.Fatal("expected ok=false for unknown event name")
	}
}
