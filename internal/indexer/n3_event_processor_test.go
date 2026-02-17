package indexer

import (
	"log/slog"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

const (
	testNeoXBridgeHash    = "bb19cfc864b73159277e1fd39694b3fd5fc613d2"
	testMessageBridgeHash = "de9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11"
)

func newTestOpStore(t *testing.T) *datastore.OperationStore {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	return datastore.NewOperationStore(store)
}

func newTestProcessor(t *testing.T) (*N3EventProcessorImpl, *datastore.OperationStore) {
	t.Helper()
	opStore := newTestOpStore(t)
	proc, err := NewN3EventProcessor(opStore, testNeoXBridgeHash, testMessageBridgeHash, slog.Default())
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	return proc, opStore
}

func TestN3_DepositNative(t *testing.T) {
	proc, _ := newTestProcessor(t)

	neoXBridge, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	toAddr := make([]byte, 20)
	toAddr[0] = 0x01
	fromAddr := make([]byte, 20)
	fromAddr[0] = 0x02

	event := state.NotificationEvent{
		ScriptHash: neoXBridge,
		Name:       "Deposit",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(42),                              // nonce
			stackitem.NewByteArray(toAddr),           // to
			intItem(1000000000),                      // amount
			stackitem.NewByteArray(fromAddr),         // from
			stackitem.NewByteArray(make([]byte, 32)), // depositHash
			stackitem.NewByteArray(make([]byte, 32)), // newRoot
		}),
	}

	op, err := proc.ProcessEvent(event, 100, "0xtx1", time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if op == nil {
		t.Fatal("expected operation, got nil")
	}
	if op.BridgeType != types.BridgeTypeNative {
		t.Fatalf("expected native, got %s", op.BridgeType)
	}
	if op.Direction != types.DirectionDeposit {
		t.Fatalf("expected deposit, got %s", op.Direction)
	}
	if op.Nonce != 42 {
		t.Fatalf("expected nonce 42, got %d", op.Nonce)
	}
	if op.Status != types.StatusPending {
		t.Fatalf("expected pending, got %s", op.Status)
	}
	if op.Amount == nil || *op.Amount != "1000000000" {
		t.Fatalf("expected amount 1000000000, got %v", op.Amount)
	}
}

func TestN3_DepositToken(t *testing.T) {
	proc, _ := newTestProcessor(t)

	neoXBridge, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	toAddr := make([]byte, 20)
	fromAddr := make([]byte, 20)
	tokenHash := make([]byte, 20)
	tokenHash[0] = 0xAA

	event := state.NotificationEvent{
		ScriptHash: neoXBridge,
		Name:       "Deposit",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(99),
			stackitem.NewByteArray(toAddr),
			intItem(500),
			stackitem.NewByteArray(fromAddr),
			stackitem.NewByteArray(make([]byte, 32)),
			stackitem.NewByteArray(make([]byte, 32)),
			stackitem.NewByteArray(tokenHash),
		}),
	}

	op, err := proc.ProcessEvent(event, 200, "0xtx2", time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if op == nil {
		t.Fatal("expected operation")
	}
	if op.BridgeType != types.BridgeTypeToken {
		t.Fatalf("expected token, got %s", op.BridgeType)
	}
	if op.TokenContract == nil {
		t.Fatal("expected token contract")
	}
}

func TestN3_MessageSend(t *testing.T) {
	proc, _ := newTestProcessor(t)

	msgBridge, _ := util.Uint160DecodeStringLE(testMessageBridgeHash)
	sender := make([]byte, 20)
	sender[0] = 0x05

	event := state.NotificationEvent{
		ScriptHash: msgBridge,
		Name:       "MessageSend",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(7),
			stackitem.NewByteArray(sender),
		}),
	}

	op, err := proc.ProcessEvent(event, 300, "0xtx3", time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if op == nil {
		t.Fatal("expected operation")
	}
	if op.BridgeType != types.BridgeTypeMessage {
		t.Fatalf("expected message, got %s", op.BridgeType)
	}
	if op.Nonce != 7 {
		t.Fatalf("expected nonce 7, got %d", op.Nonce)
	}
}

func TestN3_WithdrawalComplete(t *testing.T) {
	proc, opStore := newTestProcessor(t)

	pendingOp := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionWithdrawal,
		Nonce:             10,
		SourceChain:       chainNeoX,
		DestinationChain:  chainN3,
		SourceBlockHeight: 500,
		SourceTxHash:      "0xsource",
		FromAddress:       "0xfrom",
		ToAddress:         "NTo",
		Timestamp:         time.Now().Add(-5 * time.Minute),
		Status:            types.StatusPending,
	}
	if err := opStore.SaveOperation(pendingOp); err != nil {
		t.Fatalf("save pending: %v", err)
	}

	neoXBridge, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	event := state.NotificationEvent{
		ScriptHash: neoXBridge,
		Name:       "Withdrawal",
		Item: stackitem.NewArray([]stackitem.Item{
			intItem(10),
		}),
	}

	_, err := proc.ProcessEvent(event, 600, "0xtxComplete", time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	got, err := opStore.GetOperation(types.BridgeTypeNative, types.DirectionWithdrawal, 10)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
	if got.DestTxHash == nil || *got.DestTxHash != "0xtxComplete" {
		t.Fatalf("expected dest tx hash 0xtxComplete")
	}
}

func TestN3_UnknownEventIgnored(t *testing.T) {
	proc, _ := newTestProcessor(t)

	neoXBridge, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	event := state.NotificationEvent{
		ScriptHash: neoXBridge,
		Name:       "SomeOtherEvent",
		Item:       stackitem.NewArray([]stackitem.Item{}),
	}

	op, err := proc.ProcessEvent(event, 100, "0xtx", time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if op != nil {
		t.Fatal("expected nil for unknown event")
	}
}
