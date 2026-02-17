package indexer

import (
	"log/slog"
	"math/big"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

const (
	testTokenBridgeAddr   = "0x1212000000000000000000000000000000000004"
	testMessageBridgeAddr = "0x1212000000000000000000000000000000000009"
)

func newTestNeoXProcessor(t *testing.T) (*NeoXEventProcessorImpl, *datastore.OperationStore, *datastore.Store) {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	opStore := datastore.NewOperationStore(store)
	proc := NewNeoXEventProcessor(opStore, store, testTokenBridgeAddr, testMessageBridgeAddr, slog.Default())
	return proc, opStore, store
}

func padUint256(v uint64) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64(v))
}

func padAddress(addr common.Address) common.Hash {
	return common.BytesToHash(addr.Bytes())
}

func TestNeoX_NativeWithdrawal(t *testing.T) {
	proc, _, _ := newTestNeoXProcessor(t)

	to := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	from := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	amount := new(big.Int).SetUint64(5000000000)

	data := make([]byte, 128)
	copy(data[0:32], common.LeftPadBytes(amount.Bytes(), 32))
	copy(data[32:64], common.LeftPadBytes(from.Bytes(), 32))

	log := ethtypes.Log{
		Address: common.HexToAddress(testTokenBridgeAddr),
		Topics: []common.Hash{
			sigNativeWithdrawal,
			padUint256(77),
			padAddress(to),
		},
		Data:        data,
		BlockNumber: 1000,
		TxHash:      common.HexToHash("0xdeadbeef"),
	}

	op, pause, err := proc.ProcessLog(log, time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if pause != nil {
		t.Fatal("unexpected pause state")
	}
	if op == nil {
		t.Fatal("expected operation")
	}
	if op.BridgeType != types.BridgeTypeNative {
		t.Fatalf("expected native, got %s", op.BridgeType)
	}
	if op.Direction != types.DirectionWithdrawal {
		t.Fatalf("expected withdrawal, got %s", op.Direction)
	}
	if op.Nonce != 77 {
		t.Fatalf("expected nonce 77, got %d", op.Nonce)
	}
	if op.Status != types.StatusPending {
		t.Fatalf("expected pending, got %s", op.Status)
	}
}

func TestNeoX_TokenWithdrawal(t *testing.T) {
	proc, _, _ := newTestNeoXProcessor(t)

	tokenAddr := common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")
	to := common.HexToAddress("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
	from := common.HexToAddress("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
	amount := new(big.Int).SetUint64(999)

	data := make([]byte, 128)
	// neoN3Token(32) + amount(32) + from(32) + ...
	copy(data[32:64], common.LeftPadBytes(amount.Bytes(), 32))
	copy(data[64:96], common.LeftPadBytes(from.Bytes(), 32))

	log := ethtypes.Log{
		Address: common.HexToAddress(testTokenBridgeAddr),
		Topics: []common.Hash{
			sigTokenWithdrawal,
			padAddress(tokenAddr),
			padUint256(55),
			padAddress(to),
		},
		Data:        data,
		BlockNumber: 2000,
		TxHash:      common.HexToHash("0xcafe"),
	}

	op, _, err := proc.ProcessLog(log, time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if op == nil {
		t.Fatal("expected operation")
	}
	if op.BridgeType != types.BridgeTypeToken {
		t.Fatalf("expected token, got %s", op.BridgeType)
	}
	if op.Nonce != 55 {
		t.Fatalf("expected nonce 55, got %d", op.Nonce)
	}
	if op.TokenContract == nil {
		t.Fatal("expected token contract")
	}
}

func TestNeoX_MessageSend(t *testing.T) {
	proc, _, _ := newTestNeoXProcessor(t)

	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")

	log := ethtypes.Log{
		Address: common.HexToAddress(testMessageBridgeAddr),
		Topics: []common.Hash{
			sigMessageSend,
			padUint256(33),
			padAddress(sender),
		},
		Data:        make([]byte, 192),
		BlockNumber: 3000,
		TxHash:      common.HexToHash("0xbabe"),
	}

	op, _, err := proc.ProcessLog(log, time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if op == nil {
		t.Fatal("expected operation")
	}
	if op.BridgeType != types.BridgeTypeMessage {
		t.Fatalf("expected message, got %s", op.BridgeType)
	}
	if op.Nonce != 33 {
		t.Fatalf("expected nonce 33, got %d", op.Nonce)
	}
}

func TestNeoX_NativeDepositComplete(t *testing.T) {
	proc, opStore, _ := newTestNeoXProcessor(t)

	pendingOp := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             20,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: 100,
		SourceTxHash:      "0xsrc",
		FromAddress:       "NFrom",
		ToAddress:         "0xTo",
		Timestamp:         time.Now().Add(-3 * time.Minute),
		Status:            types.StatusPending,
	}
	if err := opStore.SaveOperation(pendingOp); err != nil {
		t.Fatalf("save: %v", err)
	}

	to := common.HexToAddress("0xTo")
	log := ethtypes.Log{
		Address: common.HexToAddress(testTokenBridgeAddr),
		Topics: []common.Hash{
			sigNativeDeposit,
			padUint256(20),
			padAddress(to),
		},
		Data:        common.LeftPadBytes(new(big.Int).SetUint64(1000).Bytes(), 32),
		BlockNumber: 5000,
		TxHash:      common.HexToHash("0xdone"),
	}

	_, _, err := proc.ProcessLog(log, time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	got, err := opStore.GetOperation(types.BridgeTypeNative, types.DirectionDeposit, 20)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
}

func TestNeoX_PauseEvents(t *testing.T) {
	proc, _, _ := newTestNeoXProcessor(t)

	tests := []struct {
		name   string
		addr   string
		topic  common.Hash
		paused bool
		scope  string
	}{
		{"BridgePause", testTokenBridgeAddr, sigBridgePause, true, "all"},
		{"BridgeUnpause", testTokenBridgeAddr, sigBridgeUnpause, false, "all"},
		{"WithdrawalPause", testTokenBridgeAddr, sigWithdrawalPause, true, "withdrawals"},
		{"MsgPause", testMessageBridgeAddr, sigPause, true, "all"},
		{"MsgUnpause", testMessageBridgeAddr, sigUnpause, false, "all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := ethtypes.Log{
				Address: common.HexToAddress(tt.addr),
				Topics:  []common.Hash{tt.topic},
			}
			_, pause, err := proc.ProcessLog(log, time.Now())
			if err != nil {
				t.Fatalf("process: %v", err)
			}
			if pause == nil {
				t.Fatal("expected pause state")
			}
			if pause.Paused != tt.paused {
				t.Fatalf("expected paused=%v, got %v", tt.paused, pause.Paused)
			}
			if pause.Scope != tt.scope {
				t.Fatalf("expected scope=%s, got %s", tt.scope, pause.Scope)
			}
		})
	}
}

func TestNeoX_RemovedLogIgnored(t *testing.T) {
	proc, _, _ := newTestNeoXProcessor(t)

	log := ethtypes.Log{
		Address: common.HexToAddress(testTokenBridgeAddr),
		Topics: []common.Hash{
			sigNativeWithdrawal,
			padUint256(1),
			padAddress(common.Address{}),
		},
		Data:        make([]byte, 128),
		BlockNumber: 100,
		Removed:     true,
	}

	// Removed logs should be handled by the indexer (not the processor).
	// The processor itself doesn't check Removed — the indexer skips them.
	// This test verifies the processor doesn't crash on removed logs.
	_, _, err := proc.ProcessLog(log, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
