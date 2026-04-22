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

func newTestNeoXProcessor(t *testing.T) (*NeoXEventProcessorImpl, *datastore.OperationStore) {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	opStore := datastore.NewOperationStore(store, nil)
	proc := NewNeoXEventProcessor(opStore, testTokenBridgeAddr, testMessageBridgeAddr, slog.Default())
	return proc, opStore
}

func padUint256(v uint64) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64(v))
}

func padAddress(addr common.Address) common.Hash {
	return common.BytesToHash(addr.Bytes())
}

// buildMessageSendLogData builds ABI-encoded non-indexed data for MessageSend
// (bytes encodedMetadata, bytes message, bytes32 messageHash, bytes32 root).
func buildMessageSendLogData(encodedMetadata []byte, message []byte, msgHash, root common.Hash) []byte {
	metaLen := len(encodedMetadata)
	msgLen := len(message)
	off0 := 128
	paddedMeta := ((metaLen + 31) / 32) * 32
	off1 := 128 + 32 + paddedMeta
	head := make([]byte, 128)
	copy(head[0:32], common.LeftPadBytes(new(big.Int).SetInt64(int64(off0)).Bytes(), 32))
	copy(head[32:64], common.LeftPadBytes(new(big.Int).SetInt64(int64(off1)).Bytes(), 32))
	copy(head[64:96], msgHash.Bytes())
	copy(head[96:128], root.Bytes())
	var out []byte
	out = append(out, head...)
	out = append(out, common.LeftPadBytes(new(big.Int).SetInt64(int64(metaLen)).Bytes(), 32)...)
	out = append(out, encodedMetadata...)
	out = append(out, make([]byte, (32-metaLen%32)%32)...)
	out = append(out, common.LeftPadBytes(new(big.Int).SetInt64(int64(msgLen)).Bytes(), 32)...)
	out = append(out, message...)
	out = append(out, make([]byte, (32-msgLen%32)%32)...)
	return out
}

func TestNeoX_NativeWithdrawal(t *testing.T) {
	proc, _ := newTestNeoXProcessor(t)

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
	if op.Status != types.StatusRelayed {
		t.Fatalf("expected relayed, got %s", op.Status)
	}
}

func TestNeoX_TokenWithdrawal(t *testing.T) {
	proc, _ := newTestNeoXProcessor(t)

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
	proc, _ := newTestNeoXProcessor(t)

	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")

	meta := common.LeftPadBytes(new(big.Int).SetUint64(0).Bytes(), 32) // AMBTypes.MessageType.EXECUTABLE
	data := buildMessageSendLogData(meta, nil, common.Hash{}, common.Hash{})

	log := ethtypes.Log{
		Address: common.HexToAddress(testMessageBridgeAddr),
		Topics: []common.Hash{
			sigMessageSend,
			padUint256(33),
			padAddress(sender),
		},
		Data:        data,
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
	if op.AMBMessageType == nil || *op.AMBMessageType != types.AMBMessageTypeExecutable {
		t.Fatalf("expected executable amb type, got %v", op.AMBMessageType)
	}
	if op.Status != types.StatusRelayed {
		t.Fatalf("expected relayed executable, got %s", op.Status)
	}
}

func TestNeoX_MessageSend_storeOnlyCompleted(t *testing.T) {
	proc, _ := newTestNeoXProcessor(t)
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	meta := common.LeftPadBytes(new(big.Int).SetUint64(1).Bytes(), 32) // STORE_ONLY
	data := buildMessageSendLogData(meta, nil, common.Hash{}, common.Hash{})
	bt := time.Now()
	log := ethtypes.Log{
		Address: common.HexToAddress(testMessageBridgeAddr),
		Topics:  []common.Hash{sigMessageSend, padUint256(7), padAddress(sender)},
		Data:    data, BlockNumber: 4000, TxHash: common.HexToHash("0xbeef"),
	}
	op, _, err := proc.ProcessLog(log, bt)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if op == nil || op.Status != types.StatusCompleted {
		t.Fatalf("expected completed store_only, got %+v", op)
	}
	if op.AMBMessageType == nil || *op.AMBMessageType != types.AMBMessageTypeStoreOnly {
		t.Fatalf("amb type: %v", op.AMBMessageType)
	}
	if op.CompletionTimestamp == nil || !op.CompletionTimestamp.Equal(bt) {
		t.Fatal("expected completion timestamp set at source")
	}
}

func TestNeoX_NativeDepositComplete(t *testing.T) {
	proc, opStore := newTestNeoXProcessor(t)

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
		Status:            types.StatusRelayed,
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

func TestNeoX_NeoToEvmRootUpdateCompletesMessageDeposit(t *testing.T) {
	proc, opStore := newTestNeoXProcessor(t)

	pendingOp := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionDeposit,
		Nonce:             12,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: 100,
		SourceTxHash:      "0xsrc",
		FromAddress:       "NFrom",
		ToAddress:         "",
		Timestamp:         time.Now().Add(-2 * time.Minute),
		Status:            types.StatusRelayed,
	}
	if err := opStore.SaveOperation(pendingOp); err != nil {
		t.Fatalf("save: %v", err)
	}

	root := common.HexToHash("0x2462a6e5b6fee1af70d96358e36eb868c5e181fa9ccd02440c179215b9feb55c")
	log := ethtypes.Log{
		Address: common.HexToAddress(testMessageBridgeAddr),
		Topics: []common.Hash{
			sigNeoToEvmRootUpdate,
			padUint256(12),
		},
		Data:        root.Bytes(),
		BlockNumber: 5001,
		TxHash:      common.HexToHash("0xfeed"),
	}

	_, _, err := proc.ProcessLog(log, time.Now())
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	got, err := opStore.GetOperation(types.BridgeTypeMessage, types.DirectionDeposit, 12)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != types.StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
}

func TestNeoX_PauseEvents(t *testing.T) {
	proc, _ := newTestNeoXProcessor(t)

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
	proc, _ := newTestNeoXProcessor(t)

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
