package indexer

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

func TestNeoXIndirectClaimTrace(t *testing.T) {
	bridge := common.HexToAddress(testTokenBridgeAddr)
	token := common.HexToAddress("0xc28736dc83f4fd43d6fb832fd93c3ee7bb26828f")
	caller := common.HexToAddress("0x1234")
	input := append(append([]byte(nil), claimTokenSelector...), common.LeftPadBytes(token.Bytes(), 32)...)
	input = append(input, common.LeftPadBytes(big.NewInt(108).Bytes(), 32)...)
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{To: &caller, Data: []byte{0xa1}})
	receipt := &ethtypes.Receipt{Status: ethtypes.ReceiptStatusSuccessful, BlockNumber: big.NewInt(12), BlockHash: common.HexToHash("0x12"), TxHash: tx.Hash()}
	id := &datastore.OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 108, SourceToken: "0xef4073a0f2b305a38ec4050e4d3d28bc40ea63f5", DestinationToken: token.Hex()}
	claimable := &datastore.LifecycleEvidence{Payload: json.RawMessage(`{"recipient":"0xce0c7fbe9c64bc4b1bdc1436f2fe8e795dd01e90","amount":"123"}`)}
	frame := &neox.CallFrame{Type: "CALL", To: caller, Calls: []neox.CallFrame{{Type: "CALL", To: common.HexToAddress("0x4567"), Calls: []neox.CallFrame{{Type: "CALL", To: bridge, Input: hexutil.Bytes(input)}}}}}
	calls := claimTraceCalls(frame, bridge)
	if len(calls) != 1 || calls[0].Position != "top-level/0/0" {
		t.Fatalf("missed indirect call: %+v", calls)
	}
	impl := common.HexToAddress("0x671bcb1b24e75a37adb283ffa509620c02eb35c6")
	obs, err := decodeTokenClaimCall(tx, receipt, bridge, impl, time.Unix(123, 0), id, claimable, calls[0].Input, calls[0].Position)
	if err != nil || obs == nil || obs.Fact != datastore.FactClaim || obs.Evidence.Position != "top-level/0/0" || obs.Evidence.TxHash != tx.Hash().Hex() {
		t.Fatalf("claim evidence: %+v %v", obs, err)
	}
	frame.Calls[0].Error = "execution reverted"
	if got := claimTraceCalls(frame, bridge); len(got) != 0 {
		t.Fatalf("failed ancestor proved claim: %+v", got)
	}
	frame.Calls[0].Error = ""
	frame.Calls[0].Calls[0].Error = "execution reverted"
	if got := claimTraceCalls(frame, bridge); len(got) != 0 {
		t.Fatalf("failed bridge call proved claim: %+v", got)
	}
	frame.Calls[0].Calls[0].Error = ""
	frame.Error = "execution reverted"
	if got := claimTraceCalls(frame, bridge); len(got) != 0 {
		t.Fatalf("failed outer call proved claim: %+v", got)
	}
	frame.Error = ""
	receipt.Status = ethtypes.ReceiptStatusFailed
	if got, _ := decodeTokenClaimCall(tx, receipt, bridge, impl, time.Now(), id, claimable, input, "top-level/0/0"); got != nil {
		t.Fatal("failed receipt proved claim")
	}
}

func TestNeoXTraceJSON(t *testing.T) {
	raw := []byte(`[{"txHash":"0x00000000000000000000000000000000000000000000000000000000000000ab","result":{"type":"CALL","to":"0x0000000000000000000000000000000000001234","input":"0x12345678","calls":[{"type":"CALL","to":"0x1212000000000000000000000000000000000004","input":"0x1698755f","error":"execution reverted"}]}}]`)
	var traces []neox.TransactionTrace
	if err := json.Unmarshal(raw, &traces); err != nil || len(traces) != 1 || len(traces[0].Result.Calls) != 1 || traces[0].Result.Calls[0].Error == "" {
		t.Fatalf("bad callTracer schema: %+v %v", traces, err)
	}
}

func TestNeoXIncompleteTracesBlockCheckpoint(t *testing.T) {
	dest := common.HexToAddress("0x1234")
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{To: &dest, Data: []byte{1}})
	block := ethtypes.NewBlockWithHeader(&ethtypes.Header{Number: big.NewInt(12)}).WithBody(ethtypes.Body{Transactions: []*ethtypes.Transaction{tx}})
	if err := verifyBlockClaimTraces(block, nil); err == nil {
		t.Fatal("missing trace accepted")
	}
	if err := verifyBlockClaimTraces(block, []neox.TransactionTrace{{TxHash: common.HexToHash("0xdead"), Result: &neox.CallFrame{}}}); err == nil {
		t.Fatal("trace for wrong transaction accepted")
	}
	if err := verifyBlockClaimTraces(block, []neox.TransactionTrace{{TxHash: tx.Hash()}}); err == nil {
		t.Fatal("missing call tree accepted")
	}
	if err := verifyBlockClaimTraces(block, []neox.TransactionTrace{{TxHash: tx.Hash(), Result: &neox.CallFrame{To: common.HexToAddress("0x4444"), Input: hexutil.Bytes(tx.Data())}}}); err == nil {
		t.Fatal("trace from different transaction accepted")
	}
	if err := verifyBlockClaimTraces(block, []neox.TransactionTrace{{TxHash: tx.Hash(), Result: &neox.CallFrame{To: dest, Input: hexutil.Bytes(tx.Data())}}}); err != nil {
		t.Fatal(err)
	}
}
