package indexer

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

func TestNeoXSilentClaimCapturedFixture(t *testing.T) {
	raw, err := os.ReadFile("../../docs/fixtures/neox-token-claim-without-event.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Expected struct {
			SourceToken      string `json:"source_token"`
			DestinationToken string `json:"destination_token"`
			Nonce            uint64 `json:"nonce"`
			SettlementTx     string `json:"settlement_tx_hash"`
			ClaimTx          string `json:"execution_tx_hash"`
			Implementation   string `json:"implementation"`
		} `json:"expected"`
		Evidence struct {
			ClaimTransaction struct {
				Result json.RawMessage `json:"result"`
			} `json:"claim_transaction"`
			ClaimReceipt struct {
				Result json.RawMessage `json:"result"`
			} `json:"claim_receipt"`
			SettlementReceipt struct {
				Result json.RawMessage `json:"result"`
			} `json:"settlement_receipt"`
		} `json:"rpc_evidence"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	var receipt, settlement ethtypes.Receipt
	if err := json.Unmarshal(fixture.Evidence.ClaimReceipt.Result, &receipt); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixture.Evidence.SettlementReceipt.Result, &settlement); err != nil {
		t.Fatal(err)
	}
	var tx ethtypes.Transaction
	if err := json.Unmarshal(fixture.Evidence.ClaimTransaction.Result, &tx); err != nil {
		t.Fatal(err)
	}
	if tx.Hash() != common.HexToHash(fixture.Expected.ClaimTx) {
		t.Fatalf("captured transaction hash mismatch: %s", tx.Hash())
	}
	if len(receipt.Logs) != 1 || receipt.Logs[0].Address == common.HexToAddress(testTokenBridgeAddr) {
		t.Fatal("claim fixture must have token Transfer only")
	}
	logs := make([]ethtypes.Log, len(settlement.Logs))
	for i, l := range settlement.Logs {
		logs[i] = *l
	}
	obs, err := decodeNeoXLifecycleTransaction(logs, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(testMessageBridgeAddr), time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	id := datastore.OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: fixture.Expected.Nonce, SourceToken: fixture.Expected.SourceToken, DestinationToken: fixture.Expected.DestinationToken}
	_, store := newTestNeoXProcessor(t)
	if err := store.ApplyLifecycleObservations(obs); err != nil {
		t.Fatal(err)
	}
	pair, err := store.FindClaimableTokenPair(id.Nonce, id.DestinationToken)
	if err != nil || pair == nil {
		t.Fatalf("lookup: %v %v", pair, err)
	}
	prior, err := store.GetLifecycle(id)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := decodeDirectTokenClaim(&tx, &receipt, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(fixture.Expected.Implementation), time.Unix(200, 0), pair, prior.Claimable)
	if err != nil || claim == nil {
		t.Fatalf("silent claim: %v %v", claim, err)
	}
	if err := store.ApplyLifecycleObservations([]datastore.LifecycleObservation{*claim}); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyLifecycleObservations([]datastore.LifecycleObservation{*claim}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, err := store.GetLifecycle(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Settlement == nil || got.Settlement.TxHash != fixture.Expected.SettlementTx || got.Claim == nil || got.Claim.TxHash != fixture.Expected.ClaimTx || got.Distribution != nil {
		t.Fatalf("incorrect lifecycle: %+v", got)
	}
	if got.Claim.Implementation != common.HexToAddress(fixture.Expected.Implementation).Hex() || got.Claim.BlockHash != receipt.BlockHash.Hex() || got.Settlement.BlockHash != settlement.BlockHash.Hex() {
		t.Fatalf("provenance: %+v", got)
	}
	var corroboration struct {
		Transfer struct {
			LogIndex  uint   `json:"log_index"`
			Recipient string `json:"recipient"`
		} `json:"transfer"`
	}
	if err := json.Unmarshal(got.Claim.Payload, &corroboration); err != nil || corroboration.Transfer.Recipient == "" {
		t.Fatalf("missing matching Transfer provenance: %+v %v", corroboration, err)
	}
	if c, _ := decodeDirectTokenClaim(&tx, &receipt, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(fixture.Expected.Implementation), time.Now(), nil, nil); c != nil {
		t.Fatal("Transfer alone identified claim")
	}
	unrelated := receipt
	transfer := *receipt.Logs[0]
	transfer.Address = common.HexToAddress("0xdead")
	unrelated.Logs = []*ethtypes.Log{&transfer}
	withoutCorroboration, err := decodeDirectTokenClaim(&tx, &unrelated, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(fixture.Expected.Implementation), time.Now(), pair, prior.Claimable)
	if err != nil || withoutCorroboration == nil {
		t.Fatalf("valid call depends on unrelated Transfer: %v", err)
	}
	var details map[string]any
	if err := json.Unmarshal(withoutCorroboration.Evidence.Payload, &details); err != nil || details["transfer"] != nil {
		t.Fatalf("unrelated Transfer corroborated claim: %+v %v", details, err)
	}
	bad := receipt
	bad.Status = ethtypes.ReceiptStatusFailed
	if c, _ := decodeDirectTokenClaim(&tx, &bad, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(fixture.Expected.Implementation), time.Now(), pair, prior.Claimable); c != nil {
		t.Fatal("reverted claim observed")
	}
	mismatch := *pair
	mismatch.Nonce++
	if c, _ := decodeDirectTokenClaim(&tx, &receipt, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(fixture.Expected.Implementation), time.Now(), &mismatch, prior.Claimable); c != nil {
		t.Fatal("mismatched nonce observed")
	}
	mismatch = *pair
	mismatch.DestinationToken = "0x000000000000000000000000000000000000beef"
	if c, _ := decodeDirectTokenClaim(&tx, &receipt, common.HexToAddress(testTokenBridgeAddr), common.HexToAddress(fixture.Expected.Implementation), time.Now(), &mismatch, prior.Claimable); c != nil {
		t.Fatal("mismatched token observed")
	}
	if c, e := decodeDirectTokenClaim(&tx, &receipt, common.HexToAddress(testTokenBridgeAddr), common.Address{}, time.Now(), pair, prior.Claimable); c != nil || e == nil {
		t.Fatal("unverified implementation accepted")
	}
}

func TestNeoXLifecycleOutcomesAndIsolation(t *testing.T) {
	bridge := common.HexToAddress(testTokenBridgeAddr)
	message := common.HexToAddress(testMessageBridgeAddr)
	src := common.HexToAddress("0x1111")
	dst := common.HexToAddress("0x2222")
	other := common.HexToAddress("0x3333")
	root := func(token common.Address, source common.Address, nonce uint64, tx common.Hash) ethtypes.Log {
		return ethtypes.Log{Address: bridge, Topics: []common.Hash{sigTokenDepositRoot, padAddress(token), padAddress(source), padUint256(nonce)}, Data: make([]byte, 32), BlockNumber: 12, BlockHash: common.HexToHash("0xab"), TxHash: tx, Index: 0}
	}
	outcome := func(sig common.Hash, token common.Address, nonce uint64, tx common.Hash) ethtypes.Log {
		return ethtypes.Log{Address: bridge, Topics: []common.Hash{sig, padAddress(token), padUint256(nonce), padAddress(common.HexToAddress("0x44"))}, Data: make([]byte, 32), BlockNumber: 12, BlockHash: common.HexToHash("0xab"), TxHash: tx, Index: 1}
	}
	for _, tc := range []struct {
		sig  common.Hash
		fact datastore.LifecycleFact
	}{{sigTokenDeposit, datastore.FactDistribution}, {sigTokenClaimable, datastore.FactClaimable}} {
		tx := common.HexToHash("0xbb")
		got, err := decodeNeoXLifecycleTransaction([]ethtypes.Log{outcome(tc.sig, dst, 7, tx), root(dst, src, 7, tx)}, bridge, message, time.Unix(123, 0))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Fact != datastore.FactSettlement || got[1].Fact != tc.fact || got[0].Evidence.TxHash != tx.Hex() || got[0].Identity.SourceToken != src.Hex() || got[0].Identity.DestinationToken != dst.Hex() {
			t.Fatalf("incorrect observations: %+v", got)
		}
	}
	got, err := decodeNeoXLifecycleTransaction([]ethtypes.Log{root(other, src, 7, common.HexToHash("0xcc")), outcome(sigTokenDeposit, dst, 7, common.HexToHash("0xcc"))}, bridge, message, time.Now())
	if err == nil || got != nil {
		t.Fatal("token-pair mismatch accepted")
	}
}

func TestNeoXMessageExecutionResult(t *testing.T) {
	data := make([]byte, 160)
	data[31] = 32
	data[95] = 64
	data[127] = 1
	data[159] = 1
	log := ethtypes.Log{Address: common.HexToAddress(testMessageBridgeAddr), Topics: []common.Hash{sigExecutionResult, padUint256(9)}, Data: data, BlockNumber: 20, BlockHash: common.HexToHash("0xaa"), TxHash: common.HexToHash("0xbb")}
	got, err := decodeNeoXLifecycleTransaction([]ethtypes.Log{log}, common.HexToAddress(testTokenBridgeAddr), log.Address, time.Now())
	if err != nil || len(got) != 1 || got[0].Fact != datastore.FactExecution {
		t.Fatalf("execution: %+v %v", got, err)
	}
	var payload struct {
		Success    bool   `json:"success"`
		ReturnData string `json:"return_data"`
	}
	if err := json.Unmarshal(got[0].Evidence.Payload, &payload); err != nil || payload.Success || payload.ReturnData != "0x00" {
		t.Fatalf("target failure should still execute: %+v %v", payload, err)
	}
}

func TestNeoXNativeClaimAndMessageStore(t *testing.T) {
	bridge := common.HexToAddress(testTokenBridgeAddr)
	message := common.HexToAddress(testMessageBridgeAddr)
	root := ethtypes.Log{Address: bridge, Topics: []common.Hash{sigNativeDepositRoot, padUint256(5)}, Data: make([]byte, 32), BlockNumber: 3, BlockHash: common.HexToHash("0x1"), TxHash: common.HexToHash("0x2"), Index: 0}
	claimable := ethtypes.Log{Address: bridge, Topics: []common.Hash{sigNativeClaimable, padUint256(5), padAddress(common.HexToAddress("0x44"))}, Data: make([]byte, 32), BlockNumber: 3, BlockHash: root.BlockHash, TxHash: root.TxHash, Index: 1}
	observations, err := decodeNeoXLifecycleTransaction([]ethtypes.Log{root, claimable}, bridge, message, time.Now())
	if err != nil || len(observations) != 2 || observations[0].Fact != datastore.FactSettlement || observations[1].Fact != datastore.FactClaimable {
		t.Fatalf("native arrival: %+v %v", observations, err)
	}
	claim := claimable
	claim.Topics[0] = sigNativeClaim
	claim.BlockNumber = 4
	claim.BlockHash = common.HexToHash("0x3")
	claim.TxHash = common.HexToHash("0x4")
	later, err := decodeNeoXLifecycleTransaction([]ethtypes.Log{claim}, bridge, message, time.Now())
	if err != nil || len(later) != 1 || later[0].Fact != datastore.FactClaim {
		t.Fatalf("native claim: %+v %v", later, err)
	}
	_, store := newTestNeoXProcessor(t)
	if err := store.ApplyLifecycleObservations(append(observations, later...)); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetLifecycle(datastore.OperationIdentity{BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 5})
	if err != nil || got.Settlement.TxHash != root.TxHash.Hex() || got.Claim.TxHash != claim.TxHash.Hex() {
		t.Fatalf("original arrival lost: %+v %v", got, err)
	}
	metadata := make([]byte, 32)
	metadata[31] = 1 // STORE_ONLY
	storeData := append(make([]byte, 64), metadata...)
	storeData[31] = 32
	storeData[63] = 32
	mr := ethtypes.Log{Address: message, Topics: []common.Hash{sigNeoToEvmRootUpdate, padUint256(5)}, Data: make([]byte, 32), BlockNumber: 3, BlockHash: root.BlockHash, TxHash: root.TxHash, Index: 0}
	st := ethtypes.Log{Address: message, Topics: []common.Hash{sigMessageStore, padUint256(5)}, Data: storeData, BlockNumber: 3, BlockHash: root.BlockHash, TxHash: root.TxHash, Index: 1}
	result, err := decodeNeoXLifecycleTransaction([]ethtypes.Log{mr, st}, bridge, message, time.Now())
	if err != nil || len(result) != 1 || result[0].Fact != datastore.FactSettlement || result[0].Evidence.Position != "1" {
		t.Fatalf("message arrival: %+v %v", result, err)
	}
	if err := store.ApplyLifecycleObservations(result); err != nil {
		t.Fatal(err)
	}
	msg, err := store.GetLifecycle(datastore.OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionDeposit, Nonce: 5})
	if err != nil || msg.Execution != nil || msg.Settlement == nil {
		t.Fatalf("store-only message: %+v %v", msg, err)
	}
}
