package indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	sigNativeDepositRoot = crypto.Keccak256Hash([]byte("NativeDepositRootUpdate(uint256,bytes32)"))
	sigTokenDepositRoot  = crypto.Keccak256Hash([]byte("TokenDepositRootUpdate(address,address,uint256,bytes32)"))
	sigMessageStore      = crypto.Keccak256Hash([]byte("Store(uint256,bytes)"))
	sigExecutionResult   = crypto.Keccak256Hash([]byte("Execution(uint256,(bool,bytes))"))
	sigProxyUpgrade      = crypto.Keccak256Hash([]byte("Upgraded(address)"))
	sigERC20Transfer     = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	claimTokenSelector   = crypto.Keccak256([]byte("claimToken(address,uint256)"))[:4]
)

func wordNonce(w common.Hash) (uint64, error) {
	n := new(big.Int).SetBytes(w[:])
	if !n.IsUint64() {
		return 0, fmt.Errorf("nonce overflows uint64")
	}
	return n.Uint64(), nil
}
func eventEvidence(l ethtypes.Log, t time.Time, payload any) datastore.LifecycleEvidence {
	var raw json.RawMessage
	if payload != nil {
		raw, _ = json.Marshal(payload)
	}
	return datastore.LifecycleEvidence{Kind: "event", Chain: "neo_x", Contract: l.Address.Hex(), BlockHeight: l.BlockNumber, BlockHash: l.BlockHash.Hex(), TxHash: l.TxHash.Hex(), ObservedAt: t, Position: fmt.Sprintf("%d", l.Index), Payload: raw}
}
func lifecycleObs(id datastore.OperationIdentity, fact datastore.LifecycleFact, e datastore.LifecycleEvidence, msg string) datastore.LifecycleObservation {
	return datastore.LifecycleObservation{Identity: id, Fact: fact, Evidence: e, MessageType: msg}
}
func neoxIdentity(bt types.BridgeType, nonce uint64, src, dst string) datastore.OperationIdentity {
	return datastore.OperationIdentity{BridgeType: bt, Direction: types.DirectionDeposit, Nonce: nonce, SourceToken: src, DestinationToken: dst}
}

// decodeNeoXLifecycleTransaction correlates the root and per-operation events
// within one committed receipt. No outcome is inferred from a missing event.
func decodeNeoXLifecycleTransaction(logs []ethtypes.Log, tokenBridge, messageBridge common.Address, blockTime time.Time) ([]datastore.LifecycleObservation, error) {
	sort.Slice(logs, func(i, j int) bool { return logs[i].Index < logs[j].Index })
	type root struct {
		log          ethtypes.Log
		nonce        uint64
		source, dest string
		bridge       types.BridgeType
	}
	var roots []root
	for _, l := range logs {
		if len(l.Topics) == 0 {
			continue
		}
		sig := l.Topics[0]
		if l.Address == tokenBridge && (sig == sigNativeDepositRoot || sig == sigTokenDepositRoot) || l.Address == messageBridge && sig == sigNeoToEvmRootUpdate {
			expected := 2
			if sig == sigTokenDepositRoot {
				expected = 4
			}
			if len(l.Topics) != expected || len(l.Data) != 32 {
				return nil, fmt.Errorf("malformed root update %s", l.TxHash.Hex())
			}
			n, err := wordNonce(l.Topics[expected-1])
			if err != nil {
				return nil, err
			}
			r := root{log: l, nonce: n, bridge: types.BridgeTypeNative}
			if sig == sigTokenDepositRoot {
				r.bridge = types.BridgeTypeToken
				r.dest = common.BytesToAddress(l.Topics[1][:]).Hex()
				r.source = common.BytesToAddress(l.Topics[2][:]).Hex()
			}
			if sig == sigNeoToEvmRootUpdate {
				r.bridge = types.BridgeTypeMessage
			}
			roots = append(roots, r)
		}
	}
	var out []datastore.LifecycleObservation
	seen := map[string]bool{}
	add := func(id datastore.OperationIdentity, fact datastore.LifecycleFact, e datastore.LifecycleEvidence, msg string) error {
		k := fmt.Sprintf("%s:%s:%d:%s:%s:%s", id.BridgeType, id.Direction, id.Nonce, id.SourceToken, id.DestinationToken, fact)
		if seen[k] {
			return fmt.Errorf("duplicate lifecycle fact %s in transaction", k)
		}
		seen[k] = true
		out = append(out, lifecycleObs(id, fact, e, msg))
		return nil
	}
	findRoot := func(bt types.BridgeType, nonce uint64, token string, position uint) *root {
		var match *root
		for i := range roots {
			r := &roots[i]
			if r.bridge == bt && r.nonce >= nonce && r.log.Index < position && (bt != types.BridgeTypeToken || common.HexToAddress(r.dest) == common.HexToAddress(token)) {
				match = r
			}
		}
		return match
	}
	for _, l := range logs {
		if len(l.Topics) == 0 {
			continue
		}
		sig := l.Topics[0]
		if l.Address == messageBridge && sig == sigExecution {
			return nil, fmt.Errorf("unverified historical Execution(uint256,uint8) implementation at %s", l.TxHash.Hex())
		}
		if l.Address == messageBridge && sig == sigMessageSend {
			if len(l.Topics) != 3 {
				return nil, fmt.Errorf("malformed MessageSend topics")
			}
			n, err := wordNonce(l.Topics[1])
			if err != nil {
				return nil, err
			}
			metadata, err := readFirstDynamicBytes(l.Data)
			if err != nil {
				return nil, err
			}
			mt, err := readAMBMessageType(metadata)
			if err != nil {
				return nil, err
			}
			kind := []string{"execute", "store", "return"}[mt]
			out = append(out, lifecycleObs(datastore.OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionWithdrawal, Nonce: n}, datastore.FactMessageType, eventEvidence(l, blockTime, map[string]string{"metadata": fmt.Sprintf("0x%x", metadata)}), kind))
		}
		if l.Address == tokenBridge && (sig == sigNativeDeposit || sig == sigNativeClaimable || sig == sigNativeClaim || sig == sigTokenDeposit || sig == sigTokenClaimable) || l.Address == messageBridge && (sig == sigMessageStore || sig == sigExecutionResult) {
			bt := types.BridgeTypeNative
			index := 1
			expected := 3
			if sig == sigTokenDeposit || sig == sigTokenClaimable {
				bt = types.BridgeTypeToken
				index = 2
				expected = 4
			}
			if sig == sigMessageStore || sig == sigExecutionResult {
				bt = types.BridgeTypeMessage
				expected = 2
			}
			if len(l.Topics) != expected {
				return nil, fmt.Errorf("malformed lifecycle event %s", l.TxHash.Hex())
			}
			n, err := wordNonce(l.Topics[index])
			if err != nil {
				return nil, err
			}
			if sig == sigNativeClaim || sig == sigExecutionResult {
				id := neoxIdentity(bt, n, "", "")
				payload := any(nil)
				if sig == sigExecutionResult {
					payload, err = decodeExecutionResult(l.Data)
					if err != nil {
						return nil, err
					}
				}
				if sig == sigNativeClaim && len(l.Data) != 32 {
					return nil, fmt.Errorf("malformed native claim")
				}
				fact := datastore.FactClaim
				if sig == sigExecutionResult {
					fact = datastore.FactExecution
				}
				if err := add(id, fact, eventEvidence(l, blockTime, payload), ""); err != nil {
					return nil, err
				}
				continue
			}
			token := ""
			if bt == types.BridgeTypeToken {
				token = common.BytesToAddress(l.Topics[1][:]).Hex()
			}
			r := findRoot(bt, n, token, l.Index)
			if r == nil {
				return nil, fmt.Errorf("outcome without matching root in transaction %s", l.TxHash.Hex())
			}
			id := neoxIdentity(bt, n, r.source, r.dest)
			if bt == types.BridgeTypeNative || bt == types.BridgeTypeToken {
				if len(l.Data) != 32 {
					return nil, fmt.Errorf("malformed asset outcome")
				}
			}
			fact := datastore.FactDistribution
			if sig == sigNativeClaimable || sig == sigTokenClaimable {
				fact = datastore.FactClaimable
			}
			var outcomePayload any
			if fact == datastore.FactClaimable {
				outcomePayload = map[string]string{"recipient": common.BytesToAddress(l.Topics[expected-1][:]).Hex(), "amount": new(big.Int).SetBytes(l.Data).String()}
			}
			if sig == sigMessageStore {
				metadata, err := decodeStoreMetadata(l.Data)
				if err != nil {
					return nil, err
				}
				mt, err := readAMBMessageType(metadata)
				if err != nil {
					return nil, err
				}
				// The metadata of a Store event originated on N3. Its source message
				// type is recorded by N3 decoding later, not forged as Neo X source.
				_ = mt
				fact = datastore.FactSettlement
				if err := add(id, fact, eventEvidence(l, blockTime, map[string]string{"metadata": fmt.Sprintf("0x%x", metadata)}), ""); err != nil {
					return nil, err
				}
				continue
			}
			{
				if err := add(id, datastore.FactSettlement, eventEvidence(r.log, blockTime, map[string]string{"root": common.BytesToHash(r.log.Data).Hex()}), ""); err != nil {
					return nil, err
				}
				if err := add(id, fact, eventEvidence(l, blockTime, outcomePayload), ""); err != nil {
					return nil, err
				}
			}
		}
	}
	for _, r := range roots {
		id := neoxIdentity(r.bridge, r.nonce, r.source, r.dest)
		k := fmt.Sprintf("%s:%s:%d:%s:%s:%s", id.BridgeType, id.Direction, id.Nonce, id.SourceToken, id.DestinationToken, datastore.FactSettlement)
		if !seen[k] {
			if err := add(id, datastore.FactSettlement, eventEvidence(r.log, blockTime, map[string]string{"root": common.BytesToHash(r.log.Data).Hex()}), ""); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func decodeStoreMetadata(data []byte) ([]byte, error) {
	if len(data) < 64 {
		return nil, fmt.Errorf("short Store metadata")
	}
	offset := new(big.Int).SetBytes(data[:32])
	if !offset.IsUint64() || offset.Uint64() != 32 {
		return nil, fmt.Errorf("bad Store offset")
	}
	size := new(big.Int).SetBytes(data[32:64])
	if !size.IsUint64() || size.Uint64() > uint64(len(data)-64) {
		return nil, fmt.Errorf("bad Store length")
	}
	return data[64 : 64+size.Uint64()], nil
}
func decodeExecutionResult(data []byte) (any, error) {
	if len(data) < 128 || new(big.Int).SetBytes(data[:32]).Uint64() != 32 || new(big.Int).SetBytes(data[64:96]).Uint64() != 64 {
		return nil, fmt.Errorf("malformed Execution result")
	}
	success := new(big.Int).SetBytes(data[32:64])
	if success.Cmp(big.NewInt(1)) > 0 {
		return nil, fmt.Errorf("invalid Execution success")
	}
	size := new(big.Int).SetBytes(data[96:128])
	if !size.IsUint64() || size.Uint64() > uint64(len(data)-128) {
		return nil, fmt.Errorf("invalid Execution return data")
	}
	return map[string]any{"success": success.Sign() != 0, "return_data": fmt.Sprintf("0x%x", data[128:128+size.Uint64()])}, nil
}

// decodeDirectTokenClaim verifies the top-level call and successful receipt;
// only implementations with verified silent-claim semantics are accepted.
func decodeDirectTokenClaim(tx *ethtypes.Transaction, receipt *ethtypes.Receipt, bridge common.Address, implementation common.Address, blockTime time.Time, pair *datastore.OperationIdentity, claimable *datastore.LifecycleEvidence) (*datastore.LifecycleObservation, error) {
	if tx.To() == nil || *tx.To() != bridge || len(tx.Data()) != 68 || string(tx.Data()[:4]) != string(claimTokenSelector) {
		return nil, nil
	}
	return decodeTokenClaimCall(tx, receipt, bridge, implementation, blockTime, pair, claimable, tx.Data(), "top-level")
}

// claimTraceCalls returns only successful external bridge calls with successful
// ancestors. Paths count child calls from the top-level transaction frame.
type claimTraceCall struct {
	Input    []byte
	Position string
}

func verifyBlockClaimTraces(block *ethtypes.Block, traces []neox.TransactionTrace) error {
	if len(traces) != len(block.Transactions()) {
		return fmt.Errorf("incomplete block traces: received %d for %d transactions", len(traces), len(block.Transactions()))
	}
	for i, tx := range block.Transactions() {
		if traces[i].TxHash != tx.Hash() || traces[i].Result == nil || traces[i].Error != "" {
			return fmt.Errorf("invalid trace for transaction %d (%s)", i, tx.Hash())
		}
		if tx.To() != nil && (traces[i].Result.To != *tx.To() || !bytes.Equal(traces[i].Result.Input, tx.Data())) {
			return fmt.Errorf("trace root does not match transaction %s", tx.Hash())
		}
	}
	return nil
}

func claimTraceCalls(frame *neox.CallFrame, bridge common.Address) []claimTraceCall {
	var result []claimTraceCall
	var visit func(*neox.CallFrame, string)
	visit = func(f *neox.CallFrame, path string) {
		if f == nil || f.Error != "" {
			return
		}
		if path != "top-level" && f.To == bridge && f.Type == "CALL" && len(f.Input) == 68 && string(f.Input[:4]) == string(claimTokenSelector) {
			result = append(result, claimTraceCall{Input: append([]byte(nil), f.Input...), Position: path})
		}
		for i := range f.Calls {
			visit(&f.Calls[i], fmt.Sprintf("%s/%d", path, i))
		}
	}
	visit(frame, "top-level")
	return result
}

func decodeTokenClaimCall(tx *ethtypes.Transaction, receipt *ethtypes.Receipt, bridge common.Address, implementation common.Address, blockTime time.Time, pair *datastore.OperationIdentity, claimable *datastore.LifecycleEvidence, input []byte, position string) (*datastore.LifecycleObservation, error) {
	if len(input) != 68 || string(input[:4]) != string(claimTokenSelector) {
		return nil, nil
	}
	if receipt == nil || receipt.Status != ethtypes.ReceiptStatusSuccessful || receipt.TxHash != tx.Hash() {
		return nil, nil
	}
	if implementation != common.HexToAddress("0x671bcb1b24e75a37adb283ffa509620c02eb35c6") {
		return nil, fmt.Errorf("unverified claimToken implementation %s", implementation.Hex())
	}
	if new(big.Int).SetBytes(input[4:16]).Sign() != 0 {
		return nil, fmt.Errorf("invalid address argument")
	}
	token := common.BytesToAddress(input[16:36])
	nonceBig := new(big.Int).SetBytes(input[36:68])
	if !nonceBig.IsUint64() {
		return nil, fmt.Errorf("claim nonce overflows uint64")
	}
	if pair == nil || pair.Nonce != nonceBig.Uint64() || common.HexToAddress(pair.DestinationToken) != token || pair.Direction != types.DirectionDeposit || pair.BridgeType != types.BridgeTypeToken {
		return nil, nil
	}
	payload := map[string]any{"selector": fmt.Sprintf("0x%x", claimTokenSelector), "token": token.Hex(), "nonce": pair.Nonce}
	if claimable != nil {
		var stored struct {
			Recipient string `json:"recipient"`
			Amount    string `json:"amount"`
		}
		if json.Unmarshal(claimable.Payload, &stored) == nil {
			amount, ok := new(big.Int).SetString(stored.Amount, 10)
			if ok {
				for _, l := range receipt.Logs {
					if l.Address == token && len(l.Topics) == 3 && l.Topics[0] == sigERC20Transfer && common.BytesToAddress(l.Topics[1][:]) == bridge && common.BytesToAddress(l.Topics[2][:]) == common.HexToAddress(stored.Recipient) && new(big.Int).SetBytes(l.Data).Cmp(amount) == 0 {
						payload["transfer"] = map[string]any{"log_index": l.Index, "recipient": stored.Recipient, "amount": stored.Amount}
						break
					}
				}
			}
		}
	}
	raw, _ := json.Marshal(payload)
	evidence := datastore.LifecycleEvidence{Kind: "call", Chain: "neo_x", Contract: bridge.Hex(), Implementation: implementation.Hex(), BlockHeight: receipt.BlockNumber.Uint64(), BlockHash: receipt.BlockHash.Hex(), TxHash: tx.Hash().Hex(), ObservedAt: blockTime, Position: position, Payload: raw}
	o := lifecycleObs(*pair, datastore.FactClaim, evidence, "")
	return &o, nil
}
