package indexer

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
)

func n3Int(n uint64) stackitem.Item { return stackitem.NewBigInteger(new(big.Int).SetUint64(n)) }
func n3Bytes(n int, size int) stackitem.Item {
	return stackitem.NewByteArray(bytes.Repeat([]byte{byte(n)}, size))
}
func n3Event(contract util.Uint160, name string, items ...stackitem.Item) n3Notification {
	return n3Notification{event: state.NotificationEvent{ScriptHash: contract, Name: name, Item: stackitem.NewArray(items)}, position: name}
}
func n3Fixture(t *testing.T, notes ...n3Notification) []datastore.LifecycleObservation {
	t.Helper()
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	message, _ := util.Uint160DecodeStringLE(testMessageBridgeHash)
	obs, err := decodeN3LifecycleTransaction(notes, token, message, 101, "block-n3", "tx-n3", time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	return obs
}
func TestN3RootOnlyAndTokenPairs(t *testing.T) {
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	notes := []n3Notification{n3Event(token, "NativeWithdrawalRootUpdate", n3Int(11), n3Bytes(1, 32)), n3Event(token, "TokenWithdrawalRootUpdate", n3Bytes(2, 20), n3Bytes(3, 20), n3Int(11), n3Bytes(4, 32)), n3Event(token, "TokenWithdrawalRootUpdate", n3Bytes(5, 20), n3Bytes(6, 20), n3Int(11), n3Bytes(7, 32))}
	obs := n3Fixture(t, notes...)
	if len(obs) != 3 {
		t.Fatalf("expected three independent arrivals: %d", len(obs))
	}
	store := newTestOpStore(t)
	if err := store.ApplyLifecycleObservations(obs); err != nil {
		t.Fatal(err)
	}
	for _, o := range obs {
		l, err := store.GetLifecycle(o.Identity)
		if err != nil || l.Settlement == nil || l.Distribution != nil || l.Execution != nil {
			t.Fatalf("arrival-only: %+v %v", l, err)
		}
		if l.Settlement.Chain != "neo_n3" || l.Settlement.BlockHeight != 101 || l.Settlement.TxHash != "tx-n3" || l.Settlement.Position != "NativeWithdrawalRootUpdate" && o.Identity.BridgeType == types.BridgeTypeNative {
			t.Fatal("incorrect provenance")
		}
	}
}
func TestN3DistributionClaimAndMessageChunks(t *testing.T) {
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	message, _ := util.Uint160DecodeStringLE(testMessageBridgeHash)
	recipient := n3Bytes(9, 20)
	notes := []n3Notification{n3Event(token, "NativeWithdrawalRootUpdate", n3Int(11), n3Bytes(1, 32)), n3Event(token, "NativeWithdrawal", n3Int(10), recipient, n3Int(7)), n3Event(token, "NativeClaimable", n3Int(11), recipient, n3Int(8)), n3Event(token, "TokenWithdrawalRootUpdate", n3Bytes(2, 20), n3Bytes(3, 20), n3Int(11), n3Bytes(4, 32)), n3Event(token, "TokenClaimable", n3Bytes(2, 20), n3Int(11), recipient, n3Int(100)), n3Event(message, "EvmToNeoRootUpdate", n3Int(4), n3Bytes(5, 32))}
	obs := n3Fixture(t, notes...)
	if len(obs) != 7 {
		t.Fatalf("unexpected evidence count: %d", len(obs))
	}
	os := newTestOpStore(t)
	if err := os.ApplyLifecycleObservations(obs); err != nil {
		t.Fatal(err)
	}
	claim := n3Event(token, "TokenClaim", n3Bytes(2, 20), n3Int(11), recipient, n3Int(100))
	claimObs := n3Fixture(t, claim)
	pair, err := os.FindClaimableTokenPairForDirection(types.DirectionWithdrawal, 11, obs[6].Identity.DestinationToken)
	if err != nil || pair == nil {
		t.Fatalf("pair lookup: %v %v", pair, err)
	}
	claimObs[0].Identity = *pair
	if err := os.ApplyLifecycleObservations(claimObs); err != nil {
		t.Fatal(err)
	}
	l, _ := os.GetLifecycle(*pair)
	if l.Settlement == nil || l.Claimable == nil || l.Claim == nil || l.Settlement.TxHash != "tx-n3" {
		t.Fatalf("claim lost relay: %+v", l)
	}
	result := stackitem.NewArray([]stackitem.Item{stackitem.NewBool(false), stackitem.NewByteArray([]byte("target failed"))})
	serialized, err := stackitem.Serialize(result)
	if err != nil {
		t.Fatal(err)
	}
	cut := len(serialized) / 2
	chunk0 := n3Event(message, "Execution", n3Int(4), n3Int(2), n3Int(0), stackitem.NewByteArray(serialized[:cut]))
	chunk1 := n3Event(message, "Execution", n3Int(4), n3Int(2), n3Int(1), stackitem.NewByteArray(serialized[cut:]))
	execObs := n3Fixture(t, chunk0, chunk1)
	if len(execObs) != 1 || execObs[0].Fact != datastore.FactExecution {
		t.Fatal("missing chunk execution")
	}
	var payload struct {
		Result json.RawMessage `json:"result"`
		Chunks []any           `json:"chunks"`
	}
	if err := json.Unmarshal(execObs[0].Evidence.Payload, &payload); err != nil || len(payload.Chunks) != 2 {
		t.Fatalf("chunk payload: %v %+v", err, payload)
	}
	decoded, err := stackitem.FromJSONWithTypes(payload.Result)
	if err != nil || decoded.(*stackitem.Array).Value().([]stackitem.Item)[0].Value() != false {
		t.Fatalf("target failure must still be committed execution: %v", err)
	}
	typed := n3Fixture(t, n3Event(message, "Execution", n3Int(5), n3Int(1), n3Int(0), result))
	if len(typed) != 1 {
		t.Fatal("typed result missing")
	}
	byteResult := n3Fixture(t, n3Event(message, "Execution", n3Int(6), n3Int(1), n3Int(0), stackitem.NewByteArray([]byte("raw"))))
	if len(byteResult) != 1 {
		t.Fatal("single byte string must not be deserialized")
	}
	for _, invalid := range [][]n3Notification{{chunk0}, {chunk0, n3Event(message, "Execution", n3Int(4), n3Int(3), n3Int(1), stackitem.NewByteArray(serialized[cut:]))}, {n3Event(message, "Execution", n3Int(4), n3Int(2), n3Int(2), stackitem.NewByteArray(serialized))}, {n3Event(message, "Execution", n3Int(4), n3Int(2), n3Int(0), n3Int(42)), chunk1}} {
		_, err := decodeN3LifecycleTransaction(invalid, token, message, 101, "block", "tx", time.Unix(100, 0))
		if err == nil {
			t.Fatal("malformed chunk accepted")
		}
	}
}
func TestN3CheckpointRollbackAndRetry(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	os := datastore.NewOperationStore(store, nil)
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	obs := n3Fixture(t, n3Event(token, "NativeWithdrawalRootUpdate", n3Int(11), n3Bytes(1, 32)), n3Event(token, "NativeWithdrawal", n3Int(11), n3Bytes(2, 20), n3Int(3)))
	state := &types.IndexerState{Chain: chainN3, LastBlockHeight: 101, LastBlockHash: "block-n3"}
	bad := append([]datastore.LifecycleObservation{}, obs...)
	bad = append(bad, obs[0])
	bad[2].Evidence.TxHash = "conflict"
	if err := store.SaveN3LifecycleCheckpoint(state, nil, nil, bad); err == nil {
		t.Fatal("conflicting evidence advanced checkpoint")
	}
	if _, err := store.GetIndexerState(chainN3); err != datastore.ErrNotFound {
		t.Fatalf("checkpoint written on failure: %v", err)
	}
	id := obs[0].Identity
	if _, err := os.GetLifecycle(id); err != datastore.ErrNotFound {
		t.Fatalf("partial evidence: %v", err)
	}
	if err := store.SaveN3LifecycleCheckpoint(state, nil, nil, obs); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveN3LifecycleCheckpoint(state, nil, nil, obs); err != nil {
		t.Fatalf("retry: %v", err)
	}
	l, _ := os.GetLifecycle(id)
	if l.Settlement == nil || l.Distribution == nil {
		t.Fatalf("missing committed observations: %+v", l)
	}
}

func TestN3MessageStoreAndSourceType(t *testing.T) {
	message, _ := util.Uint160DecodeStringLE(testMessageBridgeHash)
	metadata, err := stackitem.Serialize(stackitem.NewArray([]stackitem.Item{n3Int(1), n3Int(100), n3Bytes(2, 20)}))
	if err != nil {
		t.Fatal(err)
	}
	source := n3Event(message, "MessageSend", n3Int(12), stackitem.NewByteArray(metadata), n3Bytes(1, 2), n3Bytes(2, 32), n3Bytes(3, 32))
	sourceObs := n3Fixture(t, source)
	if len(sourceObs) != 1 || sourceObs[0].MessageType != "store" || sourceObs[0].Identity.Direction != types.DirectionDeposit {
		t.Fatalf("wrong source type: %+v", sourceObs)
	}
	relay := n3Event(message, "EvmToNeoRootUpdate", n3Int(12), n3Bytes(4, 32))
	store := n3Event(message, "Store", n3Int(12), stackitem.NewByteArray(metadata))
	received := n3Fixture(t, relay, store)
	if len(received) != 1 || received[0].Fact != datastore.FactSettlement {
		t.Fatalf("wrong destination arrival: %+v", received)
	}
	var arrival struct {
		Store struct {
			Metadata string `json:"metadata"`
			Position string `json:"position"`
		} `json:"store"`
	}
	if err := json.Unmarshal(received[0].Evidence.Payload, &arrival); err != nil || arrival.Store.Metadata == "" || arrival.Store.Position != "Store" {
		t.Fatalf("missing verified Store receipt: %+v %v", arrival, err)
	}
	malformed := n3Event(message, "Store", n3Int(12), stackitem.NewByteArray([]byte{0xff}))
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	if _, err := decodeN3LifecycleTransaction([]n3Notification{relay, malformed}, token, message, 10, "block", "tx", time.Unix(100, 0)); err == nil {
		t.Fatal("malformed required Store metadata accepted")
	}
}

func TestN3IndirectNotificationAndFaultIgnored(t *testing.T) {
	proc, _ := newTestProcessor(t)
	idx := &N3Indexer{processor: proc, contracts: []util.Uint160{proc.neoXBridge, proc.messageBridge}}
	root := n3Event(proc.neoXBridge, "NativeWithdrawalRootUpdate", n3Int(9), n3Bytes(1, 32)).event
	distributed := n3Event(proc.neoXBridge, "NativeWithdrawal", n3Int(9), n3Bytes(2, 20), n3Int(4)).event
	// The caller's transaction script does not need to mention either bridge.
	// Only notifications emitted by the watched contract count.
	fault := state.Execution{VMState: vmstate.Fault, Events: []state.NotificationEvent{root, distributed}}
	ops, obs, err := idx.decodeN3Executions([]state.Execution{fault}, 7_000_000, "block", "tx", time.Unix(100, 0))
	if err != nil || len(ops) != 0 || len(obs) != 0 {
		t.Fatalf("FAULT established evidence: %v %+v", err, obs)
	}
	halt := state.Execution{VMState: vmstate.Halt, Events: []state.NotificationEvent{root, distributed}}
	ops, obs, err = idx.decodeN3Executions([]state.Execution{halt}, 7_000_000, "block", "tx", time.Unix(100, 0))
	if err != nil || len(ops) != 0 || len(obs) != 2 || obs[1].Fact != datastore.FactDistribution || obs[0].Evidence.Position != "0:0" {
		t.Fatalf("indirect receipt missed: %v %+v", err, obs)
	}
}

func TestN3TokenDistributionPairIsolation(t *testing.T) {
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	notes := []n3Notification{
		n3Event(token, "TokenWithdrawalRootUpdate", n3Bytes(2, 20), n3Bytes(3, 20), n3Int(5), n3Bytes(4, 32)),
		n3Event(token, "TokenWithdrawal", n3Bytes(2, 20), n3Int(5), n3Bytes(9, 20), n3Int(50)),
		n3Event(token, "TokenWithdrawalRootUpdate", n3Bytes(6, 20), n3Bytes(7, 20), n3Int(5), n3Bytes(8, 32)),
		n3Event(token, "TokenClaimable", n3Bytes(6, 20), n3Int(5), n3Bytes(9, 20), n3Int(60)),
	}
	obs := n3Fixture(t, notes...)
	os := newTestOpStore(t)
	if err := os.ApplyLifecycleObservations(obs); err != nil {
		t.Fatal(err)
	}
	first, err := os.GetLifecycle(obs[0].Identity)
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.GetLifecycle(obs[1].Identity)
	if err != nil {
		t.Fatal(err)
	}
	if first.Distribution == nil || first.Claimable != nil || second.Claimable == nil || second.Distribution != nil {
		t.Fatalf("mixed pairs: %+v %+v", first, second)
	}
}

func TestN3HistoricalTokenClaimDoesNotBlockCheckpoint(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	os := datastore.NewOperationStore(store, nil)
	idx := &N3Indexer{store: store, opStore: os, logger: slog.Default()}
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	observed := n3Fixture(t, n3Event(token, "TokenClaim", n3Bytes(2, 20), n3Int(11), n3Bytes(9, 20), n3Int(100)))
	sourceToken, _ := n3Token([]stackitem.Item{n3Bytes(3, 20)}, 0)
	id := observed[0].Identity
	id.SourceToken = sourceToken
	source := &types.BridgeOperation{BridgeType: id.BridgeType, Direction: id.Direction, Nonce: id.Nonce, SourceChain: "neo_x", DestinationChain: "neo_n3", TokenContract: &id.SourceToken, DestTokenContract: &id.DestinationToken, Status: types.StatusPending}
	if err := os.SaveOperation(source); err != nil {
		t.Fatal(err)
	}
	state := &types.IndexerState{Chain: chainN3, LastBlockHeight: 101, LastBlockHash: "block-n3"}
	for attempt := 0; attempt < 2; attempt++ {
		if err := idx.applyProcessedBlockWithLifecycle(state, nil, observed); err != nil {
			t.Fatalf("attempt %d blocked: %v", attempt, err)
		}
	}
	persisted, err := store.GetIndexerState(chainN3)
	if err != nil || persisted.LastBlockHeight != 101 {
		t.Fatalf("checkpoint not advanced: %+v %v", persisted, err)
	}
	if _, err := os.GetLifecycle(id); err != datastore.ErrNotFound {
		t.Fatalf("fabricated claim: %v", err)
	}
	operation, err := os.GetOperation(id.BridgeType, id.Direction, id.Nonce)
	if err != nil || operation.Status != types.StatusPending {
		t.Fatalf("unknown legacy outcome: %+v %v", operation, err)
	}
	gaps, err := store.ListUnresolvedN3TokenClaims()
	if err != nil || len(gaps) != 1 || gaps[0].Nonce != 11 || gaps[0].Evidence.TxHash != "tx-n3" || gaps[0].Reason != "missing_claimable_pair" {
		t.Fatalf("unresolved evidence lost: %+v %v", gaps, err)
	}
}

func TestN3VerifiedRootsMatchNeoXSourceBytes(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	bs := datastore.NewBridgeStateStore(store)
	sourceRoot := "0x" + strings.Repeat("AB", 32)
	instance := datastore.BuildInstanceID("native", "neo_x", "neo_n3", nil, nil)
	if err := bs.SaveBridgeState(&types.DirectionalBridgeState{InstanceID: instance, BridgeType: types.BridgeTypeNative, SrcChain: "neo_x", DstChain: "neo_n3", SourceState: &types.ChainState{Nonce: 11, Root: &sourceRoot}}); err != nil {
		t.Fatal(err)
	}
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	obs := n3Fixture(t, n3Event(token, "NativeWithdrawalRootUpdate", n3Int(11), n3Bytes(0xab, 32)))
	checkpoint := &types.IndexerState{Chain: chainN3, LastBlockHeight: 101, LastBlockHash: "block-n3"}
	if err := store.SaveN3LifecycleCheckpoint(checkpoint, nil, nil, obs); err != nil {
		t.Fatal(err)
	}
	sync, err := monitor.NewSyncComputer(bs, slog.Default()).ComputeAll()
	if err != nil || len(sync) != 1 {
		t.Fatalf("sync: %+v %v", sync, err)
	}
	if sync[0].Status != types.DirectionalSyncStatusSynced || sync[0].DstBlockNumber == nil || *sync[0].DstBlockNumber != 101 {
		t.Fatalf("equal root bytes reported mismatch: %+v", sync[0])
	}
}

func TestN3TokenClaimRootDoesNotSubstituteForClaimable(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	os := datastore.NewOperationStore(store, nil)
	idx := &N3Indexer{store: store, opStore: os, logger: slog.Default()}
	token, _ := util.Uint160DecodeStringLE(testNeoXBridgeHash)
	obs := n3Fixture(t, n3Event(token, "TokenWithdrawalRootUpdate", n3Bytes(2, 20), n3Bytes(3, 20), n3Int(11), n3Bytes(4, 32)), n3Event(token, "TokenClaim", n3Bytes(2, 20), n3Int(11), n3Bytes(9, 20), n3Int(100)))
	checkpoint := &types.IndexerState{Chain: chainN3, LastBlockHeight: 101, LastBlockHash: "block-n3"}
	if err := idx.applyProcessedBlockWithLifecycle(checkpoint, nil, obs); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := os.GetLifecycle(obs[0].Identity)
	if err != nil || lifecycle.Settlement == nil || lifecycle.Claim != nil {
		t.Fatalf("root inferred claim: %+v %v", lifecycle, err)
	}
	unresolved, err := store.ListUnresolvedN3TokenClaims()
	if err != nil || len(unresolved) != 1 {
		t.Fatalf("missing unresolved claim: %+v %v", unresolved, err)
	}
}
