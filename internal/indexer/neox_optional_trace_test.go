package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// fakeNeoX serves the standard RPC methods the indexer needs, plus
// debug_traceBlockByNumber in a configurable mode.
type fakeNeoX struct {
	mu          sync.Mutex
	traceMode   string // "unsupported", "fail", "ok"
	failReceipt common.Hash
	blocks      map[uint64]*ethtypes.Block
	receipts    map[common.Hash]*ethtypes.Receipt
	traces      map[common.Hash]*neox.CallFrame // non-default trace roots
	calls       map[string]int
}

func (f *fakeNeoX) setTraceMode(mode string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.traceMode = mode
}

func (f *fakeNeoX) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

func (f *fakeNeoX) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(body, &req)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[req.Method]++
	result, rpcErr := f.handle(req.Method, req.Params)
	resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	if rpcErr != nil {
		resp["error"] = rpcErr
	} else {
		resp["result"] = result
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func heightParam(raw json.RawMessage) uint64 {
	var s string
	_ = json.Unmarshal(raw, &s)
	n, _ := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	return n
}

func (f *fakeNeoX) handle(method string, params []json.RawMessage) (any, map[string]any) {
	switch method {
	case "eth_blockNumber":
		return "0x67", nil
	case "eth_getBlockByNumber":
		b := f.blocks[heightParam(params[0])]
		if b == nil {
			return nil, nil
		}
		var full bool
		_ = json.Unmarshal(params[1], &full)
		raw, _ := json.Marshal(b.Header())
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		out["hash"] = b.Hash()
		out["uncles"] = []any{}
		txs := []any{}
		for _, tx := range b.Transactions() {
			if full {
				txs = append(txs, tx)
			} else {
				txs = append(txs, tx.Hash())
			}
		}
		out["transactions"] = txs
		return out, nil
	case "eth_getLogs":
		var q struct {
			FromBlock string `json:"fromBlock"`
			ToBlock   string `json:"toBlock"`
		}
		_ = json.Unmarshal(params[0], &q)
		from, _ := strconv.ParseUint(strings.TrimPrefix(q.FromBlock, "0x"), 16, 64)
		to, _ := strconv.ParseUint(strings.TrimPrefix(q.ToBlock, "0x"), 16, 64)
		logs := []*ethtypes.Log{}
		for h := from; h <= to; h++ {
			if b := f.blocks[h]; b != nil {
				for _, tx := range b.Transactions() {
					for _, l := range f.receipts[tx.Hash()].Logs {
						if l.Address == common.HexToAddress(testTokenBridgeAddr) || l.Address == common.HexToAddress(testMessageBridgeAddr) {
							logs = append(logs, l)
						}
					}
				}
			}
		}
		return logs, nil
	case "eth_getTransactionReceipt":
		var h common.Hash
		_ = json.Unmarshal(params[0], &h)
		if h == f.failReceipt {
			return nil, map[string]any{"code": -32000, "message": "receipt backend unavailable"}
		}
		return f.receipts[h], nil
	case "eth_getStorageAt":
		return common.BytesToHash(common.HexToAddress("0x671bcb1b24e75a37adb283ffa509620c02eb35c6").Bytes()), nil
	case "debug_traceBlockByNumber":
		switch f.traceMode {
		case "unsupported":
			return nil, map[string]any{"code": -32601, "message": "the method debug_traceBlockByNumber does not exist/is not available"}
		case "fail":
			return nil, map[string]any{"code": -32000, "message": "tracing timed out"}
		}
		b := f.blocks[heightParam(params[0])]
		out := []neox.TransactionTrace{}
		for _, tx := range b.Transactions() {
			frame := f.traces[tx.Hash()]
			if frame == nil {
				frame = &neox.CallFrame{Type: "CALL", To: *tx.To(), Input: hexutil.Bytes(tx.Data())}
			}
			out = append(out, neox.TransactionTrace{TxHash: tx.Hash(), Result: frame})
		}
		return out, nil
	}
	return nil, map[string]any{"code": -32601, "message": "unexpected " + method}
}

type optionalTraceFixture struct {
	url      string
	rpc      *fakeNeoX
	idx      *NeoXIndexer
	store    *datastore.Store
	opStore  *datastore.OperationStore
	direct   datastore.OperationIdentity // claimed by a top-level call in block 101
	indirect datastore.OperationIdentity // claimed through a router in block 101
	rootTx   common.Hash
	router   common.Hash
}

func claimInput(token common.Address, nonce uint64, dirty bool) []byte {
	in := append(append([]byte(nil), claimTokenSelector...), common.LeftPadBytes(token.Bytes(), 32)...)
	if dirty {
		in[4] = 0xff
	}
	return append(in, common.LeftPadBytes(new(big.Int).SetUint64(nonce).Bytes(), 32)...)
}

// newOptionalTraceFixture builds blocks 100-102:
//   - 100: relay of token nonces 7 and 8, both claimable.
//   - 101: direct claimToken(7), and a router transaction whose internal call
//     claims nonce 8 (visible only in a call trace).
//   - 102: a reverted direct claimToken with malformed arguments.
//   - 103: empty, so it can contain no claim and needs no trace.
func newOptionalTraceFixture(t *testing.T, traceMode string) *optionalTraceFixture {
	t.Helper()
	bridge := common.HexToAddress(testTokenBridgeAddr)
	src := common.HexToAddress("0xef4073a0f2b305a38ec4050e4d3d28bc40ea63f5")
	dst := common.HexToAddress("0xc28736dc83f4fd43d6fb832fd93c3ee7bb26828f")
	recipient := common.HexToAddress("0xce0c7fbe9c64bc4b1bdc1436f2fe8e795dd01e90")
	routerAddr := common.HexToAddress("0x5555")
	rpc := &fakeNeoX{traceMode: traceMode, blocks: map[uint64]*ethtypes.Block{}, receipts: map[common.Hash]*ethtypes.Receipt{}, traces: map[common.Hash]*neox.CallFrame{}, calls: map[string]int{}}

	tx := func(nonce uint64, to common.Address, data []byte) *ethtypes.Transaction {
		return ethtypes.NewTx(&ethtypes.LegacyTx{Nonce: nonce, To: &to, Gas: 100000, GasPrice: big.NewInt(1), Value: big.NewInt(0), Data: data})
	}
	relay := tx(1, bridge, []byte{0x01})
	direct := tx(2, bridge, claimInput(dst, 7, false))
	router := tx(3, routerAddr, []byte{0xa1})
	reverted := tx(4, bridge, claimInput(dst, 9, true))
	rpc.traces[router.Hash()] = &neox.CallFrame{Type: "CALL", To: routerAddr, Input: hexutil.Bytes(router.Data()), Calls: []neox.CallFrame{{Type: "CALL", To: bridge, Input: hexutil.Bytes(claimInput(dst, 8, false))}}}

	amount := common.LeftPadBytes(big.NewInt(123).Bytes(), 32)
	logsFor := map[common.Hash][]*ethtypes.Log{
		relay.Hash(): {
			{Address: bridge, Topics: []common.Hash{sigTokenDepositRoot, padAddress(dst), padAddress(src), padUint256(8)}, Data: make([]byte, 32)},
			{Address: bridge, Topics: []common.Hash{sigTokenClaimable, padAddress(dst), padUint256(7), padAddress(recipient)}, Data: amount},
			{Address: bridge, Topics: []common.Hash{sigTokenClaimable, padAddress(dst), padUint256(8), padAddress(recipient)}, Data: amount},
		},
		direct.Hash(): {{Address: dst, Topics: []common.Hash{sigERC20Transfer, padAddress(bridge), padAddress(recipient)}, Data: amount}},
		router.Hash(): {{Address: dst, Topics: []common.Hash{sigERC20Transfer, padAddress(bridge), padAddress(recipient)}, Data: amount}},
	}
	status := map[common.Hash]uint64{reverted.Hash(): ethtypes.ReceiptStatusFailed}
	parent := common.Hash{}
	var logIndex uint
	blockTxs := map[uint64][]*ethtypes.Transaction{100: {relay}, 101: {direct, router}, 102: {reverted}}
	for _, height := range []uint64{100, 101, 102, 103} {
		txs := blockTxs[height]
		txRoot := common.HexToHash("0x01")
		if len(txs) == 0 {
			txRoot = ethtypes.EmptyTxsHash
		}
		header := &ethtypes.Header{ParentHash: parent, UncleHash: ethtypes.EmptyUncleHash, TxHash: txRoot, ReceiptHash: ethtypes.EmptyReceiptsHash,
			Difficulty: big.NewInt(0), Number: new(big.Int).SetUint64(height), GasLimit: 30000000, Time: 1700000000 + height, Extra: []byte{}}
		block := ethtypes.NewBlockWithHeader(header).WithBody(ethtypes.Body{Transactions: txs})
		rpc.blocks[height] = block
		parent = block.Hash()
		for i, t := range txs {
			st, ok := status[t.Hash()]
			if !ok {
				st = ethtypes.ReceiptStatusSuccessful
			}
			r := &ethtypes.Receipt{Status: st, CumulativeGasUsed: 1, TxHash: t.Hash(), BlockHash: block.Hash(), BlockNumber: block.Number(), TransactionIndex: uint(i), Logs: []*ethtypes.Log{}}
			for _, l := range logsFor[t.Hash()] {
				l.BlockNumber, l.BlockHash, l.TxHash, l.TxIndex, l.Index = height, block.Hash(), t.Hash(), uint(i), logIndex
				logIndex++
				r.Logs = append(r.Logs, l)
			}
			r.Bloom = ethtypes.CreateBloom(r)
			rpc.receipts[t.Hash()] = r
		}
	}
	srv := httptest.NewServer(rpc)
	t.Cleanup(srv.Close)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := neox.NewClient(context.Background(), []string{srv.URL}, logger, 0)
	if err != nil {
		t.Fatal(err)
	}
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, logger)
	opStore := datastore.NewOperationStore(store, nil)
	proc := NewNeoXEventProcessor(opStore, testTokenBridgeAddr, testMessageBridgeAddr, logger)
	idx := NewNeoXIndexer(client, store, opStore, nil, proc, []string{testTokenBridgeAddr, testMessageBridgeAddr}, 100, 1, logger, nil)
	id := func(n uint64) datastore.OperationIdentity {
		return datastore.OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: n, SourceToken: src.Hex(), DestinationToken: dst.Hex()}
	}
	return &optionalTraceFixture{url: srv.URL, rpc: rpc, idx: idx, store: store, opStore: opStore, direct: id(7), indirect: id(8), rootTx: relay.Hash(), router: router.Hash()}
}

func (f *optionalTraceFixture) lifecycle(t *testing.T, id datastore.OperationIdentity) *datastore.OperationLifecycle {
	t.Helper()
	l, err := f.opStore.GetLifecycle(id)
	if err != nil {
		t.Fatalf("lifecycle %d: %v", id.Nonce, err)
	}
	return l
}

func (f *optionalTraceFixture) checkpoint(t *testing.T) uint64 {
	t.Helper()
	s, err := f.store.GetIndexerState(chainNeoX)
	if errors.Is(err, datastore.ErrNotFound) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return s.LastBlockHeight
}

// Production regression: an endpoint answering -32601 must not stop the
// checkpoint or event-derived lifecycle writes.
func TestNeoXTraceUnsupportedDoesNotStopIndexing(t *testing.T) {
	f := newOptionalTraceFixture(t, "unsupported")
	ctx := context.Background()
	if err := f.idx.processBlockRange(ctx, 100, 103); err != nil {
		t.Fatalf("range stopped by unsupported tracing: %v", err)
	}
	if got := f.checkpoint(t); got != 103 {
		t.Fatalf("checkpoint %d, want 103", got)
	}
	if n := f.rpc.count("debug_traceBlockByNumber"); n != 1 {
		t.Fatalf("unsupported tracing requested %d times, want a single probe", n)
	}
	direct := f.lifecycle(t, f.direct)
	if direct.Settlement == nil || direct.Settlement.TxHash != f.rootTx.Hex() || direct.Claimable == nil {
		t.Fatalf("event-derived lifecycle missing: %+v", direct)
	}
	if direct.Claim == nil || direct.Claim.Kind != "call" || direct.Claim.Position != "top-level" {
		t.Fatalf("verified direct claim lost without traces: %+v", direct.Claim)
	}
	indirect := f.lifecycle(t, f.indirect)
	if indirect.Settlement == nil || indirect.Claimable == nil {
		t.Fatalf("indirect operation arrival lost: %+v", indirect)
	}
	// The router transaction succeeded, but success alone is not a claim.
	if indirect.Claim != nil {
		t.Fatalf("claim inferred without call evidence: %+v", indirect.Claim)
	}
	// Absence of a claim is unknown, not verified, across the whole range.
	gaps, err := f.store.NeoXClaimCoverageGaps(100, 103)
	if err != nil || len(gaps) != 1 || gaps[0] != (datastore.NeoXClaimTraceGap{FromHeight: 100, ToHeight: 102, Reason: "trace_unsupported"}) {
		t.Fatalf("claim coverage gap: %+v %v", gaps, err)
	}
	// Enrichment against the same endpoint is suppressed by the capability
	// cache: no extra trace request, gap kept, checkpoint untouched.
	f.idx.enrichClaimTraceGap(ctx)
	if f.rpc.count("debug_traceBlockByNumber") != 1 {
		t.Fatal("enrichment re-probed a known unsupported endpoint")
	}
	if gaps, _ := f.store.NeoXClaimCoverageGaps(100, 103); len(gaps) != 1 || f.checkpoint(t) != 103 {
		t.Fatalf("enrichment changed coverage or checkpoint: %+v", gaps)
	}
}

// A temporary trace failure keeps the claim outcome unresolved, and later
// enrichment recovers it idempotently without moving the live checkpoint.
func TestNeoXTemporaryTraceFailureRecoversLater(t *testing.T) {
	f := newOptionalTraceFixture(t, "fail")
	ctx := context.Background()
	if err := f.idx.processBlockRange(ctx, 100, 102); err != nil {
		t.Fatal(err)
	}
	if n := f.rpc.count("debug_traceBlockByNumber"); n != 1 {
		t.Fatalf("failed tracing retried %d times within one range", n)
	}
	if f.lifecycle(t, f.indirect).Claim != nil {
		t.Fatal("claim inferred while traces were unavailable")
	}
	if gaps, _ := f.store.ListNeoXClaimTraceGaps(); len(gaps) != 1 || gaps[0].Reason != "trace_unavailable" {
		t.Fatalf("gap: %+v", gaps)
	}
	directBefore := *f.lifecycle(t, f.direct).Claim

	// Still failing: the gap stays and the next attempt backs off.
	f.idx.enrichClaimTraceGap(ctx)
	f.idx.enrichClaimTraceGap(ctx)
	if n := f.rpc.count("debug_traceBlockByNumber"); n != 2 {
		t.Fatalf("enrichment did not back off: %d trace calls", n)
	}

	f.rpc.setTraceMode("ok")
	f.idx.nextEnrich = f.idx.nextEnrich.AddDate(-1, 0, 0)
	for i := 0; i < 5; i++ {
		f.idx.enrichClaimTraceGap(ctx)
	}
	if gaps, _ := f.store.NeoXClaimCoverageGaps(100, 102); len(gaps) != 0 {
		t.Fatalf("gap not enriched: %+v", gaps)
	}
	claim := f.lifecycle(t, f.indirect).Claim
	if claim == nil || claim.TxHash != f.router.Hex() || claim.Position != "top-level/0" {
		t.Fatalf("indirect claim not recovered: %+v", claim)
	}
	if after := f.lifecycle(t, f.direct).Claim; after.TxHash != directBefore.TxHash || after.Position != directBefore.Position || !after.ObservedAt.Equal(directBefore.ObservedAt) {
		t.Fatalf("enrichment changed the direct claim: %+v", after)
	}
	if f.checkpoint(t) != 102 {
		t.Fatal("enrichment moved the live checkpoint")
	}
	// Reprocessing the same range with traces is idempotent.
	if err := f.idx.processBlockRange(ctx, 100, 102); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if gaps, _ := f.store.ListNeoXClaimTraceGaps(); len(gaps) != 0 {
		t.Fatalf("traced retry recorded a gap: %+v", gaps)
	}
	if err := f.idx.enrichClaimTraceBlock(ctx, 101); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("enriching a resolved block: %v", err)
	}
}

// Required evidence failures still stop the range, with or without traces.
func TestNeoXRequiredEvidenceStillStopsCheckpoint(t *testing.T) {
	ctx := context.Background()
	t.Run("malformed watched event", func(t *testing.T) {
		f := newOptionalTraceFixture(t, "unsupported")
		r := f.rpc.receipts[f.rootTx]
		r.Logs[1].Topics = r.Logs[1].Topics[:3]
		if err := f.idx.processBlockRange(ctx, 100, 102); err == nil {
			t.Fatal("malformed watched event accepted")
		}
		if f.checkpoint(t) != 0 {
			t.Fatal("checkpoint advanced")
		}
		if gaps, _ := f.store.ListNeoXClaimTraceGaps(); len(gaps) != 0 {
			t.Fatal("gap committed without its checkpoint")
		}
	})
	t.Run("required receipt failure", func(t *testing.T) {
		if testing.Short() {
			t.Skip("waits for the RPC retry backoff")
		}
		f := newOptionalTraceFixture(t, "unsupported")
		f.rpc.failReceipt = f.rootTx
		if err := f.idx.processBlockRange(ctx, 100, 102); err == nil {
			t.Fatal("missing receipt accepted")
		}
		if f.checkpoint(t) != 0 {
			t.Fatal("checkpoint advanced")
		}
		if _, err := f.opStore.GetLifecycle(f.direct); !errors.Is(err, datastore.ErrNotFound) {
			t.Fatalf("partial observations committed: %v", err)
		}
	})
	t.Run("conflicting claim evidence", func(t *testing.T) {
		f := newOptionalTraceFixture(t, "ok")
		if err := f.idx.processBlockRange(ctx, 100, 102); err != nil {
			t.Fatal(err)
		}
		// A different transaction for an already recorded claim conflicts.
		e := *f.lifecycle(t, f.direct).Claim
		e.TxHash = common.HexToHash("0xbad").Hex()
		state := &types.IndexerState{Chain: chainNeoX, LastBlockHeight: 103, LastBlockHash: "0x103"}
		if err := f.store.SaveNeoXLifecycleCheckpoint(state, nil, nil, []datastore.LifecycleObservation{{Identity: f.direct, Fact: datastore.FactClaim, Evidence: e}}); err == nil {
			t.Fatal("conflicting claim accepted")
		}
		if f.checkpoint(t) != 102 {
			t.Fatal("conflict advanced checkpoint")
		}
	})
}
