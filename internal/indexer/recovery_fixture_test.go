package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// Mainnet bridge contracts, as in .env.example.
const (
	mainnetNeoXTokenBridge   = "0x1212000000000000000000000000000000000004"
	mainnetNeoXMessageBridge = "0x1212000000000000000000000000000000000009"
	mainnetN3TokenBridge     = "bb19cfc864b73159277e1fd39694b3fd5fc613d2"
	mainnetN3MessageBridge   = "de9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11"
	recoveryFixturePath      = "../../docs/fixtures/recovery-incidents.json"
)

// rpcExchange is one captured JSON-RPC call against a public mainnet node.
type rpcExchange struct {
	Method   string          `json:"method"`
	Params   json.RawMessage `json:"params"`
	Response json.RawMessage `json:"response"` // the full JSON-RPC response object
}

type recoveryCase struct {
	Chain       string `json:"chain"`
	Start       uint64 `json:"start"`
	Target      uint64 `json:"target"`
	EventFilter string `json:"event_filter,omitempty"` // fake plugin, see eventFilterFor
	Reconcile   bool   `json:"reconcile,omitempty"`    // run Neo X claim reconciliation after the replay
}

type recoveryFixture struct {
	Description string                   `json:"description"`
	CapturedOn  string                   `json:"captured_on"`
	Endpoints   map[string]string        `json:"endpoints"`
	Cases       map[string]*recoveryCase `json:"cases"`
	// Exchanges per chain, deduplicated across cases.
	Exchanges map[string][]rpcExchange `json:"exchanges"`
}

func rpcKey(method string, params json.RawMessage) string {
	var v any
	_ = json.Unmarshal(params, &v)
	canonical, _ := json.Marshal(v)
	return method + string(canonical)
}

// rpcServer forwards to upstream and records (capture), or answers only
// from recorded exchanges (replay). Unknown calls fail the test in replay.
func rpcServer(t *testing.T, upstream string, recorded []rpcExchange) (*httptest.Server, func() []rpcExchange) {
	t.Helper()
	var mu sync.Mutex
	seen := map[string]bool{}
	var captured []rpcExchange
	answers := map[string]json.RawMessage{}
	for _, e := range recorded {
		answers[rpcKey(e.Method, e.Params)] = e.Response
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "batch requests are not supported", http.StatusBadRequest)
			return
		}
		key := rpcKey(req.Method, req.Params)
		var resp json.RawMessage
		if upstream == "" {
			var ok bool
			if resp, ok = answers[key]; !ok {
				t.Errorf("unrecorded RPC %s %s", req.Method, req.Params)
				http.Error(w, "unrecorded", http.StatusNotFound)
				return
			}
		} else {
			res, err := http.Post(upstream, "application/json", bytes.NewReader(body))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			resp, _ = io.ReadAll(res.Body)
			res.Body.Close()
			var generic map[string]json.RawMessage
			_ = json.Unmarshal(resp, &generic)
			delete(generic, "id")
			resp, _ = json.Marshal(generic)
			mu.Lock()
			if !seen[key] {
				seen[key] = true
				captured = append(captured, rpcExchange{Method: req.Method, Params: req.Params, Response: resp})
			}
			mu.Unlock()
		}
		var generic map[string]json.RawMessage
		_ = json.Unmarshal(resp, &generic)
		generic["id"] = req.ID
		_ = json.NewEncoder(w).Encode(generic)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []rpcExchange {
		mu.Lock()
		defer mu.Unlock()
		return captured
	}
}

func openRecoveryStore(t *testing.T) (*datastore.Store, *datastore.OperationStore) {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return store, datastore.NewOperationStore(store, datastore.NewBridgeStateStore(store))
}

// newFixtureRecoverer connects the real RPC clients to url.
func newFixtureRecoverer(t *testing.T, chain, url string, store *datastore.Store, opStore *datastore.OperationStore) *Recoverer {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()
	if chain == chainNeoX {
		client, err := neox.NewClient(ctx, []string{url}, logger, 0)
		if err != nil {
			t.Fatal(err)
		}
		return NewNeoXRecoverer(client, store, opStore, mainnetNeoXTokenBridge, mainnetNeoXMessageBridge, 1, logger)
	}
	client, err := n3.NewClient(ctx, []string{url}, 1, logger, 0)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewN3Recoverer(client, store, opStore, mainnetN3TokenBridge, mainnetN3MessageBridge, 1, logger)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func loadRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	raw, err := os.ReadFile(recoveryFixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var f recoveryFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return &f
}

// replayRecoveryCase runs the real recoverer for a captured case against a
// server that answers only with the recorded mainnet responses.
func replayRecoveryCase(t *testing.T, f *recoveryFixture, name string, store *datastore.Store, opStore *datastore.OperationStore) {
	t.Helper()
	c := f.Cases[name]
	if c == nil {
		t.Fatalf("missing fixture case %s", name)
	}
	srv, _ := rpcServer(t, "", f.Exchanges[c.Chain])
	r := newFixtureRecoverer(t, c.Chain, srv.URL, store, opStore)
	if c.EventFilter != "" {
		filter := eventFilterFor(c.EventFilter)
		filter.upstream = srv.URL
		useFakeEventFilter(t, r, filter)
	}
	ctx := context.Background()
	if err := r.Begin(ctx, c.Start, c.Target, c.EventFilter != ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx, RecoveryRunOptions{Batch: 100000}, nil); err != nil {
		t.Fatal(err)
	}
	if c.Reconcile {
		if _, err := r.ReconcileClaims(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// fakeEventFilter stands in for a node serving the AxLabs event filter
// plugin. No such endpoint is reachable from the test environment, so its
// answers are synthetic; the blocks it points to are real mainnet blocks.
type fakeEventFilter struct {
	mu        sync.Mutex
	liveFrom  *uint32
	last      uint32
	blocks    map[string][]uint32 // contract ("0x"+LE hash) -> heights
	txs       map[uint32]string   // height -> listed transaction
	pageLimit int                 // server-side result cap; negative means zero
	queries   int
	upstream  string // standard N3 methods are forwarded here
	wrongHash bool   // answer getblockhash as a node on another chain
}

func (f *fakeEventFilter) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		var probe struct{ Method string }
		_ = json.Unmarshal(body, &probe)
		if probe.Method == "getblockhash" && f.wrongHash {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": "0x" + strings.Repeat("ab", 32)})
			return
		}
		if probe.Method != "geteventfilterstatus" && probe.Method != "geteventfilterlogs" && f.upstream != "" {
			res, err := http.Post(f.upstream, "application/json", bytes.NewReader(body))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer res.Body.Close()
			_, _ = io.Copy(w, res.Body)
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		defer f.mu.Unlock()
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "geteventfilterstatus":
			resp["result"] = map[string]any{"liveIndexedFrom": f.liveFrom, "lastIndexedBlock": f.last, "replayInProgress": false}
		case "geteventfilterlogs":
			f.queries++
			contract, from, to := req.Params[0].(string), uint32(req.Params[2].(float64)), uint32(req.Params[3].(float64))
			if to == 0 {
				t.Errorf("toBlock 0 (latest) sent to the event filter")
			}
			limit := int(req.Params[4].(float64))
			if f.pageLimit != 0 {
				limit = min(limit, max(f.pageLimit, 0)) // negative: a zero result cap
			}
			logs := []map[string]any{}
			for _, h := range f.blocks[contract] {
				if h >= from && h <= to && len(logs) < limit {
					logs = append(logs, map[string]any{"blockindex": h, "txhash": f.txs[h], "contract": contract, "eventname": "x", "index": 0})
				}
			}
			resp["result"] = logs
		default:
			resp["error"] = map[string]any{"code": -32601, "message": "method not found"}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// eventFilterFor returns a fake plugin listing real mainnet bridge events:
// "live" indexed live from block 1; "replayed" only from 13051517, so the
// NDMEME block lies in its replayed history.
func eventFilterFor(name string) *fakeEventFilter {
	live := uint32(1)
	if name == "replayed" {
		live = 13051517
	}
	return &fakeEventFilter{liveFrom: &live, last: 15_000_000,
		blocks: map[string][]uint32{"0x" + mainnetN3TokenBridge: {13051516}, "0x" + mainnetN3MessageBridge: {14720004}},
		txs: map[uint32]string{13051516: "0x7f3a35997b347304c5f3de909e20c4c8ddc4fb17b488646ab36dbf820f2cc4a0",
			14720004: "0x757635d8b27c8ced0a6b667e558a7d9c1696d8287dc5471900efb9349f956761"}}
}

func useFakeEventFilter(t *testing.T, r *Recoverer, f *fakeEventFilter) {
	t.Helper()
	srv := f.serve(t)
	filter, err := n3.NewEventFilter(context.Background(), []string{srv.URL}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r.UseEventFilter(filter)
}
