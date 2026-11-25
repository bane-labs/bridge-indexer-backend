package n3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// pluginNode answers like a node with the AxLabs event filter plugin, capping
// each page at cap events; without the plugin it answers -32601.
func pluginNode(t *testing.T, plugin bool, events map[string][]uint32, cap int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		switch {
		case !plugin:
			resp["error"] = map[string]any{"code": -32601, "message": "Method not found"}
		case req.Method == "geteventfilterstatus":
			resp["result"] = map[string]any{"liveIndexedFrom": 10, "lastIndexedBlock": 1000}
		case req.Method == "geteventfilterlogs":
			from, to := uint32(req.Params[2].(float64)), uint32(req.Params[3].(float64))
			logs := []map[string]any{}
			for _, h := range events[req.Params[0].(string)] {
				if h >= from && h <= to && len(logs) < min(cap, int(req.Params[4].(float64))) {
					logs = append(logs, map[string]any{"blockindex": h, "txhash": fmt.Sprintf("0xtx%d", h)})
				}
			}
			resp["result"] = logs
		case req.Method == "gettransactionheight":
			var h uint32
			_, _ = fmt.Sscanf(req.Params[0].(string), "0xtx%d", &h)
			resp["result"] = h
		case req.Method == "getapplicationlog":
			resp["result"] = map[string]any{"executions": []any{map[string]any{"vmstate": "HALT", "notifications": []any{map[string]any{"contract": "0xaa"}}}}}
		case req.Method == "getblockhash":
			resp["result"] = fmt.Sprintf("0x%064x", int(req.Params[0].(float64)))
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestEventFilterUsesOnlyThePluginEndpoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	backup := pluginNode(t, false, nil, 0)
	primary := pluginNode(t, true, map[string][]uint32{"0xaa": {12, 12, 13, 40, 41, 42, 900}, "0xbb": {13, 500}}, 2)
	f, err := NewEventFilter(context.Background(), []string{backup.URL, primary.URL}, logger)
	if err != nil || f.Endpoint() != primary.URL {
		t.Fatalf("plugin endpoint: %v %v", f, err)
	}
	// A two-event page cap must not hide later blocks.
	got, err := f.BlocksWithEvents(context.Background(), []string{"aa", "bb"}, 12, 600)
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint32{12, 13, 40, 41, 42, 500}; len(got) != len(want) || got[0] != 12 || got[5] != 500 {
		t.Fatalf("blocks %v, want %v", got, want)
	}
	st, _ := f.Status(context.Background())
	if !st.LiveCovers(10, 1000) || st.LiveCovers(9, 20) || st.LiveCovers(500, 1001) {
		t.Fatalf("live window: %+v", st)
	}
	if _, err := NewEventFilter(context.Background(), []string{backup.URL}, logger); err == nil {
		t.Fatal("backup without the plugin accepted")
	}
	if _, err := f.BlocksWithEvents(context.Background(), []string{"aa"}, 0, 0); err == nil {
		t.Fatal("toBlock 0 (latest) accepted")
	}
	if err := f.Verify(context.Background(), []string{"aa"}); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if h, err := f.BlockHash(context.Background(), 12); err != nil || h != fmt.Sprintf("%064x", 12) {
		t.Fatalf("block hash: %s %v", h, err)
	}
	// A plugin with a zero result cap answers [] to everything: the control fails.
	zero, err := NewEventFilter(context.Background(), []string{pluginNode(t, true, map[string][]uint32{"0xaa": {12}}, 0).URL}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := zero.BlocksWithEvents(context.Background(), []string{"aa"}, 12, 600); err != nil || len(got) != 0 {
		t.Fatalf("zero cap: %v %v", got, err)
	}
	if err := zero.Verify(context.Background(), []string{"aa"}); err == nil {
		t.Fatal("empty-answering plugin passed the positive control")
	}
	// An event whose application log lacks the contract fails it too.
	if err := f.Verify(context.Background(), []string{"bb"}); err == nil {
		t.Fatal("listed event without a matching notification passed")
	}
}
