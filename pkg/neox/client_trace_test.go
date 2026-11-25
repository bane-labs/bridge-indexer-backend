package neox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// traceEndpoint serves eth_blockNumber and either a trace or -32601.
func traceEndpoint(t *testing.T, supportsTrace bool, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("bad request: %s", body)
			return
		}
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		switch {
		case req.Method == "eth_blockNumber":
			resp["result"] = "0x10"
		case req.Method == "eth_chainId":
			resp["result"] = "0xba93"
		case req.Method == "debug_traceBlockByNumber" && supportsTrace:
			calls.Add(1)
			resp["result"] = []any{}
		case req.Method == "debug_traceBlockByNumber":
			calls.Add(1)
			resp["error"] = map[string]any{"code": -32601, "message": "the method debug_traceBlockByNumber does not exist/is not available"}
		default:
			resp["error"] = map[string]any{"code": -32601, "message": "unexpected " + req.Method}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTraceUnsupportedIsCachedPerEndpoint(t *testing.T) {
	ctx := context.Background()
	var plainCalls, tracingCalls atomic.Int32
	plain := traceEndpoint(t, false, &plainCalls)
	tracing := traceEndpoint(t, true, &tracingCalls)
	c, err := NewClient(ctx, []string{plain.URL, tracing.URL}, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := c.TraceBlock(ctx, 1); !errors.Is(err, ErrTraceUnsupported) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if got := plainCalls.Load(); got != 1 {
		t.Fatalf("unsupported endpoint traced %d times, want 1 probe", got)
	}
	if c.active != 0 {
		t.Fatal("optional trace failure moved core indexing to another endpoint")
	}
	if n, err := c.BlockNumber(ctx); err != nil || n != 16 {
		t.Fatalf("standard RPC unusable after trace failure: %d %v", n, err)
	}
	if err := c.failover(ctx); err != nil {
		t.Fatal(err)
	}
	if traces, err := c.TraceBlock(ctx, 1); err != nil || traces == nil {
		t.Fatalf("tracing disabled on unrelated endpoint: %v", err)
	}
	if tracingCalls.Load() != 1 {
		t.Fatal("tracing endpoint was not queried")
	}
	if err := c.failover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TraceBlock(ctx, 1); !errors.Is(err, ErrTraceUnsupported) || plainCalls.Load() != 1 {
		t.Fatalf("cached capability lost after failover back: %v calls=%d", err, plainCalls.Load())
	}
	c.traceMu.Lock()
	c.traceUnsupported[plain.URL] = c.traceUnsupported[plain.URL].Add(-2 * traceReprobeInterval)
	c.traceMu.Unlock()
	if _, err := c.TraceBlock(ctx, 1); !errors.Is(err, ErrTraceUnsupported) || plainCalls.Load() != 2 {
		t.Fatalf("expired capability was not re-probed: %v calls=%d", err, plainCalls.Load())
	}
}
