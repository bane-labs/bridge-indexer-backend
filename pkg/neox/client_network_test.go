package neox

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// networkEndpoint answers like a node on the given chain. While down it
// fails every request; other counts the requests besides eth_chainId.
type networkEndpoint struct {
	chainID string
	down    atomic.Bool
	other   atomic.Int32
}

func (n *networkEndpoint) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(body, &req)
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = n.chainID
		case "eth_blockNumber":
			n.other.Add(1)
			resp["result"] = "0x10"
		default:
			n.other.Add(1)
			resp["error"] = map[string]any{"code": -32601, "message": "unexpected"}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClientRefusesMixedNetworks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mainnet := (&networkEndpoint{chainID: "0xba93"}).serve(t)
	testnet := (&networkEndpoint{chainID: "0xba9304"}).serve(t)
	if _, err := NewClient(context.Background(), []string{mainnet.URL, testnet.URL}, logger, 0); err == nil || !strings.Contains(err.Error(), "different networks") {
		t.Fatalf("mixed endpoints accepted: %v", err)
	}
	c, err := NewClient(context.Background(), []string{mainnet.URL}, logger, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := c.ChainID(context.Background()); id != 47763 {
		t.Fatalf("chain ID %d", id)
	}
}

// An endpoint unreachable at construction is checked when failover first
// reaches it; one on another network is never used.
func TestFailoverSkipsEndpointOnAnotherNetwork(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	primary := &networkEndpoint{chainID: "0xba93"}
	stray := &networkEndpoint{chainID: "0xba9304"}
	stray.down.Store(true)
	p, s := primary.serve(t), stray.serve(t)
	c, err := NewClient(ctx, []string{p.URL, s.URL}, logger, 0)
	if err != nil {
		t.Fatalf("construction with an unreachable endpoint: %v", err)
	}
	stray.down.Store(false) // comes up, on testnet
	if err := c.failover(ctx); err != nil {
		t.Fatalf("failover: %v", err)
	}
	if c.endpoints[c.active] != p.URL {
		t.Fatalf("failover switched to an endpoint on another network: %s", c.endpoints[c.active])
	}
	if n, err := c.BlockNumber(ctx); err != nil || n != 16 {
		t.Fatalf("block number: %d %v", n, err)
	}
	if stray.other.Load() != 0 {
		t.Fatalf("endpoint on another network served %d requests", stray.other.Load())
	}
	primary.down.Store(true)
	if err := c.failover(ctx); err == nil {
		t.Fatal("failover to another network accepted when the only matching endpoint is down")
	}
}
