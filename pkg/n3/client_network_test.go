package n3

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// n3Node answers getversion and getnativecontracts with the recorded
// mainnet responses, with the network magic overridden.
func n3Node(t *testing.T, network uint32) *httptest.Server {
	t.Helper()
	raw, err := os.ReadFile("../../docs/fixtures/recovery-incidents.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Exchanges map[string][]struct {
			Method   string          `json:"method"`
			Response json.RawMessage `json:"response"`
		} `json:"exchanges"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	answers := map[string]map[string]any{}
	for _, e := range f.Exchanges["neo_n3"] {
		if e.Method == "getversion" || e.Method == "getnativecontracts" {
			var resp map[string]any
			_ = json.Unmarshal(e.Response, &resp)
			answers[e.Method] = resp
		}
	}
	if len(answers) != 2 {
		t.Fatal("recorded getversion/getnativecontracts missing")
	}
	answers["getversion"]["result"].(map[string]any)["protocol"].(map[string]any)["network"] = network
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		recorded, ok := answers[req.Method]
		if !ok {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": recorded["result"]})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestN3ClientRefusesMixedNetworks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mainnet, testnet := n3Node(t, 860833102), n3Node(t, 894710606)
	if _, err := NewClient(context.Background(), []string{mainnet.URL, testnet.URL}, 4, logger, 0); err == nil || !strings.Contains(err.Error(), "different networks") {
		t.Fatalf("mixed endpoints accepted: %v", err)
	}
	c, err := NewClient(context.Background(), []string{mainnet.URL, n3Node(t, 860833102).URL}, 4, logger, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if n, _ := c.Network(context.Background()); n != 860833102 {
		t.Fatalf("network %d", n)
	}
}
