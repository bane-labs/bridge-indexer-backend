//go:build capture

package indexer

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestCaptureRecoveryFixtures regenerates docs/fixtures/recovery-incidents.json
// by running the real recoverer against public mainnet RPC nodes through a
// recording proxy:
//
//	go test -tags capture -run TestCaptureRecoveryFixtures ./internal/indexer
func TestCaptureRecoveryFixtures(t *testing.T) {
	neoxRPC := envOr("CAPTURE_NEOX_RPC", "https://mainnet-1.rpc.banelabs.org")
	n3RPC := envOr("CAPTURE_N3_RPC", "http://seed1.neo.org:10332")
	f := recoveryFixture{
		Endpoints:   map[string]string{chainNeoX: neoxRPC, chainN3: n3RPC},
		Exchanges:   map[string][]rpcExchange{},
		Description: "Mainnet RPC exchanges recorded while the recovery command replayed each incident interval. Tests replay them through the same code; every request can be re-sent to a mainnet node to re-verify the response.",
		CapturedOn:  time.Now().UTC().Format(time.RFC3339),
		Cases: map[string]*recoveryCase{
			// NDMEME deposit nonce 27: N3 source and Neo X destination, each in its own heights.
			"ndmeme_27_n3_source":      {Chain: chainN3, Start: 13051516, Target: 13051516},
			"ndmeme_27_neox_dest":      {Chain: chainNeoX, Start: 0x74acee, Target: 0x74acee},
			"neo_withdrawal_11_n3":     {Chain: chainN3, Start: 6818342, Target: 6818342},
			"message_withdrawal_14_n3": {Chain: chainN3, Start: 14720004, Target: 14720004},
			"message_withdrawal_15_n3": {Chain: chainN3, Start: 14720342, Target: 14720342},
			// Located through a (fake) event filter: only the boundaries and
			// the listed block are fetched from mainnet.
			"ndmeme_27_n3_located": {Chain: chainN3, Start: 13051400, Target: 13051600, EventFilter: "live"},
			// The same block, but in the fake plugin's replayed history.
			"ndmeme_27_n3_replayed": {Chain: chainN3, Start: 13051516, Target: 13051516, EventFilter: "replayed"},
		},
	}
	seen := map[string]bool{}
	for name, c := range f.Cases {
		srv, captured := rpcServer(t, f.Endpoints[c.Chain], nil)
		store, opStore := openRecoveryStore(t)
		r := newFixtureRecoverer(t, c.Chain, srv.URL, store, opStore)
		if c.EventFilter != "" {
			filter := eventFilterFor(c.EventFilter)
			filter.upstream = srv.URL // recorded like every other mainnet call
			useFakeEventFilter(t, r, filter)
		}
		if err := r.Begin(t.Context(), c.Start, c.Target, c.EventFilter != ""); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := r.Run(t.Context(), RecoveryRunOptions{Batch: 100}, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, e := range captured() {
			if key := c.Chain + rpcKey(e.Method, e.Params); !seen[key] {
				seen[key] = true
				f.Exchanges[c.Chain] = append(f.Exchanges[c.Chain], e)
			}
		}
	}
	raw, err := json.MarshalIndent(f, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recoveryFixturePath, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
