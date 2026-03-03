//go:build integration

package testutil

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
)

// Known mainnet block heights containing bridge events
const (
	// N3 mainnet: block known to contain NeoXBridge Deposit events
	N3MainnetEventBlock uint32 = 6877325
	// NeoX mainnet: block range known to contain TokenBridge events
	NeoXMainnetEventBlockFrom uint64 = 3200
	NeoXMainnetEventBlockTo   uint64 = 3700

	// Mainnet contract hashes (LE, no 0x prefix)
	N3NeoXBridgeHashMainnet    = "bb19cfc864b73159277e1fd39694b3fd5fc613d2"
	N3MessageBridgeHashMainnet = "de9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11"

	// Testnet contract hashes
	N3NeoXBridgeHashTestnet    = "2ba94444d43c9a084a5660982a9f95f43f07422e"
	N3MessageBridgeHashTestnet = "de9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11"

	// NeoX contract addresses (EVM, with 0x prefix)
	NeoXTokenBridgeAddr   = "0x1212000000000000000000000000000000000004"
	NeoXMessageBridgeAddr = "0x1212000000000000000000000000000000000009"
)

// IsTestnet returns true if INTEGRATION_TEST_NETWORK == "testnet"
func IsTestnet() bool {
	return os.Getenv("INTEGRATION_TEST_NETWORK") == "testnet"
}

// N3Endpoint returns the N3 RPC endpoint from env or default mainnet.
func N3Endpoint(t *testing.T) string {
	t.Helper()
	if ep := os.Getenv("INTEGRATION_N3_RPC"); ep != "" {
		return ep
	}
	return "http://seed1.neo.org:10332"
}

// NeoXEndpoint returns the NeoX RPC endpoint from env or default mainnet.
func NeoXEndpoint(t *testing.T) string {
	t.Helper()
	if ep := os.Getenv("INTEGRATION_NEOX_RPC"); ep != "" {
		return ep
	}
	return "https://mainnet-1.rpc.banelabs.org"
}

// N3NeoXBridgeHash returns the NeoXBridge script hash for the current network.
func N3NeoXBridgeHash() string {
	if IsTestnet() {
		return N3NeoXBridgeHashTestnet
	}
	return N3NeoXBridgeHashMainnet
}

// N3MessageBridgeHash returns the MessageBridge script hash for the current network.
func N3MessageBridgeHash() string {
	return N3MessageBridgeHashMainnet // same on both networks
}

// SkipIfNoNetwork pings the N3 RPC endpoint and skips the test if unreachable.
// Always use t.Skip(), never t.Fatal() for connectivity issues.
func SkipIfNoNetwork(t *testing.T) {
	t.Helper()
	ep := N3Endpoint(t)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(ep)
	if err != nil {
		t.Skipf("skipping integration test: N3 RPC unreachable at %s: %v", ep, err)
		return
	}
	resp.Body.Close()
}

// TestContext returns a context with 30s timeout and registers cleanup.
func TestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// NewTestStore creates an in-memory BadgerDB store and operation store for testing.
// Registers t.Cleanup to close the database.
func NewTestStore(t *testing.T) (*datastore.Store, *datastore.OperationStore) {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := datastore.NewStoreFromDB(db, slog.Default())
	return store, datastore.NewOperationStore(store)
}
