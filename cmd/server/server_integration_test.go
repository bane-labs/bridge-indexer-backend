//go:build integration

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/api"
	"github.com/bane-labs/bridge-indexer-backend/internal/indexer"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// TestServerStartup_BootAndShutdown wires all components together (store, RPC
// clients, indexers, monitor, Echo) and verifies the HTTP API responds before
// performing a clean shutdown.
func TestServerStartup_BootAndShutdown(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)
	logger := slog.Default()

	// 1. In-memory store
	store, opStore := testutil.NewTestStore(t)

	// 2. RPC clients
	n3Client, err := n3.NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, logger)
	if err != nil {
		t.Skipf("skipping: N3 client creation failed: %v", err)
	}
	t.Cleanup(func() { n3Client.Close() })

	neoxClient, err := neox.NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, logger)
	if err != nil {
		t.Skipf("skipping: NeoX client creation failed: %v", err)
	}
	t.Cleanup(func() { neoxClient.Close() })

	// 3. Event processors
	n3EventProc, err := indexer.NewN3EventProcessor(
		opStore,
		testutil.N3NeoXBridgeHash(),
		testutil.N3MessageBridgeHash(),
		logger,
	)
	if err != nil {
		t.Fatalf("NewN3EventProcessor: %v", err)
	}
	neoxEventProc := indexer.NewNeoXEventProcessor(
		opStore,
		testutil.NeoXTokenBridgeAddr,
		testutil.NeoXMessageBridgeAddr,
		logger,
	)

	// 4. Indexers — created but NOT started
	n3Contracts := []string{testutil.N3NeoXBridgeHash(), testutil.N3MessageBridgeHash()}
	n3Indexer, err := indexer.NewN3Indexer(n3Client, store, opStore, nil, n3EventProc, n3Contracts, 0, 1, logger)
	if err != nil {
		t.Fatalf("NewN3Indexer: %v", err)
	}
	_ = n3Indexer

	neoxContracts := []string{testutil.NeoXTokenBridgeAddr, testutil.NeoXMessageBridgeAddr}
	neoxIndexer := indexer.NewNeoXIndexer(neoxClient, store, opStore, nil, neoxEventProc, neoxContracts, 0, 1, logger)
	_ = neoxIndexer

	// 5. Monitor components — created but NOT started
	multiAlerter := alerter.NewMultiAlerter(&alerter.NoopAlerter{})
	pauseDetector := monitor.NewPauseDetector(store, logger)
	stuckDetector := monitor.NewStuckDetector(opStore, multiAlerter, 5*time.Minute, logger)
	healthAgg := monitor.NewHealthAggregator(opStore, pauseDetector, logger)
	mon := monitor.NewMonitor(stuckDetector, pauseDetector, healthAgg, logger)
	_ = mon

	// 6. Echo + routes
	e := echo.New()
	e.HideBanner = true
	handler := api.NewHandler(store, opStore, healthAgg)
	api.RegisterRoutes(e, handler)

	// 7. Start HTTP server on random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := listener.Addr().String()
	t.Logf("server listening on %s", addr)

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && serveErr != http.ErrServerClosed {
			t.Errorf("srv.Serve: %v", serveErr)
		}
	}()

	// Give the server a moment to start accepting connections.
	time.Sleep(50 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}

	// 8. GET /health — expect 200 with valid JSON
	t.Run("health_endpoint", func(t *testing.T) {
		resp, err := client.Get("http://" + addr + "/health")
		if err != nil {
			t.Fatalf("GET /health: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("GET /health: status=%d body=%s", resp.StatusCode, body)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("GET /health: invalid JSON: %v", err)
		}
		t.Logf("GET /health: %v", result)
	})

	// 9. GET /indexer/state — expect 200
	t.Run("indexer_state_endpoint", func(t *testing.T) {
		resp, err := client.Get("http://" + addr + "/indexer/state")
		if err != nil {
			t.Fatalf("GET /indexer/state: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("GET /indexer/state: status=%d body=%s", resp.StatusCode, body)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("GET /indexer/state: invalid JSON: %v", err)
		}
		t.Logf("GET /indexer/state: %v", result)
	})

	// 10. Shutdown cleanly
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Errorf("srv.Shutdown: %v", err)
	}
}

// TestServerStartup_RPCConnectivity verifies that both N3 and NeoX RPC clients
// can connect and return block counts.
func TestServerStartup_RPCConnectivity(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	ctx := testutil.TestContext(t)
	logger := slog.Default()

	// N3 client
	n3Client, err := n3.NewClient(ctx, []string{testutil.N3Endpoint(t)}, 1, logger)
	if err != nil {
		t.Skipf("skipping: N3 client creation failed: %v", err)
	}
	t.Cleanup(func() { n3Client.Close() })

	n3Block, err := n3Client.GetBlockCount(ctx)
	if err != nil {
		t.Skipf("skipping: N3 GetBlockCount failed: %v", err)
	}
	t.Logf("N3 block count: %d", n3Block)

	if n3Block == 0 {
		t.Error("N3 block count is 0, expected > 0")
	}

	// NeoX client
	neoxClient, err := neox.NewClient(ctx, []string{testutil.NeoXEndpoint(t)}, logger)
	if err != nil {
		t.Skipf("skipping: NeoX client creation failed: %v", err)
	}
	t.Cleanup(func() { neoxClient.Close() })

	neoxBlock, err := neoxClient.BlockNumber(ctx)
	if err != nil {
		t.Skipf("skipping: NeoX BlockNumber failed: %v", err)
	}
	t.Logf("NeoX block number: %d", neoxBlock)

	if neoxBlock == 0 {
		t.Error("NeoX block number is 0, expected > 0")
	}
}
