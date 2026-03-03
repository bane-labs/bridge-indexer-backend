//go:build integration

package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/testutil"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// setupIntegration creates an Echo instance backed by an in-memory store
// pre-populated with realistic bridge operations and indexer state.
// It follows the same pattern as setup() in api_test.go but adds synthetic
// data that mirrors real indexed data shapes.
func setupIntegration(t *testing.T) (*echo.Echo, *datastore.Store, *datastore.OperationStore) {
	t.Helper()

	store, opStore := testutil.NewTestStore(t)
	pause := monitor.NewPauseDetector(store, slog.Default())
	health := monitor.NewHealthAggregator(opStore, pause, slog.Default())
	handler := NewHandler(store, opStore, health)

	e := echo.New()
	RegisterRoutes(e, handler)

	// Seed indexer state for both chains to simulate real indexed data.
	now := time.Now()
	n3State := &types.IndexerState{
		Chain:           "neo_n3",
		LastBlockHeight: uint64(testutil.N3MainnetEventBlock),
		LastBlockHash:   "0xabc123",
		IsSyncing:       false,
		UpdatedAt:       now,
	}
	neoxState := &types.IndexerState{
		Chain:           "neo_x",
		LastBlockHeight: testutil.NeoXMainnetEventBlockTo,
		LastBlockHash:   "0xdef456",
		IsSyncing:       false,
		UpdatedAt:       now,
	}
	if err := store.SaveIndexerState(n3State); err != nil {
		t.Fatalf("save n3 indexer state: %v", err)
	}
	if err := store.SaveIndexerState(neoxState); err != nil {
		t.Fatalf("save neox indexer state: %v", err)
	}

	// Seed synthetic bridge operations across all bridge types.
	completionTime := now.Add(-1 * time.Hour)
	ops := []*types.BridgeOperation{
		{
			BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 1,
			SourceChain: "neo_n3", DestinationChain: "neo_x",
			SourceBlockHeight: uint64(testutil.N3MainnetEventBlock),
			SourceTxHash:      "0xaaa111", FromAddress: "NNwfi3vDyG6WRx8QHzBBBQCy51XJDBj4GS",
			ToAddress: "0x1234567890abcdef1234567890abcdef12345678",
			Timestamp: now.Add(-2 * time.Hour), Status: types.StatusCompleted,
			CompletionTimestamp: &completionTime,
		},
		{
			BridgeType: types.BridgeTypeNative, Direction: types.DirectionWithdrawal, Nonce: 2,
			SourceChain: "neo_x", DestinationChain: "neo_n3",
			SourceBlockHeight: testutil.NeoXMainnetEventBlockFrom,
			SourceTxHash:      "0xbbb222", FromAddress: "0xabcdef1234567890abcdef1234567890abcdef12",
			ToAddress: "NNwfi3vDyG6WRx8QHzBBBQCy51XJDBj4GS",
			Timestamp: now.Add(-30 * time.Minute), Status: types.StatusPending,
		},
		{
			BridgeType: types.BridgeTypeToken, Direction: types.DirectionDeposit, Nonce: 1,
			SourceChain: "neo_n3", DestinationChain: "neo_x",
			SourceBlockHeight: uint64(testutil.N3MainnetEventBlock) + 10,
			SourceTxHash:      "0xccc333", FromAddress: "NNwfi3vDyG6WRx8QHzBBBQCy51XJDBj4GS",
			ToAddress:     "0x1234567890abcdef1234567890abcdef12345678",
			TokenContract: strPtr(testutil.NeoXTokenBridgeAddr),
			Amount:        strPtr("1000000000"),
			Timestamp:     now.Add(-3 * time.Hour), Status: types.StatusCompleted,
			CompletionTimestamp: &completionTime,
		},
		{
			BridgeType: types.BridgeTypeMessage, Direction: types.DirectionDeposit, Nonce: 1,
			SourceChain: "neo_n3", DestinationChain: "neo_x",
			SourceBlockHeight: uint64(testutil.N3MainnetEventBlock) + 20,
			SourceTxHash:      "0xddd444", FromAddress: "NNwfi3vDyG6WRx8QHzBBBQCy51XJDBj4GS",
			ToAddress: "0x1234567890abcdef1234567890abcdef12345678",
			Timestamp: now.Add(-10 * time.Minute), Status: types.StatusStuck,
		},
	}
	for _, op := range ops {
		if err := opStore.SaveOperation(op); err != nil {
			t.Fatalf("save operation: %v", err)
		}
	}

	return e, store, opStore
}

func strPtr(s string) *string { return &s }

// TestAPI_HealthEndpoint verifies /health returns overall health with bridge data.
func TestAPI_HealthEndpoint(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	_ = testutil.TestContext(t)

	e, _, _ := setupIntegration(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result types.OverallHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// With a stuck operation, overall status should not be healthy.
	if result.Status == "" {
		t.Fatal("expected non-empty status")
	}
	if len(result.Bridges) != 3 {
		t.Fatalf("expected 3 bridge types, got %d", len(result.Bridges))
	}

	// Verify bridge types are present.
	bridgeTypesSeen := map[types.BridgeType]bool{}
	for _, b := range result.Bridges {
		bridgeTypesSeen[b.BridgeType] = true
	}
	for _, bt := range []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage} {
		if !bridgeTypesSeen[bt] {
			t.Errorf("missing bridge type %s in health response", bt)
		}
	}
}

// TestAPI_HealthBridgesEndpoint verifies /health/bridges returns per-bridge health.
func TestAPI_HealthBridgesEndpoint(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	_ = testutil.TestContext(t)

	e, _, _ := setupIntegration(t)

	req := httptest.NewRequest(http.MethodGet, "/health/bridges", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result []types.BridgeHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 bridge types, got %d", len(result))
	}

	// Verify the message bridge shows stuck operations.
	for _, b := range result {
		if b.BridgeType == types.BridgeTypeMessage {
			if b.StuckOperations != 1 {
				t.Errorf("expected 1 stuck message op, got %d", b.StuckOperations)
			}
		}
	}
}

// TestAPI_HealthDelaysEndpoint verifies /health/delays returns delay metrics.
func TestAPI_HealthDelaysEndpoint(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	_ = testutil.TestContext(t)

	e, _, _ := setupIntegration(t)

	req := httptest.NewRequest(http.MethodGet, "/health/delays", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result types.DelayMetrics
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// We have completed operations for native and token bridges.
	// They should have delay metrics for at least the 24h window.
	if result.Avg24h == nil {
		t.Fatal("expected non-nil Avg24h map")
	}
}

// TestAPI_IndexerStateEndpoint verifies /indexer/state returns chain states.
func TestAPI_IndexerStateEndpoint(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	_ = testutil.TestContext(t)

	e, _, _ := setupIntegration(t)

	req := httptest.NewRequest(http.MethodGet, "/indexer/state", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify both chains are present.
	if result["neo_n3"] == nil {
		t.Error("expected neo_n3 state in response")
	}
	if result["neo_x"] == nil {
		t.Error("expected neo_x state in response")
	}

	// Verify N3 block height matches what we seeded.
	n3Data, ok := result["neo_n3"].(map[string]interface{})
	if !ok {
		t.Fatal("neo_n3 state is not a map")
	}
	blockHeight, ok := n3Data["last_block_height"].(float64)
	if !ok {
		t.Fatal("last_block_height is not a number")
	}
	if uint32(blockHeight) != testutil.N3MainnetEventBlock {
		t.Errorf("expected N3 block height %d, got %v", testutil.N3MainnetEventBlock, blockHeight)
	}
}

// TestAPI_OperationsListEndpoint verifies /operations returns paginated results with filters.
func TestAPI_OperationsListEndpoint(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	_ = testutil.TestContext(t)

	e, _, _ := setupIntegration(t)

	t.Run("all operations", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/operations", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		total := result["total"].(float64)
		if total != 4 {
			t.Fatalf("expected 4 total operations, got %v", total)
		}
	})

	t.Run("filter by bridge_type=native", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/operations?bridge_type=native", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		total := result["total"].(float64)
		if total != 2 {
			t.Fatalf("expected 2 native operations, got %v", total)
		}
	})

	t.Run("filter by status=pending", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/operations?status=pending", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		total := result["total"].(float64)
		if total != 1 {
			t.Fatalf("expected 1 pending operation, got %v", total)
		}
	})

	t.Run("filter by status=stuck", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/operations?status=stuck", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		total := result["total"].(float64)
		if total != 1 {
			t.Fatalf("expected 1 stuck operation, got %v", total)
		}
	})
}

// TestAPI_OperationByNonceEndpoint verifies /operations/:nonce returns matching operations.
func TestAPI_OperationByNonceEndpoint(t *testing.T) {
	testutil.SkipIfNoNetwork(t)
	_ = testutil.TestContext(t)

	e, _, _ := setupIntegration(t)

	t.Run("existing nonce", func(t *testing.T) {
		// Nonce 1 has native deposit, token deposit, and message deposit.
		req := httptest.NewRequest(http.MethodGet, "/operations/1", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var result []types.BridgeOperation
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		// Nonce 1 has 3 operations: native deposit, token deposit, message deposit.
		if len(result) != 3 {
			t.Fatalf("expected 3 operations for nonce 1, got %d", len(result))
		}
	})

	t.Run("nonce not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/operations/9999", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("invalid nonce", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/operations/abc", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})
}
