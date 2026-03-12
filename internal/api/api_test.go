package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func setup(t *testing.T) (*echo.Echo, *Handler, *datastore.OperationStore) {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	store := datastore.NewStoreFromDB(db, slog.Default())
	opStore := datastore.NewOperationStore(store, nil)
	pause := monitor.NewPauseDetector(store, slog.Default())
	health := monitor.NewHealthAggregator(opStore, pause, slog.Default())
	handler := NewHandler(store, opStore, health)

	e := echo.New()
	RegisterRoutes(e, handler)
	return e, handler, opStore
}

func TestGetHealth(t *testing.T) {
	e, _, _ := setup(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result types.OverallHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result.Status != types.HealthStatusHealthy {
		t.Fatalf("expected healthy, got %s", result.Status)
	}
}

func TestGetHealthBridges(t *testing.T) {
	e, _, _ := setup(t)

	req := httptest.NewRequest(http.MethodGet, "/health/bridges", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result []types.BridgeHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 bridge types, got %d", len(result))
	}
}

func TestGetHealthDelays(t *testing.T) {
	e, _, _ := setup(t)

	req := httptest.NewRequest(http.MethodGet, "/health/delays", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGetIndexerState(t *testing.T) {
	e, _, _ := setup(t)

	req := httptest.NewRequest(http.MethodGet, "/indexer/state", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGetOperations(t *testing.T) {
	e, _, opStore := setup(t)

	op := &types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 1,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 100,
		SourceTxHash: "0xtx", FromAddress: "f", ToAddress: "t",
		Timestamp: time.Now(), Status: types.StatusPending,
	}
	_ = opStore.SaveOperation(op)

	req := httptest.NewRequest(http.MethodGet, "/operations?bridge_type=native&status=pending", nil)
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
		t.Fatalf("expected 1 operation, got %v", total)
	}
}

func TestGetOperationByNonce(t *testing.T) {
	e, _, opStore := setup(t)

	op := &types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 42,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 100,
		SourceTxHash: "0xtx", FromAddress: "f", ToAddress: "t",
		Timestamp: time.Now(), Status: types.StatusPending,
	}
	_ = opStore.SaveOperation(op)

	req := httptest.NewRequest(http.MethodGet, "/operations/42", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result []types.BridgeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 1 || result[0].Nonce != 42 {
		t.Fatalf("expected nonce 42, got %v", result)
	}
}

func TestGetOperationByNonce_NotFound(t *testing.T) {
	e, _, _ := setup(t)

	req := httptest.NewRequest(http.MethodGet, "/operations/999", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGetOperationByNonce_InvalidNonce(t *testing.T) {
	e, _, _ := setup(t)

	req := httptest.NewRequest(http.MethodGet, "/operations/abc", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetIndexerState_NotFound(t *testing.T) {
	e, _, _ := setup(t)

	// Fresh database has no indexer state, should return null values
	req := httptest.NewRequest(http.MethodGet, "/indexer/state", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Both states should be nil when not found
	if result["neo_n3"] != nil || result["neo_x"] != nil {
		t.Fatalf("expected nil states for fresh indexer, got %v", result)
	}
}

func TestGetOperationByNonce_MultipleResults(t *testing.T) {
	e, _, opStore := setup(t)

	// Save operations with same nonce but different bridge types/directions
	op1 := &types.BridgeOperation{
		BridgeType: types.BridgeTypeNative, Direction: types.DirectionDeposit, Nonce: 100,
		SourceChain: "neo_n3", DestinationChain: "neo_x", SourceBlockHeight: 100,
		SourceTxHash: "0xtx1", FromAddress: "f", ToAddress: "t",
		Timestamp: time.Now(), Status: types.StatusPending,
	}
	op2 := &types.BridgeOperation{
		BridgeType: types.BridgeTypeToken, Direction: types.DirectionWithdrawal, Nonce: 100,
		SourceChain: "neo_x", DestinationChain: "neo_n3", SourceBlockHeight: 200,
		SourceTxHash: "0xtx2", FromAddress: "f", ToAddress: "t",
		Timestamp: time.Now(), Status: types.StatusCompleted,
	}
	_ = opStore.SaveOperation(op1)
	_ = opStore.SaveOperation(op2)

	req := httptest.NewRequest(http.MethodGet, "/operations/100", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var result []types.BridgeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(result))
	}
}
