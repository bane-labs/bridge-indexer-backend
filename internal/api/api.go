package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// IndexerStateResponse represents the indexer state for all chains.
type IndexerStateResponse struct {
	NeoN3 *types.IndexerState `json:"neo_n3"`
	NeoX  *types.IndexerState `json:"neo_x"`
}

// OperationsResponse represents the paginated operations list response.
type OperationsResponse struct {
	Operations []types.BridgeOperation `json:"operations"`
	Total      int                     `json:"total"`
}

// ErrorResponse represents an API error response.
type ErrorResponse struct {
	Error string `json:"error"`
}

type Handler struct {
	store   *datastore.Store
	opStore *datastore.OperationStore
	health  *monitor.HealthAggregator
}

func NewHandler(store *datastore.Store, opStore *datastore.OperationStore, health *monitor.HealthAggregator) *Handler {
	return &Handler{store: store, opStore: opStore, health: health}
}

func RegisterRoutes(e *echo.Echo, h *Handler) {
	e.GET("/health", h.GetHealth)
	e.GET("/health/bridges", h.GetHealthBridges)
	e.GET("/health/delays", h.GetHealthDelays)
	e.GET("/indexer/state", h.GetIndexerState)
	e.GET("/operations", h.GetOperations)
	e.GET("/operations/:nonce", h.GetOperationByNonce)
}

// GetHealth godoc
// @Summary      Get overall bridge health
// @Description  Returns overall health status including per-bridge health summaries
// @Tags         health
// @Produce      json
// @Success      200  {object}  types.OverallHealth
// @Failure      500  {object}  ErrorResponse
// @Router       /health [get]

func (h *Handler) GetHealth(c echo.Context) error {
	overall, err := h.health.GetOverallHealth()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, overall)
}

// GetHealthBridges godoc
// @Summary      Get per-bridge health details
// @Description  Returns health metrics for each bridge type (native, token, message)
// @Tags         health
// @Produce      json
// @Success      200  {array}   types.BridgeHealth
// @Failure      500  {object}  ErrorResponse
// @Router       /health/bridges [get]

func (h *Handler) GetHealthBridges(c echo.Context) error {
	bridges, err := h.health.ComputeHealth()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, bridges)
}

// GetHealthDelays godoc
// @Summary      Get average completion delays
// @Description  Returns average completion times per bridge type for 24h and 7d windows
// @Tags         health
// @Produce      json
// @Success      200  {object}  types.DelayMetrics
// @Failure      500  {object}  ErrorResponse
// @Router       /health/delays [get]

func (h *Handler) GetHealthDelays(c echo.Context) error {
	delays, err := h.health.GetDelays()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, delays)
}

// GetIndexerState godoc
// @Summary      Get indexer sync state
// @Description  Returns current indexed block height and sync status for each chain
// @Tags         indexer
// @Produce      json
// @Success      200  {object}  IndexerStateResponse
// @Router       /indexer/state [get]

func (h *Handler) GetIndexerState(c echo.Context) error {
	n3State, err := h.store.GetIndexerState("neo_n3")
	if err != nil && !errors.Is(err, datastore.ErrNotFound) {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	neoxState, err := h.store.GetIndexerState("neo_x")
	if err != nil && !errors.Is(err, datastore.ErrNotFound) {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	result := map[string]interface{}{
		"neo_n3": n3State,
		"neo_x":  neoxState,
	}
	return c.JSON(http.StatusOK, result)
}

// GetOperations godoc
// @Summary      List bridge operations
// @Description  Returns bridge operations with optional filtering by type, direction, status, and pagination
// @Tags         operations
// @Produce      json
// @Param        bridge_type  query  string  false  "Filter by bridge type"   Enums(native, token, message)
// @Param        direction    query  string  false  "Filter by direction"     Enums(deposit, withdrawal)
// @Param        status       query  string  false  "Filter by status"        Enums(pending, completed, stuck)
// @Param        limit        query  int     false  "Page size (default: 50)"
// @Param        offset       query  int     false  "Page offset (default: 0)"
// @Success      200  {object}  OperationsResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /operations [get]

func (h *Handler) GetOperations(c echo.Context) error {
	filter := datastore.OperationFilter{}

	if bt := c.QueryParam("bridge_type"); bt != "" {
		bridgeType := types.BridgeType(bt)
		filter.BridgeType = &bridgeType
	}
	if dir := c.QueryParam("direction"); dir != "" {
		direction := types.Direction(dir)
		filter.Direction = &direction
	}
	if st := c.QueryParam("status"); st != "" {
		status := types.OperationStatus(st)
		filter.Status = &status
	}
	if lim := c.QueryParam("limit"); lim != "" {
		if v, err := strconv.Atoi(lim); err == nil && v > 0 {
			filter.Limit = v
		}
	}
	if off := c.QueryParam("offset"); off != "" {
		if v, err := strconv.Atoi(off); err == nil && v >= 0 {
			filter.Offset = v
		}
	}

	ops, total, err := h.opStore.ListOperations(filter)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"operations": ops,
		"total":      total,
	})
}

// GetOperationByNonce godoc
// @Summary      Get operations by nonce
// @Description  Returns all bridge operations matching the given nonce across all bridge types and directions
// @Tags         operations
// @Produce      json
// @Param        nonce  path  int  true  "Operation nonce"
// @Success      200  {array}   types.BridgeOperation
// @Failure      400  {object}  ErrorResponse
// @Failure      404  {object}  ErrorResponse
// @Router       /operations/{nonce} [get]

func (h *Handler) GetOperationByNonce(c echo.Context) error {
	nonceStr := c.Param("nonce")
	nonce, err := strconv.ParseUint(nonceStr, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid nonce"})
	}

	var results []types.BridgeOperation
	bridgeTypes := []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage}
	directions := []types.Direction{types.DirectionDeposit, types.DirectionWithdrawal}

	for _, bt := range bridgeTypes {
		for _, dir := range directions {
			op, err := h.opStore.GetOperation(bt, dir, nonce)
			if err != nil && !errors.Is(err, datastore.ErrNotFound) {
				return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
			}
			if errors.Is(err, datastore.ErrNotFound) {
				continue
			}
			if err == nil {
				results = append(results, *op)
			}
		}
	}

	if len(results) == 0 {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "no operations found for nonce"})
	}

	return c.JSON(http.StatusOK, results)
}
