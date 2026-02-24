package api

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

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

func (h *Handler) GetHealth(c echo.Context) error {
	overall, err := h.health.GetOverallHealth()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, overall)
}

func (h *Handler) GetHealthBridges(c echo.Context) error {
	bridges, err := h.health.ComputeHealth()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, bridges)
}

func (h *Handler) GetHealthDelays(c echo.Context) error {
	delays, err := h.health.GetDelays()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, delays)
}

func (h *Handler) GetIndexerState(c echo.Context) error {
	n3State, _ := h.store.GetIndexerState("neo_n3")
	neoxState, _ := h.store.GetIndexerState("neo_x")

	result := map[string]interface{}{
		"neo_n3": n3State,
		"neo_x":  neoxState,
	}
	return c.JSON(http.StatusOK, result)
}

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
