package monitor

import (
	"log/slog"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type HealthAggregator struct {
	opStore *datastore.OperationStore
	pause   *PauseDetector
	logger  *slog.Logger
}

func NewHealthAggregator(opStore *datastore.OperationStore, pause *PauseDetector, logger *slog.Logger) *HealthAggregator {
	return &HealthAggregator{opStore: opStore, pause: pause, logger: logger}
}

func (h *HealthAggregator) ComputeHealth() ([]types.BridgeHealth, error) {
	bridgeTypes := []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage}
	var results []types.BridgeHealth

	for _, bt := range bridgeTypes {
		bh, err := h.computeBridgeHealth(bt)
		if err != nil {
			return nil, err
		}
		results = append(results, bh)
	}
	return results, nil
}

func (h *HealthAggregator) GetOverallHealth() (*types.OverallHealth, error) {
	bridges, err := h.ComputeHealth()
	if err != nil {
		return nil, err
	}

	worst := types.HealthStatusHealthy
	for _, b := range bridges {
		if b.Status == types.HealthStatusUnhealthy {
			worst = types.HealthStatusUnhealthy
			break
		}
		if b.Status == types.HealthStatusDegraded {
			worst = types.HealthStatusDegraded
		}
	}

	return &types.OverallHealth{
		Status:    worst,
		Bridges:   bridges,
		UpdatedAt: time.Now(),
	}, nil
}

func (h *HealthAggregator) GetDelays() (*types.DelayMetrics, error) {
	metrics := &types.DelayMetrics{
		Avg24h: make(map[types.BridgeType]float64),
		Avg7d:  make(map[types.BridgeType]float64),
	}

	bridgeTypes := []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage}
	completed := types.StatusCompleted

	for _, bt := range bridgeTypes {
		btCopy := bt
		ops, _, err := h.opStore.ListOperations(datastore.OperationFilter{
			BridgeType: &btCopy,
			Status:     &completed,
		})
		if err != nil {
			return nil, err
		}

		now := time.Now()
		var sum24h, sum7d float64
		var count24h, count7d int

		for _, op := range ops {
			if op.CompletionTimestamp == nil {
				continue
			}
			duration := op.CompletionTimestamp.Sub(op.Timestamp).Seconds()

			if now.Sub(*op.CompletionTimestamp) <= 24*time.Hour {
				sum24h += duration
				count24h++
			}
			if now.Sub(*op.CompletionTimestamp) <= 7*24*time.Hour {
				sum7d += duration
				count7d++
			}
		}

		if count24h > 0 {
			metrics.Avg24h[bt] = sum24h / float64(count24h)
		}
		if count7d > 0 {
			metrics.Avg7d[bt] = sum7d / float64(count7d)
		}
	}

	return metrics, nil
}

func (h *HealthAggregator) computeBridgeHealth(bt types.BridgeType) (types.BridgeHealth, error) {
	btCopy := bt
	pending := types.StatusPending
	stuck := types.StatusStuck
	completed := types.StatusCompleted

	pendingOps, pendingCount, err := h.opStore.ListOperations(datastore.OperationFilter{BridgeType: &btCopy, Status: &pending})
	if err != nil {
		return types.BridgeHealth{}, err
	}
	_ = pendingOps

	_, stuckCount, err := h.opStore.ListOperations(datastore.OperationFilter{BridgeType: &btCopy, Status: &stuck})
	if err != nil {
		return types.BridgeHealth{}, err
	}

	_, completedCount, err := h.opStore.ListOperations(datastore.OperationFilter{BridgeType: &btCopy, Status: &completed})
	if err != nil {
		return types.BridgeHealth{}, err
	}

	depPaused := h.pause.IsDepositsPaused(bt)
	wdPaused := h.pause.IsWithdrawalsPaused(bt)

	status := types.HealthStatusHealthy
	total := completedCount + stuckCount
	if depPaused && wdPaused {
		status = types.HealthStatusUnhealthy
	} else if total > 0 && float64(stuckCount)/float64(total) >= 0.5 {
		status = types.HealthStatusUnhealthy
	} else if stuckCount > 0 || depPaused || wdPaused {
		status = types.HealthStatusDegraded
	}

	var uptime *float64
	if total > 0 {
		u := float64(completedCount) / float64(total) * 100
		uptime = &u
	}

	return types.BridgeHealth{
		BridgeType:        bt,
		Status:            status,
		DepositsPaused:    depPaused,
		WithdrawalsPaused: wdPaused,
		UptimePercentage:  uptime,
		PendingOperations: pendingCount,
		StuckOperations:   stuckCount,
	}, nil
}
