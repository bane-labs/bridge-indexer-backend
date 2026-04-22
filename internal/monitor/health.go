package monitor

import (
	"log/slog"
	"sync"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type HealthAggregator struct {
	opStore *datastore.OperationStore
	pause   *PauseDetector
	logger  *slog.Logger

	mu             sync.RWMutex
	healthCache    []types.BridgeHealth
	healthCachedAt time.Time
	delaysCache    *types.DelayMetrics
	delaysCachedAt time.Time
	cacheTTL       time.Duration
}

func NewHealthAggregator(opStore *datastore.OperationStore, pause *PauseDetector, logger *slog.Logger) *HealthAggregator {
	return &HealthAggregator{
		opStore:  opStore,
		pause:    pause,
		logger:   logger,
		cacheTTL: 10 * time.Second,
	}
}

func (h *HealthAggregator) ComputeHealth() ([]types.BridgeHealth, error) {
	h.mu.RLock()
	if len(h.healthCache) > 0 && time.Since(h.healthCachedAt) < h.cacheTTL {
		cached := cloneBridgeHealths(h.healthCache)
		h.mu.RUnlock()
		return cached, nil
	}
	h.mu.RUnlock()

	bridgeTypes := []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage}
	var results []types.BridgeHealth

	for _, bt := range bridgeTypes {
		bh, err := h.computeBridgeHealth(bt)
		if err != nil {
			return nil, err
		}
		results = append(results, bh)
	}

	h.mu.Lock()
	h.healthCache = cloneBridgeHealths(results)
	h.healthCachedAt = time.Now()
	h.mu.Unlock()

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
	h.mu.RLock()
	if h.delaysCache != nil && time.Since(h.delaysCachedAt) < h.cacheTTL {
		cached := cloneDelayMetrics(h.delaysCache)
		h.mu.RUnlock()
		return cached, nil
	}
	h.mu.RUnlock()

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

	h.mu.Lock()
	h.delaysCache = cloneDelayMetrics(metrics)
	h.delaysCachedAt = time.Now()
	h.mu.Unlock()

	return metrics, nil
}

func (h *HealthAggregator) computeBridgeHealth(bt types.BridgeType) (types.BridgeHealth, error) {
	btCopy := bt
	pending := types.StatusPending
	stuck := types.StatusStuck
	completed := types.StatusCompleted

	pendingCount, err := h.opStore.CountOperations(datastore.OperationFilter{BridgeType: &btCopy, Status: &pending})
	if err != nil {
		return types.BridgeHealth{}, err
	}

	stuckCount, err := h.opStore.CountOperations(datastore.OperationFilter{BridgeType: &btCopy, Status: &stuck})
	if err != nil {
		return types.BridgeHealth{}, err
	}

	completedCount, err := h.opStore.CountOperations(datastore.OperationFilter{BridgeType: &btCopy, Status: &completed})
	if err != nil {
		return types.BridgeHealth{}, err
	}

	depPaused := h.pause.IsDepositsPaused(bt)
	wdPaused := h.pause.IsWithdrawalsPaused(bt)

	status := types.HealthStatusHealthy
	total := completedCount + stuckCount
	// Stuck operations are indexed for operators but do not imply bridge health;
	// only on-chain pause flags affect status (PD §6).
	if bt == types.BridgeTypeMessage {
		if depPaused && wdPaused {
			status = types.HealthStatusUnhealthy
		} else if depPaused || wdPaused {
			status = types.HealthStatusDegraded
		}
	} else {
		if depPaused && wdPaused {
			status = types.HealthStatusUnhealthy
		} else if depPaused || wdPaused {
			status = types.HealthStatusDegraded
		}
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

func cloneBridgeHealths(in []types.BridgeHealth) []types.BridgeHealth {
	out := make([]types.BridgeHealth, len(in))
	copy(out, in)
	return out
}

func cloneDelayMetrics(in *types.DelayMetrics) *types.DelayMetrics {
	if in == nil {
		return nil
	}
	out := &types.DelayMetrics{
		Avg24h: make(map[types.BridgeType]float64, len(in.Avg24h)),
		Avg7d:  make(map[types.BridgeType]float64, len(in.Avg7d)),
	}
	for k, v := range in.Avg24h {
		out.Avg24h[k] = v
	}
	for k, v := range in.Avg7d {
		out.Avg7d[k] = v
	}
	return out
}
