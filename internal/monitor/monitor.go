package monitor

import (
	"context"
	"log/slog"
	"time"
)

type Monitor struct {
	stuck  *StuckDetector
	pause  *PauseDetector
	health *HealthAggregator
	logger *slog.Logger
}

func NewMonitor(stuck *StuckDetector, pause *PauseDetector, health *HealthAggregator, logger *slog.Logger) *Monitor {
	return &Monitor{stuck: stuck, pause: pause, health: health, logger: logger}
}

func (m *Monitor) Start(ctx context.Context) error {
	stuckTicker := time.NewTicker(30 * time.Second)
	defer stuckTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Monitor stopping")
			return nil
		case <-stuckTicker.C:
			if err := m.stuck.Check(ctx); err != nil {
				m.logger.Error("Stuck check failed", "error", err)
			}
		}
	}
}

func (m *Monitor) StuckDetector() *StuckDetector       { return m.stuck }
func (m *Monitor) PauseDetector() *PauseDetector       { return m.pause }
func (m *Monitor) HealthAggregator() *HealthAggregator { return m.health }
