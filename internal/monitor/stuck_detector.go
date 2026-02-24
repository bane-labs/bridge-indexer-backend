package monitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type StuckDetector struct {
	opStore   *datastore.OperationStore
	alerter   alerter.Alerter
	threshold time.Duration
	logger    *slog.Logger
}

func NewStuckDetector(opStore *datastore.OperationStore, a alerter.Alerter, threshold time.Duration, logger *slog.Logger) *StuckDetector {
	return &StuckDetector{
		opStore:   opStore,
		alerter:   a,
		threshold: threshold,
		logger:    logger,
	}
}

func (d *StuckDetector) Check(ctx context.Context) error {
	pending := types.StatusPending
	ops, _, err := d.opStore.ListOperations(datastore.OperationFilter{Status: &pending})
	if err != nil {
		return err
	}

	now := time.Now()
	for _, op := range ops {
		if now.Sub(op.Timestamp) <= d.threshold {
			continue
		}

		d.logger.Warn("Operation stuck",
			"bridge_type", op.BridgeType,
			"direction", op.Direction,
			"nonce", op.Nonce,
			"age", now.Sub(op.Timestamp).String(),
		)

		stuckOp := op
		if err := d.opStore.MarkStuck(op.BridgeType, op.Direction, op.Nonce); err != nil {
			d.logger.Error("Failed to mark stuck", "nonce", op.Nonce, "error", err)
			continue
		}

		if d.alerter != nil {
			_ = d.alerter.Send(ctx, alerter.Alert{
				Title:      "Stuck Bridge Operation",
				Message:    "Operation exceeded threshold",
				Severity:   "warning",
				BridgeType: op.BridgeType,
				Operation:  &stuckOp,
				Timestamp:  now,
			})
		}
	}

	return nil
}
