package monitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// StuckThresholds defines how long a pending operation may age before it is marked stuck.
// Per-type values of 0 mean "use Default for that bridge type".
type StuckThresholds struct {
	Default time.Duration
	Native  time.Duration
	Token   time.Duration
	Message time.Duration
}

// ForBridgeType returns the effective limit for the given bridge type.
func (t StuckThresholds) ForBridgeType(bt types.BridgeType) time.Duration {
	var override time.Duration
	switch bt {
	case types.BridgeTypeNative:
		override = t.Native
	case types.BridgeTypeToken:
		override = t.Token
	case types.BridgeTypeMessage:
		override = t.Message
	default:
		return t.Default
	}
	if override > 0 {
		return override
	}
	return t.Default
}

type StuckDetector struct {
	opStore    *datastore.OperationStore
	alerter    alerter.Alerter
	thresholds StuckThresholds
	logger     *slog.Logger
}

func NewStuckDetector(opStore *datastore.OperationStore, a alerter.Alerter, thresholds StuckThresholds, logger *slog.Logger) *StuckDetector {
	return &StuckDetector{
		opStore:    opStore,
		alerter:    a,
		thresholds: thresholds,
		logger:     logger,
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
		// Message bridge withdrawals (NeoX→N3) are executable by the recipient at
		// their discretion. Never executing is a valid user choice and does not
		// indicate a bridge failure, so skip stuck detection for this direction.
		if op.BridgeType == types.BridgeTypeMessage && op.Direction == types.DirectionWithdrawal {
			continue
		}
		limit := d.thresholds.ForBridgeType(op.BridgeType)
		if now.Sub(op.Timestamp) <= limit {
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
