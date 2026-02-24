package alerter

import (
	"context"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type Alert struct {
	Title      string
	Message    string
	Severity   string // "warning", "critical"
	BridgeType types.BridgeType
	Operation  *types.BridgeOperation
	Timestamp  time.Time
}

type Alerter interface {
	Send(ctx context.Context, alert Alert) error
}

type MultiAlerter struct {
	alerters []Alerter
}

func NewMultiAlerter(alerters ...Alerter) *MultiAlerter {
	return &MultiAlerter{alerters: alerters}
}

func (m *MultiAlerter) Send(ctx context.Context, alert Alert) error {
	var lastErr error
	for _, a := range m.alerters {
		if err := a.Send(ctx, alert); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
