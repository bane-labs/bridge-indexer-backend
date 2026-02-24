package alerter

import "context"

type NoopAlerter struct{}

func (n *NoopAlerter) Send(_ context.Context, _ Alert) error {
	return nil
}
