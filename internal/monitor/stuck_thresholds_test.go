package monitor

import (
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestStuckThresholds_ForBridgeType(t *testing.T) {
	t.Parallel()
	th := StuckThresholds{
		Default: 5 * time.Minute,
		Native:  30 * time.Minute,
		Token:   0,
		Message: 24 * time.Hour,
	}
	if got := th.ForBridgeType(types.BridgeTypeNative); got != 30*time.Minute {
		t.Fatalf("native: got %v", got)
	}
	if got := th.ForBridgeType(types.BridgeTypeToken); got != 5*time.Minute {
		t.Fatalf("token (inherit): got %v", got)
	}
	if got := th.ForBridgeType(types.BridgeTypeMessage); got != 24*time.Hour {
		t.Fatalf("message: got %v", got)
	}
}
