package monitor

import (
	"log/slog"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type PauseState struct {
	NativeDepositsPaused     bool
	NativeWithdrawalsPaused  bool
	TokenDepositsPaused      bool
	TokenWithdrawalsPaused   bool
	MessageDepositsPaused    bool
	MessageWithdrawalsPaused bool
}

type PauseDetector struct {
	store  *datastore.Store
	state  PauseState
	logger *slog.Logger
}

func NewPauseDetector(store *datastore.Store, logger *slog.Logger) *PauseDetector {
	return &PauseDetector{store: store, logger: logger}
}

func (d *PauseDetector) UpdatePause(bridgeType types.BridgeType, paused bool, scope string) {
	switch scope {
	case "all":
		switch bridgeType {
		case types.BridgeTypeNative:
			d.state.NativeDepositsPaused = paused
			d.state.NativeWithdrawalsPaused = paused
		case types.BridgeTypeToken:
			d.state.TokenDepositsPaused = paused
			d.state.TokenWithdrawalsPaused = paused
		case types.BridgeTypeMessage:
			d.state.MessageDepositsPaused = paused
			d.state.MessageWithdrawalsPaused = paused
		default:
			d.state.NativeDepositsPaused = paused
			d.state.NativeWithdrawalsPaused = paused
			d.state.TokenDepositsPaused = paused
			d.state.TokenWithdrawalsPaused = paused
		}
	case "deposits":
		switch bridgeType {
		case types.BridgeTypeNative:
			d.state.NativeDepositsPaused = paused
		case types.BridgeTypeToken:
			d.state.TokenDepositsPaused = paused
		case types.BridgeTypeMessage:
			d.state.MessageDepositsPaused = paused
		}
	case "withdrawals":
		switch bridgeType {
		case types.BridgeTypeNative:
			d.state.NativeWithdrawalsPaused = paused
		case types.BridgeTypeToken:
			d.state.TokenWithdrawalsPaused = paused
		case types.BridgeTypeMessage:
			d.state.MessageWithdrawalsPaused = paused
		}
	case "sending":
		if bridgeType == types.BridgeTypeMessage {
			d.state.MessageDepositsPaused = paused
		}
	case "executing":
		if bridgeType == types.BridgeTypeMessage {
			d.state.MessageWithdrawalsPaused = paused
		}
	}
}

func (d *PauseDetector) GetPauseState() PauseState {
	return d.state
}

func (d *PauseDetector) IsDepositsPaused(bt types.BridgeType) bool {
	switch bt {
	case types.BridgeTypeNative:
		return d.state.NativeDepositsPaused
	case types.BridgeTypeToken:
		return d.state.TokenDepositsPaused
	case types.BridgeTypeMessage:
		return d.state.MessageDepositsPaused
	}
	return false
}

func (d *PauseDetector) IsWithdrawalsPaused(bt types.BridgeType) bool {
	switch bt {
	case types.BridgeTypeNative:
		return d.state.NativeWithdrawalsPaused
	case types.BridgeTypeToken:
		return d.state.TokenWithdrawalsPaused
	case types.BridgeTypeMessage:
		return d.state.MessageWithdrawalsPaused
	}
	return false
}
