package indexer

import (
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type N3EventProcessorImpl struct {
	opStore       *datastore.OperationStore
	neoXBridge    util.Uint160
	messageBridge util.Uint160
	logger        *slog.Logger
}

func NewN3EventProcessor(
	opStore *datastore.OperationStore,
	neoXBridgeHash string,
	messageBridgeHash string,
	logger *slog.Logger,
) (*N3EventProcessorImpl, error) {
	nxb, err := util.Uint160DecodeStringLE(neoXBridgeHash)
	if err != nil {
		return nil, fmt.Errorf("invalid NeoXBridge hash: %w", err)
	}
	mb, err := util.Uint160DecodeStringLE(messageBridgeHash)
	if err != nil {
		return nil, fmt.Errorf("invalid MessageBridge hash: %w", err)
	}
	return &N3EventProcessorImpl{
		opStore:       opStore,
		neoXBridge:    nxb,
		messageBridge: mb,
		logger:        logger,
	}, nil
}

func (p *N3EventProcessorImpl) ProcessEvent(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	if event.ScriptHash.Equals(p.neoXBridge) {
		return p.processNeoXBridgeEvent(event, blockHeight, txHash, blockTime)
	}
	if event.ScriptHash.Equals(p.messageBridge) {
		return p.processMessageBridgeEvent(event, blockHeight, txHash, blockTime)
	}
	return nil, nil
}

func (p *N3EventProcessorImpl) processNeoXBridgeEvent(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	switch event.Name {
	case "Deposit":
		return p.handleN3Deposit(event, blockHeight, txHash, blockTime)
	case "Withdrawal", "WithdrawCompleted":
		return p.handleN3WithdrawalComplete(event, blockHeight, txHash, blockTime)
	default:
		return nil, nil
	}
}

func (p *N3EventProcessorImpl) processMessageBridgeEvent(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	switch event.Name {
	case "MessageSend":
		return p.handleN3MessageSend(event, blockHeight, txHash, blockTime)
	case "MessageExecuted":
		return p.handleN3MessageExecuted(event, blockHeight, txHash, blockTime)
	default:
		return nil, nil
	}
}

func (p *N3EventProcessorImpl) handleN3Deposit(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("Deposit event: %w", err)
	}
	if len(items) < 4 {
		return nil, fmt.Errorf("Deposit event: expected >= 4 items, got %d", len(items))
	}

	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("Deposit nonce: %w", err)
	}

	to, err := itemToAddressString(items[1])
	if err != nil {
		return nil, fmt.Errorf("Deposit to: %w", err)
	}

	amount, err := itemToBigIntString(items[2])
	if err != nil {
		return nil, fmt.Errorf("Deposit amount: %w", err)
	}

	from, err := itemToAddressString(items[3])
	if err != nil {
		return nil, fmt.Errorf("Deposit from: %w", err)
	}

	bridgeType := types.BridgeTypeNative
	var tokenContract *string
	if len(items) >= 7 {
		tc, tcErr := itemToHexString(items[6])
		if tcErr == nil && tc != "" {
			bridgeType = types.BridgeTypeToken
			tokenContract = &tc
		}
	}

	op := &types.BridgeOperation{
		BridgeType:        bridgeType,
		Direction:         types.DirectionDeposit,
		Nonce:             nonce,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: uint64(blockHeight),
		SourceTxHash:      txHash,
		TokenContract:     tokenContract,
		Amount:            &amount,
		FromAddress:       from,
		ToAddress:         to,
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}

	return op, nil
}

func (p *N3EventProcessorImpl) handleN3WithdrawalComplete(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("Withdrawal event: %w", err)
	}
	if len(items) < 1 {
		return nil, fmt.Errorf("Withdrawal event: expected >= 1 items, got %d", len(items))
	}

	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("Withdrawal nonce: %w", err)
	}

	destBlock := uint64(blockHeight)
	for _, bt := range []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken} {
		err := p.opStore.UpdateOperationCompletion(
			bt, types.DirectionWithdrawal, nonce,
			destBlock, txHash, blockTime,
		)
		if err == nil {
			p.logger.Info("Completed withdrawal via N3", "bridge_type", bt, "nonce", nonce)
			return nil, nil
		}
	}

	p.logger.Debug("No pending withdrawal found for nonce", "nonce", nonce)
	return nil, nil
}

func (p *N3EventProcessorImpl) handleN3MessageSend(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("MessageSend event: %w", err)
	}
	if len(items) < 2 {
		return nil, fmt.Errorf("MessageSend event: expected >= 2 items, got %d", len(items))
	}

	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("MessageSend nonce: %w", err)
	}

	from, err := itemToAddressString(items[1])
	if err != nil {
		from = "unknown"
	}

	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionDeposit,
		Nonce:             nonce,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: uint64(blockHeight),
		SourceTxHash:      txHash,
		FromAddress:       from,
		ToAddress:         "",
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}

	return op, nil
}

func (p *N3EventProcessorImpl) handleN3MessageExecuted(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("MessageExecuted event: %w", err)
	}
	if len(items) < 1 {
		return nil, fmt.Errorf("MessageExecuted event: expected >= 1 items, got %d", len(items))
	}

	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("MessageExecuted nonce: %w", err)
	}

	destBlock := uint64(blockHeight)
	err = p.opStore.UpdateOperationCompletion(
		types.BridgeTypeMessage, types.DirectionWithdrawal, nonce,
		destBlock, txHash, blockTime,
	)
	if err != nil {
		p.logger.Debug("No pending message withdrawal found for nonce", "nonce", nonce)
	}

	return nil, nil
}

func getArrayItems(item *stackitem.Array) ([]stackitem.Item, error) {
	if item == nil {
		return nil, fmt.Errorf("nil stack item")
	}
	arr, ok := item.Value().([]stackitem.Item)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", item.Value())
	}
	return arr, nil
}

func itemToUint64(item stackitem.Item) (uint64, error) {
	bi, err := item.TryInteger()
	if err != nil {
		return 0, fmt.Errorf("TryInteger: %w", err)
	}
	if !bi.IsUint64() {
		return 0, fmt.Errorf("integer overflow: %s", bi.String())
	}
	return bi.Uint64(), nil
}

func itemToBigIntString(item stackitem.Item) (string, error) {
	bi, err := item.TryInteger()
	if err != nil {
		return "", fmt.Errorf("TryInteger: %w", err)
	}
	return bi.String(), nil
}

func itemToAddressString(item stackitem.Item) (string, error) {
	b, err := item.TryBytes()
	if err != nil {
		return "", fmt.Errorf("TryBytes: %w", err)
	}
	if len(b) == 20 {
		hash, _ := util.Uint160DecodeBytesBE(b)
		return hash.StringLE(), nil
	}
	return fmt.Sprintf("%x", b), nil
}

func itemToHexString(item stackitem.Item) (string, error) {
	b, err := item.TryBytes()
	if err != nil {
		return "", err
	}
	if len(b) == 0 {
		return "", nil
	}
	return fmt.Sprintf("%x", b), nil
}

// Ensure N3EventProcessorImpl satisfies the interface used by tests.
var _ interface {
	ProcessEvent(state.NotificationEvent, uint32, string, time.Time) (*types.BridgeOperation, error)
} = (*N3EventProcessorImpl)(nil)

// intItem is a helper for building test stack items.
func intItem(v int64) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(v))
}
