package indexer

import (
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type N3EventProcessorImpl struct {
	storer        EventStorer
	neoXBridge    util.Uint160
	messageBridge util.Uint160
	logger        *slog.Logger
}

func NewN3EventProcessor(
	storer EventStorer,
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
		storer:        storer,
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
	case "NativeDeposit":
		return p.handleNativeDeposit(event, blockHeight, txHash, blockTime)
	case "TokenDeposit":
		return p.handleTokenDeposit(event, blockHeight, txHash, blockTime)
	case "NativeWithdrawal":
		return p.handleNativeWithdrawalComplete(event, blockHeight, txHash, blockTime)
	case "TokenWithdrawal":
		return p.handleTokenWithdrawalComplete(event, blockHeight, txHash, blockTime)
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
	case "Execution":
		return p.handleN3Execution(event, blockHeight, txHash, blockTime)
	default:
		return nil, nil
	}
}

// handleNativeDeposit processes NativeDeposit events from the NeoXBridge contract.
// Contract event: NativeDeposit(Nonce int, Recipient Hash160, Amount int, Depositor Hash160, DepositHash ByteArray, NewDepositRoot ByteArray)
func (p *N3EventProcessorImpl) handleNativeDeposit(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("NativeDeposit event: %w", err)
	}
	if len(items) < 4 {
		return nil, fmt.Errorf("NativeDeposit event: expected >= 4 items, got %d", len(items))
	}

	// items[0] = Nonce
	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("NativeDeposit nonce: %w", err)
	}

	// items[1] = Recipient (20-byte Hash160)
	to, err := itemToAddressString(items[1])
	if err != nil {
		return nil, fmt.Errorf("NativeDeposit recipient: %w", err)
	}

	// items[2] = Amount
	amount, err := itemToBigIntString(items[2])
	if err != nil {
		return nil, fmt.Errorf("NativeDeposit amount: %w", err)
	}

	// items[3] = Depositor (20-byte Hash160)
	from, err := itemToAddressString(items[3])
	if err != nil {
		return nil, fmt.Errorf("NativeDeposit depositor: %w", err)
	}

	// items[5] = NewDepositRoot (optional)
	var root *string
	if len(items) >= 6 {
		if r, rErr := itemToHexString(items[5]); rErr == nil && r != "" {
			root = &r
		}
	}

	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionDeposit,
		Nonce:             nonce,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: uint64(blockHeight),
		SourceTxHash:      txHash,
		Root:              root,
		Amount:            &amount,
		FromAddress:       from,
		ToAddress:         to,
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}

	return op, nil
}

// handleTokenDeposit processes TokenDeposit events from the NeoXBridge contract.
// Contract event: TokenDeposit(NeoN3Token Hash160, NeoXToken Hash160, Nonce int, Recipient Hash160, Value int, Depositor Hash160, DepositHash ByteArray, NewDepositRoot ByteArray)
func (p *N3EventProcessorImpl) handleTokenDeposit(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit event: %w", err)
	}
	if len(items) < 6 {
		return nil, fmt.Errorf("TokenDeposit event: expected >= 6 items, got %d", len(items))
	}

	// items[0] = NeoN3Token (20-byte Hash160)
	tokenContract, err := itemToAddressString(items[0])
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit token: %w", err)
	}

	// items[1] = NeoXToken (destination chain token address)
	destToken, err := itemToAddressString(items[1])
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit NeoXToken: %w", err)
	}

	// items[2] = Nonce
	nonce, err := itemToUint64(items[2])
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit nonce: %w", err)
	}

	// items[3] = Recipient (20-byte Hash160)
	to, err := itemToAddressString(items[3])
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit recipient: %w", err)
	}

	// items[4] = Value
	amount, err := itemToBigIntString(items[4])
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit value: %w", err)
	}

	// items[5] = Depositor (20-byte Hash160)
	from, err := itemToAddressString(items[5])
	if err != nil {
		return nil, fmt.Errorf("TokenDeposit depositor: %w", err)
	}

	// items[7] = NewDepositRoot (optional)
	var root *string
	if len(items) >= 8 {
		if r, rErr := itemToHexString(items[7]); rErr == nil && r != "" {
			root = &r
		}
	}

	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeToken,
		Direction:         types.DirectionDeposit,
		Nonce:             nonce,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: uint64(blockHeight),
		SourceTxHash:      txHash,
		TokenContract:     &tokenContract,
		DestTokenContract: &destToken,
		Root:              root,
		Amount:            &amount,
		FromAddress:       from,
		ToAddress:         to,
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}

	return op, nil
}

// handleNativeWithdrawalComplete processes NativeWithdrawal events (withdrawal completion on N3 side).
// Contract event: NativeWithdrawal(Nonce int, Recipient Hash160, Amount int)
func (p *N3EventProcessorImpl) handleNativeWithdrawalComplete(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("NativeWithdrawal event: %w", err)
	}
	if len(items) < 1 {
		return nil, fmt.Errorf("NativeWithdrawal event: expected >= 1 items, got %d", len(items))
	}

	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("NativeWithdrawal nonce: %w", err)
	}

	destBlock := uint64(blockHeight)
	err = p.storer.UpdateOperationCompletion(
		types.BridgeTypeNative, types.DirectionWithdrawal, nonce,
		destBlock, txHash, blockTime,
	)
	if err == nil {
		p.logger.Info("Completed native withdrawal via N3", "nonce", nonce)
	} else {
		p.logger.Debug("No pending native withdrawal found for nonce", "nonce", nonce)
	}

	return nil, nil
}

// handleTokenWithdrawalComplete processes TokenWithdrawal events (withdrawal completion on N3 side).
// Contract event: TokenWithdrawal(NeoN3Token Hash160, Nonce int, Recipient Hash160, Value int)
func (p *N3EventProcessorImpl) handleTokenWithdrawalComplete(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("TokenWithdrawal event: %w", err)
	}
	if len(items) < 2 {
		return nil, fmt.Errorf("TokenWithdrawal event: expected >= 2 items, got %d", len(items))
	}

	// items[0] = NeoN3Token (skip)
	destToken, err := itemToAddressString(items[0])
	if err != nil {
		return nil, fmt.Errorf("TokenWithdrawal token: %w", err)
	}
	// items[1] = Nonce
	nonce, err := itemToUint64(items[1])
	if err != nil {
		return nil, fmt.Errorf("TokenWithdrawal nonce: %w", err)
	}

	destBlock := uint64(blockHeight)
	err = p.storer.UpdateTokenOperationCompletionByDestToken(
		types.DirectionWithdrawal, nonce, destToken,
		destBlock, txHash, blockTime,
	)
	if err == nil {
		p.logger.Info("Completed token withdrawal via N3", "nonce", nonce)
	} else {
		p.logger.Debug("No pending token withdrawal found for nonce", "nonce", nonce)
	}

	return nil, nil
}

// handleN3MessageSend processes MessageSend events from the MessageBridge contract.
// Contract event: MessageSend(Nonce int, Metadata ByteArray, Message ByteArray, MessageHash ByteArray, NeoToEvmRoot ByteArray)
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
	if len(items) < 1 {
		return nil, fmt.Errorf("MessageSend event: expected >= 1 items, got %d", len(items))
	}

	// items[0] = Nonce
	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("MessageSend nonce: %w", err)
	}

	// items[1] = Metadata (encoded bytes, not a sender address)
	// The sender is embedded within the metadata; we don't parse it here.
	from := "unknown"

	// items[4] = NeoToEvmRoot (optional)
	var root *string
	if len(items) >= 5 {
		if r, rErr := itemToHexString(items[4]); rErr == nil && r != "" {
			root = &r
		}
	}

	op := &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionDeposit,
		Nonce:             nonce,
		SourceChain:       chainN3,
		DestinationChain:  chainNeoX,
		SourceBlockHeight: uint64(blockHeight),
		SourceTxHash:      txHash,
		Root:              root,
		FromAddress:       from,
		ToAddress:         "",
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}

	return op, nil
}

// handleN3Execution processes Execution events from the MessageBridge contract (message withdrawal completion).
// Contract event: Execution(Nonce int, NumberOfChunks int, Index int, Result Any)
func (p *N3EventProcessorImpl) handleN3Execution(
	event state.NotificationEvent,
	blockHeight uint32,
	txHash string,
	blockTime time.Time,
) (*types.BridgeOperation, error) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return nil, fmt.Errorf("Execution event: %w", err)
	}
	if len(items) < 1 {
		return nil, fmt.Errorf("Execution event: expected >= 1 items, got %d", len(items))
	}

	// items[0] = Nonce
	nonce, err := itemToUint64(items[0])
	if err != nil {
		return nil, fmt.Errorf("Execution nonce: %w", err)
	}

	destBlock := uint64(blockHeight)
	err = p.storer.UpdateOperationCompletion(
		types.BridgeTypeMessage, types.DirectionWithdrawal, nonce,
		destBlock, txHash, blockTime,
	)
	if err != nil {
		p.logger.Debug("No pending message withdrawal found for nonce", "nonce", nonce)
	} else {
		p.logger.Info("Completed message withdrawal via N3", "nonce", nonce)
	}
	return nil, nil
}

// --- Helper functions ---

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
