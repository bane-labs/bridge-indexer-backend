package indexer

import (
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

var (
	sigNativeWithdrawal = crypto.Keccak256Hash([]byte("NativeWithdrawal(uint256,address,uint256,address,bytes32,bytes32)"))
	sigNativeDeposit    = crypto.Keccak256Hash([]byte("NativeDeposit(uint256,address,uint256)"))
	sigNativeClaim      = crypto.Keccak256Hash([]byte("NativeClaim(uint256,address,uint256)"))
	sigNativeClaimable  = crypto.Keccak256Hash([]byte("NativeClaimable(uint256,address,uint256)"))

	sigTokenWithdrawal = crypto.Keccak256Hash([]byte("TokenWithdrawal(address,address,uint256,address,uint256,address,bytes32,bytes32)"))
	sigTokenDeposit    = crypto.Keccak256Hash([]byte("TokenDeposit(address,uint256,address,uint256)"))
	sigTokenClaim      = crypto.Keccak256Hash([]byte("TokenClaim(address,uint256,address,uint256)"))
	sigTokenClaimable  = crypto.Keccak256Hash([]byte("TokenClaimable(address,uint256,address,uint256)"))

	sigMessageSend = crypto.Keccak256Hash([]byte("MessageSend(uint256,address,bytes,bytes,bytes32,bytes32)"))
	sigExecution   = crypto.Keccak256Hash([]byte("Execution(uint256,uint8)"))

	sigBridgePause         = crypto.Keccak256Hash([]byte("BridgePause()"))
	sigBridgeUnpause       = crypto.Keccak256Hash([]byte("BridgeUnpause()"))
	sigWithdrawalPause     = crypto.Keccak256Hash([]byte("WithdrawalPause()"))
	sigWithdrawalUnpause   = crypto.Keccak256Hash([]byte("WithdrawalUnpause()"))
	sigNativeBridgePause   = crypto.Keccak256Hash([]byte("NativeBridgePause()"))
	sigNativeBridgeUnpause = crypto.Keccak256Hash([]byte("NativeBridgeUnpause()"))
	sigPause               = crypto.Keccak256Hash([]byte("Pause()"))
	sigUnpause             = crypto.Keccak256Hash([]byte("Unpause()"))
	sigSendingPause        = crypto.Keccak256Hash([]byte("SendingPause()"))
	sigSendingUnpause      = crypto.Keccak256Hash([]byte("SendingUnpause()"))
	sigExecutingPause      = crypto.Keccak256Hash([]byte("ExecutingPause()"))
	sigExecutingUnpause    = crypto.Keccak256Hash([]byte("ExecutingUnpause()"))
)

type PauseState struct {
	BridgeType types.BridgeType
	Paused     bool
	Scope      string // "deposits", "withdrawals", "all", "sending", "executing"
}

type NeoXEventProcessorImpl struct {
	opStore       *datastore.OperationStore
	store         *datastore.Store
	tokenBridge   common.Address
	messageBridge common.Address
	logger        *slog.Logger
}

func NewNeoXEventProcessor(
	opStore *datastore.OperationStore,
	store *datastore.Store,
	tokenBridgeAddr string,
	messageBridgeAddr string,
	logger *slog.Logger,
) *NeoXEventProcessorImpl {
	return &NeoXEventProcessorImpl{
		opStore:       opStore,
		store:         store,
		tokenBridge:   common.HexToAddress(tokenBridgeAddr),
		messageBridge: common.HexToAddress(messageBridgeAddr),
		logger:        logger,
	}
}

func (p *NeoXEventProcessorImpl) ProcessLog(
	log ethtypes.Log,
	blockTime time.Time,
) (*types.BridgeOperation, *PauseState, error) {
	if len(log.Topics) == 0 {
		return nil, nil, nil
	}

	topic0 := log.Topics[0]

	if log.Address == p.tokenBridge {
		return p.processTokenBridgeLog(log, topic0, blockTime)
	}
	if log.Address == p.messageBridge {
		return p.processMessageBridgeLog(log, topic0, blockTime)
	}

	return nil, nil, nil
}

func (p *NeoXEventProcessorImpl) processTokenBridgeLog(
	log ethtypes.Log,
	topic0 common.Hash,
	blockTime time.Time,
) (*types.BridgeOperation, *PauseState, error) {
	switch topic0 {
	case sigNativeWithdrawal:
		op, err := p.handleNativeWithdrawal(log, blockTime)
		return op, nil, err
	case sigNativeDeposit, sigNativeClaimable, sigNativeClaim:
		op, err := p.handleNativeDepositComplete(log, blockTime)
		return op, nil, err
	case sigTokenWithdrawal:
		op, err := p.handleTokenWithdrawal(log, blockTime)
		return op, nil, err
	case sigTokenDeposit, sigTokenClaimable, sigTokenClaim:
		op, err := p.handleTokenDepositComplete(log, blockTime)
		return op, nil, err
	case sigBridgePause:
		return nil, &PauseState{Paused: true, Scope: "all"}, nil
	case sigBridgeUnpause:
		return nil, &PauseState{Paused: false, Scope: "all"}, nil
	case sigWithdrawalPause:
		return nil, &PauseState{Paused: true, Scope: "withdrawals"}, nil
	case sigWithdrawalUnpause:
		return nil, &PauseState{Paused: false, Scope: "withdrawals"}, nil
	case sigNativeBridgePause:
		return nil, &PauseState{BridgeType: types.BridgeTypeNative, Paused: true, Scope: "all"}, nil
	case sigNativeBridgeUnpause:
		return nil, &PauseState{BridgeType: types.BridgeTypeNative, Paused: false, Scope: "all"}, nil
	}
	return nil, nil, nil
}

func (p *NeoXEventProcessorImpl) processMessageBridgeLog(
	log ethtypes.Log,
	topic0 common.Hash,
	blockTime time.Time,
) (*types.BridgeOperation, *PauseState, error) {
	switch topic0 {
	case sigMessageSend:
		op, err := p.handleMessageSend(log, blockTime)
		return op, nil, err
	case sigExecution:
		op, err := p.handleMessageExecution(log, blockTime)
		return op, nil, err
	case sigPause:
		return nil, &PauseState{BridgeType: types.BridgeTypeMessage, Paused: true, Scope: "all"}, nil
	case sigUnpause:
		return nil, &PauseState{BridgeType: types.BridgeTypeMessage, Paused: false, Scope: "all"}, nil
	case sigSendingPause:
		return nil, &PauseState{BridgeType: types.BridgeTypeMessage, Paused: true, Scope: "sending"}, nil
	case sigSendingUnpause:
		return nil, &PauseState{BridgeType: types.BridgeTypeMessage, Paused: false, Scope: "sending"}, nil
	case sigExecutingPause:
		return nil, &PauseState{BridgeType: types.BridgeTypeMessage, Paused: true, Scope: "executing"}, nil
	case sigExecutingUnpause:
		return nil, &PauseState{BridgeType: types.BridgeTypeMessage, Paused: false, Scope: "executing"}, nil
	}
	return nil, nil, nil
}

func (p *NeoXEventProcessorImpl) handleNativeWithdrawal(log ethtypes.Log, blockTime time.Time) (*types.BridgeOperation, error) {
	// NativeWithdrawal(uint256 indexed nonce, address indexed to, uint256 amount, address from, bytes32 withdrawalHash, bytes32 withdrawalRoot)
	if len(log.Topics) < 3 {
		return nil, fmt.Errorf("NativeWithdrawal: expected >= 3 topics, got %d", len(log.Topics))
	}

	nonce := new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
	to := common.BytesToAddress(log.Topics[2].Bytes())

	amount := "0"
	from := common.Address{}
	if len(log.Data) >= 64 {
		amount = new(big.Int).SetBytes(log.Data[:32]).String()
		from = common.BytesToAddress(log.Data[32:64])
	}

	// data layout: amount(32) + from(32) + withdrawalHash(32) + withdrawalRoot(32)
	var root *string
	if len(log.Data) >= 128 {
		r := common.BytesToHash(log.Data[96:128]).Hex()
		root = &r
	}

	amountStr := amount
	return &types.BridgeOperation{
		BridgeType:        types.BridgeTypeNative,
		Direction:         types.DirectionWithdrawal,
		Nonce:             nonce,
		SourceChain:       chainNeoX,
		DestinationChain:  chainN3,
		SourceBlockHeight: log.BlockNumber,
		SourceTxHash:      log.TxHash.Hex(),
		Root:              root,
		Amount:            &amountStr,
		FromAddress:       from.Hex(),
		ToAddress:         to.Hex(),
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}, nil
}

func (p *NeoXEventProcessorImpl) handleNativeDepositComplete(log ethtypes.Log, blockTime time.Time) (*types.BridgeOperation, error) {
	if len(log.Topics) < 3 {
		return nil, nil
	}

	nonce := new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
	destBlock := log.BlockNumber

	err := p.opStore.UpdateOperationCompletion(
		types.BridgeTypeNative, types.DirectionDeposit, nonce,
		destBlock, log.TxHash.Hex(), blockTime,
	)
	if err != nil {
		p.logger.Debug("No pending native deposit for nonce", "nonce", nonce)
	}
	return nil, nil
}

func (p *NeoXEventProcessorImpl) handleTokenWithdrawal(log ethtypes.Log, blockTime time.Time) (*types.BridgeOperation, error) {
	// TokenWithdrawal(address indexed neoXToken, address neoN3Token, uint256 indexed nonce, address indexed to, uint256 amount, address from, bytes32 withdrawalHash, bytes32 withdrawalRoot)
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("TokenWithdrawal: expected >= 4 topics, got %d", len(log.Topics))
	}

	tokenAddr := common.BytesToAddress(log.Topics[1].Bytes())
	nonce := new(big.Int).SetBytes(log.Topics[2].Bytes()).Uint64()
	to := common.BytesToAddress(log.Topics[3].Bytes())

	amount := "0"
	from := common.Address{}
	var destTokenStr string
	if len(log.Data) >= 96 {
		// data: neoN3Token(32) + amount(32) + from(32) + withdrawalHash(32) + withdrawalRoot(32)
		destTokenStr = common.BytesToAddress(log.Data[0:32]).Hex()
		amount = new(big.Int).SetBytes(log.Data[32:64]).String()
		from = common.BytesToAddress(log.Data[64:96])
	}

	// withdrawalRoot at offset 128
	var root *string
	if len(log.Data) >= 160 {
		r := common.BytesToHash(log.Data[128:160]).Hex()
		root = &r
	}

	tokenStr := tokenAddr.Hex()
	amountStr := amount
	var destToken *string
	if destTokenStr != "" {
		destToken = &destTokenStr
	}
	return &types.BridgeOperation{
		BridgeType:        types.BridgeTypeToken,
		Direction:         types.DirectionWithdrawal,
		Nonce:             nonce,
		SourceChain:       chainNeoX,
		DestinationChain:  chainN3,
		SourceBlockHeight: log.BlockNumber,
		SourceTxHash:      log.TxHash.Hex(),
		TokenContract:     &tokenStr,
		DestTokenContract: destToken,
		Root:              root,
		Amount:            &amountStr,
		FromAddress:       from.Hex(),
		ToAddress:         to.Hex(),
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}, nil
}

func (p *NeoXEventProcessorImpl) handleTokenDepositComplete(log ethtypes.Log, blockTime time.Time) (*types.BridgeOperation, error) {
	if len(log.Topics) < 3 {
		return nil, nil
	}

	nonce := new(big.Int).SetBytes(log.Topics[2].Bytes()).Uint64()
	destBlock := log.BlockNumber

	err := p.opStore.UpdateOperationCompletion(
		types.BridgeTypeToken, types.DirectionDeposit, nonce,
		destBlock, log.TxHash.Hex(), blockTime,
	)
	if err != nil {
		p.logger.Debug("No pending token deposit for nonce", "nonce", nonce)
	}
	return nil, nil
}

func (p *NeoXEventProcessorImpl) handleMessageSend(log ethtypes.Log, blockTime time.Time) (*types.BridgeOperation, error) {
	// MessageSend(uint256 indexed nonce, address indexed sender, bytes encodedMetadata, bytes message, bytes32 messageHash, bytes32 newEvmToNeoRoot)
	if len(log.Topics) < 3 {
		return nil, fmt.Errorf("MessageSend: expected >= 3 topics, got %d", len(log.Topics))
	}

	nonce := new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
	sender := common.BytesToAddress(log.Topics[2].Bytes())

	// The newEvmToNeoRoot is the last 32 bytes of log.Data.
	// Data layout: offset(encodedMetadata) + offset(message) + messageHash(32) + newEvmToNeoRoot(32) + ...
	// We extract the root from the last 32 bytes as it's always appended at the end by the contract.
	var root *string
	if len(log.Data) >= 32 {
		r := common.BytesToHash(log.Data[len(log.Data)-32:]).Hex()
		root = &r
	}

	return &types.BridgeOperation{
		BridgeType:        types.BridgeTypeMessage,
		Direction:         types.DirectionWithdrawal,
		Nonce:             nonce,
		SourceChain:       chainNeoX,
		DestinationChain:  chainN3,
		SourceBlockHeight: log.BlockNumber,
		SourceTxHash:      log.TxHash.Hex(),
		Root:              root,
		FromAddress:       sender.Hex(),
		ToAddress:         "",
		Timestamp:         blockTime,
		Status:            types.StatusPending,
	}, nil
}

func (p *NeoXEventProcessorImpl) handleMessageExecution(log ethtypes.Log, blockTime time.Time) (*types.BridgeOperation, error) {
	// Execution(uint256 indexed nonce, AMBTypes.Result result)
	if len(log.Topics) < 2 {
		return nil, nil
	}

	nonce := new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
	destBlock := log.BlockNumber

	err := p.opStore.UpdateOperationCompletion(
		types.BridgeTypeMessage, types.DirectionDeposit, nonce,
		destBlock, log.TxHash.Hex(), blockTime,
	)
	if err != nil {
		p.logger.Debug("No pending message deposit for nonce", "nonce", nonce)
	}
	return nil, nil
}
