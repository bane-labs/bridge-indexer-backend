package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

const (
	chainNeoX                     = "neo_x"
	neoxPollInterval              = 3 * time.Second
	neoxSafetyDepth        uint64 = 20
	neoxMaxBlockRange      uint64 = 5000
	neoxCheckpointInterval        = 50 // flush checkpoint every N no-op ranges
)

type NeoXIndexer struct {
	client           *neox.Client
	store            *datastore.Store
	opStore          *datastore.OperationStore
	bridgeStateStore *datastore.BridgeStateStore
	processor        *NeoXEventProcessorImpl
	contracts        []common.Address
	startBlock       uint64
	concurrency      int
	logger           *slog.Logger
	pauseDetector    *monitor.PauseDetector
}

func NewNeoXIndexer(
	client *neox.Client,
	store *datastore.Store,
	opStore *datastore.OperationStore,
	bridgeStateStore *datastore.BridgeStateStore,
	processor *NeoXEventProcessorImpl,
	contractAddresses []string,
	startBlock uint64,
	concurrency int,
	logger *slog.Logger,
	pauseDetector *monitor.PauseDetector,
) *NeoXIndexer {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &NeoXIndexer{
		client:           client,
		store:            store,
		opStore:          opStore,
		bridgeStateStore: bridgeStateStore,
		processor:        processor,
		contracts:        neox.HexToAddresses(contractAddresses),
		startBlock:       startBlock,
		concurrency:      concurrency,
		logger:           logger,
		pauseDetector:    pauseDetector,
	}
}

func (idx *NeoXIndexer) Start(ctx context.Context) error {
	state, err := idx.store.GetIndexerState(chainNeoX)
	if err != nil && err != datastore.ErrNotFound {
		return fmt.Errorf("get indexer state: %w", err)
	}

	var startHeight uint64
	if state != nil {
		startHeight = state.LastBlockHeight + 1
		idx.logger.Info("Resuming NeoX indexer", "from_height", startHeight)
	} else {
		startHeight = idx.startBlock
		idx.logger.Info("Starting NeoX indexer", "from_height", startHeight)
	}

	return idx.runSerial(ctx, startHeight)
}

// runSerial is the original serial loop (concurrency=1 fallback).
func (idx *NeoXIndexer) runSerial(ctx context.Context, startHeight uint64) error {
	height := startHeight
	iterCount := 0

	for {
		select {
		case <-ctx.Done():
			idx.logger.Info("NeoX indexer stopping")
			return nil
		default:
		}

		chainHeight, err := idx.client.BlockNumber(ctx)
		if err != nil {
			idx.logger.Error("Failed to get NeoX block number", "error", err)
			time.Sleep(neoxPollInterval)
			continue
		}

		safeHeight := uint64(0)
		if chainHeight > neoxSafetyDepth {
			safeHeight = chainHeight - neoxSafetyDepth
		}

		if height > safeHeight {
			time.Sleep(neoxPollInterval)
			continue
		}

		endHeight := safeHeight
		if endHeight-height > neoxMaxBlockRange {
			endHeight = height + neoxMaxBlockRange
		}

		if err := idx.processBlockRange(ctx, height, endHeight); err != nil {
			idx.logger.Error("Failed to process NeoX block range", "from", height, "to", endHeight, "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		iterCount++
		if iterCount%10 == 0 {
			newChainHeight, err := idx.client.BlockNumber(ctx)
			if err != nil {
				idx.logger.Warn("Failed to refresh NeoX chain height", "error", err)
			} else {
				newSafeHeight := uint64(0)
				if newChainHeight > neoxSafetyDepth {
					newSafeHeight = newChainHeight - neoxSafetyDepth
				}
				if newSafeHeight > safeHeight {
					safeHeight = newSafeHeight
				}
			}
		}

		if interval := idx.client.CallInterval(); interval > 0 {
			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return nil
			}
		}

		height = endHeight + 1
	}
}

// processBlockRange processes a single range (serial path). Writes to store directly.
func (idx *NeoXIndexer) processBlockRange(ctx context.Context, from, to uint64) error {
	state, ops, err := idx.processBlockRangeWithResult(ctx, from, to)
	if err != nil {
		return err
	}

	if len(ops) > 0 {
		extraOps, bsErr := idx.buildBridgeStateWriteOps(ops)
		if bsErr != nil {
			return fmt.Errorf("build bridge state write ops: %w", bsErr)
		}
		if len(extraOps) > 0 {
			if err := idx.store.SaveIndexerStateWithOperationsAndExtra(state, ops, extraOps); err != nil {
				return fmt.Errorf("save state with ops: %w", err)
			}
		} else {
			if err := idx.store.SaveIndexerStateWithOperations(state, ops); err != nil {
				return fmt.Errorf("save state with ops: %w", err)
			}
		}
	} else {
		if err := idx.store.SaveIndexerState(state); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
	}

	if to%10000 == 0 || to == from {
		idx.logger.Info("NeoX indexer progress", "height", to)
	}

	return nil
}

// processBlockRangeWithResult does FilterLogs + event processing without writing to store.
// This is the core work unit used by both serial and parallel paths.
func (idx *NeoXIndexer) processBlockRangeWithResult(ctx context.Context, from, to uint64) (*types.IndexerState, []*types.BridgeOperation, error) {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: idx.contracts,
	}

	logs, err := idx.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("filter logs %d-%d: %w", from, to, err)
	}

	var allOps []*types.BridgeOperation
	headerCache := map[uint64]*ethtypes.Header{}

	for _, l := range logs {
		if l.Removed {
			idx.logger.Warn("Reorg detected: removed log",
				"block", l.BlockNumber,
				"tx", l.TxHash.Hex(),
			)
			continue
		}

		if idx.processor == nil {
			continue
		}

		// Per-log header fetch with cache to get accurate block timestamp.
		header, ok := headerCache[l.BlockNumber]
		if !ok {
			var hErr error
			header, hErr = idx.client.HeaderByNumber(ctx, new(big.Int).SetUint64(l.BlockNumber))
			if hErr != nil {
				idx.logger.Warn("Failed to fetch header for log",
					"block", l.BlockNumber,
					"tx", l.TxHash.Hex(),
					"error", hErr,
				)
				continue
			}
			headerCache[l.BlockNumber] = header
		}
		blockTime := time.Unix(int64(header.Time), 0)

		op, pauseState, pErr := idx.processor.ProcessLog(l, blockTime)
		if pErr != nil {
			idx.logger.Warn("Failed to process log",
				"tx", l.TxHash.Hex(),
				"error", pErr,
			)
			continue
		}
		if op != nil {
			allOps = append(allOps, op)
		}
		if pauseState != nil {
			idx.logger.Info("Bridge pause state change",
				"bridge_type", pauseState.BridgeType,
				"paused", pauseState.Paused,
				"scope", pauseState.Scope,
			)
			if idx.pauseDetector != nil {
				idx.pauseDetector.UpdatePause(pauseState.BridgeType, pauseState.Paused, pauseState.Scope)
			}
		}
	}

	// Fetch to-block header for LastBlockHash (may already be cached).
	toHeader, ok := headerCache[to]
	if !ok {
		var hErr error
		toHeader, hErr = idx.client.HeaderByNumber(ctx, new(big.Int).SetUint64(to))
		if hErr != nil {
			idx.logger.Warn("Failed to fetch header for to-block",
				"block", to,
				"error", hErr,
			)
		}
	}

	lastBlockHash := ""
	if toHeader != nil {
		lastBlockHash = toHeader.Hash().Hex()
	}

	newState := &types.IndexerState{
		Chain:           chainNeoX,
		LastBlockHeight: to,
		LastBlockHash:   lastBlockHash,
		IsSyncing:       true,
		UpdatedAt:       time.Now(),
	}

	if to%10000 == 0 {
		idx.logger.Info("NeoX indexer progress", "height", to, "logs_in_range", len(logs))
	}

	return newState, allOps, nil
}

// buildBridgeStateWriteOps builds atomic write ops for bridge state updates.
func (idx *NeoXIndexer) buildBridgeStateWriteOps(ops []*types.BridgeOperation) ([]datastore.WriteOp, error) {
	if idx.bridgeStateStore == nil {
		return nil, nil
	}
	return idx.bridgeStateStore.BuildSourceStateWriteOps(ops)
}
