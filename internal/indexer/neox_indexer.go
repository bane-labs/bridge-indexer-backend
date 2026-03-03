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
	client      *neox.Client
	store       *datastore.Store
	opStore     *datastore.OperationStore
	processor   *NeoXEventProcessorImpl
	contracts   []common.Address
	startBlock  uint64
	concurrency int
	logger        *slog.Logger
	pauseDetector *monitor.PauseDetector
}

func NewNeoXIndexer(
	client *neox.Client,
	store *datastore.Store,
	opStore *datastore.OperationStore,
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
		client:      client,
		store:       store,
		opStore:     opStore,
		processor:   processor,
		contracts:   neox.HexToAddresses(contractAddresses),
		startBlock:  startBlock,
		concurrency: concurrency,
		logger:        logger,
		pauseDetector: pauseDetector,
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

	if idx.concurrency <= 1 {
		return idx.runSerial(ctx, startHeight)
	}
	return idx.runParallel(ctx, startHeight)
}

// runSerial is the original serial loop (concurrency=1 fallback).
func (idx *NeoXIndexer) runSerial(ctx context.Context, startHeight uint64) error {
	height := startHeight

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

		height = endHeight + 1
	}
}

// runParallel uses a sliding-window pipeline of concurrent FilterLogs requests.
func (idx *NeoXIndexer) runParallel(ctx context.Context, startHeight uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type rangeResult struct {
		from, to uint64
		state    *types.IndexerState
		ops      []*types.BridgeOperation
		err      error
		ready    chan struct{}
	}

	slots := make(chan *rangeResult, idx.concurrency*2)
	sem := make(chan struct{}, idx.concurrency)

	// Producer: carve block ranges and dispatch concurrent FilterLogs calls.
	go func() {
		defer close(slots)
		height := startHeight

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			chainHeight, err := idx.client.BlockNumber(ctx)
			if err != nil {
				idx.logger.Error("Failed to get NeoX block number", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(neoxPollInterval):
				}
				continue
			}

			safeHeight := uint64(0)
			if chainHeight > neoxSafetyDepth {
				safeHeight = chainHeight - neoxSafetyDepth
			}
			if height > safeHeight {
				select {
				case <-ctx.Done():
					return
				case <-time.After(neoxPollInterval):
				}
				continue
			}

			for height <= safeHeight {
				select {
				case <-ctx.Done():
					return
				case sem <- struct{}{}:
				}

				endHeight := height + neoxMaxBlockRange
				if endHeight > safeHeight {
					endHeight = safeHeight
				}

				r := &rangeResult{from: height, to: endHeight, ready: make(chan struct{})}
				select {
				case slots <- r:
				case <-ctx.Done():
					<-sem
					return
				}

				go func(r *rangeResult) {
					defer func() { <-sem }()
					r.state, r.ops, r.err = idx.processBlockRangeWithResult(ctx, r.from, r.to)
					close(r.ready)
				}(r)

				height = endHeight + 1
			}
		}
	}()

	// Consumer: drain results in order, write to datastore.
	noOpCount := 0
	var lastNoOpState *types.IndexerState

	for r := range slots {
		select {
		case <-r.ready:
		case <-ctx.Done():
			idx.logger.Info("NeoX indexer stopping")
			return nil
		}

		if r.err != nil {
			idx.logger.Error("Failed to process NeoX block range", "from", r.from, "to", r.to, "error", r.err)
			cancel()
			for range slots {
			}
			return r.err
		}

		if len(r.ops) > 0 {
			// Flush any pending no-op checkpoint first.
			if lastNoOpState != nil {
				if err := idx.store.SaveIndexerState(lastNoOpState); err != nil {
					return fmt.Errorf("save noop checkpoint at %d: %w", lastNoOpState.LastBlockHeight, err)
				}
				noOpCount = 0
				lastNoOpState = nil
			}
			if err := idx.store.SaveIndexerStateWithOperations(r.state, r.ops); err != nil {
				return fmt.Errorf("save state with ops at %d: %w", r.to, err)
			}
			continue
		}

		// No-op range: batch checkpoints.
		noOpCount++
		lastNoOpState = r.state
		if noOpCount >= neoxCheckpointInterval {
			if err := idx.store.SaveIndexerState(lastNoOpState); err != nil {
				return fmt.Errorf("save checkpoint at %d: %w", r.to, err)
			}
			noOpCount = 0
			lastNoOpState = nil
		}
	}

	// Flush final no-op checkpoint.
	if lastNoOpState != nil {
		if err := idx.store.SaveIndexerState(lastNoOpState); err != nil {
			return fmt.Errorf("save final checkpoint: %w", err)
		}
	}

	return nil
}

// processBlockRange processes a single range (serial path). Writes to store directly.
func (idx *NeoXIndexer) processBlockRange(ctx context.Context, from, to uint64) error {
	state, ops, err := idx.processBlockRangeWithResult(ctx, from, to)
	if err != nil {
		return err
	}

	if len(ops) > 0 {
		if err := idx.store.SaveIndexerStateWithOperations(state, ops); err != nil {
			return fmt.Errorf("save state with ops: %w", err)
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
