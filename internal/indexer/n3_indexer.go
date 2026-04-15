package indexer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
)

const (
	chainN3            = "neo_n3"
	n3PollInterval     = 15 * time.Second
	checkpointInterval = 200
)

type N3Indexer struct {
	client           *n3.Client
	store            *datastore.Store
	opStore          *datastore.OperationStore
	bridgeStateStore *datastore.BridgeStateStore
	processor        *N3EventProcessorImpl
	contracts        []util.Uint160
	startBlock       uint64
	concurrency      int
	logger           *slog.Logger
}

type n3BlockResult struct {
	height uint32
	state  *types.IndexerState
	ops    []*types.BridgeOperation
	err    error
}

func NewN3Indexer(
	client *n3.Client,
	store *datastore.Store,
	opStore *datastore.OperationStore,
	bridgeStateStore *datastore.BridgeStateStore,
	processor *N3EventProcessorImpl,
	contractHashes []string,
	startBlock uint64,
	concurrency int,
	logger *slog.Logger,
) (*N3Indexer, error) {
	contracts := make([]util.Uint160, 0, len(contractHashes))
	for _, h := range contractHashes {
		hash, err := util.Uint160DecodeStringLE(h)
		if err != nil {
			return nil, fmt.Errorf("invalid N3 contract hash %q: %w", h, err)
		}
		contracts = append(contracts, hash)
	}

	return &N3Indexer{
		client:           client,
		store:            store,
		opStore:          opStore,
		bridgeStateStore: bridgeStateStore,
		processor:        processor,
		contracts:        contracts,
		startBlock:       startBlock,
		concurrency:      concurrency,
		logger:           logger,
	}, nil
}

func (idx *N3Indexer) Start(ctx context.Context) error {
	state, err := idx.store.GetIndexerState(chainN3)
	if err != nil && err != datastore.ErrNotFound {
		return fmt.Errorf("get indexer state: %w", err)
	}

	var startHeight uint32
	if state != nil {
		startHeight = uint32(state.LastBlockHeight) + 1
		idx.logger.Info("Resuming N3 indexer", "from_height", startHeight)
	} else {
		startHeight = uint32(idx.startBlock)
		idx.logger.Info("Starting N3 indexer", "from_height", startHeight)
	}

	return idx.run(ctx, startHeight)
}

func (idx *N3Indexer) run(ctx context.Context, startHeight uint32) error {
	return idx.runSerial(ctx, startHeight)
}

func (idx *N3Indexer) runSerial(ctx context.Context, startHeight uint32) error {
	height := startHeight
	workers := idx.blockWorkerCount()
	batchSize := workers * 2
	if batchSize < 1 {
		batchSize = 1
	}

	for {
		select {
		case <-ctx.Done():
			idx.logger.Info("N3 indexer stopping")
			return nil
		default:
		}

		blockCount, err := idx.client.GetBlockCount(ctx)
		if err != nil {
			idx.logger.Error("Failed to get N3 block count", "error", err)
			time.Sleep(n3PollInterval)
			continue
		}

		chainHeight := blockCount - 1

		if height > chainHeight {
			time.Sleep(n3PollInterval)
			continue
		}

		iterCount := 0
		for height <= chainHeight {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			iterCount++
			if iterCount%10 == 0 {
				newBlockCount, err := idx.client.GetBlockCount(ctx)
				if err != nil {
					idx.logger.Warn("Failed to refresh N3 chain height", "error", err)
				} else {
					chainHeight = newBlockCount - 1
				}
			}

			batchEnd := height
			if workers > 1 {
				nextEnd := height + uint32(batchSize) - 1
				if nextEnd < batchEnd || nextEnd > chainHeight {
					nextEnd = chainHeight
				}
				batchEnd = nextEnd
			}

			results, err := idx.processBlockBatchWithResult(ctx, height, batchEnd, workers)
			if err != nil {
				idx.logger.Error("Failed to process N3 block batch", "from", height, "to", batchEnd, "error", err)
				time.Sleep(5 * time.Second)
				break
			}

			for _, r := range results {
				if r.err != nil {
					idx.logger.Error("Failed to process N3 block", "height", r.height, "error", r.err)
					time.Sleep(5 * time.Second)
					// Retry from failed height.
					height = r.height
					goto nextOuterTick
				}
				if err := idx.applyProcessedBlock(r.state, r.ops); err != nil {
					idx.logger.Error("Failed to persist N3 block", "height", r.height, "error", err)
					time.Sleep(5 * time.Second)
					// Retry from failed height.
					height = r.height
					goto nextOuterTick
				}
				height = r.height + 1
			}
		}
	nextOuterTick:
	}
}

func (idx *N3Indexer) processBlock(ctx context.Context, height uint32) error {
	state, ops, err := idx.processBlockWithResult(ctx, height)
	if err != nil {
		return err
	}
	return idx.applyProcessedBlock(state, ops)
}

func (idx *N3Indexer) applyProcessedBlock(state *types.IndexerState, ops []*types.BridgeOperation) error {
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
	return nil
}

func (idx *N3Indexer) processBlockBatchWithResult(ctx context.Context, from, to uint32, workers int) ([]n3BlockResult, error) {
	if to < from {
		return nil, nil
	}
	if workers < 1 {
		workers = 1
	}

	total := int(to-from) + 1
	results := make([]n3BlockResult, total)
	jobs := make(chan uint32, total)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range jobs {
				state, ops, err := idx.processBlockWithResult(ctx, h)
				results[int(h-from)] = n3BlockResult{
					height: h,
					state:  state,
					ops:    ops,
					err:    err,
				}
			}
		}()
	}

	for h := from; h <= to; h++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, ctx.Err()
		case jobs <- h:
		}
	}
	close(jobs)
	wg.Wait()
	return results, nil
}

func (idx *N3Indexer) blockWorkerCount() int {
	// Conservative parallelism across blocks; per-block processing is already parallel.
	// Scale with available RPC pool to utilize multiple endpoints.
	workers := 1
	switch {
	case idx.concurrency >= 120:
		workers = 6
	case idx.concurrency >= 80:
		workers = 5
	case idx.concurrency >= 40:
		workers = 4
	case idx.concurrency >= 20:
		workers = 3
	case idx.concurrency >= 10:
		workers = 2
	}
	if idx.client != nil {
		switch ps := idx.client.PoolSize(); {
		case ps >= 56: // ~7 endpoints x 8 conns
			if workers < 8 {
				workers = 8
			}
		case ps >= 35:
			if workers < 6 {
				workers = 6
			}
		case ps >= 14:
			if workers < 4 {
				workers = 4
			}
		case ps >= 7:
			if workers < 3 {
				workers = 3
			}
		}
	}
	return workers
}

func (idx *N3Indexer) processBlockWithResult(ctx context.Context, height uint32) (*types.IndexerState, []*types.BridgeOperation, error) {
	blk, err := idx.client.GetBlockByIndex(ctx, height)
	if err != nil {
		return nil, nil, fmt.Errorf("get block %d: %w", height, err)
	}

	// Empty block, no need for any RPC calls
	if len(blk.Transactions) == 0 {
		state := &types.IndexerState{
			Chain:           chainN3,
			LastBlockHeight: uint64(height),
			LastBlockHash:   blk.Hash().StringLE(),
			IsSyncing:       true,
			UpdatedAt:       time.Now(),
		}
		if height%1000 == 0 {
			idx.logger.Info("N3 indexer progress", "height", height)
		}
		return state, nil, nil
	}

	// Concurrent AppLog fetching for multiple transactions
	type txResult struct {
		ops []*types.BridgeOperation
	}
	txResults := make([]txResult, len(blk.Transactions))
	var wg sync.WaitGroup
	sem := make(chan struct{}, idx.concurrency)

	for i, tx := range blk.Transactions {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, tx *transaction.Transaction) {
			defer wg.Done()
			defer func() { <-sem }()
			// Pre-filter: skip transactions whose Script doesn't contain any watched contract hash
			if !idx.txMayInvokeContracts(tx.Script, idx.contracts) {
				return
			}
			txHash := tx.Hash()
			appLog, err := idx.client.GetApplicationLog(ctx, txHash)
			if err != nil {
				idx.logger.Warn("Failed to get app log", "tx", txHash.StringLE(), "error", err)
				return
			}
			var ops []*types.BridgeOperation
			for _, exec := range appLog.Executions {
				if exec.VMState != vmstate.Halt {
					continue
				}
				for _, event := range exec.Events {
					if !idx.isWatchedContract(event.ScriptHash) {
						continue
					}
					if idx.processor == nil {
						continue
					}
					blockTime := time.UnixMilli(int64(blk.Timestamp))
					op, err := idx.processor.ProcessEvent(event, height, txHash.StringLE(), blockTime)
					if err != nil {
						idx.logger.Warn("Failed to process event",
							"event", event.Name,
							"tx", txHash.StringLE(),
							"error", err,
						)
						continue
					}
					if op != nil {
						ops = append(ops, op)
					}
				}
			}
			txResults[i] = txResult{ops: ops}
		}(i, tx)
	}
	wg.Wait()

	var allOps []*types.BridgeOperation
	for _, r := range txResults {
		allOps = append(allOps, r.ops...)
	}

	state := &types.IndexerState{
		Chain:           chainN3,
		LastBlockHeight: uint64(height),
		LastBlockHash:   blk.Hash().StringLE(),
		IsSyncing:       true,
		UpdatedAt:       time.Now(),
	}

	if height%1000 == 0 {
		idx.logger.Info("N3 indexer progress", "height", height)
	}

	return state, allOps, nil
}

// txMayInvokeContracts checks if a transaction's Script bytecode may invoke any watched contracts.
// This is a conservative pre-filter: false positives are possible (extra AppLog calls),
// but false negatives are not (will never skip a transaction that actually invokes a watched contract).
func (idx *N3Indexer) txMayInvokeContracts(script []byte, contracts []util.Uint160) bool {
	for _, c := range contracts {
		if bytes.Contains(script, c[:]) {
			return true
		}
	}
	return false
}

func (idx *N3Indexer) isWatchedContract(hash util.Uint160) bool {
	for _, c := range idx.contracts {
		if c.Equals(hash) {
			return true
		}
	}
	return false
}

// buildBridgeStateWriteOps builds atomic write ops for bridge state updates.
func (idx *N3Indexer) buildBridgeStateWriteOps(ops []*types.BridgeOperation) ([]datastore.WriteOp, error) {
	if idx.bridgeStateStore == nil {
		return nil, nil
	}
	return idx.bridgeStateStore.BuildSourceStateWriteOps(ops)
}
