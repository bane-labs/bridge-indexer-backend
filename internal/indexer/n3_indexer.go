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

			if err := idx.processBlock(ctx, height); err != nil {
				idx.logger.Error("Failed to process N3 block", "height", height, "error", err)
				time.Sleep(5 * time.Second)
				break
			}

			if interval := idx.client.CallInterval(); interval > 0 {
				select {
				case <-time.After(interval):
				case <-ctx.Done():
					return nil
				}
			}

			height++
		}
	}
}

func (idx *N3Indexer) processBlock(ctx context.Context, height uint32) error {
	state, ops, err := idx.processBlockWithResult(ctx, height)
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
	return nil
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
