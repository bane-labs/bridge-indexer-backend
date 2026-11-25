package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
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
	height       uint32
	state        *types.IndexerState
	ops          []*types.BridgeOperation
	observations []datastore.LifecycleObservation
	err          error
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
				if err := idx.applyProcessedBlockWithLifecycle(r.state, r.ops, r.observations); err != nil {
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
	state, ops, observations, err := idx.processBlockWithLifecycle(ctx, height)
	if err != nil {
		return err
	}
	return idx.applyProcessedBlockWithLifecycle(state, ops, observations)
}

func (idx *N3Indexer) applyProcessedBlock(state *types.IndexerState, ops []*types.BridgeOperation) error {
	return idx.applyProcessedBlockWithLifecycle(state, ops, nil)
}

func (idx *N3Indexer) applyProcessedBlockWithLifecycle(state *types.IndexerState, ops []*types.BridgeOperation, observations []datastore.LifecycleObservation) error {
	resolved, unresolved, err := idx.resolveN3TokenClaims(observations)
	if err != nil {
		return err
	}
	extra, err := idx.buildBridgeStateWriteOps(ops)
	if err != nil {
		return err
	}
	if err := idx.store.SaveN3LifecycleCheckpoint(state, ops, extra, resolved, unresolved...); err != nil {
		return fmt.Errorf("save N3 lifecycle checkpoint: %w", err)
	}
	for _, claim := range unresolved {
		if idx.logger != nil {
			idx.logger.Warn("N3 token claim awaits historical pair recovery", "nonce", claim.Nonce, "destination_token", claim.DestinationToken, "tx", claim.Evidence.TxHash, "reason", claim.Reason)
		}
	}
	return nil
}

// resolveN3TokenClaims pairs each N3 TokenClaim with exactly one verified
// claimable, from the same or an earlier block of observations or from the
// store. Claims without a unique pair are returned as unresolved records.
// Live indexing and historical recovery share this resolution.
func (idx *N3Indexer) resolveN3TokenClaims(observations []datastore.LifecycleObservation) ([]datastore.LifecycleObservation, []datastore.UnresolvedN3TokenClaim, error) {
	resolved := make([]datastore.LifecycleObservation, 0, len(observations))
	var unresolved []datastore.UnresolvedN3TokenClaim
	for i := range observations {
		o := observations[i]
		if o.Fact != datastore.FactClaim || o.Identity.BridgeType != types.BridgeTypeToken {
			resolved = append(resolved, o)
			continue
		}
		var pair *datastore.OperationIdentity
		ambiguous := false
		for j := range observations {
			candidate := &observations[j]
			if candidate.Fact == datastore.FactClaimable && candidate.Identity.BridgeType == types.BridgeTypeToken && candidate.Identity.Nonce == o.Identity.Nonce && candidate.Identity.DestinationToken == o.Identity.DestinationToken && candidate.Evidence.BlockHeight <= o.Evidence.BlockHeight {
				if pair != nil {
					ambiguous = true
					break
				}
				copy := candidate.Identity
				pair = &copy
			}
		}
		if !ambiguous {
			stored, err := idx.opStore.FindClaimableTokenPairForDirection(types.DirectionWithdrawal, o.Identity.Nonce, o.Identity.DestinationToken)
			if err != nil {
				if !errors.Is(err, datastore.ErrAmbiguousClaimPair) {
					return nil, nil, err
				}
				ambiguous = true
			} else if pair != nil && stored != nil && *pair != *stored {
				ambiguous = true
			} else if pair == nil {
				pair = stored
			}
		}
		if pair != nil && o.Identity.SourceToken != "" && o.Identity.SourceToken != pair.SourceToken {
			ambiguous = true
		}
		if pair == nil || ambiguous {
			reason := "missing_claimable_pair"
			if ambiguous {
				reason = "ambiguous_claimable_pair"
			}
			unresolved = append(unresolved, datastore.UnresolvedN3TokenClaim{Nonce: o.Identity.Nonce, DestinationToken: o.Identity.DestinationToken, Evidence: o.Evidence, Reason: reason})
			continue
		}
		o.Identity = *pair
		resolved = append(resolved, o)
	}
	return resolved, unresolved, nil
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
				state, ops, observations, err := idx.processBlockWithLifecycle(ctx, h)
				results[int(h-from)] = n3BlockResult{
					height:       h,
					state:        state,
					ops:          ops,
					observations: observations,
					err:          err,
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
	state, ops, _, err := idx.processBlockWithLifecycle(ctx, height)
	return state, ops, err
}

func (idx *N3Indexer) processBlockWithLifecycle(ctx context.Context, height uint32) (*types.IndexerState, []*types.BridgeOperation, []datastore.LifecycleObservation, error) {
	blk, err := idx.client.GetBlockByIndex(ctx, height)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get block %d: %w", height, err)
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
		return state, nil, nil, nil
	}

	// Concurrent AppLog fetching for multiple transactions
	type txResult struct {
		ops          []*types.BridgeOperation
		observations []datastore.LifecycleObservation
		err          error
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
			txHash := tx.Hash()
			appLog, err := idx.client.GetApplicationLog(ctx, txHash)
			if err != nil {
				txResults[i].err = fmt.Errorf("get application log %s: %w", txHash.StringLE(), err)
				return
			}
			if appLog == nil || len(appLog.Executions) == 0 || !appLog.Container.Equals(txHash) {
				txResults[i].err = fmt.Errorf("missing or mismatched application log for %s", txHash.StringLE())
				return
			}
			ops, observations, err := idx.decodeN3Executions(appLog.Executions, height, blk.Hash().StringLE(), txHash.StringLE(), time.UnixMilli(int64(blk.Timestamp)))
			txResults[i] = txResult{ops: ops, observations: observations, err: err}
		}(i, tx)
	}
	wg.Wait()

	var allOps []*types.BridgeOperation
	var allObservations []datastore.LifecycleObservation
	for _, r := range txResults {
		if r.err != nil {
			return nil, nil, nil, r.err
		}
		allOps = append(allOps, r.ops...)
		allObservations = append(allObservations, r.observations...)
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

	return state, allOps, allObservations, nil
}

// decodeN3Executions reads notifications by emitting contract. The invoking
// script can call the bridge indirectly without containing its contract hash.
func (idx *N3Indexer) decodeN3Executions(executions []state.Execution, height uint32, blockHash, txHash string, blockTime time.Time) ([]*types.BridgeOperation, []datastore.LifecycleObservation, error) {
	var ops []*types.BridgeOperation
	var observations []datastore.LifecycleObservation
	for execIndex, execution := range executions {
		if execution.VMState != vmstate.Halt {
			continue
		}
		var notifications []n3Notification
		for eventIndex, event := range execution.Events {
			if !idx.isWatchedContract(event.ScriptHash) {
				continue
			}
			notifications = append(notifications, n3Notification{event: event, position: fmt.Sprintf("%d:%d", execIndex, eventIndex)})
			if idx.processor == nil {
				continue
			}
			if err := checkN3NotificationVersion(event.ScriptHash.StringLE(), event.Name, uint64(height)); err != nil {
				return nil, nil, fmt.Errorf("%s in %s: %w", event.Name, txHash, err)
			}
			if event.ScriptHash.Equals(idx.processor.neoXBridge) && (event.Name == "NativeDeposit" || event.Name == "GasDeposit" || event.Name == "TokenDeposit") ||
				event.ScriptHash.Equals(idx.processor.messageBridge) && event.Name == "MessageSend" {
				op, err := idx.processor.ProcessEvent(event, height, txHash, blockTime)
				if err != nil {
					return nil, nil, fmt.Errorf("decode %s in %s: %w", event.Name, txHash, err)
				}
				if op != nil {
					ops = append(ops, op)
				}
			}
		}
		if idx.processor != nil {
			decoded, err := decodeN3LifecycleTransaction(notifications, idx.processor.neoXBridge, idx.processor.messageBridge, height, blockHash, txHash, blockTime)
			if err != nil {
				return nil, nil, fmt.Errorf("decode N3 lifecycle in %s: %w", txHash, err)
			}
			observations = append(observations, decoded...)
		}
	}
	return ops, observations, nil
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
