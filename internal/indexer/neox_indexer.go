package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"sync"
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
	neoxSafetyDepth        uint64 = 0
	neoxMaxBlockRange      uint64 = 250 // claim detection fetches every block's transactions
	neoxCheckpointInterval        = 50  // flush checkpoint every N no-op ranges
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
	state, ops, observations, err := idx.processBlockRangeWithResult(ctx, from, to)
	if err != nil {
		return err
	}

	extraOps, err := idx.buildBridgeStateWriteOps(ops)
	if err != nil {
		return fmt.Errorf("build bridge state write ops: %w", err)
	}
	if err := idx.store.SaveNeoXLifecycleCheckpoint(state, ops, extraOps, observations); err != nil {
		return fmt.Errorf("save Neo X checkpoint: %w", err)
	}

	if to%10000 == 0 || to == from {
		idx.logger.Info("NeoX indexer progress", "height", to)
	}

	return nil
}

// processBlockRangeWithResult does FilterLogs + event processing without writing to store.
// This is the core work unit used by both serial and parallel paths.
func (idx *NeoXIndexer) processBlockRangeWithResult(ctx context.Context, from, to uint64) (*types.IndexerState, []*types.BridgeOperation, []datastore.LifecycleObservation, error) {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: idx.contracts,
	}

	logs, err := idx.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("filter logs %d-%d: %w", from, to, err)
	}

	// Collect unique block numbers that need headers (log blocks + to-block).
	needHeaders := map[uint64]struct{}{to: {}}
	for _, l := range logs {
		if !l.Removed {
			needHeaders[l.BlockNumber] = struct{}{}
		}
	}

	// Fetch all required headers concurrently.
	headerCache := idx.prefetchHeaders(ctx, needHeaders)
	for height := range needHeaders {
		if headerCache[height] == nil {
			return nil, nil, nil, fmt.Errorf("missing Neo X header %d", height)
		}
	}
	// The block's transactions are required: a successful claimToken call has
	// no bridge log in the pinned implementation.
	var observations []datastore.LifecycleObservation
	byTx := make(map[common.Hash][]ethtypes.Log)
	for _, l := range logs {
		if l.Removed {
			return nil, nil, nil, fmt.Errorf("removed Neo X log at %d", l.BlockNumber)
		}
		if l.BlockHash != headerCache[l.BlockNumber].Hash() {
			return nil, nil, nil, fmt.Errorf("log/header hash mismatch at %d", l.BlockNumber)
		}
		byTx[l.TxHash] = append(byTx[l.TxHash], l)
	}
	for txHash, txLogs := range byTx {
		receipt, err := idx.client.TransactionReceipt(ctx, txHash)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("receipt %s: %w", txHash, err)
		}
		if receipt.BlockHash != txLogs[0].BlockHash || receipt.TxHash != txHash {
			return nil, nil, nil, fmt.Errorf("receipt provenance mismatch %s", txHash)
		}
		if receipt.Status != ethtypes.ReceiptStatusSuccessful {
			return nil, nil, nil, fmt.Errorf("bridge logs in failed transaction %s", txHash)
		}
		for _, l := range txLogs {
			matched := false
			for _, recorded := range receipt.Logs {
				if recorded.Index == l.Index && recorded.Address == l.Address && reflect.DeepEqual(recorded.Topics, l.Topics) && reflect.DeepEqual(recorded.Data, l.Data) {
					matched = true
					break
				}
			}
			if !matched {
				return nil, nil, nil, fmt.Errorf("filtered log missing from receipt %s index %d", txHash, l.Index)
			}
		}
		if idx.processor != nil {
			decoded, err := decodeNeoXLifecycleTransaction(txLogs, idx.processor.tokenBridge, idx.processor.messageBridge, time.Unix(int64(headerCache[txLogs[0].BlockNumber].Time), 0))
			if err != nil {
				return nil, nil, nil, err
			}
			observations = append(observations, decoded...)
		}
	}
	if idx.processor != nil && idx.opStore != nil {
		for height := from; height <= to; height++ {
			block, err := idx.client.BlockByNumber(ctx, new(big.Int).SetUint64(height))
			if err != nil {
				return nil, nil, nil, fmt.Errorf("fetch claim block %d: %w", height, err)
			}
			if h := headerCache[height]; h != nil && h.Hash() != block.Hash() {
				return nil, nil, nil, fmt.Errorf("claim block hash mismatch %d", height)
			}
			for _, tx := range block.Transactions() {
				if tx.To() == nil || *tx.To() != idx.processor.tokenBridge || len(tx.Data()) != 68 || string(tx.Data()[:4]) != string(claimTokenSelector) {
					continue
				}
				n, err := wordNonce(common.BytesToHash(tx.Data()[36:68]))
				if err != nil {
					return nil, nil, nil, err
				}
				token := common.BytesToAddress(tx.Data()[16:36]).Hex()
				pair, err := idx.opStore.FindClaimableTokenPair(n, token)
				if err != nil {
					return nil, nil, nil, err
				}
				if pair == nil {
					for _, o := range observations {
						if o.Fact == datastore.FactClaimable && o.Evidence.BlockHeight <= height && o.Identity.Nonce == n && common.HexToAddress(o.Identity.DestinationToken) == common.HexToAddress(token) {
							id := o.Identity
							pair = &id
							break
						}
					}
				}
				if pair == nil {
					continue
				} // historical claims require recovery of the original claimable
				var claimable *datastore.LifecycleEvidence
				for _, o := range observations {
					if o.Fact == datastore.FactClaimable && o.Identity == *pair && o.Evidence.BlockHeight <= height {
						copy := o.Evidence
						claimable = &copy
						break
					}
				}
				if claimable == nil {
					prior, err := idx.opStore.GetLifecycle(*pair)
					if err != nil {
						return nil, nil, nil, err
					}
					claimable = prior.Claimable
				}
				if claimable == nil || claimable.BlockHeight > height {
					continue
				}
				receipt, err := idx.client.TransactionReceipt(ctx, tx.Hash())
				if err != nil {
					return nil, nil, nil, err
				}
				if receipt.BlockHash != block.Hash() || receipt.BlockNumber.Uint64() != height {
					return nil, nil, nil, fmt.Errorf("claim receipt provenance mismatch")
				}
				if receipt.Status != ethtypes.ReceiptStatusSuccessful {
					continue
				}
				// An end-of-block EIP-1967 lookup cannot identify the active
				// implementation at a claim if this proxy upgraded in the block.
				for _, observed := range logs {
					if observed.BlockNumber == height && observed.Address == idx.processor.tokenBridge && len(observed.Topics) > 0 && observed.Topics[0] == sigProxyUpgrade {
						return nil, nil, nil, fmt.Errorf("implementation changed in claim block %d", height)
					}
				}
				impl, err := idx.client.StorageAt(ctx, idx.processor.tokenBridge, common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"), new(big.Int).SetUint64(height))
				if err != nil {
					return nil, nil, nil, err
				}
				claim, err := decodeDirectTokenClaim(tx, receipt, idx.processor.tokenBridge, common.BytesToAddress(impl), time.Unix(int64(block.Time()), 0), pair, claimable)
				if err != nil {
					return nil, nil, nil, err
				}
				if claim != nil {
					observations = append(observations, *claim)
				}
			}
		}
	}

	var allOps []*types.BridgeOperation
	for _, l := range logs {
		if idx.processor == nil {
			continue
		}

		header, ok := headerCache[l.BlockNumber]
		if !ok {
			idx.logger.Warn("Missing header for log (skipped)",
				"block", l.BlockNumber,
				"tx", l.TxHash.Hex(),
			)
			return nil, nil, nil, fmt.Errorf("missing header for Neo X log at %d", l.BlockNumber)
		}
		blockTime := time.Unix(int64(header.Time), 0)

		op, pauseState, pErr := idx.processor.ProcessLog(l, blockTime)
		if pErr != nil {
			idx.logger.Warn("Failed to process log",
				"tx", l.TxHash.Hex(),
				"error", pErr,
			)
			return nil, nil, nil, fmt.Errorf("decode Neo X log %s: %w", l.TxHash, pErr)
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

	lastBlockHash := ""
	if toHeader, ok := headerCache[to]; ok {
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

	return newState, allOps, observations, nil
}

// prefetchHeaders fetches all required block headers concurrently.
func (idx *NeoXIndexer) prefetchHeaders(ctx context.Context, blocks map[uint64]struct{}) map[uint64]*ethtypes.Header {
	const maxConcurrent = 10
	sem := make(chan struct{}, maxConcurrent)

	type result struct {
		num uint64
		hdr *ethtypes.Header
	}

	results := make(chan result, len(blocks))
	var wg sync.WaitGroup

	for bn := range blocks {
		wg.Add(1)
		sem <- struct{}{}
		go func(num uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			hdr, err := idx.client.HeaderByNumber(ctx, new(big.Int).SetUint64(num))
			if err != nil {
				idx.logger.Warn("Failed to fetch header", "block", num, "error", err)
				return
			}
			results <- result{num: num, hdr: hdr}
		}(bn)
	}

	wg.Wait()
	close(results)

	cache := make(map[uint64]*ethtypes.Header, len(blocks))
	for r := range results {
		cache[r.num] = r.hdr
	}
	return cache
}

// buildBridgeStateWriteOps builds atomic write ops for bridge state updates.
func (idx *NeoXIndexer) buildBridgeStateWriteOps(ops []*types.BridgeOperation) ([]datastore.WriteOp, error) {
	if idx.bridgeStateStore == nil {
		return nil, nil
	}
	return idx.bridgeStateStore.BuildSourceStateWriteOps(ops)
}
