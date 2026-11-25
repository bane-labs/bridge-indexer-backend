package indexer

import (
	"context"
	"errors"
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
	chainNeoX                      = "neo_x"
	neoxPollInterval               = 3 * time.Second
	neoxSafetyDepth         uint64 = 0
	neoxMaxBlockRange       uint64 = 250 // claim detection fetches every block's transactions
	neoxCheckpointInterval         = 50  // flush checkpoint every N no-op ranges
	neoxEnrichRetryInterval        = time.Minute
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
	nextEnrich       time.Time
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
			idx.enrichClaimTraceGap(ctx)
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
	state, ops, observations, gaps, err := idx.processBlockRangeWithResult(ctx, from, to)
	if err != nil {
		return err
	}

	extraOps, err := idx.buildBridgeStateWriteOps(ops)
	if err != nil {
		return fmt.Errorf("build bridge state write ops: %w", err)
	}
	if err := idx.store.SaveNeoXLifecycleCheckpoint(state, ops, extraOps, observations, gaps...); err != nil {
		return fmt.Errorf("save Neo X checkpoint: %w", err)
	}

	if to%10000 == 0 || to == from {
		idx.logger.Info("NeoX indexer progress", "height", to)
	}

	return nil
}

// processBlockRangeWithResult does FilterLogs + event processing without writing to store.
// This is the core work unit used by both serial and parallel paths.
func (idx *NeoXIndexer) processBlockRangeWithResult(ctx context.Context, from, to uint64) (*types.IndexerState, []*types.BridgeOperation, []datastore.LifecycleObservation, []datastore.NeoXClaimTraceGap, error) {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: idx.contracts,
	}

	logs, err := idx.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("filter logs %d-%d: %w", from, to, err)
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
			return nil, nil, nil, nil, fmt.Errorf("missing Neo X header %d", height)
		}
	}
	// The block's transactions are required: a successful claimToken call has
	// no bridge log in the pinned implementation.
	var observations []datastore.LifecycleObservation
	byTx := make(map[common.Hash][]ethtypes.Log)
	for _, l := range logs {
		if l.Removed {
			return nil, nil, nil, nil, fmt.Errorf("removed Neo X log at %d", l.BlockNumber)
		}
		if l.BlockHash != headerCache[l.BlockNumber].Hash() {
			return nil, nil, nil, nil, fmt.Errorf("log/header hash mismatch at %d", l.BlockNumber)
		}
		byTx[l.TxHash] = append(byTx[l.TxHash], l)
	}
	for txHash, txLogs := range byTx {
		receipt, err := idx.client.TransactionReceipt(ctx, txHash)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("receipt %s: %w", txHash, err)
		}
		if receipt.BlockHash != txLogs[0].BlockHash || receipt.TxHash != txHash {
			return nil, nil, nil, nil, fmt.Errorf("receipt provenance mismatch %s", txHash)
		}
		if receipt.Status != ethtypes.ReceiptStatusSuccessful {
			return nil, nil, nil, nil, fmt.Errorf("bridge logs in failed transaction %s", txHash)
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
				return nil, nil, nil, nil, fmt.Errorf("filtered log missing from receipt %s index %d", txHash, l.Index)
			}
		}
		if idx.processor != nil {
			decoded, err := decodeNeoXLifecycleTransaction(txLogs, idx.processor.tokenBridge, idx.processor.messageBridge, time.Unix(int64(headerCache[txLogs[0].BlockNumber].Time), 0))
			if err != nil {
				return nil, nil, nil, nil, err
			}
			observations = append(observations, decoded...)
		}
	}
	// Blocks are required; their call traces are optional enrichment. A block
	// without traces still yields verified direct claims and is recorded as a
	// claim coverage gap, never as evidence that no indirect claim happened.
	var gaps []datastore.NeoXClaimTraceGap
	if idx.processor != nil && idx.opStore != nil {
		tracing, reason := true, ""
		for height := from; height <= to; height++ {
			block, err := idx.client.BlockByNumber(ctx, new(big.Int).SetUint64(height))
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("fetch claim block %d: %w", height, err)
			}
			if h := headerCache[height]; h != nil && h.Hash() != block.Hash() {
				return nil, nil, nil, nil, fmt.Errorf("claim block hash mismatch %d", height)
			}
			var traces []neox.TransactionTrace
			if tracing {
				raw, traceErr := idx.client.TraceBlock(ctx, height)
				traces, reason, err = idx.checkTraces(ctx, block, raw, traceErr)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				// One failed request per range bounds the load on a struggling
				// endpoint; a block-specific invalid trace does not stop tracing.
				// Skipped blocks keep the reason that stopped tracing.
				tracing = reason == "" || reason == "trace_invalid"
			}
			claims, err := idx.blockClaims(ctx, block, traces, logs, observations)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			observations = append(observations, claims...)
			if reason != "" && len(block.Transactions()) > 0 {
				gaps = appendClaimTraceGap(gaps, height, reason)
			}
		}
		if len(gaps) > 0 {
			idx.logger.Warn("Neo X indirect claim coverage deferred", "from", from, "to", to, "gaps", len(gaps), "reason", gaps[0].Reason)
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
			return nil, nil, nil, nil, fmt.Errorf("missing header for Neo X log at %d", l.BlockNumber)
		}
		blockTime := time.Unix(int64(header.Time), 0)

		op, pauseState, pErr := idx.processor.ProcessLog(l, blockTime)
		if pErr != nil {
			idx.logger.Warn("Failed to process log",
				"tx", l.TxHash.Hex(),
				"error", pErr,
			)
			return nil, nil, nil, nil, fmt.Errorf("decode Neo X log %s: %w", l.TxHash, pErr)
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

	return newState, allOps, observations, gaps, nil
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

// checkTraces classifies an optional trace response. Only cancellation is an
// error; unsupported, failed, or inconsistent traces become a gap reason.
func (idx *NeoXIndexer) checkTraces(ctx context.Context, block *ethtypes.Block, traces []neox.TransactionTrace, err error) ([]neox.TransactionTrace, string, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, "", ctxErr
	}
	switch {
	case errors.Is(err, neox.ErrTraceUnsupported):
		return nil, "trace_unsupported", nil
	case err != nil:
		idx.logger.Warn("Neo X block trace unavailable", "block", block.NumberU64(), "error", err)
		return nil, "trace_unavailable", nil
	}
	if err := verifyBlockClaimTraces(block, traces); err != nil {
		idx.logger.Warn("Neo X block trace rejected", "block", block.NumberU64(), "error", err)
		return nil, "trace_invalid", nil
	}
	return traces, "", nil
}

// appendClaimTraceGap extends the last gap when height continues it.
func appendClaimTraceGap(gaps []datastore.NeoXClaimTraceGap, height uint64, reason string) []datastore.NeoXClaimTraceGap {
	if n := len(gaps); n > 0 && gaps[n-1].ToHeight+1 == height && gaps[n-1].Reason == reason {
		gaps[n-1].ToHeight = height
		return gaps
	}
	return append(gaps, datastore.NeoXClaimTraceGap{FromHeight: height, ToHeight: height, Reason: reason})
}

// blockClaims finds verified claimToken calls in one block. With traces nil,
// only direct top-level calls are inspected: a successful receipt for a
// transaction sent to the bridge proves that outer call succeeded. Indirect
// calls need traces and are left to the caller's gap record.
func (idx *NeoXIndexer) blockClaims(ctx context.Context, block *ethtypes.Block, traces []neox.TransactionTrace, logs []ethtypes.Log, rangeObs []datastore.LifecycleObservation) ([]datastore.LifecycleObservation, error) {
	bridge := idx.processor.tokenBridge
	height := block.NumberU64()
	var claims []datastore.LifecycleObservation
	for txIndex, tx := range block.Transactions() {
		direct := tx.To() != nil && *tx.To() == bridge
		var calls []claimTraceCall
		if traces != nil {
			if traces[txIndex].Result.Error != "" {
				continue
			}
			calls = claimTraceCalls(traces[txIndex].Result, bridge)
		}
		if direct && len(tx.Data()) == 68 && string(tx.Data()[:4]) == string(claimTokenSelector) {
			calls = append([]claimTraceCall{{Input: tx.Data(), Position: "top-level"}}, calls...)
		}
		if len(calls) == 0 {
			continue
		}
		// Receipt success is checked before validating call arguments, so a
		// reverted transaction with malformed input cannot stop the range.
		receipt, err := idx.client.TransactionReceipt(ctx, tx.Hash())
		if err != nil {
			return nil, fmt.Errorf("claim receipt %s: %w", tx.Hash(), err)
		}
		if receipt.BlockHash != block.Hash() || receipt.BlockNumber.Uint64() != height {
			return nil, fmt.Errorf("claim receipt provenance mismatch")
		}
		if receipt.Status != ethtypes.ReceiptStatusSuccessful {
			continue
		}
		for _, call := range calls {
			if len(call.Input) != 68 || string(call.Input[:4]) != string(claimTokenSelector) {
				continue
			}
			if new(big.Int).SetBytes(call.Input[4:16]).Sign() != 0 {
				return nil, fmt.Errorf("invalid claim token address in call")
			}
			if direct && call.Position != "top-level" {
				continue
			} // proxy internal calls are not separate claims
			n, err := wordNonce(common.BytesToHash(call.Input[36:68]))
			if err != nil {
				return nil, err
			}
			token := common.BytesToAddress(call.Input[16:36]).Hex()
			pair, err := idx.opStore.FindClaimableTokenPair(n, token)
			if err != nil {
				return nil, err
			}
			if pair == nil {
				for _, o := range rangeObs {
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
			for _, o := range rangeObs {
				if o.Fact == datastore.FactClaimable && o.Identity == *pair && o.Evidence.BlockHeight <= height {
					copy := o.Evidence
					claimable = &copy
					break
				}
			}
			if claimable == nil {
				prior, err := idx.opStore.GetLifecycle(*pair)
				if err != nil {
					return nil, err
				}
				claimable = prior.Claimable
			}
			if claimable == nil || claimable.BlockHeight > height {
				continue
			}
			// An end-of-block EIP-1967 lookup cannot identify the active
			// implementation at a claim if this proxy upgraded in the block.
			for _, observed := range logs {
				if observed.BlockNumber == height && observed.Address == bridge && len(observed.Topics) > 0 && observed.Topics[0] == sigProxyUpgrade {
					return nil, fmt.Errorf("implementation changed in claim block %d", height)
				}
			}
			impl, err := idx.client.StorageAt(ctx, bridge, common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"), new(big.Int).SetUint64(height))
			if err != nil {
				return nil, err
			}
			claim, err := decodeTokenClaimCall(tx, receipt, bridge, common.BytesToAddress(impl), time.Unix(int64(block.Time()), 0), pair, claimable, call.Input, call.Position)
			if err != nil {
				return nil, err
			}
			if claim != nil {
				claims = append(claims, *claim)
			}
		}
	}
	return claims, nil
}

// enrichClaimTraceGap traces the lowest gap block while the live indexer is
// idle. Failures leave the gap in place and back off; the live checkpoint is
// never moved or reset by enrichment.
func (idx *NeoXIndexer) enrichClaimTraceGap(ctx context.Context) {
	if idx.processor == nil || idx.opStore == nil || time.Now().Before(idx.nextEnrich) {
		return
	}
	gaps, err := idx.store.ListNeoXClaimTraceGaps()
	if err != nil || len(gaps) == 0 {
		if err != nil {
			idx.logger.Warn("List Neo X claim trace gaps", "error", err)
		}
		return
	}
	if err := idx.enrichClaimTraceBlock(ctx, gaps[0].FromHeight); err != nil {
		idx.nextEnrich = time.Now().Add(neoxEnrichRetryInterval)
		if !errors.Is(err, neox.ErrTraceUnsupported) {
			idx.logger.Warn("Neo X claim trace enrichment failed", "block", gaps[0].FromHeight, "remaining_gaps", len(gaps), "error", err)
		}
	}
}

// enrichClaimTraceBlock re-derives every claim in a committed gap block from
// its traces. Direct claims already stored are rewritten identically.
func (idx *NeoXIndexer) enrichClaimTraceBlock(ctx context.Context, height uint64) error {
	raw, traceErr := idx.client.TraceBlock(ctx, height)
	if errors.Is(traceErr, neox.ErrTraceUnsupported) {
		return traceErr
	}
	block, err := idx.client.BlockByNumber(ctx, new(big.Int).SetUint64(height))
	if err != nil {
		return err
	}
	traces, reason, err := idx.checkTraces(ctx, block, raw, traceErr)
	if err != nil {
		return err
	}
	if reason != "" {
		return fmt.Errorf("claim traces at block %d: %s", height, reason)
	}
	logs, err := idx.client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: block.Number(), ToBlock: block.Number(), Addresses: idx.contracts})
	if err != nil {
		return err
	}
	claims, err := idx.blockClaims(ctx, block, traces, logs, nil)
	if err != nil {
		return err
	}
	return idx.store.ResolveNeoXClaimTraceGap(height, block.Hash().Hex(), claims)
}
