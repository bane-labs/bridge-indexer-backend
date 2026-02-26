package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

const backfillN3Concurrency = 100

// backfillN3ScanBuffer is the number of extra blocks to scan beyond the
// maximum SourceBlockHeight of stuck withdrawals. Completion events typically
// appear within minutes of the source event, so 100 000 blocks (~1 day on N3)
// provides a generous safety margin.
const backfillN3ScanBuffer = 100_000

type Backfiller struct {
	opStore       *datastore.OperationStore
	neoxClient    *neox.Client
	n3Client      *n3.Client
	neoxContracts []string
	n3Contracts   []string
	logger        *slog.Logger
}

func NewBackfiller(
	opStore *datastore.OperationStore,
	neoxClient *neox.Client,
	n3Client *n3.Client,
	neoxContracts []string,
	n3Contracts []string,
	logger *slog.Logger,
) *Backfiller {
	return &Backfiller{
		opStore:       opStore,
		neoxClient:    neoxClient,
		n3Client:      n3Client,
		neoxContracts: neoxContracts,
		n3Contracts:   n3Contracts,
		logger:        logger,
	}
}

func (b *Backfiller) Run(ctx context.Context) (int, error) {
	stuck := types.StatusStuck
	ops, _, err := b.opStore.ListOperations(datastore.OperationFilter{Status: &stuck})
	if err != nil {
		return 0, fmt.Errorf("list stuck operations: %w", err)
	}

	if len(ops) == 0 {
		b.logger.Info("Backfill complete", "resolved", 0, "still_stuck", 0)
		return 0, nil
	}

	deposits := make(map[uint64]*types.BridgeOperation)
	withdrawals := make(map[withdrawalKey]*types.BridgeOperation)
	totalStuck := len(ops)

	var minBlock, maxBlock uint32
	hasMinBlock := false

	for i := range ops {
		op := &ops[i]
		switch op.Direction {
		case types.DirectionDeposit:
			deposits[op.Nonce] = op
		case types.DirectionWithdrawal:
			key := withdrawalKey{bridgeType: op.BridgeType, nonce: op.Nonce}
			withdrawals[key] = op
			height := uint32(op.SourceBlockHeight)
			if !hasMinBlock || height < minBlock {
				minBlock = height
			}
			if height > maxBlock {
				maxBlock = height
			}
			hasMinBlock = true
		}
	}

	resolved := 0

	if len(deposits) > 0 {
		depositResolved, err := b.resolveDeposits(ctx, deposits)
		if err != nil {
			return resolved, err
		}
		resolved += depositResolved
	}

	if len(withdrawals) > 0 {
		withdrawalResolved, err := b.resolveWithdrawals(ctx, withdrawals, minBlock, maxBlock, hasMinBlock)
		if err != nil {
			return resolved, err
		}
		resolved += withdrawalResolved
	}

	stillStuck := totalStuck - resolved
	if stillStuck < 0 {
		stillStuck = 0
	}

	b.logger.Info("Backfill complete", "resolved", resolved, "still_stuck", stillStuck)
	return resolved, nil
}

type withdrawalKey struct {
	bridgeType types.BridgeType
	nonce      uint64
}

func (b *Backfiller) resolveDeposits(ctx context.Context, deposits map[uint64]*types.BridgeOperation) (int, error) {
	addrs := make([]common.Address, len(b.neoxContracts))
	for i, s := range b.neoxContracts {
		addrs[i] = common.HexToAddress(s)
	}

	// Find the earliest source block among stuck deposits — completion events
	// cannot exist before the source operation, so we can skip earlier blocks.
	var fromBlock uint64
	hasFrom := false
	for _, op := range deposits {
		if !hasFrom || op.SourceBlockHeight < fromBlock {
			fromBlock = op.SourceBlockHeight
			hasFrom = true
		}
	}
	if !hasFrom {
		return 0, nil
	}

	chainHeight, err := b.neoxClient.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("get NeoX chain height: %w", err)
	}

	resolved := 0
	resolvedNonces := make(map[uint64]struct{})
	headerTimes := make(map[uint64]time.Time)

	// Scan in batches of neoxMaxBlockRange to avoid RPC limits.
	for batchFrom := fromBlock; batchFrom <= chainHeight; batchFrom += neoxMaxBlockRange {
		batchTo := batchFrom + neoxMaxBlockRange - 1
		if batchTo > chainHeight {
			batchTo = chainHeight
		}

		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(batchFrom),
			ToBlock:   new(big.Int).SetUint64(batchTo),
			Addresses: addrs,
		}
		logs, err := b.neoxClient.FilterLogs(ctx, query)
		if err != nil {
			return resolved, fmt.Errorf("filter NeoX logs %d-%d: %w", batchFrom, batchTo, err)
		}

		for _, log := range logs {
			if log.Removed {
				continue
			}
			if len(log.Topics) == 0 {
				continue
			}

			nonce, ok := extractNeoXNonce(log)
			if !ok {
				continue
			}

			op, exists := deposits[nonce]
			if !exists {
				continue
			}
			if _, done := resolvedNonces[nonce]; done {
				continue
			}

			blockTime, err := b.neoxBlockTime(ctx, log.BlockNumber, headerTimes)
			if err != nil {
				return resolved, err
			}

			err = b.opStore.UpdateOperationCompletion(
				op.BridgeType,
				op.Direction,
				op.Nonce,
				log.BlockNumber,
				log.TxHash.Hex(),
				blockTime,
			)
			if err != nil {
				return resolved, fmt.Errorf("update NeoX completion nonce %d: %w", nonce, err)
			}

			resolvedNonces[nonce] = struct{}{}
			resolved++
			b.logger.Info("Backfill resolved operation",
				"bridge_type", op.BridgeType,
				"direction", op.Direction,
				"nonce", op.Nonce,
			)
		}
	}

	return resolved, nil
}

func (b *Backfiller) resolveWithdrawals(
	ctx context.Context,
	withdrawals map[withdrawalKey]*types.BridgeOperation,
	minBlock uint32,
	maxBlock uint32,
	hasMinBlock bool,
) (int, error) {
	if !hasMinBlock {
		return 0, nil
	}

	current, err := b.n3Client.GetBlockCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("get N3 block count: %w", err)
	}
	if current == 0 {
		return 0, nil
	}
	chainHeight := current - 1

	// Cap the scan at maxBlock + buffer instead of chain tip. Completion
	// events appear shortly after the source event, so scanning far beyond
	// the latest stuck SourceBlockHeight is unnecessary.
	scanEnd := maxBlock + backfillN3ScanBuffer
	if uint32(chainHeight) < scanEnd {
		scanEnd = uint32(chainHeight)
	}

	contractSet := make(map[string]struct{}, len(b.n3Contracts))
	for _, contract := range b.n3Contracts {
		contractSet[contract] = struct{}{}
	}

	totalBlocks := int(scanEnd-minBlock) + 1
	b.logger.Info("Backfill scanning N3 blocks",
		"from", minBlock, "to", scanEnd,
		"total", totalBlocks, "concurrency", backfillN3Concurrency,
		"chain_height", chainHeight, "scan_buffer", backfillN3ScanBuffer,
	)

	// Concurrent block scan using a semaphore + ordered result collection.
	type blockMatch struct {
		height  uint32
		txHash  string
		blkTime time.Time
		key     withdrawalKey
	}

	var (
		scanned   int64 // atomic — blocks dispatched so far
		resolved  int32 // atomic
		firstErr  error
		errMu     sync.Mutex
		matches   []blockMatch
		matchesMu sync.Mutex
		wg        sync.WaitGroup
		sem       = make(chan struct{}, backfillN3Concurrency)
	)

	// Progress logger: print stats every 30 seconds.
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				done := atomic.LoadInt64(&scanned)
				pct := float64(done) / float64(totalBlocks) * 100
				b.logger.Info("Backfill progress",
					"scanned", done, "total", totalBlocks,
					"percent", fmt.Sprintf("%.1f%%", pct),
					"matches", len(matches),
				)
			case <-progressDone:
				return
			}
		}
	}()

	for h := minBlock; h <= scanEnd; h++ {
		height := h
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			atomic.AddInt64(&scanned, 1)

			errMu.Lock()
			if firstErr != nil {
				errMu.Unlock()
				return
			}
			errMu.Unlock()

			blk, err := b.n3Client.GetBlockByIndex(ctx, height)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("get N3 block %d: %w", height, err)
				}
				errMu.Unlock()
				return
			}

			if len(blk.Transactions) == 0 {
				return
			}

			blkTime := time.UnixMilli(int64(blk.Timestamp))
			for _, tx := range blk.Transactions {
				txHash := tx.Hash()
				txHashStr := txHash.StringLE()
				appLog, err := b.n3Client.GetApplicationLog(ctx, txHash)
				if err != nil {
					b.logger.Warn("Failed to get app log during backfill", "tx", txHashStr, "error", err)
					continue
				}
				for _, exec := range appLog.Executions {
					if exec.VMState != vmstate.Halt {
						continue
					}
					for _, event := range exec.Events {
						if _, ok := contractSet[event.ScriptHash.StringLE()]; !ok {
							continue
						}
						key, ok := extractN3WithdrawalKey(event)
						if !ok {
							continue
						}
						if _, exists := withdrawals[key]; !exists {
							continue
						}
						matchesMu.Lock()
						matches = append(matches, blockMatch{
							height:  height,
							txHash:  txHashStr,
							blkTime: blkTime,
							key:     key,
						})
						matchesMu.Unlock()
					}
				}
			}
		}()
	}
	wg.Wait()
	close(progressDone)

	if firstErr != nil {
		return 0, firstErr
	}

	// Apply matches sequentially (UpdateOperationCompletion is not goroutine-safe for same key).
	resolvedKeys := make(map[withdrawalKey]struct{})
	for _, m := range matches {
		if _, done := resolvedKeys[m.key]; done {
			continue
		}
		op := withdrawals[m.key]
		err := b.opStore.UpdateOperationCompletion(
			op.BridgeType, op.Direction, op.Nonce,
			uint64(m.height), m.txHash, m.blkTime,
		)
		if err != nil {
			return int(resolved), fmt.Errorf("update N3 completion nonce %d: %w", op.Nonce, err)
		}
		resolvedKeys[m.key] = struct{}{}
		atomic.AddInt32(&resolved, 1)
		b.logger.Info("Backfill resolved operation",
			"bridge_type", op.BridgeType,
			"direction", op.Direction,
			"nonce", op.Nonce,
		)
	}

	return int(resolved), nil
}

func extractNeoXNonce(log ethtypes.Log) (uint64, bool) {
	switch log.Topics[0] {
	case sigNativeClaim, sigNativeClaimable, sigNativeDeposit:
		if len(log.Topics) < 2 {
			return 0, false
		}
		return new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64(), true
	case sigTokenClaim, sigTokenClaimable, sigTokenDeposit:
		if len(log.Topics) < 3 {
			return 0, false
		}
		return new(big.Int).SetBytes(log.Topics[2].Bytes()).Uint64(), true
	case sigExecution:
		if len(log.Topics) < 2 {
			return 0, false
		}
		return new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64(), true
	default:
		return 0, false
	}
}

func (b *Backfiller) neoxBlockTime(ctx context.Context, height uint64, cache map[uint64]time.Time) (time.Time, error) {
	if blockTime, ok := cache[height]; ok {
		return blockTime, nil
	}

	header, err := b.neoxClient.HeaderByNumber(ctx, new(big.Int).SetUint64(height))
	if err != nil {
		return time.Time{}, fmt.Errorf("get Neo X header %d: %w", height, err)
	}

	blockTime := time.Unix(int64(header.Time), 0)
	cache[height] = blockTime
	return blockTime, nil
}

func extractN3WithdrawalKey(event state.NotificationEvent) (withdrawalKey, bool) {
	items, err := getArrayItems(event.Item)
	if err != nil {
		return withdrawalKey{}, false
	}

	switch event.Name {
	case "NativeWithdrawal":
		if len(items) < 1 {
			return withdrawalKey{}, false
		}
		nonce, err := itemToUint64(items[0])
		if err != nil {
			return withdrawalKey{}, false
		}
		return withdrawalKey{bridgeType: types.BridgeTypeNative, nonce: nonce}, true
	case "TokenWithdrawal":
		if len(items) < 2 {
			return withdrawalKey{}, false
		}
		nonce, err := itemToUint64(items[1])
		if err != nil {
			return withdrawalKey{}, false
		}
		return withdrawalKey{bridgeType: types.BridgeTypeToken, nonce: nonce}, true
	case "Execution":
		if len(items) < 1 {
			return withdrawalKey{}, false
		}
		nonce, err := itemToUint64(items[0])
		if err != nil {
			return withdrawalKey{}, false
		}
		return withdrawalKey{bridgeType: types.BridgeTypeMessage, nonce: nonce}, true
	default:
		return withdrawalKey{}, false
	}
}
