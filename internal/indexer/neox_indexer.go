package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

const (
	chainNeoX         = "neo_x"
	neoxPollInterval  = 3 * time.Second
	neoxSafetyDepth   = 20
	neoxMaxBlockRange = 500
)

type NeoXIndexer struct {
	client    *neox.Client
	store     *datastore.Store
	opStore   *datastore.OperationStore
	processor *NeoXEventProcessorImpl
	contracts []common.Address
	logger    *slog.Logger
}

func NewNeoXIndexer(
	client *neox.Client,
	store *datastore.Store,
	opStore *datastore.OperationStore,
	processor *NeoXEventProcessorImpl,
	contractAddresses []string,
	logger *slog.Logger,
) *NeoXIndexer {
	return &NeoXIndexer{
		client:    client,
		store:     store,
		opStore:   opStore,
		processor: processor,
		contracts: neox.HexToAddresses(contractAddresses),
		logger:    logger,
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
		idx.logger.Info("Starting NeoX indexer from genesis")
	}

	return idx.run(ctx, startHeight)
}

func (idx *NeoXIndexer) run(ctx context.Context, startHeight uint64) error {
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

func (idx *NeoXIndexer) processBlockRange(ctx context.Context, from, to uint64) error {
	header, err := idx.client.HeaderByNumber(ctx, new(big.Int).SetUint64(to))
	if err != nil {
		return fmt.Errorf("get header %d: %w", to, err)
	}
	blockTime := time.Unix(int64(header.Time), 0)

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: idx.contracts,
	}

	logs, err := idx.client.FilterLogs(ctx, query)
	if err != nil {
		return fmt.Errorf("filter logs %d-%d: %w", from, to, err)
	}

	var allOps []*types.BridgeOperation

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

		op, pauseState, err := idx.processor.ProcessLog(l, blockTime)
		if err != nil {
			idx.logger.Warn("Failed to process log",
				"tx", l.TxHash.Hex(),
				"error", err,
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
		}
	}

	newState := &types.IndexerState{
		Chain:           chainNeoX,
		LastBlockHeight: to,
		LastBlockHash:   header.Hash().Hex(),
		IsSyncing:       true,
		UpdatedAt:       time.Now(),
	}

	if len(allOps) > 0 {
		if err := idx.store.SaveIndexerStateWithOperations(newState, allOps); err != nil {
			return fmt.Errorf("save state with ops: %w", err)
		}
	} else {
		if err := idx.store.SaveIndexerState(newState); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
	}

	if to%1000 == 0 || to == from {
		idx.logger.Info("NeoX indexer progress", "height", to, "logs_in_range", len(logs))
	}

	return nil
}
