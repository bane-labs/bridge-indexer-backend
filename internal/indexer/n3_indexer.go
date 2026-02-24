package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
)

const (
	chainN3        = "neo_n3"
	n3PollInterval = 15 * time.Second
)

type N3EventProcessor interface {
	ProcessNotifications(ctx context.Context, blockHeight uint32, txHash string, notifications []N3Notification) ([]*types.BridgeOperation, error)
}

type N3Notification struct {
	ContractHash util.Uint160
	EventName    string
	Items        []interface{}
}

type N3Indexer struct {
	client    *n3.Client
	store     *datastore.Store
	opStore   *datastore.OperationStore
	processor N3EventProcessor
	contracts []util.Uint160
	logger    *slog.Logger
}

func NewN3Indexer(
	client *n3.Client,
	store *datastore.Store,
	opStore *datastore.OperationStore,
	processor N3EventProcessor,
	contractHashes []string,
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
		client:    client,
		store:     store,
		opStore:   opStore,
		processor: processor,
		contracts: contracts,
		logger:    logger,
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
		idx.logger.Info("Starting N3 indexer from genesis")
	}

	return idx.run(ctx, startHeight)
}

func (idx *N3Indexer) run(ctx context.Context, startHeight uint32) error {
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

		for height <= chainHeight {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if err := idx.processBlock(ctx, height); err != nil {
				idx.logger.Error("Failed to process N3 block", "height", height, "error", err)
				time.Sleep(5 * time.Second)
				break
			}
			height++
		}
	}
}

func (idx *N3Indexer) processBlock(ctx context.Context, height uint32) error {
	blk, err := idx.client.GetBlockByIndex(ctx, height)
	if err != nil {
		return fmt.Errorf("get block %d: %w", height, err)
	}

	var allOps []*types.BridgeOperation

	for _, tx := range blk.Transactions {
		txHash := tx.Hash()

		appLog, err := idx.client.GetApplicationLog(ctx, txHash)
		if err != nil {
			idx.logger.Warn("Failed to get app log", "tx", txHash.StringLE(), "error", err)
			continue
		}

		for _, exec := range appLog.Executions {
			if exec.VMState != vmstate.Halt {
				continue
			}

			for _, event := range exec.Events {
				if !idx.isWatchedContract(event.ScriptHash) {
					continue
				}

				// The event processor will be implemented in Task 7.
				// For now we just log that we found a bridge event.
				idx.logger.Debug("Found bridge event",
					"contract", event.ScriptHash.StringLE(),
					"event", event.Name,
					"tx", txHash.StringLE(),
					"height", height,
				)
			}
		}
	}

	newState := &types.IndexerState{
		Chain:           chainN3,
		LastBlockHeight: uint64(height),
		LastBlockHash:   blk.Hash().StringLE(),
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

	if height%1000 == 0 {
		idx.logger.Info("N3 indexer progress", "height", height)
	}

	return nil
}

func (idx *N3Indexer) isWatchedContract(hash util.Uint160) bool {
	for _, c := range idx.contracts {
		if c.Equals(hash) {
			return true
		}
	}
	return false
}
