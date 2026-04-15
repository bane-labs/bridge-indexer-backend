package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	neio "github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/bane-labs/bridge-indexer-backend/pkg/blockcache"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
)

func newCacheFillN3Command() *cobra.Command {
	var fromBlock uint32
	var toBlock uint32

	cmd := &cobra.Command{
		Use:   "cache-fill-n3",
		Short: "Fetch missing N3 app logs into cache",
		RunE: func(cmd *cobra.Command, args []string) error {
			hasFrom := cmd.Flags().Changed("from-block")
			hasTo := cmd.Flags().Changed("to-block")
			if hasFrom != hasTo {
				return fmt.Errorf("provide both --from-block and --to-block, or neither for auto mode")
			}
			if hasFrom {
				if toBlock < fromBlock {
					return fmt.Errorf("--to-block must be >= --from-block")
				}
				return runCacheFillN3Range(fromBlock, toBlock)
			}
			return runCacheFillN3Auto()
		},
	}
	cmd.Flags().Uint32Var(&fromBlock, "from-block", 0, "start N3 block height (inclusive)")
	cmd.Flags().Uint32Var(&toBlock, "to-block", 0, "end N3 block height (inclusive)")
	return cmd
}

func runCacheFillN3Range(fromBlock, toBlock uint32) error {
	logger := slog.New(slog.NewTextHandler(io.Writer(io.Discard), nil))
	v := viper.New()
	cfg, err := loadConfig(v, logger)
	if err != nil {
		return err
	}
	if cfg.BlockCacheDir == "" {
		return fmt.Errorf("BLOCK_CACHE_DIR is not set")
	}

	ctx := context.Background()
	cache, err := blockcache.New(cfg.BlockCacheDir, logger)
	if err != nil {
		return fmt.Errorf("open block cache: %w", err)
	}
	defer func() { _ = cache.Close() }()

	client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, 8, logger, cfg.N3RPCCallInterval)
	if err != nil {
		return fmt.Errorf("create n3 client: %w", err)
	}
	defer client.Close()
	client.SetCache(cache)

	start := time.Now()
	var blocksScanned uint32
	var txsSeen, appLogsFetched int

	for h := fromBlock; h <= toBlock; h++ {
		blk, err := client.GetBlockByIndex(ctx, h)
		if err != nil {
			return fmt.Errorf("get n3 block %d: %w", h, err)
		}
		blocksScanned++
		txsSeen += len(blk.Transactions)

		for _, tx := range blk.Transactions {
			if _, err := client.GetApplicationLog(ctx, tx.Hash()); err != nil {
				// Keep scanning even if some tx app logs are unavailable.
				continue
			}
			appLogsFetched++
		}
	}

	cache.Flush()
	fmt.Printf(
		"cache-fill-n3 completed: blocks=%d txs=%d app_logs=%d range=%d-%d elapsed=%s\n",
		blocksScanned, txsSeen, appLogsFetched, fromBlock, toBlock, time.Since(start).Round(time.Second),
	)
	return nil
}

func runCacheFillN3Auto() error {
	logger := slog.New(slog.NewTextHandler(io.Writer(io.Discard), nil))
	v := viper.New()
	cfg, err := loadConfig(v, logger)
	if err != nil {
		return err
	}
	if cfg.BlockCacheDir == "" {
		return fmt.Errorf("BLOCK_CACHE_DIR is not set")
	}

	minBlock, maxBlock, missingTxs, err := inferMissingN3AppLogs(cfg.BlockCacheDir)
	if err != nil {
		return err
	}
	if minBlock == 0 && maxBlock == 0 {
		return fmt.Errorf("no n3:blk entries found in cache")
	}
	if len(missingTxs) == 0 {
		fmt.Printf("cache-fill-n3 auto: no missing app logs (range=%d-%d)\n", minBlock, maxBlock)
		return nil
	}

	ctx := context.Background()
	cache, err := blockcache.New(cfg.BlockCacheDir, logger)
	if err != nil {
		return fmt.Errorf("open block cache: %w", err)
	}
	defer func() { _ = cache.Close() }()

	client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, 8, logger, cfg.N3RPCCallInterval)
	if err != nil {
		return fmt.Errorf("create n3 client: %w", err)
	}
	defer client.Close()
	client.SetCache(cache)

	start := time.Now()
	fetched := 0
	for _, tx := range missingTxs {
		if _, err := client.GetApplicationLog(ctx, tx); err != nil {
			continue
		}
		fetched++
	}
	cache.Flush()

	fmt.Printf(
		"cache-fill-n3 auto completed: range=%d-%d missing=%d fetched=%d elapsed=%s\n",
		minBlock, maxBlock, len(missingTxs), fetched, time.Since(start).Round(time.Second),
	)
	return nil
}

func inferMissingN3AppLogs(cacheDir string) (uint32, uint32, []util.Uint256, error) {
	opts := badger.DefaultOptions(cacheDir)
	opts.Logger = nil
	opts.ReadOnly = true
	opts.BypassLockGuard = true
	db, err := badger.Open(opts)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("open cache db: %w", err)
	}
	defer func() { _ = db.Close() }()

	minBlock := uint32(0)
	maxBlock := uint32(0)
	missingByHash := make(map[string]util.Uint256)

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte("n3:blk:")})
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			heightStr := strings.TrimPrefix(key, "n3:blk:")
			heightU64, parseErr := strconv.ParseUint(heightStr, 10, 32)
			if parseErr != nil {
				continue
			}
			height := uint32(heightU64)
			if minBlock == 0 || height < minBlock {
				minBlock = height
			}
			if height > maxBlock {
				maxBlock = height
			}

			if err := item.Value(func(val []byte) error {
				blk := block.New(false)
				r := neio.NewBinReaderFromBuf(val)
				blk.DecodeBinary(r)
				if r.Err != nil {
					return nil
				}
				for _, tx := range blk.Transactions {
					txHash := tx.Hash()
					txHashLE := txHash.StringLE()
					appKey := []byte(fmt.Sprintf("n3:applog:%s", txHashLE))
					if _, getErr := txn.Get(appKey); getErr != nil {
						missingByHash[txHashLE] = txHash
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, nil, fmt.Errorf("scan cache db: %w", err)
	}

	if len(missingByHash) == 0 {
		return minBlock, maxBlock, nil, nil
	}
	keys := make([]string, 0, len(missingByHash))
	for k := range missingByHash {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	missing := make([]util.Uint256, 0, len(keys))
	for _, k := range keys {
		missing = append(missing, missingByHash[k])
	}
	return minBlock, maxBlock, missing, nil
}
