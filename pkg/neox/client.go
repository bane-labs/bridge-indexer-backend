package neox

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/bane-labs/bridge-indexer-backend/pkg/blockcache"
)

type Client struct {
	eth          *ethclient.Client
	endpoints    []string
	active       int
	logger       *slog.Logger
	callInterval time.Duration
	cache        *blockcache.Cache
	cacheReads   bool
}

// SetCache attaches an optional block cache. Cache reads are only enabled if
// existing NeoX data is detected (from a previous run).
func (c *Client) SetCache(cache *blockcache.Cache) {
	c.cache = cache
	if cache.HasPrefix([]byte("neox:")) {
		c.cacheReads = true
		c.logger.Info("NeoX block cache: reads enabled (data found)")
	} else {
		c.logger.Info("NeoX block cache: write-only (no existing data)")
	}
}

func NewClient(ctx context.Context, endpoints []string, logger *slog.Logger, callInterval time.Duration) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no Neo X RPC endpoints provided")
	}

	c := &Client{
		endpoints:    endpoints,
		logger:       logger,
		callInterval: callInterval,
	}

	if err := c.connect(ctx, 0); err != nil {
		return nil, fmt.Errorf("failed to connect to any Neo X endpoint: %w", err)
	}

	return c, nil
}

func (c *Client) connect(ctx context.Context, startIdx int) error {
	for i := 0; i < len(c.endpoints); i++ {
		idx := (startIdx + i) % len(c.endpoints)
		ep := c.endpoints[idx]

		eth, err := ethclient.DialContext(ctx, ep)
		if err != nil {
			c.logger.Warn("Failed to connect to Neo X RPC", "endpoint", ep, "error", err)
			continue
		}

		c.eth = eth
		c.active = idx
		c.logger.Info("Connected to Neo X RPC", "endpoint", ep)
		return nil
	}
	return fmt.Errorf("all %d Neo X endpoints unreachable", len(c.endpoints))
}

func (c *Client) failover(ctx context.Context) error {
	next := (c.active + 1) % len(c.endpoints)
	c.logger.Warn("Attempting Neo X failover", "from", c.endpoints[c.active], "to", c.endpoints[next])
	return c.connect(ctx, next)
}

func retryEVM[T any](c *Client, ctx context.Context, op func() (T, error)) (T, error) {
	const maxRetries = 3
	var lastErr error
	var zero T

	for attempt := 0; attempt <= maxRetries; attempt++ {
		val, err := op()
		if err == nil {
			return val, nil
		}
		lastErr = err

		if attempt < maxRetries {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			c.logger.Warn("Neo X RPC call failed, retrying", "attempt", attempt+1, "backoff", backoff, "error", err)
			time.Sleep(backoff)

			if fErr := c.failover(ctx); fErr != nil {
				c.logger.Warn("Failover failed", "error", fErr)
			}
		}
	}
	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	return retryEVM(c, ctx, func() (uint64, error) {
		return c.eth.BlockNumber(ctx)
	})
}

func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return retryEVM(c, ctx, func() (*types.Block, error) {
		return c.eth.BlockByNumber(ctx, number)
	})
}

func (c *Client) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return retryEVM(c, ctx, func() (*types.Receipt, error) { return c.eth.TransactionReceipt(ctx, hash) })
}

func (c *Client) StorageAt(ctx context.Context, address common.Address, slot common.Hash, height *big.Int) ([]byte, error) {
	return retryEVM(c, ctx, func() ([]byte, error) { return c.eth.StorageAt(ctx, address, slot, height) })
}

func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	if c.cacheReads && query.FromBlock != nil && query.ToBlock != nil {
		from := query.FromBlock.Uint64()
		to := query.ToBlock.Uint64()
		if c.cacheRangeCovered(from, to) {
			logs, ok := c.logsFromCache(from, to)
			if ok {
				return logs, nil
			}
		}
	}

	logs, err := retryEVM(c, ctx, func() ([]types.Log, error) {
		return c.eth.FilterLogs(ctx, query)
	})
	if err != nil {
		return nil, err
	}

	if c.cache != nil && query.FromBlock != nil && query.ToBlock != nil {
		c.cacheFilterLogs(query.FromBlock.Uint64(), query.ToBlock.Uint64(), logs)
	}
	return logs, nil
}

func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if c.cacheReads && number != nil {
		key := neoxHeaderKey(number.Uint64())
		if data, ok := c.cache.Get(key); ok {
			var hdr types.Header
			if err := rlp.DecodeBytes(data, &hdr); err == nil {
				return &hdr, nil
			}
		}
	}

	hdr, err := retryEVM(c, ctx, func() (*types.Header, error) {
		return c.eth.HeaderByNumber(ctx, number)
	})
	if err != nil {
		return nil, err
	}

	if c.cache != nil && number != nil {
		if data, rErr := rlp.EncodeToBytes(hdr); rErr == nil {
			_ = c.cache.Put(neoxHeaderKey(number.Uint64()), data)
		}
	}
	return hdr, nil
}

var neoxWatermarkKey = []byte("neox:wm")
var neoxWatermarkFromKey = []byte("neox:wm_from")

func neoxHeaderKey(height uint64) []byte {
	return fmt.Appendf(nil, "neox:hdr:%d", height)
}

func neoxLogsKey(height uint64) []byte {
	return fmt.Appendf(nil, "neox:logs:%d", height)
}

// logsFromCache reads cached logs for [from, to] and merges them.
// Blocks with no cache entry are treated as having no events (the watermark
// guarantees they were scanned). Only blocks that had events get entries.
func (c *Client) logsFromCache(from, to uint64) ([]types.Log, bool) {
	var merged []types.Log
	for h := from; h <= to; h++ {
		data, ok := c.cache.Get(neoxLogsKey(h))
		if !ok {
			continue // watermark covers this block → no events
		}
		var blockLogs []types.Log
		if err := json.Unmarshal(data, &blockLogs); err != nil {
			return nil, false
		}
		merged = append(merged, blockLogs...)
	}
	return merged, true
}

func (c *Client) cacheRangeCovered(from, to uint64) bool {
	if c.cache == nil {
		return false
	}
	wm := c.cache.GetWatermark(neoxWatermarkKey)
	if wm < to || wm < from {
		return false
	}
	rawFrom, ok := c.cache.Get(neoxWatermarkFromKey)
	if !ok || len(rawFrom) < 8 {
		// Backward-compatibility safety: do not trust old caches lacking the
		// lower-bound watermark metadata; fallback to live RPC.
		return false
	}
	wmFrom := binary.BigEndian.Uint64(rawFrom)
	return from >= wmFrom
}

// cacheFilterLogs stores only blocks that contain events and advances the
// watermark. Blocks with no events are not stored — logsFromCache treats
// missing entries within the watermark range as "no events."
func (c *Client) cacheFilterLogs(from, to uint64, logs []types.Log) {
	if len(logs) > 0 {
		byBlock := make(map[uint64][]types.Log)
		for i := range logs {
			byBlock[logs[i].BlockNumber] = append(byBlock[logs[i].BlockNumber], logs[i])
		}

		var entries []blockcache.Entry
		for bn, blockLogs := range byBlock {
			data, err := json.Marshal(blockLogs)
			if err != nil {
				continue
			}
			entries = append(entries, blockcache.Entry{Key: neoxLogsKey(bn), Value: data})
		}
		if len(entries) > 0 {
			_ = c.cache.PutBatch(entries)
		}
	}

	wm := c.cache.GetWatermark(neoxWatermarkKey)
	if to > wm {
		_ = c.cache.SetWatermark(neoxWatermarkKey, to)
	}
	wmFromRaw, ok := c.cache.Get(neoxWatermarkFromKey)
	if !ok || len(wmFromRaw) < 8 {
		_ = c.cache.SetWatermark(neoxWatermarkFromKey, from)
	} else {
		wmFrom := binary.BigEndian.Uint64(wmFromRaw)
		if from < wmFrom {
			_ = c.cache.SetWatermark(neoxWatermarkFromKey, from)
		}
	}
}

func (c *Client) Close() {
	if c.eth != nil {
		c.eth.Close()
	}
}

// CallInterval returns the configured interval between RPC calls.
func (c *Client) CallInterval() time.Duration {
	return c.callInterval
}

func HexToAddresses(hexAddrs []string) []common.Address {
	addrs := make([]common.Address, len(hexAddrs))
	for i, h := range hexAddrs {
		addrs[i] = common.HexToAddress(h)
	}
	return addrs
}
