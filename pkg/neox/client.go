package neox

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"

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

	// traceUnsupported maps an endpoint URL to the time it may be probed
	// again after reporting that debug tracing does not exist there.
	traceMu          sync.Mutex
	traceUnsupported map[string]time.Time

	// chainID is pinned at construction: every endpoint the client uses,
	// including after failover, must report it.
	chainID uint64
}

const chainIDTimeout = 15 * time.Second

func endpointChainID(ctx context.Context, eth *ethclient.Client) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, chainIDTimeout)
	defer cancel()
	id, err := eth.ChainID(ctx)
	if err != nil {
		return 0, err
	}
	return id.Uint64(), nil
}

// ErrTraceUnsupported reports that the active endpoint does not provide
// debug_traceBlockByNumber. It never applies to the other endpoints.
var ErrTraceUnsupported = errors.New("debug_traceBlockByNumber unsupported by endpoint")

const (
	traceReprobeInterval = 10 * time.Minute
	traceTimeout         = 45 * time.Second
	rpcMethodNotFound    = -32601
)

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

	// Every reachable endpoint must be on the same network. An endpoint that
	// cannot answer now is checked when failover first reaches it.
	networks := map[uint64][]string{}
	for _, ep := range endpoints {
		eth, err := ethclient.DialContext(ctx, ep)
		if err != nil {
			logger.Warn("Failed to connect to Neo X RPC", "endpoint", ep, "error", err)
			continue
		}
		id, err := endpointChainID(ctx, eth)
		eth.Close()
		if err != nil {
			logger.Warn("Neo X RPC did not report its chain ID", "endpoint", ep, "error", err)
			continue
		}
		networks[id] = append(networks[id], ep)
	}
	if len(networks) == 0 {
		return nil, fmt.Errorf("failed to connect to any Neo X endpoint: none reported a chain ID")
	}
	if len(networks) > 1 {
		return nil, fmt.Errorf("Neo X endpoints are on different networks (chain ID to endpoints: %v)", networks)
	}
	for id := range networks {
		c.chainID = id
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
		// Never switch to an endpoint on another network.
		id, err := endpointChainID(ctx, eth)
		if err != nil || id != c.chainID {
			eth.Close()
			c.logger.Warn("Skipping Neo X RPC endpoint: chain ID unavailable or different", "endpoint", ep, "chain_id", id, "expected", c.chainID, "error", err)
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
		if ctx.Err() != nil {
			return zero, err // cancelled: retrying cannot succeed
		}

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

// ChainID returns the EIP-155 chain ID shared by every endpoint the client
// uses (checked at construction and on each failover).
func (c *Client) ChainID(context.Context) (uint64, error) {
	return c.chainID, nil
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

// CallFrame matches Geth's callTracer output. A successful outer receipt does
// not imply that a nested call succeeded: each ancestor must be checked.
type CallFrame struct {
	Type  string         `json:"type"`
	To    common.Address `json:"to"`
	Input hexutil.Bytes  `json:"input"`
	Error string         `json:"error,omitempty"`
	Calls []CallFrame    `json:"calls,omitempty"`
}

type TransactionTrace struct {
	TxHash common.Hash `json:"txHash"`
	Result *CallFrame  `json:"result"`
	Error  string      `json:"error,omitempty"`
}

// TraceBlock returns one call tree per transaction from the active endpoint.
// Tracing is optional enrichment: one attempt, no retry, and no failover, so
// it can neither overload the endpoint nor move core indexing off it. An
// endpoint answering -32601 is skipped until its re-probe time; that state is
// keyed by endpoint URL, so another endpoint is still traced after failover.
func (c *Client) TraceBlock(ctx context.Context, height uint64) ([]TransactionTrace, error) {
	endpoint := c.endpoints[c.active]
	c.traceMu.Lock()
	until, known := c.traceUnsupported[endpoint]
	c.traceMu.Unlock()
	if known && time.Now().Before(until) {
		return nil, ErrTraceUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, traceTimeout)
	defer cancel()
	var traces []TransactionTrace
	err := c.eth.Client().CallContext(ctx, &traces, "debug_traceBlockByNumber", hexutil.EncodeUint64(height), map[string]any{"tracer": "callTracer", "timeout": "30s"})
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) && rpcErr.ErrorCode() == rpcMethodNotFound {
		c.traceMu.Lock()
		if c.traceUnsupported == nil {
			c.traceUnsupported = make(map[string]time.Time)
		}
		c.traceUnsupported[endpoint] = time.Now().Add(traceReprobeInterval)
		c.traceMu.Unlock()
		if !known {
			c.logger.Warn("Neo X endpoint does not support block tracing; indirect claim coverage is deferred", "endpoint", endpoint, "reprobe_after", traceReprobeInterval)
		}
		return nil, fmt.Errorf("%w: %v", ErrTraceUnsupported, err)
	}
	if err != nil {
		return nil, err
	}
	c.traceMu.Lock()
	delete(c.traceUnsupported, endpoint)
	c.traceMu.Unlock()
	return traces, nil
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
