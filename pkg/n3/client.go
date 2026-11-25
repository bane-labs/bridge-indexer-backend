package n3

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	neio "github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/util"

	"github.com/bane-labs/bridge-indexer-backend/pkg/blockcache"
)

type pooledRPC struct {
	client     *rpcclient.Client
	lastCallNs atomic.Int64
}

// Client wraps a pool of Neo N3 RPC connections for concurrent use.
type Client struct {
	pool         []*pooledRPC
	endpoints    []string
	poolSize     int
	counter      atomic.Uint64
	logger       *slog.Logger
	callInterval time.Duration
	cache        *blockcache.Cache
	cacheReads   bool // true when the cache has data worth reading
}

// SetCache attaches an optional block cache. When set, GetBlockByIndex and
// GetApplicationLog store RPC responses for future re-syncs. Cache reads are
// only enabled if existing N3 data is detected (i.e. a previous run already
// populated the cache). This avoids pointless BadgerDB lookups during the
// initial sync where every Get would be a miss.
func (c *Client) SetCache(cache *blockcache.Cache) {
	c.cache = cache
	if cache.HasPrefix([]byte("n3:")) {
		c.cacheReads = true
		c.logger.Info("N3 block cache: reads enabled (data found)")
	} else {
		c.logger.Info("N3 block cache: write-only (no existing data)")
	}
}

// NewClient creates a Client with a pool of poolSize RPC connections,
// distributed evenly across the provided endpoints.
// At least one connection must succeed or an error is returned.
func NewClient(ctx context.Context, endpoints []string, poolSize int, logger *slog.Logger, callInterval time.Duration) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no Neo N3 RPC endpoints provided")
	}
	if poolSize <= 0 {
		poolSize = 1
	}
	if poolSize > 500 {
		poolSize = 500
	}

	c := &Client{
		endpoints:    endpoints,
		poolSize:     poolSize,
		logger:       logger,
		callInterval: callInterval,
	}

	pool := make([]*pooledRPC, 0, poolSize)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ep := endpoints[i%len(endpoints)]
			rpc, err := rpcclient.New(ctx, ep, rpcclient.Options{
				RequestTimeout: 45 * time.Second,
			})
			if err != nil {
				logger.Warn("Failed to create Neo N3 RPC client", "endpoint", ep, "poolIdx", i, "error", err)
				return
			}
			if err := rpc.Init(); err != nil {
				logger.Warn("Failed to init Neo N3 RPC client", "endpoint", ep, "poolIdx", i, "error", err)
				return
			}
			logger.Info("Pool connection established", "endpoint", ep, "poolIdx", i)
			mu.Lock()
			pool = append(pool, &pooledRPC{client: rpc})
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if len(pool) == 0 {
		return nil, fmt.Errorf("failed to connect to any Neo N3 endpoint (tried %d connections across %d endpoints)", poolSize, len(endpoints))
	}

	c.pool = pool
	logger.Info("Neo N3 connection pool ready", "active", len(pool), "requested", poolSize)
	return c, nil
}

// pick returns the next pooled connection using round-robin.
func (c *Client) pick() *pooledRPC {
	idx := c.counter.Add(1) % uint64(len(c.pool))
	return c.pool[idx]
}

// retry executes op with up to 5 retries using exponential backoff,
// picking a different connection each time.
func retry[T any](c *Client, ctx context.Context, op func(rpc *rpcclient.Client) (T, error)) (T, error) {
	const maxRetries = 5
	var lastErr error
	var zero T

	for attempt := 0; attempt <= maxRetries; attempt++ {
		conn := c.pick()
		if err := c.waitForPacing(ctx, conn); err != nil {
			return zero, err
		}
		val, err := op(conn.client)
		if err == nil {
			return val, nil
		}
		lastErr = err

		if attempt < maxRetries {
			backoff := time.Duration(1<<attempt) * 500 * time.Millisecond // 500ms, 1s, 2s, 4s, 8s
			c.logger.Warn("Neo N3 RPC call failed, retrying",
				"attempt", attempt+1, "backoff", backoff, "error", err)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func (c *Client) waitForPacing(ctx context.Context, conn *pooledRPC) error {
	if c.callInterval <= 0 {
		return nil
	}
	now := time.Now()
	lastNs := conn.lastCallNs.Load()
	if lastNs == 0 {
		conn.lastCallNs.CompareAndSwap(0, now.UnixNano())
		return nil
	}
	last := time.Unix(0, lastNs)
	nextAllowed := last.Add(c.callInterval)
	if now.Before(nextAllowed) {
		wait := time.Until(nextAllowed)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		now = time.Now()
	}
	conn.lastCallNs.Store(now.UnixNano())
	return nil
}

// Network returns the N3 network magic (getversion).
func (c *Client) Network(ctx context.Context) (uint32, error) {
	return retry(c, ctx, func(rpc *rpcclient.Client) (uint32, error) {
		v, err := rpc.GetVersion()
		if err != nil {
			return 0, err
		}
		return uint32(v.Protocol.Network), nil
	})
}

func (c *Client) GetBlockCount(ctx context.Context) (uint32, error) {
	return retry(c, ctx, func(rpc *rpcclient.Client) (uint32, error) {
		return rpc.GetBlockCount()
	})
}

func (c *Client) GetBlockByIndex(ctx context.Context, index uint32) (*block.Block, error) {
	if c.cacheReads {
		key := n3BlockKey(index)
		if data, ok := c.cache.Get(key); ok {
			blk := block.New(false)
			r := neio.NewBinReaderFromBuf(data)
			blk.DecodeBinary(r)
			if r.Err == nil {
				return blk, nil
			}
		}
	}

	blk, err := retry(c, ctx, func(rpc *rpcclient.Client) (*block.Block, error) {
		return rpc.GetBlockByIndex(index)
	})
	if err != nil {
		return nil, err
	}

	if c.cache != nil {
		w := neio.NewBufBinWriter()
		blk.EncodeBinary(w.BinWriter)
		if w.Err == nil {
			_ = c.cache.Put(n3BlockKey(index), w.Bytes())
		}
	}
	return blk, nil
}

func (c *Client) GetApplicationLog(ctx context.Context, txHash util.Uint256) (*result.ApplicationLog, error) {
	if c.cacheReads {
		key := n3AppLogKey(txHash)
		if data, ok := c.cache.Get(key); ok {
			var appLog result.ApplicationLog
			if err := json.Unmarshal(data, &appLog); err == nil {
				return &appLog, nil
			}
		}
	}

	appLog, err := retry(c, ctx, func(rpc *rpcclient.Client) (*result.ApplicationLog, error) {
		return rpc.GetApplicationLog(txHash, nil)
	})
	if err != nil {
		return nil, err
	}

	if c.cache != nil {
		if data, jErr := json.Marshal(appLog); jErr == nil {
			_ = c.cache.Put(n3AppLogKey(txHash), data)
		}
	}
	return appLog, nil
}

func n3BlockKey(height uint32) []byte {
	return fmt.Appendf(nil, "n3:blk:%d", height)
}

func n3AppLogKey(txHash util.Uint256) []byte {
	return fmt.Appendf(nil, "n3:applog:%s", txHash.StringLE())
}

func (c *Client) Close() {
	for _, rpc := range c.pool {
		if rpc != nil && rpc.client != nil {
			rpc.client.Close()
		}
	}
}

// CallInterval returns the configured interval between RPC calls.
func (c *Client) CallInterval() time.Duration {
	return c.callInterval
}

// PoolSize returns active pooled RPC connections.
func (c *Client) PoolSize() int {
	return len(c.pool)
}
