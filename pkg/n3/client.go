package n3

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// Client wraps a pool of Neo N3 RPC connections for concurrent use.
type Client struct {
	pool      []*rpcclient.Client
	endpoints []string
	poolSize  int
	counter   atomic.Uint64
	logger    *slog.Logger
}

// NewClient creates a Client with a pool of poolSize RPC connections,
// distributed evenly across the provided endpoints.
// At least one connection must succeed or an error is returned.
func NewClient(ctx context.Context, endpoints []string, poolSize int, logger *slog.Logger) (*Client, error) {
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
		endpoints: endpoints,
		poolSize:  poolSize,
		logger:    logger,
	}

	pool := make([]*rpcclient.Client, 0, poolSize)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ep := endpoints[i%len(endpoints)]
			rpc, err := rpcclient.New(ctx, ep, rpcclient.Options{
				RequestTimeout: 30 * time.Second,
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
			pool = append(pool, rpc)
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

// pick returns the next connection from the pool using round-robin.
func (c *Client) pick() *rpcclient.Client {
	idx := c.counter.Add(1) % uint64(len(c.pool))
	return c.pool[idx]
}

// retry executes op with up to 2 retries, picking a different connection each time.
func retry[T any](c *Client, ctx context.Context, op func(rpc *rpcclient.Client) (T, error)) (T, error) {
	const maxRetries = 2
	var lastErr error
	var zero T

	for attempt := 0; attempt <= maxRetries; attempt++ {
		rpc := c.pick()
		val, err := op(rpc)
		if err == nil {
			return val, nil
		}
		lastErr = err

		if attempt < maxRetries {
			c.logger.Warn("Neo N3 RPC call failed, retrying",
				"attempt", attempt+1, "backoff", "50ms", "error", err)
			time.Sleep(50 * time.Millisecond)
		}
	}
	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func (c *Client) GetBlockCount(ctx context.Context) (uint32, error) {
	return retry(c, ctx, func(rpc *rpcclient.Client) (uint32, error) {
		return rpc.GetBlockCount()
	})
}

func (c *Client) GetBlockByIndex(ctx context.Context, index uint32) (*block.Block, error) {
	return retry(c, ctx, func(rpc *rpcclient.Client) (*block.Block, error) {
		return rpc.GetBlockByIndex(index)
	})
}

func (c *Client) GetApplicationLog(ctx context.Context, txHash util.Uint256) (*result.ApplicationLog, error) {
	return retry(c, ctx, func(rpc *rpcclient.Client) (*result.ApplicationLog, error) {
		return rpc.GetApplicationLog(txHash, nil)
	})
}

func (c *Client) Close() {
	for _, rpc := range c.pool {
		if rpc != nil {
			rpc.Close()
		}
	}
}
