package n3

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

type Client struct {
	rpc       *rpcclient.Client
	endpoints []string
	active    int
	logger    *slog.Logger
}

func NewClient(ctx context.Context, endpoints []string, logger *slog.Logger) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no Neo N3 RPC endpoints provided")
	}

	c := &Client{
		endpoints: endpoints,
		logger:    logger,
	}

	if err := c.connect(ctx, 0); err != nil {
		return nil, fmt.Errorf("failed to connect to any Neo N3 endpoint: %w", err)
	}

	return c, nil
}

func (c *Client) connect(ctx context.Context, startIdx int) error {
	for i := 0; i < len(c.endpoints); i++ {
		idx := (startIdx + i) % len(c.endpoints)
		ep := c.endpoints[idx]

		rpc, err := rpcclient.New(ctx, ep, rpcclient.Options{})
		if err != nil {
			c.logger.Warn("Failed to create Neo N3 RPC client", "endpoint", ep, "error", err)
			continue
		}

		if err := rpc.Init(); err != nil {
			c.logger.Warn("Failed to init Neo N3 RPC client", "endpoint", ep, "error", err)
			continue
		}

		c.rpc = rpc
		c.active = idx
		c.logger.Info("Connected to Neo N3 RPC", "endpoint", ep)
		return nil
	}
	return fmt.Errorf("all %d Neo N3 endpoints unreachable", len(c.endpoints))
}

func (c *Client) failover(ctx context.Context) error {
	next := (c.active + 1) % len(c.endpoints)
	c.logger.Warn("Attempting Neo N3 failover", "from", c.endpoints[c.active], "to", c.endpoints[next])
	return c.connect(ctx, next)
}

func retry[T any](c *Client, ctx context.Context, op func() (T, error)) (T, error) {
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
			c.logger.Warn("Neo N3 RPC call failed, retrying", "attempt", attempt+1, "backoff", backoff, "error", err)
			time.Sleep(backoff)

			if fErr := c.failover(ctx); fErr != nil {
				c.logger.Warn("Failover failed", "error", fErr)
			}
		}
	}
	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func (c *Client) GetBlockCount(ctx context.Context) (uint32, error) {
	return retry(c, ctx, func() (uint32, error) {
		return c.rpc.GetBlockCount()
	})
}

func (c *Client) GetBlockByIndex(ctx context.Context, index uint32) (*block.Block, error) {
	return retry(c, ctx, func() (*block.Block, error) {
		return c.rpc.GetBlockByIndex(index)
	})
}

func (c *Client) GetApplicationLog(ctx context.Context, txHash util.Uint256) (*result.ApplicationLog, error) {
	return retry(c, ctx, func() (*result.ApplicationLog, error) {
		return c.rpc.GetApplicationLog(txHash, nil)
	})
}

func (c *Client) Close() {
	if c.rpc != nil {
		c.rpc.Close()
	}
}
