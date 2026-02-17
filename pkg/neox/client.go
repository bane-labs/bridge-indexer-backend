package neox

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Client struct {
	eth       *ethclient.Client
	endpoints []string
	active    int
	logger    *slog.Logger
}

func NewClient(ctx context.Context, endpoints []string, logger *slog.Logger) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no Neo X RPC endpoints provided")
	}

	c := &Client{
		endpoints: endpoints,
		logger:    logger,
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

func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return retryEVM(c, ctx, func() ([]types.Log, error) {
		return c.eth.FilterLogs(ctx, query)
	})
}

func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return retryEVM(c, ctx, func() (*types.Header, error) {
		return c.eth.HeaderByNumber(ctx, number)
	})
}

func (c *Client) Close() {
	if c.eth != nil {
		c.eth.Close()
	}
}

func HexToAddresses(hexAddrs []string) []common.Address {
	addrs := make([]common.Address, len(hexAddrs))
	for i, h := range hexAddrs {
		addrs[i] = common.HexToAddress(h)
	}
	return addrs
}
