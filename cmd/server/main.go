package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	echoSwagger "github.com/swaggo/echo-swagger"

	_ "github.com/bane-labs/bridge-indexer-backend/docs/swagger"
	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/api"
	"github.com/bane-labs/bridge-indexer-backend/internal/config"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/indexer"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// @title           Bridge Indexer API
// @version         1.0
// @description     API for monitoring cross-chain bridge operations between Neo N3 and NeoX.
// @host            localhost:8080
// @BasePath        /
// @schemes         http

func main() {
	rootCmd := &cobra.Command{
		Use:   "bridge-indexer",
		Short: "Neo X <-> Neo N3 Bridge Indexer & Monitoring Backend",
		RunE:  run,
	}

	backfillCmd := &cobra.Command{
		Use:   "backfill",
		Short: "Resolve stuck operations by querying target chains for completion events",
		RunE:  runBackfill,
	}
	rootCmd.AddCommand(backfillCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	v := viper.New()
	cfg, err := config.Load(v)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	logger.Info("Configuration loaded",
		"n3_endpoints", len(cfg.N3RPCEndpoints),
		"neox_endpoints", len(cfg.NeoXRPCEndpoints),
		"db_path", cfg.DBPath,
		"n3_rpc_interval", cfg.N3RPCCallInterval,
		"neox_rpc_interval", cfg.NeoXRPCCallInterval,
	)

	if err := os.MkdirAll(cfg.DBPath, 0o755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	store, err := datastore.NewStore(cfg.DBPath, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize BadgerDB: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("Failed to close datastore", "error", closeErr)
		}
	}()
	bridgeStateStore := datastore.NewBridgeStateStore(store)
	opStore := datastore.NewOperationStore(store, bridgeStateStore)
	logger.Info("Database initialized", "path", cfg.DBPath)

	// Initialize static directional bridge instances (native + message × deposit + withdrawal).
	for _, inst := range []struct {
		bt, src, dst string
	}{
		{"native", "neo_n3", "neo_x"},
		{"native", "neo_x", "neo_n3"},
		{"message", "neo_n3", "neo_x"},
		{"message", "neo_x", "neo_n3"},
	} {
		id := datastore.BuildInstanceID(inst.bt, inst.src, inst.dst, nil, nil)
		if _, err := bridgeStateStore.GetBridgeState(id); err == datastore.ErrNotFound {
			initState := &types.DirectionalBridgeState{
				InstanceID: id,
				BridgeType: types.BridgeType(inst.bt),
				SrcChain:   inst.src,
				DstChain:   inst.dst,
			}
			if initErr := bridgeStateStore.SaveBridgeState(initState); initErr != nil {
				logger.Warn("Failed to initialize bridge state", "instance_id", id, "error", initErr)
			}
		} else if err != nil {
			logger.Error("Failed to check bridge state at startup", "instance_id", id, "error", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n3PoolSize := cfg.N3SyncConcurrency
	if n3PoolSize > 500 {
		n3PoolSize = 500
	}
	n3Client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, n3PoolSize, logger, cfg.N3RPCCallInterval)
	if err != nil {
		return fmt.Errorf("failed to create Neo N3 client: %w", err)
	}
	defer n3Client.Close()

	neoxClient, err := neox.NewClient(ctx, cfg.NeoXRPCEndpoints, logger, cfg.NeoXRPCCallInterval)
	if err != nil {
		return fmt.Errorf("failed to create Neo X client: %w", err)
	}
	defer neoxClient.Close()

	n3EventProc, err := indexer.NewN3EventProcessor(
		opStore,
		strings.TrimPrefix(cfg.N3NeoXBridge, "0x"),
		strings.TrimPrefix(cfg.N3MessageBridge, "0x"),
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create N3 event processor: %w", err)
	}
	n3Contracts := []string{
		strings.TrimPrefix(cfg.N3NeoXBridge, "0x"),
		strings.TrimPrefix(cfg.N3MessageBridge, "0x"),
	}
	n3Indexer, err := indexer.NewN3Indexer(n3Client, store, opStore, bridgeStateStore, n3EventProc, n3Contracts, cfg.N3StartBlock, cfg.N3SyncConcurrency, logger)
	if err != nil {
		return fmt.Errorf("failed to create N3 indexer: %w", err)
	}

	neoxEventProc := indexer.NewNeoXEventProcessor(opStore, store, cfg.NeoXTokenBridge, cfg.NeoXMessageBridge, logger)
	neoxContracts := []string{cfg.NeoXTokenBridge, cfg.NeoXMessageBridge}
	pauseDetector := monitor.NewPauseDetector(store, logger)
	neoxIndexer := indexer.NewNeoXIndexer(neoxClient, store, opStore, bridgeStateStore, neoxEventProc, neoxContracts, cfg.NeoXStartBlock, cfg.NeoXSyncConcurrency, logger, pauseDetector)

	var alerters []alerter.Alerter
	if cfg.DiscordConfigured() {
		alerters = append(alerters, alerter.NewDiscordAlerter(cfg.DiscordWebhookURL))
		logger.Info("Discord alerting enabled")
	}
	if cfg.SMTPConfigured() {
		alerters = append(alerters, alerter.NewEmailAlerter(
			cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUser, cfg.SMTPPass, cfg.SMTPFrom, cfg.AlertRecipients,
		))
		logger.Info("Email alerting enabled")
	}
	if len(alerters) == 0 {
		alerters = append(alerters, &alerter.NoopAlerter{})
		logger.Info("No alerting channels configured, using noop")
	}
	multiAlerter := alerter.NewMultiAlerter(alerters...)

	stuckDetector := monitor.NewStuckDetector(opStore, multiAlerter, cfg.StuckThreshold, logger)
	healthAgg := monitor.NewHealthAggregator(opStore, pauseDetector, logger)
	mon := monitor.NewMonitor(stuckDetector, pauseDetector, healthAgg, logger)

	e := echo.New()
	e.HideBanner = true
	handler := api.NewHandler(store, opStore, healthAgg)
	api.RegisterRoutes(e, handler)
	e.GET("/swagger/*", echoSwagger.WrapHandler)
	logger.Info("API routes registered")

	go func() {
		logger.Info("Starting N3 indexer")
		if idxErr := n3Indexer.Start(ctx); idxErr != nil {
			logger.Error("N3 indexer error", "error", idxErr)
		}
	}()

	go func() {
		logger.Info("Starting NeoX indexer")
		if idxErr := neoxIndexer.Start(ctx); idxErr != nil {
			logger.Error("NeoX indexer error", "error", idxErr)
		}
	}()

	go func() {
		logger.Info("Starting monitor")
		if monErr := mon.Start(ctx); monErr != nil {
			logger.Error("Monitor error", "error", monErr)
		}
	}()

	// Auto-trigger backfill once both chains have caught up to chain tip.
	go func() {
		var backfillOnce sync.Once
		const syncGapThreshold uint64 = 50
		const requiredConsecutive = 3

		n3ConsecutiveSynced := 0
		neoxConsecutiveSynced := 0

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			// Check NeoX sync gap.
			neoxChainHeight, err := neoxClient.BlockNumber(ctx)
			if err != nil {
				logger.Warn("Auto-backfill: failed to get NeoX chain height", "error", err)
				neoxConsecutiveSynced = 0
				continue
			}
			neoxState, err := store.GetIndexerState("neo_x")
			if err != nil {
				if errors.Is(err, datastore.ErrNotFound) {
					neoxConsecutiveSynced = 0
					continue
				}
				logger.Warn("Auto-backfill: failed to get NeoX indexer state", "error", err)
				neoxConsecutiveSynced = 0
				continue
			}
			if neoxChainHeight-neoxState.LastBlockHeight < syncGapThreshold {
				neoxConsecutiveSynced++
			} else {
				neoxConsecutiveSynced = 0
			}

			// Check N3 sync gap.
			n3BlockCount, err := n3Client.GetBlockCount(ctx)
			if err != nil {
				logger.Warn("Auto-backfill: failed to get N3 chain height", "error", err)
				n3ConsecutiveSynced = 0
				continue
			}
			n3ChainHeight := uint64(n3BlockCount) - 1
			n3State, err := store.GetIndexerState("neo_n3")
			if err != nil {
				if errors.Is(err, datastore.ErrNotFound) {
					n3ConsecutiveSynced = 0
					continue
				}
				logger.Warn("Auto-backfill: failed to get N3 indexer state", "error", err)
				n3ConsecutiveSynced = 0
				continue
			}
			if n3ChainHeight-n3State.LastBlockHeight < syncGapThreshold {
				n3ConsecutiveSynced++
			} else {
				n3ConsecutiveSynced = 0
			}

			// Both chains synced for enough consecutive checks — run backfill once.
			if neoxConsecutiveSynced >= requiredConsecutive && n3ConsecutiveSynced >= requiredConsecutive {
				backfillOnce.Do(func() {
					logger.Info("Both chains synced, running backfill...")
					neoxContracts := []string{cfg.NeoXTokenBridge, cfg.NeoXMessageBridge}
					n3Contracts := []string{
						strings.TrimPrefix(cfg.N3NeoXBridge, "0x"),
						strings.TrimPrefix(cfg.N3MessageBridge, "0x"),
					}
					bf := indexer.NewBackfiller(opStore, neoxClient, n3Client, neoxContracts, n3Contracts, logger)
					resolved, bfErr := bf.Run(ctx)
					if bfErr != nil {
						logger.Error("Auto-backfill failed", "error", bfErr)
						return
					}
					logger.Info("Auto-backfill complete", "resolved", resolved)
				})
				return
			}
		}
	}()

	addr := ":" + cfg.ServerPort
	srv := &http.Server{
		Addr:              addr,
		Handler:           e,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("Bridge Indexer Backend starting", "port", cfg.ServerPort)
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErrCh <- serveErr
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		logger.Info("Shutdown signal received", "signal", sig)
	case serveErr := <-serverErrCh:
		logger.Error("Server failed", "error", serveErr)
	}

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("Shutdown error", "error", shutdownErr)
	}

	logger.Info("Bridge Indexer Backend stopped")
	return nil
}

func runBackfill(cmd *cobra.Command, args []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	v := viper.New()
	cfg, err := config.Load(v)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	logger.Info("Configuration loaded",
		"n3_endpoints", len(cfg.N3RPCEndpoints),
		"neox_endpoints", len(cfg.NeoXRPCEndpoints),
		"db_path", cfg.DBPath,
	)

	if err := os.MkdirAll(cfg.DBPath, 0o755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	store, err := datastore.NewStore(cfg.DBPath, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize BadgerDB: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("Failed to close datastore", "error", closeErr)
		}
	}()
	opStore := datastore.NewOperationStore(store, nil) // nil: backfill does not call UpdateOperationCompletion, so no dest bridge state updates needed
	logger.Info("Database initialized", "path", cfg.DBPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n3Client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, 100, logger, cfg.N3RPCCallInterval)
	if err != nil {
		return fmt.Errorf("failed to create Neo N3 client: %w", err)
	}
	defer n3Client.Close()

	neoxClient, err := neox.NewClient(ctx, cfg.NeoXRPCEndpoints, logger, cfg.NeoXRPCCallInterval)
	if err != nil {
		return fmt.Errorf("failed to create Neo X client: %w", err)
	}
	defer neoxClient.Close()

	neoxContracts := []string{cfg.NeoXTokenBridge, cfg.NeoXMessageBridge}

	n3Contracts := []string{
		strings.TrimPrefix(cfg.N3NeoXBridge, "0x"),
		strings.TrimPrefix(cfg.N3MessageBridge, "0x"),
	}

	backfiller := indexer.NewBackfiller(opStore, neoxClient, n3Client, neoxContracts, n3Contracts, logger)
	resolved, err := backfiller.Run(ctx)
	if err != nil {
		return fmt.Errorf("backfill failed: %w", err)
	}

	logger.Info("Backfill finished", "resolved", resolved)
	return nil
}
