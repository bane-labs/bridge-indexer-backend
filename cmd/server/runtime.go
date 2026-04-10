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
	"github.com/labstack/echo/v4/middleware"
	"github.com/spf13/viper"
	echoSwagger "github.com/swaggo/echo-swagger"

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

type appRuntime struct {
	store       *datastore.Store
	opStore     *datastore.OperationStore
	n3Client    *n3.Client
	neoxClient  *neox.Client
	n3Indexer   *indexer.N3Indexer
	neoxIndexer *indexer.NeoXIndexer
	monitor     *monitor.Monitor
	server      *http.Server
}

func loadConfig(v *viper.Viper, logger *slog.Logger) (*config.Config, error) {
	cfg, err := config.Load(v)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	logger.Info("Configuration loaded",
		"n3_endpoints", len(cfg.N3RPCEndpoints),
		"neox_endpoints", len(cfg.NeoXRPCEndpoints),
		"db_path", cfg.DBPath,
		"n3_rpc_interval", cfg.N3RPCCallInterval,
		"neox_rpc_interval", cfg.NeoXRPCCallInterval,
	)
	return cfg, nil
}

func openStore(dbPath string, logger *slog.Logger) (*datastore.Store, error) {
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}
	store, err := datastore.NewStore(dbPath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize BadgerDB: %w", err)
	}
	logger.Info("Database initialized", "path", dbPath)
	return store, nil
}

func openStoreReadOnly(dbPath string, logger *slog.Logger) (*datastore.Store, error) {
	store, err := datastore.NewReadOnlyStore(dbPath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize BadgerDB: %w", err)
	}
	logger.Info("Database initialized (read-only)", "path", dbPath)
	return store, nil
}

func (rt *appRuntime) Close(logger *slog.Logger) {
	if rt.neoxClient != nil {
		rt.neoxClient.Close()
	}
	if rt.n3Client != nil {
		rt.n3Client.Close()
	}
	if rt.store != nil {
		if err := rt.store.Close(); err != nil {
			logger.Error("Failed to close datastore", "error", err)
		}
	}
}

func buildRuntime(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*appRuntime, error) {
	store, err := openStore(cfg.DBPath, logger)
	if err != nil {
		return nil, err
	}
	bridgeStateStore := datastore.NewBridgeStateStore(store)
	opStore := datastore.NewOperationStore(store, bridgeStateStore)
	initializeStaticBridgeStates(bridgeStateStore, logger)

	n3PoolSize := cfg.N3SyncConcurrency
	if n3PoolSize > 500 {
		n3PoolSize = 500
	}
	n3Client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, n3PoolSize, logger, cfg.N3RPCCallInterval)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("failed to create Neo N3 client: %w", err)
	}

	neoxClient, err := neox.NewClient(ctx, cfg.NeoXRPCEndpoints, logger, cfg.NeoXRPCCallInterval)
	if err != nil {
		n3Client.Close()
		_ = store.Close()
		return nil, fmt.Errorf("failed to create Neo X client: %w", err)
	}

	n3Contracts, neoxContracts := bridgeContracts(cfg)
	n3EventProc, err := indexer.NewN3EventProcessor(opStore, n3Contracts[0], n3Contracts[1], logger)
	if err != nil {
		neoxClient.Close()
		n3Client.Close()
		_ = store.Close()
		return nil, fmt.Errorf("failed to create N3 event processor: %w", err)
	}

	n3Indexer, err := indexer.NewN3Indexer(
		n3Client, store, opStore, bridgeStateStore, n3EventProc, n3Contracts,
		cfg.N3StartBlock, cfg.N3SyncConcurrency, logger,
	)
	if err != nil {
		neoxClient.Close()
		n3Client.Close()
		_ = store.Close()
		return nil, fmt.Errorf("failed to create N3 indexer: %w", err)
	}

	neoxEventProc := indexer.NewNeoXEventProcessor(opStore, cfg.NeoXTokenBridge, cfg.NeoXMessageBridge, logger)
	pauseDetector := monitor.NewPauseDetector(store, logger)
	neoxIndexer := indexer.NewNeoXIndexer(
		neoxClient, store, opStore, bridgeStateStore, neoxEventProc, neoxContracts,
		cfg.NeoXStartBlock, cfg.NeoXSyncConcurrency, logger, pauseDetector,
	)

	multiAlerter := buildAlerter(cfg, logger)
	stuckDetector := monitor.NewStuckDetector(opStore, multiAlerter, cfg.StuckThreshold, logger)
	healthAgg := monitor.NewHealthAggregator(opStore, pauseDetector, logger)
	mon := monitor.NewMonitor(stuckDetector, pauseDetector, healthAgg, logger)

	e := newHTTPAPI(cfg, store, opStore, healthAgg, logger)
	server := &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           e,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &appRuntime{
		store:       store,
		opStore:     opStore,
		n3Client:    n3Client,
		neoxClient:  neoxClient,
		n3Indexer:   n3Indexer,
		neoxIndexer: neoxIndexer,
		monitor:     mon,
		server:      server,
	}, nil
}

func initializeStaticBridgeStates(bridgeStateStore *datastore.BridgeStateStore, logger *slog.Logger) {
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
}

func buildAlerter(cfg *config.Config, logger *slog.Logger) alerter.Alerter {
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
	return alerter.NewMultiAlerter(alerters...)
}

func newHTTPAPI(
	cfg *config.Config,
	store *datastore.Store,
	opStore *datastore.OperationStore,
	healthAgg *monitor.HealthAggregator,
	logger *slog.Logger,
) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	if len(cfg.CORSAllowedOrigins) > 0 {
		e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
			AllowOrigins:     cfg.CORSAllowedOrigins,
			AllowMethods:     []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions},
			AllowHeaders:     []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization, echo.HeaderXRequestedWith, "X-Request-Id"},
			ExposeHeaders:    []string{"X-Request-Id"},
			AllowCredentials: cfg.CORSAllowCredentials,
			MaxAge:           int((12 * time.Hour).Seconds()),
		}))
		logger.Info("CORS enabled", "origins", len(cfg.CORSAllowedOrigins), "credentials", cfg.CORSAllowCredentials)
	} else {
		logger.Info("CORS disabled, no allowed origins configured")
	}
	handler := api.NewHandler(store, opStore, healthAgg)
	api.RegisterRoutes(e, handler)
	e.GET("/swagger/*", echoSwagger.WrapHandler)
	logger.Info("API routes registered")
	return e
}

func bridgeContracts(cfg *config.Config) (n3Contracts []string, neoxContracts []string) {
	n3Contracts = []string{
		strings.TrimPrefix(cfg.N3NeoXBridge, "0x"),
		strings.TrimPrefix(cfg.N3MessageBridge, "0x"),
	}
	neoxContracts = []string{cfg.NeoXTokenBridge, cfg.NeoXMessageBridge}
	return n3Contracts, neoxContracts
}

func startBackgroundWorkers(ctx context.Context, cfg *config.Config, rt *appRuntime, logger *slog.Logger) {
	go func() {
		logger.Info("Starting N3 indexer")
		if err := rt.n3Indexer.Start(ctx); err != nil {
			logger.Error("N3 indexer error", "error", err)
		}
	}()
	go func() {
		logger.Info("Starting NeoX indexer")
		if err := rt.neoxIndexer.Start(ctx); err != nil {
			logger.Error("NeoX indexer error", "error", err)
		}
	}()
	go func() {
		logger.Info("Starting monitor")
		if err := rt.monitor.Start(ctx); err != nil {
			logger.Error("Monitor error", "error", err)
		}
	}()
	go runAutoBackfillWhenSynced(ctx, cfg, rt, logger)
}

func runAutoBackfillWhenSynced(ctx context.Context, cfg *config.Config, rt *appRuntime, logger *slog.Logger) {
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

		neoxChainHeight, err := rt.neoxClient.BlockNumber(ctx)
		if err != nil {
			logger.Warn("Auto-backfill: failed to get NeoX chain height", "error", err)
			neoxConsecutiveSynced = 0
			continue
		}
		neoxState, err := rt.store.GetIndexerState("neo_x")
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

		n3BlockCount, err := rt.n3Client.GetBlockCount(ctx)
		if err != nil {
			logger.Warn("Auto-backfill: failed to get N3 chain height", "error", err)
			n3ConsecutiveSynced = 0
			continue
		}
		n3ChainHeight := uint64(n3BlockCount) - 1
		n3State, err := rt.store.GetIndexerState("neo_n3")
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

		if neoxConsecutiveSynced >= requiredConsecutive && n3ConsecutiveSynced >= requiredConsecutive {
			backfillOnce.Do(func() {
				logger.Info("Both chains synced, running backfill...")
				n3Contracts, neoxContracts := bridgeContracts(cfg)
				bf := indexer.NewBackfiller(rt.opStore, rt.neoxClient, rt.n3Client, neoxContracts, n3Contracts, logger)
				resolved, err := bf.Run(ctx)
				if err != nil {
					logger.Error("Auto-backfill failed", "error", err)
					return
				}
				logger.Info("Auto-backfill complete", "resolved", resolved)
			})
			return
		}
	}
}

func runServerLoop(cancel context.CancelFunc, srv *http.Server, port string, logger *slog.Logger) error {
	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("Bridge Indexer Backend starting", "port", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		logger.Info("Shutdown signal received", "signal", sig)
	case err := <-serverErrCh:
		logger.Error("Server failed", "error", err)
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Shutdown error", "error", err)
	}
	logger.Info("Bridge Indexer Backend stopped")
	return nil
}
