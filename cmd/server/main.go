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
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	echoSwagger "github.com/swaggo/echo-swagger"

	"github.com/bane-labs/bridge-indexer-backend/internal/alerter"
	"github.com/bane-labs/bridge-indexer-backend/internal/api"
	"github.com/bane-labs/bridge-indexer-backend/internal/config"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/indexer"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
	_ "github.com/bane-labs/bridge-indexer-backend/docs/swagger"
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
	opStore := datastore.NewOperationStore(store)
	logger.Info("Database initialized", "path", cfg.DBPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n3PoolSize := cfg.N3SyncConcurrency
	if n3PoolSize > 500 {
		n3PoolSize = 500
	}
	n3Client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, n3PoolSize, logger)
	if err != nil {
		return fmt.Errorf("failed to create Neo N3 client: %w", err)
	}
	defer n3Client.Close()

	neoxClient, err := neox.NewClient(ctx, cfg.NeoXRPCEndpoints, logger)
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
	n3Indexer, err := indexer.NewN3Indexer(n3Client, store, opStore, n3EventProc, n3Contracts, cfg.N3StartBlock, cfg.N3SyncConcurrency, logger)
	if err != nil {
		return fmt.Errorf("failed to create N3 indexer: %w", err)
	}

	neoxEventProc := indexer.NewNeoXEventProcessor(opStore, store, cfg.NeoXTokenBridge, cfg.NeoXMessageBridge, logger)
	neoxContracts := []string{cfg.NeoXTokenBridge, cfg.NeoXMessageBridge}
	neoxIndexer := indexer.NewNeoXIndexer(neoxClient, store, opStore, neoxEventProc, neoxContracts, cfg.NeoXStartBlock, cfg.NeoXSyncConcurrency, logger)

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

	pauseDetector := monitor.NewPauseDetector(store, logger)
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
