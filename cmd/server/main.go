package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/bane-labs/bridge-indexer-backend/internal/config"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
)

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

	e := echo.New()

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("Shutdown error", "error", shutdownErr)
	}

	logger.Info("Bridge Indexer Backend stopped")
	return nil
}
