package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dbPath := "./data"
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		logger.Error("Failed to create database directory", "error", err)
		os.Exit(1)
	}

	store, err := datastore.NewBadgerDbStore(datastore.NewDbOptions(&dbPath), logger)
	if err != nil {
		logger.Error("Failed to initialize BadgerDB", "error", err)
		os.Exit(1)
	}
	ds := datastore.NewDataStore(store)
	defer func() {
		if closeErr := ds.Close(); closeErr != nil {
			logger.Error("Failed to close datastore", "error", closeErr)
		}
	}()

	e := echo.New()

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           e,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("Bridge Indexer Backend starting", "port", "8080")
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
}
