package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	_ "github.com/bane-labs/bridge-indexer-backend/docs/swagger"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/indexer"
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
	statusCmd := newStatusCommand()
	rootCmd.AddCommand(backfillCmd)
	rootCmd.AddCommand(statusCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	v := viper.New()
	cfg, err := loadConfig(v, logger)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt, err := buildRuntime(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer rt.Close(logger)
	startBackgroundWorkers(ctx, cfg, rt, logger)
	return runServerLoop(cancel, rt.server, cfg.ServerPort, logger)
}

func runBackfill(cmd *cobra.Command, args []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	v := viper.New()
	cfg, err := loadConfig(v, logger)
	if err != nil {
		return err
	}
	store, err := openStore(cfg.DBPath, logger)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("Failed to close datastore", "error", closeErr)
		}
	}()
	opStore := datastore.NewOperationStore(store, nil) // nil: backfill does not call UpdateOperationCompletion, so no dest bridge state updates needed

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

	n3Contracts, neoxContracts := bridgeContracts(cfg)

	backfiller := indexer.NewBackfiller(opStore, neoxClient, n3Client, neoxContracts, n3Contracts, logger)
	resolved, err := backfiller.Run(ctx)
	if err != nil {
		return fmt.Errorf("backfill failed: %w", err)
	}

	logger.Info("Backfill finished", "resolved", resolved)
	return nil
}
