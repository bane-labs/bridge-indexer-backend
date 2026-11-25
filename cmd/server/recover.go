package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/bane-labs/bridge-indexer-backend/internal/config"
	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/indexer"
	"github.com/bane-labs/bridge-indexer-backend/internal/monitor"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

var recoverFlags struct {
	chain     string
	start     uint64
	target    string
	batch     uint64
	maxBlocks uint64
	json      bool
	conc      int
	filter    bool
	located   uint64
	scan      bool
}

// newRecoverCommand is the historical lifecycle recovery tool. It opens the
// database read-write, so Badger's directory lock makes it exclusive with a
// running server: stop the server first.
func newRecoverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Replay historical blocks to recover lifecycle evidence and missing source operations",
		Long: `Replays one chain's blocks through the live decoders and commits each bounded
range atomically with its recovery watermark. Bounds are block heights of the
named chain. The live checkpoint is never changed. See docs/operation-lifecycle.md
("Recovery runbook").`,
	}
	chainFlag := func(c *cobra.Command) {
		c.Flags().StringVar(&recoverFlags.chain, "chain", "", "chain to replay: neo_x or neo_n3")
		_ = c.MarkFlagRequired("chain")
	}
	begin := &cobra.Command{
		Use:   "begin",
		Short: "Create the recovery manifest for [--start, --target] on --chain",
		RunE:  func(cmd *cobra.Command, _ []string) error { return withRecoverer(cmd, runRecoverBegin) },
	}
	chainFlag(begin)
	begin.Flags().Uint64Var(&recoverFlags.start, "start", 0, "first height to replay (the destination contract's deployment height on this chain)")
	begin.Flags().StringVar(&recoverFlags.target, "target", "", "inclusive target height, or 'head' or 'checkpoint' (the live checkpoint)")
	begin.Flags().BoolVar(&recoverFlags.filter, "event-filter", false, "neo_n3: trust the AxLabs event filter plugin's live index to locate bridge blocks (see the runbook)")
	_ = begin.MarkFlagRequired("start")
	_ = begin.MarkFlagRequired("target")

	extend := &cobra.Command{
		Use:   "extend",
		Short: "Raise the target, keeping progress (cutover catch-up)",
		RunE:  func(cmd *cobra.Command, _ []string) error { return withRecoverer(cmd, runRecoverExtend) },
	}
	chainFlag(extend)
	extend.Flags().StringVar(&recoverFlags.target, "target", "", "new inclusive target height, or 'head' or 'checkpoint'")
	_ = extend.MarkFlagRequired("target")

	run := &cobra.Command{
		Use:   "run",
		Short: "Replay from the watermark toward the target; safe to interrupt and rerun",
		RunE:  func(cmd *cobra.Command, _ []string) error { return withRecoverer(cmd, runRecoverRun) },
	}
	chainFlag(run)
	run.Flags().Uint64Var(&recoverFlags.batch, "batch", 0, "blocks per committed range (default 100000 on neo_x, 250 with --claim-scan, 100 on neo_n3); eth_getLogs failures halve it")
	run.Flags().Uint64Var(&recoverFlags.maxBlocks, "max-blocks", 0, "stop after this many blocks (0 = through the target)")
	run.Flags().BoolVar(&recoverFlags.scan, "claim-scan", false, "neo_x: fetch every block and scan it for silent claims instead of deferring claims to 'recover claims' (slow)")
	run.Flags().Uint64Var(&recoverFlags.located, "located-batch", 10000, "neo_n3 with --event-filter: blocks per range located through the plugin")
	run.Flags().IntVar(&recoverFlags.conc, "concurrency", 1, "neo_x: parallel block fetches per range; raise only for a dedicated node (public nodes rate-limit)")

	claims := &cobra.Command{
		Use:   "claims",
		Short: "neo_x: reconcile silent token claims through the committed height from contract state",
		RunE:  func(cmd *cobra.Command, _ []string) error { return withRecoverer(cmd, runRecoverClaims) },
	}
	chainFlag(claims)

	status := &cobra.Command{
		Use:   "status",
		Short: "Report progress, unresolved evidence, completion and handoff readiness",
		RunE:  runRecoverStatus,
	}
	status.Flags().StringVar(&recoverFlags.chain, "chain", "", "neo_x or neo_n3 (default both)")
	status.Flags().BoolVar(&recoverFlags.json, "json", false, "print JSON")

	cmd.AddCommand(begin, extend, run, claims, status)
	return cmd
}

type recoverEnv struct {
	ctx     context.Context
	cfg     *config.Config
	store   *datastore.Store
	opStore *datastore.OperationStore
	rec     *indexer.Recoverer
	logger  *slog.Logger
}

func withRecoverer(cmd *cobra.Command, fn func(*recoverEnv) error) error {
	if recoverFlags.chain != "neo_x" && recoverFlags.chain != "neo_n3" {
		return fmt.Errorf("--chain must be neo_x or neo_n3, got %q", recoverFlags.chain)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := loadConfig(viper.New(), logger)
	if err != nil {
		return err
	}
	store, err := openStore(cfg.DBPath, logger)
	if err != nil {
		return fmt.Errorf("%w (stop the server first: recovery needs exclusive access)", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("Failed to close datastore", "error", err)
		}
	}()
	ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	env := &recoverEnv{ctx: ctx, cfg: cfg, store: store, opStore: datastore.NewOperationStore(store, datastore.NewBridgeStateStore(store)), logger: logger}
	n3Contracts, neoxContracts := bridgeContracts(cfg)
	if recoverFlags.chain == "neo_x" {
		client, err := neox.NewClient(ctx, cfg.NeoXRPCEndpoints, logger, cfg.NeoXRPCCallInterval)
		if err != nil {
			return err
		}
		defer client.Close()
		env.rec = indexer.NewNeoXRecoverer(client, store, env.opStore, neoxContracts[0], neoxContracts[1], recoverFlags.conc, logger)
	} else {
		pool := min(max(cfg.N3SyncConcurrency, 1), 500)
		client, err := n3.NewClient(ctx, cfg.N3RPCEndpoints, pool, logger, cfg.N3RPCCallInterval)
		if err != nil {
			return err
		}
		defer client.Close()
		if env.rec, err = indexer.NewN3Recoverer(client, store, env.opStore, n3Contracts[0], n3Contracts[1], cfg.N3SyncConcurrency, logger); err != nil {
			return err
		}
	}
	return fn(env)
}

// resolveTargetHeight accepts a height, "head" (current chain height) or
// "checkpoint" (the live indexer's persisted height on this chain).
func resolveTargetHeight(spec string, head func() (uint64, error), checkpoint func() (uint64, error)) (uint64, error) {
	switch strings.TrimSpace(spec) {
	case "head":
		return head()
	case "checkpoint":
		return checkpoint()
	}
	h, err := strconv.ParseUint(spec, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("--target must be a height, 'head' or 'checkpoint': %w", err)
	}
	return h, nil
}

func (e *recoverEnv) target() (uint64, error) {
	return resolveTargetHeight(recoverFlags.target,
		func() (uint64, error) { return e.rec.Head(e.ctx) },
		func() (uint64, error) {
			s, err := e.store.GetIndexerState(recoverFlags.chain)
			if err != nil {
				return 0, fmt.Errorf("live checkpoint for %s: %w", recoverFlags.chain, err)
			}
			return s.LastBlockHeight, nil
		})
}

func runRecoverBegin(e *recoverEnv) error {
	target, err := e.target()
	if err != nil {
		return err
	}
	if err := e.rec.Begin(e.ctx, recoverFlags.start, target, recoverFlags.filter); err != nil {
		return err
	}
	return printRecoveryStatus(e.opStore, recoverFlags.chain)
}

func runRecoverExtend(e *recoverEnv) error {
	target, err := e.target()
	if err != nil {
		return err
	}
	if err := e.rec.Extend(e.ctx, target); err != nil {
		return err
	}
	return printRecoveryStatus(e.opStore, recoverFlags.chain)
}

func runRecoverRun(e *recoverEnv) error {
	batch := recoverFlags.batch
	if batch == 0 {
		switch {
		case recoverFlags.chain == "neo_n3":
			batch = 100
		case recoverFlags.scan:
			batch = 250
		default:
			batch = 100000
		}
	}
	e.rec.ScanClaims(recoverFlags.scan)
	var blocks uint64
	thresholds := monitor.StuckThresholds{Default: e.cfg.StuckThreshold, Native: e.cfg.StuckThresholdNative, Token: e.cfg.StuckThresholdToken, Message: e.cfg.StuckThresholdMessage}
	opts := indexer.RecoveryRunOptions{Batch: batch, MaxBlocks: recoverFlags.maxBlocks, LocatedBatch: recoverFlags.located, StuckAfter: thresholds.ForBridgeType}
	if m, err := e.opStore.GetRecoveryManifest(recoverFlags.chain); err == nil && m.TrustedIndex == indexer.EventFilterIndex {
		// Only endpoints serving the plugin are used for it; the others
		// (backups) still serve every standard call.
		if f, err := n3.NewEventFilter(e.ctx, e.cfg.N3RPCEndpoints, e.logger); err == nil {
			e.rec.UseEventFilter(f)
		} else {
			e.logger.Warn("N3 event filter unavailable; every block will be scanned", "error", err)
		}
	}
	err := e.rec.Run(e.ctx, opts, func(p indexer.RecoveryProgress) {
		blocks += p.To - p.From + 1
		rate := float64(blocks) / max(p.Elapsed.Seconds(), 1e-3)
		remaining := p.Target + 1 - p.NextHeight
		e.logger.Info("Recovery range committed", "chain", p.Chain, "from", p.From, "to", p.To, "next", p.NextHeight, "target", p.Target,
			"remaining_blocks", remaining, "blocks_per_sec", fmt.Sprintf("%.1f", rate), "eta", (time.Duration(float64(remaining)/rate) * time.Second).Round(time.Second),
			"observations", p.Observations, "inserted_operations", p.Inserted, "inserted_without_destination", p.WithoutDestination, "legacy_completed", p.LegacyCompleted,
			"trace_gap_blocks", p.TraceGapBlocks, "unresolved_claims", p.Unresolved, "located", p.Located)
	})
	if errors.Is(err, context.Canceled) {
		e.logger.Warn("Recovery interrupted; the last committed range is kept. Rerun 'recover run' to resume.")
	} else if err != nil {
		return err
	}
	return printRecoveryStatus(e.opStore, recoverFlags.chain)
}

func runRecoverClaims(e *recoverEnv) error {
	summary, err := e.rec.ReconcileClaims(e.ctx)
	if err != nil {
		return err
	}
	e.logger.Info("Claim reconciliation committed", "through", summary.Through, "claims_identified", summary.Claims,
		"outstanding", summary.Outstanding, "unresolved", summary.Unresolved, "claim_gaps_cleared", summary.GapsCleared)
	return printRecoveryStatus(e.opStore, recoverFlags.chain)
}

func runRecoverStatus(cmd *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := loadConfig(viper.New(), logger)
	if err != nil {
		return err
	}
	store, err := openStoreReadOnly(cfg.DBPath, logger)
	if err != nil {
		return fmt.Errorf("%w (Badger cannot be read while the server or 'recover run' has it open; stop them first)", err)
	}
	defer func() { _ = store.Close() }()
	opStore := datastore.NewOperationStore(store, nil)
	chains := []string{"neo_x", "neo_n3"}
	if recoverFlags.chain != "" {
		chains = []string{recoverFlags.chain}
	}
	for _, chain := range chains {
		if err := printRecoveryStatus(opStore, chain); err != nil {
			return err
		}
	}
	return nil
}

func printRecoveryStatus(opStore *datastore.OperationStore, chain string) error {
	st, err := opStore.RecoveryStatus(chain)
	if errors.Is(err, datastore.ErrNotFound) {
		fmt.Printf("%s: no recovery manifest (coverage unknown)\n", chain)
		return nil
	}
	if err != nil {
		return err
	}
	if recoverFlags.json {
		return json.NewEncoder(os.Stdout).Encode(st)
	}
	m := st.Manifest
	live := "none"
	if st.LiveCheckpoint != nil {
		live = strconv.FormatUint(*st.LiveCheckpoint, 10)
	}
	fmt.Printf("%s: start %d, target %d (%s), committed through %d, live checkpoint %s\n", chain, m.StartHeight, m.TargetHeight, m.TargetHash, int64(m.NextHeight)-1, live)
	fmt.Printf("  ranges %d, observations %d, recovered operations %d, claim-trace gap blocks %d, unresolved N3 claims %d\n",
		m.Batches, m.Observations, m.RecoveredOperations, st.ClaimTraceGapBlocks, st.UnresolvedClaims)
	if st.ClaimsDeferredBlocks > 0 || m.Claims != nil {
		fmt.Printf("  claims deferred over %d blocks", st.ClaimsDeferredBlocks)
		if c := m.Claims; c != nil {
			fmt.Printf("; last reconciliation through %d: %d identified, %d outstanding, %d unresolved, gaps cleared %t", c.Through, c.Claims, c.Outstanding, c.Unresolved, c.GapsCleared)
		}
		fmt.Println()
	}
	if m.TrustedIndex != "" {
		fmt.Printf("  trusted index: %s; %d blocks covered through it, not fetched\n", m.TrustedIndex, m.LocatedBlocks)
	}
	complete := strconv.FormatBool(st.Complete)
	if st.Complete && st.IndexTrusted {
		complete += " (index-trusted)"
	}
	fmt.Printf("  contiguous %t, complete %s, handoff ready %t\n", st.Contiguous, complete, st.HandoffReady)
	for _, reason := range st.Blocking {
		fmt.Printf("  blocking: %s\n", reason)
	}
	return nil
}
