package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

var (
	statusAPIBaseURL  string
	statusRefreshSecs int
)

type chainStatus struct {
	name          string
	indexedHeight uint64
	tipHeight     *uint64
	gap           *uint64
	err           error
}

type statusAPIClient struct {
	baseURL string
	http    *http.Client
}

type indexerStateResponse struct {
	NeoN3 *types.IndexerState `json:"neo_n3"`
	NeoX  *types.IndexerState `json:"neo_x"`
}

type operationsResponse struct {
	Total int `json:"total"`
}

type operationsListResponse struct {
	Operations []types.BridgeOperation `json:"operations"`
	Total      int                     `json:"total"`
}

type syncSummary struct {
	total        int
	synced       int
	sourceAhead  int
	rootMismatch int
	unknown      int
	topAhead     []types.DirectionalInstanceSync
	mismatches   []types.DirectionalInstanceSync
	allInstances []types.DirectionalInstanceSync
}

type bridgePairState struct {
	bridgeType  types.BridgeType
	n3Token     *string
	neoXToken   *string
	deposit     *types.DirectionalInstanceSync
	withdrawal  *types.DirectionalInstanceSync
	displayName string
}

type statusSample struct {
	at      time.Time
	indexed uint64
}

type statusTrend struct {
	samples map[string]statusSample
}

type tokenSymbolResolver struct {
	http          *http.Client
	n3Endpoints   []string
	neoxEndpoints []string
	cache         map[string]string
}

func runStatus(cmd *cobra.Command, args []string) error {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	v := viper.New()
	cfg, err := loadConfig(v, logger)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	apiClient := &statusAPIClient{
		baseURL: statusAPIBaseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}

	n3Client, n3Err := n3.NewClient(ctx, cfg.N3RPCEndpoints, 3, logger, cfg.N3RPCCallInterval)
	if n3Client != nil {
		defer n3Client.Close()
	}
	neoxClient, neoxErr := neox.NewClient(ctx, cfg.NeoXRPCEndpoints, logger, cfg.NeoXRPCCallInterval)
	if neoxClient != nil {
		defer neoxClient.Close()
	}

	refresh := 3 * time.Second
	if statusRefreshSecs > 0 {
		refresh = time.Duration(statusRefreshSecs) * time.Second
	}
	ticker := time.NewTicker(refresh)
	defer ticker.Stop()
	trend := &statusTrend{samples: make(map[string]statusSample)}
	symbolResolver := &tokenSymbolResolver{
		http:          &http.Client{Timeout: 6 * time.Second},
		n3Endpoints:   cfg.N3RPCEndpoints,
		neoxEndpoints: cfg.NeoXRPCEndpoints,
		cache:         make(map[string]string),
	}

	for {
		if err := renderStatusSnapshot(ctx, apiClient, n3Client, neoxClient, n3Err, neoxErr, trend, symbolResolver); err != nil {
			fmt.Fprintf(os.Stderr, "status render error: %v\n", err)
		}

		select {
		case <-ctx.Done():
			fmt.Println("\nstatus watcher stopped")
			return nil
		case <-ticker.C:
		}
	}
}

func renderStatusSnapshot(
	ctx context.Context,
	apiClient *statusAPIClient,
	n3Client *n3.Client,
	neoxClient *neox.Client,
	n3InitErr error,
	neoxInitErr error,
	trend *statusTrend,
	symbolResolver *tokenSymbolResolver,
) error {
	n3Chain := chainStatus{name: "neo_n3"}
	neoxChain := chainStatus{name: "neo_x"}

	n3State, neoxState, apiErr := apiClient.getIndexerState(ctx)
	if apiErr != nil {
		n3Chain.err = fmt.Errorf("api: %w", apiErr)
		neoxChain.err = fmt.Errorf("api: %w", apiErr)
	}
	if n3State != nil {
		n3Chain.indexedHeight = n3State.LastBlockHeight
	}
	if neoxState != nil {
		neoxChain.indexedHeight = neoxState.LastBlockHeight
	}

	if n3InitErr != nil {
		n3Chain.err = n3InitErr
	} else if n3Client != nil {
		n3Tip, tipErr := n3Client.GetBlockCount(ctx)
		if tipErr != nil {
			n3Chain.err = tipErr
		} else if n3Tip > 0 {
			v := uint64(n3Tip - 1)
			n3Chain.tipHeight = &v
			if v >= n3Chain.indexedHeight {
				g := v - n3Chain.indexedHeight
				n3Chain.gap = &g
			}
		}
	}

	if neoxInitErr != nil {
		neoxChain.err = neoxInitErr
	} else if neoxClient != nil {
		neoxTip, tipErr := neoxClient.BlockNumber(ctx)
		if tipErr != nil {
			neoxChain.err = tipErr
		} else {
			neoxChain.tipHeight = &neoxTip
			if neoxTip >= neoxChain.indexedHeight {
				g := neoxTip - neoxChain.indexedHeight
				neoxChain.gap = &g
			}
		}
	}
	now := time.Now()
	n3BPS, n3ETA := trend.estimateSyncRateAndETA(n3Chain, now)
	neoxBPS, neoxETA := trend.estimateSyncRateAndETA(neoxChain, now)

	fmt.Print("\033[H\033[2J")
	fmt.Println("Bridge Indexer Live Status")
	fmt.Printf("Time: %s\n", now.Format(time.RFC3339))
	fmt.Printf("API:  %s\n", apiClient.baseURL)
	fmt.Println(strings.Repeat("=", 78))
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CHAIN\tINDEXED\tCHAIN_TIP\tGAP\tSYNC\tBPS\tETA_TO_SYNC\tNOTES")
	fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
		n3Chain.name, n3Chain.indexedHeight, uint64OrDash(n3Chain.tipHeight), uint64OrDash(n3Chain.gap), syncPct(n3Chain.indexedHeight, n3Chain.tipHeight), bpsOrDash(n3BPS), n3ETA, errOrOK(n3Chain.err))
	fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
		neoxChain.name, neoxChain.indexedHeight, uint64OrDash(neoxChain.tipHeight), uint64OrDash(neoxChain.gap), syncPct(neoxChain.indexedHeight, neoxChain.tipHeight), bpsOrDash(neoxBPS), neoxETA, errOrOK(neoxChain.err))
	_ = w.Flush()

	fmt.Println()
	fmt.Println("Operations By Bridge")
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BRIDGE\tPENDING\tCOMPLETED\tSTUCK\tTOTAL")
	totalPending, totalCompleted, totalStuck := 0, 0, 0
	for _, bt := range []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage} {
		p, pErr := apiClient.getOperationCount(ctx, bt, types.StatusPending)
		c, cErr := apiClient.getOperationCount(ctx, bt, types.StatusCompleted)
		s, sErr := apiClient.getOperationCount(ctx, bt, types.StatusStuck)
		if pErr != nil || cErr != nil || sErr != nil {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				bt, intOrErr(p, pErr), intOrErr(c, cErr), intOrErr(s, sErr), "api error")
			continue
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n", bt, p, c, s, p+c+s)
		totalPending += p
		totalCompleted += c
		totalStuck += s
	}
	fmt.Fprintf(w, "all\t%d\t%d\t%d\t%d\n", totalPending, totalCompleted, totalStuck, totalPending+totalCompleted+totalStuck)
	_ = w.Flush()

	fmt.Println()
	printStuckMessageOverview(ctx, apiClient, now)

	if overall, err := apiClient.getOverallHealth(ctx); err == nil && overall != nil {
		fmt.Printf("Overall Health: %s\n", overall.Status)
		fmt.Println()
	}

	syncs, syncErr := apiClient.getSyncInstances(ctx)
	if syncErr != nil {
		fmt.Printf("Directional Sync: unavailable (%v)\n", syncErr)
		fmt.Println()
	} else {
		summary := summarizeSync(syncs)
		fmt.Println("Directional Sync Summary")
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "TOTAL\tSYNCED\tSOURCE_AHEAD\tROOT_MISMATCH\tUNKNOWN")
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\n", summary.total, summary.synced, summary.sourceAhead, summary.rootMismatch, summary.unknown)
		_ = w.Flush()

		if len(summary.topAhead) > 0 {
			fmt.Println()
			fmt.Println("Top Source-Ahead Instances (by delta nonce)")
			w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "INSTANCE_ID\tDELTA\tSRC_NONCE\tDST_NONCE\tSRC_ROOT\tDST_ROOT\tREASON")
			for _, it := range summary.topAhead {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					it.ID, deltaOrDash(it.DeltaNonce),
					uint64PtrOrDash(it.SrcNonce), uint64PtrOrDash(it.DstNonce),
					truncRoot(it.SrcRoot), truncRoot(it.DstRoot),
					it.Reason)
			}
			_ = w.Flush()
		}

		if len(summary.mismatches) > 0 {
			fmt.Println()
			fmt.Println("Root Mismatches")
			w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "INSTANCE_ID\tDELTA\tSRC_NONCE\tDST_NONCE\tSRC_ROOT\tDST_ROOT\tREASON")
			for _, it := range summary.mismatches {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					it.ID, deltaOrDash(it.DeltaNonce),
					uint64PtrOrDash(it.SrcNonce), uint64PtrOrDash(it.DstNonce),
					truncRoot(it.SrcRoot), truncRoot(it.DstRoot),
					it.Reason)
			}
			_ = w.Flush()
		}

		bridgeStates := buildBridgePairStates(summary.allInstances)
		if len(bridgeStates) > 0 {
			enrichBridgePairDisplayNames(ctx, symbolResolver, bridgeStates)
			fmt.Println()
			fmt.Println("Bridge State by Bridge")
			for _, bs := range bridgeStates {
				fmt.Println()
				printBridgeStateTable(bs)
			}
		}
		fmt.Println()
	}

	fmt.Println("Press Ctrl+C to exit.")
	return nil
}

const stuckMessageOverviewLimit = 50

func printStuckMessageOverview(ctx context.Context, apiClient *statusAPIClient, now time.Time) {
	q := url.Values{}
	q.Set("bridge_type", string(types.BridgeTypeMessage))
	q.Set("status", string(types.StatusStuck))
	q.Set("limit", strconv.Itoa(stuckMessageOverviewLimit))

	ops, total, err := apiClient.getOperations(ctx, q)
	fmt.Println("Stuck Message Operations")
	if err != nil {
		fmt.Printf("  (unavailable: %v)\n\n", err)
		return
	}
	if total == 0 {
		fmt.Println("  (none)")
		fmt.Println()
		return
	}
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].Timestamp.Before(ops[j].Timestamp)
	})
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DIRECTION\tNONCE\tROUTE\tAGE\tSOURCE_TX")
	for _, op := range ops {
		route := op.SourceChain + "→" + op.DestinationChain
		age := formatOpAge(now.Sub(op.Timestamp))
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\n",
			op.Direction, op.Nonce, route, age, truncTxHash(op.SourceTxHash))
	}
	_ = w.Flush()
	if total > len(ops) {
		fmt.Printf("  (showing %d of %d stuck)\n", len(ops), total)
	}
	fmt.Println()
}

func truncTxHash(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "-"
	}
	if len(s) <= 22 {
		return s
	}
	return s[:10] + ".." + s[len(s)-6:]
}

func formatOpAge(d time.Duration) string {
	if d < time.Minute {
		return "<1m"
	}
	d = d.Round(time.Minute)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h == 0 {
		return fmt.Sprintf("%dm", m)
	}
	if h > 48 {
		return fmt.Sprintf("%dd", h/24)
	}
	return fmt.Sprintf("%dh%02dm", h, m)
}

func uint64OrDash(v *uint64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *v)
}

func errOrOK(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

func intOrErr(v int, err error) string {
	if err != nil {
		return "err"
	}
	return strconv.Itoa(v)
}

func deltaOrDash(v *int64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *v)
}

func uint64PtrOrDash(v *uint64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *v)
}

func truncRoot(v *string) string {
	if v == nil {
		return "-"
	}
	s := *v
	if len(s) > 18 {
		return s[:10] + ".." + s[len(s)-6:]
	}
	return s
}

func syncPct(indexed uint64, tip *uint64) string {
	if tip == nil || *tip == 0 {
		return "-"
	}
	if indexed >= *tip {
		return "100.0%"
	}
	return fmt.Sprintf("%.1f%%", (float64(indexed)/float64(*tip))*100)
}

func (t *statusTrend) estimateSyncRateAndETA(chain chainStatus, now time.Time) (float64, string) {
	prev, ok := t.samples[chain.name]
	t.samples[chain.name] = statusSample{at: now, indexed: chain.indexedHeight}

	if !ok || chain.gap == nil || chain.tipHeight == nil {
		return 0, "-"
	}

	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return 0, "-"
	}
	progressed := int64(chain.indexedHeight) - int64(prev.indexed)
	if progressed <= 0 {
		return 0, "-"
	}

	rate := float64(progressed) / elapsed // blocks/sec
	if rate <= 0 {
		return 0, "-"
	}

	etaSeconds := float64(*chain.gap) / rate
	if etaSeconds < 0 {
		return rate, "-"
	}

	return rate, formatETADuration(time.Duration(etaSeconds * float64(time.Second)))
}

func bpsOrDash(rate float64) string {
	if rate <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", rate)
}

func formatETADuration(d time.Duration) string {
	if d < time.Minute {
		return "<1m"
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h == 0 {
		return fmt.Sprintf("%dm", m)
	}
	if h > 48 {
		return fmt.Sprintf("%dd", h/24)
	}
	return fmt.Sprintf("%dh%02dm", h, m)
}

func (c *statusAPIClient) getIndexerState(ctx context.Context) (*types.IndexerState, *types.IndexerState, error) {
	var resp indexerStateResponse
	if err := c.getJSON(ctx, "/indexer/state", nil, &resp); err != nil {
		return nil, nil, err
	}
	return resp.NeoN3, resp.NeoX, nil
}

func (c *statusAPIClient) getOperationCount(ctx context.Context, bridgeType types.BridgeType, status types.OperationStatus) (int, error) {
	q := url.Values{}
	q.Set("bridge_type", string(bridgeType))
	q.Set("status", string(status))
	q.Set("limit", "1")

	var resp operationsResponse
	if err := c.getJSON(ctx, "/operations", q, &resp); err != nil {
		return 0, err
	}
	return resp.Total, nil
}

func (c *statusAPIClient) getOperations(ctx context.Context, q url.Values) ([]types.BridgeOperation, int, error) {
	var resp operationsListResponse
	if err := c.getJSON(ctx, "/operations", q, &resp); err != nil {
		return nil, 0, err
	}
	return resp.Operations, resp.Total, nil
}

func (c *statusAPIClient) getSyncInstances(ctx context.Context) ([]types.DirectionalInstanceSync, error) {
	var resp []types.DirectionalInstanceSync
	if err := c.getJSON(ctx, "/sync/instances", nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *statusAPIClient) getOverallHealth(ctx context.Context) (*types.OverallHealth, error) {
	var resp types.OverallHealth
	if err := c.getJSON(ctx, "/health", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func summarizeSync(items []types.DirectionalInstanceSync) syncSummary {
	out := syncSummary{total: len(items), allInstances: items}
	for _, it := range items {
		switch it.Status {
		case types.DirectionalSyncStatusSynced:
			out.synced++
		case types.DirectionalSyncStatusSourceAhead:
			out.sourceAhead++
			out.topAhead = append(out.topAhead, it)
		case types.DirectionalSyncStatusRootMismatch:
			out.rootMismatch++
			out.mismatches = append(out.mismatches, it)
		default:
			out.unknown++
		}
	}
	sort.Slice(out.topAhead, func(i, j int) bool {
		di, dj := int64(-1), int64(-1)
		if out.topAhead[i].DeltaNonce != nil {
			di = *out.topAhead[i].DeltaNonce
		}
		if out.topAhead[j].DeltaNonce != nil {
			dj = *out.topAhead[j].DeltaNonce
		}
		if di == dj {
			return out.topAhead[i].ID < out.topAhead[j].ID
		}
		return di > dj
	})
	if len(out.topAhead) > 5 {
		out.topAhead = out.topAhead[:5]
	}
	sort.Slice(out.mismatches, func(i, j int) bool {
		return out.mismatches[i].ID < out.mismatches[j].ID
	})
	return out
}

func buildBridgePairStates(items []types.DirectionalInstanceSync) []bridgePairState {
	byKey := make(map[string]*bridgePairState)
	order := make([]string, 0)

	for i := range items {
		it := items[i]
		key, n3Token, neoXToken := bridgePairKey(it)
		if key == "" {
			continue
		}

		bs, ok := byKey[key]
		if !ok {
			bs = &bridgePairState{
				bridgeType:  it.BridgeType,
				n3Token:     n3Token,
				neoXToken:   neoXToken,
				displayName: bridgePairDisplayName(it.BridgeType, n3Token, neoXToken),
			}
			byKey[key] = bs
			order = append(order, key)
		}

		switch {
		case it.SrcChain == "neo_n3" && it.DstChain == "neo_x":
			itCopy := it
			bs.deposit = &itCopy
		case it.SrcChain == "neo_x" && it.DstChain == "neo_n3":
			itCopy := it
			bs.withdrawal = &itCopy
		}
	}

	out := make([]bridgePairState, 0, len(order))
	for _, key := range order {
		out = append(out, *byKey[key])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].bridgeType == out[j].bridgeType {
			return out[i].displayName < out[j].displayName
		}
		return out[i].bridgeType < out[j].bridgeType
	})
	return out
}

func bridgePairKey(it types.DirectionalInstanceSync) (string, *string, *string) {
	switch it.BridgeType {
	case types.BridgeTypeNative, types.BridgeTypeMessage:
		return string(it.BridgeType), nil, nil
	case types.BridgeTypeToken:
		var n3Token, neoXToken *string
		switch {
		case it.SrcChain == "neo_n3" && it.DstChain == "neo_x":
			n3Token = it.SrcToken
			neoXToken = it.DstToken
		case it.SrcChain == "neo_x" && it.DstChain == "neo_n3":
			n3Token = it.DstToken
			neoXToken = it.SrcToken
		default:
			return "", nil, nil
		}
		n3Norm := normalizeTokenDisplayAddr(strPtrOrUnknown(n3Token))
		neoXNorm := normalizeTokenDisplayAddr(strPtrOrUnknown(neoXToken))
		return fmt.Sprintf("%s|%s|%s", it.BridgeType, n3Norm, neoXNorm), n3Token, neoXToken
	default:
		return "", nil, nil
	}
}

func bridgePairDisplayName(bt types.BridgeType, n3Token, neoXToken *string) string {
	if bt != types.BridgeTypeToken {
		return fmt.Sprintf("%s bridge", bt)
	}
	return fmt.Sprintf("token bridge (n3=%s, neox=%s)", strPtrOrUnknown(n3Token), strPtrOrUnknown(neoXToken))
}

func strPtrOrUnknown(v *string) string {
	if v == nil || *v == "" {
		return "unknown"
	}
	return *v
}

func normalizeTokenDisplayAddr(v string) string {
	s := strings.TrimSpace(strings.ToLower(v))
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return "unknown"
	}
	return s
}

func (b bridgePairState) n3DepositNonce() string {
	if b.deposit == nil {
		return "-"
	}
	return uint64PtrOrDash(b.deposit.SrcNonce)
}

func (b bridgePairState) neoXDepositNonce() string {
	if b.deposit == nil {
		return "-"
	}
	return uint64PtrOrDash(b.deposit.DstNonce)
}

func (b bridgePairState) n3DepositRoot() string {
	if b.deposit == nil {
		return "-"
	}
	return rootOrDash(b.deposit.SrcRoot)
}

func (b bridgePairState) neoXDepositRoot() string {
	if b.deposit == nil {
		return "-"
	}
	return rootOrDash(b.deposit.DstRoot)
}

func (b bridgePairState) n3WithdrawalNonce() string {
	if b.withdrawal == nil {
		return "-"
	}
	return uint64PtrOrDash(b.withdrawal.DstNonce)
}

func (b bridgePairState) neoXWithdrawalNonce() string {
	if b.withdrawal == nil {
		return "-"
	}
	return uint64PtrOrDash(b.withdrawal.SrcNonce)
}

func (b bridgePairState) n3WithdrawalRoot() string {
	if b.withdrawal == nil {
		return "-"
	}
	return rootOrDash(b.withdrawal.DstRoot)
}

func (b bridgePairState) neoXWithdrawalRoot() string {
	if b.withdrawal == nil {
		return "-"
	}
	return rootOrDash(b.withdrawal.SrcRoot)
}

func (b bridgePairState) depositStatus() string {
	if b.deposit == nil {
		return "-"
	}
	return string(b.deposit.Status)
}

func (b bridgePairState) withdrawalStatus() string {
	if b.withdrawal == nil {
		return "-"
	}
	return string(b.withdrawal.Status)
}

func rootOrDash(v *string) string {
	if v == nil {
		return "-"
	}
	return *v
}

func printBridgeStateTable(bs bridgePairState) {
	rows := [][4]string{
		{"deposit nonce", bs.n3DepositNonce(), bs.neoXDepositNonce(), bs.depositStatus()},
		{"deposit root", bs.n3DepositRoot(), bs.neoXDepositRoot(), bs.depositStatus()},
		{"withdrawal nonce", bs.n3WithdrawalNonce(), bs.neoXWithdrawalNonce(), bs.withdrawalStatus()},
		{"withdrawal root", bs.n3WithdrawalRoot(), bs.neoXWithdrawalRoot(), bs.withdrawalStatus()},
	}

	headers := [4]string{"field", "neo_n3", "neo_x", "status"}
	widths := [4]int{len(headers[0]), len(headers[1]), len(headers[2]), len(headers[3])}
	for _, r := range rows {
		if len(r[0]) > widths[0] {
			widths[0] = len(r[0])
		}
		if len(r[1]) > widths[1] {
			widths[1] = len(r[1])
		}
		if len(r[2]) > widths[2] {
			widths[2] = len(r[2])
		}
		if len(r[3]) > widths[3] {
			widths[3] = len(r[3])
		}
	}
	titleWidth := len(bs.displayName)
	if titleWidth < widths[0]+widths[1]+widths[2]+widths[3]+13 {
		titleWidth = widths[0] + widths[1] + widths[2] + widths[3] + 13
	}

	fmt.Printf("+-%s-+\n", strings.Repeat("-", titleWidth))
	fmt.Printf("| %-*s |\n", titleWidth, bs.displayName)
	fmt.Printf("+-%s-+\n", strings.Repeat("-", titleWidth))
	fmt.Printf("| %-*s | %-*s | %-*s | %-*s |\n", widths[0], headers[0], widths[1], headers[1], widths[2], headers[2], widths[3], headers[3])
	fmt.Printf("+-%s-+-%s-+-%s-+-%s-+\n", strings.Repeat("-", widths[0]), strings.Repeat("-", widths[1]), strings.Repeat("-", widths[2]), strings.Repeat("-", widths[3]))
	for _, r := range rows {
		fmt.Printf("| %-*s | %-*s | %-*s | %-*s |\n", widths[0], r[0], widths[1], r[1], widths[2], r[2], widths[3], r[3])
	}
	fmt.Printf("+-%s-+-%s-+-%s-+-%s-+\n", strings.Repeat("-", widths[0]), strings.Repeat("-", widths[1]), strings.Repeat("-", widths[2]), strings.Repeat("-", widths[3]))
}

func enrichBridgePairDisplayNames(ctx context.Context, resolver *tokenSymbolResolver, states []bridgePairState) {
	if resolver == nil {
		return
	}
	for i := range states {
		if states[i].bridgeType != types.BridgeTypeToken {
			continue
		}
		n3Addr := strPtrOrUnknown(states[i].n3Token)
		neoXAddr := strPtrOrUnknown(states[i].neoXToken)

		n3Symbol := resolver.resolveN3Symbol(ctx, n3Addr)
		neoXSymbol := resolver.resolveNeoXSymbol(ctx, neoXAddr)

		n3Label := n3Addr
		if n3Symbol != "" {
			n3Label = fmt.Sprintf("%s:%s", n3Symbol, n3Addr)
		}
		neoXLabel := neoXAddr
		if neoXSymbol != "" {
			neoXLabel = fmt.Sprintf("%s:%s", neoXSymbol, neoXAddr)
		}
		states[i].displayName = fmt.Sprintf("token bridge (n3=%s, neox=%s)", n3Label, neoXLabel)
	}
}

func (r *tokenSymbolResolver) resolveN3Symbol(ctx context.Context, contract string) string {
	if contract == "" || contract == "unknown" {
		return ""
	}
	cacheKey := "n3|" + strings.ToLower(contract)
	if v, ok := r.cache[cacheKey]; ok {
		return v
	}
	for _, ep := range r.n3Endpoints {
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			continue
		}
		body := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "invokefunction",
			"params":  []interface{}{contract, "symbol"},
		}
		raw, err := r.rpcCall(ctx, ep, body)
		if err != nil {
			continue
		}
		s := parseN3Symbol(raw)
		if s != "" {
			r.cache[cacheKey] = s
			return s
		}
	}
	r.cache[cacheKey] = ""
	return ""
}

func (r *tokenSymbolResolver) resolveNeoXSymbol(ctx context.Context, contract string) string {
	if contract == "" || contract == "unknown" {
		return ""
	}
	cacheKey := "neox|" + strings.ToLower(contract)
	if v, ok := r.cache[cacheKey]; ok {
		return v
	}
	for _, ep := range r.neoxEndpoints {
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			continue
		}
		body := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "eth_call",
			"params": []interface{}{
				map[string]string{
					"to":   contract,
					"data": "0x95d89b41", // symbol()
				},
				"latest",
			},
		}
		raw, err := r.rpcCall(ctx, ep, body)
		if err != nil {
			continue
		}
		s := parseEthCallSymbol(raw)
		if s != "" {
			r.cache[cacheKey] = s
			return s
		}
	}
	r.cache[cacheKey] = ""
	return ""
}

func (r *tokenSymbolResolver) rpcCall(ctx context.Context, endpoint string, body map[string]interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(b)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("http %s", res.Status)
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return nil, err
	}
	if raw["error"] != nil {
		return nil, fmt.Errorf("rpc error")
	}
	return raw, nil
}

func parseN3Symbol(raw map[string]interface{}) string {
	result, ok := raw["result"].(map[string]interface{})
	if !ok {
		return ""
	}
	stack, ok := result["stack"].([]interface{})
	if !ok || len(stack) == 0 {
		return ""
	}
	item, ok := stack[0].(map[string]interface{})
	if !ok {
		return ""
	}
	typ, _ := item["type"].(string)
	val, _ := item["value"].(string)
	if val == "" {
		return ""
	}
	switch typ {
	case "String":
		return strings.TrimSpace(strings.TrimRight(val, "\x00"))
	case "ByteString", "Buffer", "ByteArray":
		if b, err := base64.StdEncoding.DecodeString(val); err == nil {
			return strings.TrimSpace(strings.TrimRight(string(b), "\x00"))
		}
		if b, err := hex.DecodeString(strings.TrimPrefix(val, "0x")); err == nil {
			return strings.TrimSpace(strings.TrimRight(string(b), "\x00"))
		}
	}
	return ""
}

func parseEthCallSymbol(raw map[string]interface{}) string {
	hexOut, ok := raw["result"].(string)
	if !ok || hexOut == "" || hexOut == "0x" {
		return ""
	}
	out, err := hex.DecodeString(strings.TrimPrefix(hexOut, "0x"))
	if err != nil || len(out) == 0 {
		return ""
	}

	// bytes32 response fallback
	if len(out) == 32 {
		return strings.TrimSpace(strings.TrimRight(string(out), "\x00"))
	}

	// ABI dynamic string encoding: offset(32) + len(32) + data
	if len(out) < 96 {
		return ""
	}
	dataLen := int(out[63])
	if dataLen <= 0 {
		return ""
	}
	start := 64
	end := start + dataLen
	if end > len(out) {
		return ""
	}
	return strings.TrimSpace(strings.TrimRight(string(out[start:end]), "\x00"))
}

func (c *statusAPIClient) getJSON(ctx context.Context, path string, q url.Values, dest interface{}) error {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return fmt.Errorf("invalid api url %q: %w", c.baseURL, err)
	}
	u.Path = path
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("http %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(dest)
}

func newStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Live status dashboard for indexer progress and operations",
		RunE:  runStatus,
	}
	cmd.Flags().StringVar(&statusAPIBaseURL, "api-url", "http://localhost:8080", "Base URL of running bridge indexer API")
	cmd.Flags().IntVar(&statusRefreshSecs, "refresh-seconds", 3, "Dashboard refresh interval in seconds")
	return cmd
}
