package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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

	for {
		if err := renderStatusSnapshot(ctx, apiClient, n3Client, neoxClient, n3Err, neoxErr); err != nil {
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

	fmt.Print("\033[H\033[2J")
	fmt.Printf("Bridge Indexer Live Status  |  %s\n\n", time.Now().Format(time.RFC3339))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CHAIN\tINDEXED\tCHAIN_TIP\tGAP\tNOTES")
	fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\n",
		n3Chain.name, n3Chain.indexedHeight, uint64OrDash(n3Chain.tipHeight), uint64OrDash(n3Chain.gap), errOrOK(n3Chain.err))
	fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\n",
		neoxChain.name, neoxChain.indexedHeight, uint64OrDash(neoxChain.tipHeight), uint64OrDash(neoxChain.gap), errOrOK(neoxChain.err))
	_ = w.Flush()

	fmt.Println()
	fmt.Println("Operations")
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BRIDGE\tPENDING\tCOMPLETED\tSTUCK\tTOTAL")
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
	}
	_ = w.Flush()

	fmt.Println()
	fmt.Println("Press Ctrl+C to exit.")
	return nil
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
