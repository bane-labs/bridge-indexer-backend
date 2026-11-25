package n3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"
)

// EventFilter queries the AxLabs neo-event-filter-plugin
// (github.com/AxLabs/neo-event-filter-plugin) on the one endpoint that serves
// it. It is a locator: it says which blocks hold notifications of a contract.
// The notifications themselves are always read from getapplicationlog, which
// records VM state and per-transaction positions the plugin does not.
type EventFilter struct {
	endpoint string
	http     *http.Client
}

// ErrEventFilterUnavailable: no configured endpoint serves the plugin.
var ErrEventFilterUnavailable = errors.New("no N3 endpoint serves geteventfilterstatus")

// EventFilterStatus is geteventfilterstatus. Blocks before LiveIndexedFrom
// were indexed by the plugin's historical replay, which re-executes old
// transactions against current state and is not a trustworthy locator.
type EventFilterStatus struct {
	ReplayedUpTo     *uint32 `json:"replayedUpTo"`
	LiveIndexedFrom  *uint32 `json:"liveIndexedFrom"`
	LastIndexedBlock uint32  `json:"lastIndexedBlock"`
	ReplayInProgress bool    `json:"replayInProgress"`
}

// LiveCovers reports whether [from, to] lies in the live-indexed window.
func (s EventFilterStatus) LiveCovers(from, to uint32) bool {
	return s.LiveIndexedFrom != nil && from >= *s.LiveIndexedFrom && to <= s.LastIndexedBlock && from <= to
}

type eventFilterLog struct {
	BlockIndex uint32  `json:"blockindex"`
	TxHash     *string `json:"txhash"`
}

// NewEventFilter probes endpoints in order and uses the first that answers
// geteventfilterstatus. Endpoints without the plugin (method not found,
// unreachable) are skipped; they remain usable for standard calls.
func NewEventFilter(ctx context.Context, endpoints []string, logger *slog.Logger) (*EventFilter, error) {
	for _, ep := range endpoints {
		f := &EventFilter{endpoint: ep, http: &http.Client{Timeout: 60 * time.Second}}
		st, err := f.Status(ctx)
		if err != nil {
			logger.Info("N3 endpoint does not serve the event filter plugin", "endpoint", ep, "error", err)
			continue
		}
		logger.Info("N3 event filter plugin found", "endpoint", ep, "live_indexed_from", st.LiveIndexedFrom, "last_indexed_block", st.LastIndexedBlock, "replay_in_progress", st.ReplayInProgress)
		return f, nil
	}
	return nil, ErrEventFilterUnavailable
}

// Endpoint returns the endpoint serving the plugin.
func (f *EventFilter) Endpoint() string { return f.endpoint }

func (f *EventFilter) call(ctx context.Context, method string, params []any, out any) error {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := f.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %s", method, res.Status)
	}
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("%s: %d %s", method, resp.Error.Code, resp.Error.Message)
	}
	return json.Unmarshal(resp.Result, out)
}

// Status returns geteventfilterstatus.
func (f *EventFilter) Status(ctx context.Context) (EventFilterStatus, error) {
	var st EventFilterStatus
	err := f.call(ctx, "geteventfilterstatus", []any{}, &st)
	return st, err
}

// BlockHash returns the hash of height on the plugin's own node (LE string,
// without prefix), so its index can be tied to the chain being recovered.
func (f *EventFilter) BlockHash(ctx context.Context, height uint32) (string, error) {
	var hash string
	if err := f.call(ctx, "getblockhash", []any{height}, &hash); err != nil {
		return "", err
	}
	return strings.TrimPrefix(strings.ToLower(hash), "0x"), nil
}

// Verify is a positive control: the plugin must list at least one event of
// the contracts inside its live window, and the listed transaction's real
// application log on the same node must hold a HALT notification from that
// contract at the listed height. A plugin that answers every query with an
// empty page (for example a non-positive result cap) or indexes another
// chain fails it.
func (f *EventFilter) Verify(ctx context.Context, contracts []string) error {
	st, err := f.Status(ctx)
	if err != nil {
		return err
	}
	if st.LiveIndexedFrom == nil || st.LastIndexedBlock == 0 || st.LastIndexedBlock < *st.LiveIndexedFrom {
		return errors.New("event filter has no live-indexed window")
	}
	for _, contract := range contracts {
		var logs []eventFilterLog
		if err := f.call(ctx, "geteventfilterlogs", []any{"0x" + contract, "", *st.LiveIndexedFrom, st.LastIndexedBlock, 1}, &logs); err != nil {
			return err
		}
		if len(logs) == 0 || logs[0].TxHash == nil {
			continue
		}
		var appLog struct {
			Executions []struct {
				VMState       string `json:"vmstate"`
				Notifications []struct {
					Contract string `json:"contract"`
				} `json:"notifications"`
			} `json:"executions"`
		}
		if err := f.call(ctx, "getapplicationlog", []any{*logs[0].TxHash}, &appLog); err != nil {
			return err
		}
		var height uint32
		if err := f.call(ctx, "gettransactionheight", []any{*logs[0].TxHash}, &height); err != nil {
			return err
		}
		if height != logs[0].BlockIndex {
			return fmt.Errorf("event filter lists %s at %d, the node at %d", *logs[0].TxHash, logs[0].BlockIndex, height)
		}
		for _, e := range appLog.Executions {
			for _, n := range e.Notifications {
				if e.VMState == "HALT" && strings.EqualFold(strings.TrimPrefix(n.Contract, "0x"), contract) {
					return nil
				}
			}
		}
		return fmt.Errorf("event filter lists %s, whose application log has no HALT notification from %s", *logs[0].TxHash, contract)
	}
	return errors.New("event filter lists no event of the bridge contracts in its live window")
}

// BlocksWithEvents returns the ascending heights in [from, to] holding at
// least one notification of any contract. contracts are hashes in their
// usual LE string form. Pages are requested until one comes back empty, so a
// server-side result cap cannot silently truncate the answer: a block cut off
// mid-page is already a candidate, and the next page starts after it.
func (f *EventFilter) BlocksWithEvents(ctx context.Context, contracts []string, from, to uint32) ([]uint32, error) {
	if to == 0 || to < from {
		// toBlock 0 means "latest" to the plugin; never send it.
		return nil, fmt.Errorf("event filter range %d-%d unsupported", from, to)
	}
	seen := map[uint32]bool{}
	var heights []uint32
	for _, contract := range contracts {
		cursor := from
		for cursor <= to {
			var logs []eventFilterLog
			if err := f.call(ctx, "geteventfilterlogs", []any{"0x" + contract, "", cursor, to, 1000}, &logs); err != nil {
				return nil, err
			}
			if len(logs) == 0 {
				break
			}
			last := cursor
			for _, l := range logs {
				if l.BlockIndex < cursor || l.BlockIndex > to {
					return nil, fmt.Errorf("event filter returned block %d outside %d-%d", l.BlockIndex, cursor, to)
				}
				if !seen[l.BlockIndex] {
					seen[l.BlockIndex] = true
					heights = append(heights, l.BlockIndex)
				}
				last = max(last, l.BlockIndex)
			}
			if last == to {
				break
			}
			cursor = last + 1
		}
	}
	slices.Sort(heights)
	return heights, nil
}
