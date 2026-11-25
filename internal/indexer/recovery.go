package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/bane-labs/bridge-indexer-backend/pkg/n3"
	"github.com/bane-labs/bridge-indexer-backend/pkg/neox"
)

// recordingStorer captures the live event processors' direct legacy writes
// instead of performing them, so nothing bypasses the atomic recovery commit.
// Native and token completions are derived from lifecycle facts, which map
// to them one-to-one; message deposit completions (a NeoToEvmRootUpdate
// completes only its own nonce) are kept and replayed as legacy completions.
type recordingStorer struct {
	chain       string
	completions []datastore.LifecycleObservation
}

func (r *recordingStorer) UpdateOperationCompletion(bt types.BridgeType, dir types.Direction, nonce uint64, destBlock uint64, destTx string, at time.Time) error {
	if bt == types.BridgeTypeMessage && dir == types.DirectionDeposit {
		r.completions = append(r.completions, datastore.LifecycleObservation{
			Identity: datastore.OperationIdentity{BridgeType: bt, Direction: dir, Nonce: nonce},
			Evidence: datastore.LifecycleEvidence{Chain: r.chain, BlockHeight: destBlock, TxHash: destTx, ObservedAt: at},
		})
	}
	return nil
}
func (r *recordingStorer) UpdateTokenOperationCompletionByDestToken(types.Direction, uint64, string, uint64, string, time.Time) error {
	return nil
}

func (r *recordingStorer) take() []datastore.LifecycleObservation {
	out := r.completions
	r.completions = nil
	return out
}

// Recoverer replays one chain's historical blocks through the live decoders
// and commits each bounded range with datastore.CommitRecoveryRange.
type Recoverer struct {
	chain     string
	opStore   *datastore.OperationStore
	contracts []string
	logger    *slog.Logger
	neox      *NeoXIndexer
	n3        *N3Indexer
	n3Workers int
	recorded  *recordingStorer
	filter    *n3.EventFilter // optional N3 locator, see UseEventFilter
}

// EventFilterIndex names the AxLabs neo-event-filter-plugin as a manifest's
// trusted index. Only its live-indexed window is ever used.
const EventFilterIndex = "axlabs/neo-event-filter-plugin live index"

// ScanClaims makes a Neo X recovery fetch every block of each range and
// inspect its transactions (and traces, when available) for silent
// claimToken calls, as live indexing does, instead of deferring claims to
// ReconcileClaims. It is far slower: every block is fetched.
func (r *Recoverer) ScanClaims(scan bool) {
	if r.neox != nil {
		r.neox.deferClaims = !scan
	}
}

// UseEventFilter lets an N3 recovery whose manifest trusts EventFilterIndex
// fetch only the blocks the plugin lists inside its live-indexed window.
func (r *Recoverer) UseEventFilter(f *n3.EventFilter) { r.filter = f }

// RecoveryRunOptions bounds one invocation.
type RecoveryRunOptions struct {
	Batch     uint64 // blocks per committed range
	MaxBlocks uint64 // stop after this many blocks; 0 = through the target
	// MinLogsSpan is the smallest Neo X range an eth_getLogs failure shrinks
	// to (default 1000).
	MinLogsSpan uint64
	// LocatedBatch is the range size when blocks are located through the
	// event filter (default 10000).
	LocatedBatch uint64
	// StuckAfter is the live stuck threshold per bridge type. A recovered
	// operation without destination evidence older than it is stored as
	// legacy stuck; nil keeps every recovered operation pending.
	StuckAfter func(types.BridgeType) time.Duration
}

// NewNeoXRecoverer builds a Neo X recoverer. The indexer it wraps has no
// bridge-state store and no pause detector: all writes go through the commit.
// concurrency bounds parallel block fetches; use 1 for a shared endpoint.
func NewNeoXRecoverer(client *neox.Client, store *datastore.Store, opStore *datastore.OperationStore, tokenBridge, messageBridge string, concurrency int, logger *slog.Logger) *Recoverer {
	contracts := []string{tokenBridge, messageBridge}
	recorded := &recordingStorer{chain: chainNeoX}
	proc := NewNeoXEventProcessor(recorded, tokenBridge, messageBridge, logger)
	idx := NewNeoXIndexer(client, store, opStore, nil, proc, contracts, 0, 1, logger, nil)
	idx.blockFetchConcurrency = concurrency
	idx.headerFetchConcurrency = max(concurrency, 1)
	idx.deferClaims = true // event-only by default; see ScanClaims
	return &Recoverer{chain: chainNeoX, opStore: opStore, contracts: contracts, logger: logger, neox: idx, recorded: recorded}
}

// NewN3Recoverer builds an N3 recoverer with the live N3 block decoder.
func NewN3Recoverer(client *n3.Client, store *datastore.Store, opStore *datastore.OperationStore, tokenBridge, messageBridge string, concurrency int, logger *slog.Logger) (*Recoverer, error) {
	contracts := []string{tokenBridge, messageBridge}
	recorded := &recordingStorer{chain: chainN3}
	proc, err := NewN3EventProcessor(recorded, tokenBridge, messageBridge, logger)
	if err != nil {
		return nil, err
	}
	idx, err := NewN3Indexer(client, store, opStore, nil, proc, contracts, 0, concurrency, logger)
	if err != nil {
		return nil, err
	}
	return &Recoverer{chain: chainN3, opStore: opStore, contracts: contracts, logger: logger, n3: idx, n3Workers: idx.blockWorkerCount(), recorded: recorded}, nil
}

// Chain returns the chain this recoverer replays.
func (r *Recoverer) Chain() string { return r.chain }

// head returns the chain's current height.
func (r *Recoverer) head(ctx context.Context) (uint64, error) {
	if r.neox != nil {
		return r.neox.client.BlockNumber(ctx)
	}
	count, err := r.n3.client.GetBlockCount(ctx)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, errors.New("empty N3 chain")
	}
	return uint64(count - 1), nil
}

// blockHash returns the hash of height on this chain and its parent's hash.
func (r *Recoverer) blockHash(ctx context.Context, height uint64) (string, string, error) {
	if r.neox != nil {
		h, err := r.neox.client.HeaderByNumber(ctx, new(big.Int).SetUint64(height))
		if err != nil {
			return "", "", err
		}
		return h.Hash().Hex(), h.ParentHash.Hex(), nil
	}
	if height > math.MaxUint32 {
		return "", "", fmt.Errorf("N3 height %d out of range", height)
	}
	b, err := r.n3.client.GetBlockByIndex(ctx, uint32(height))
	if err != nil {
		return "", "", err
	}
	return b.Hash().StringLE(), b.PrevHash.StringLE(), nil
}

// ResolveTarget validates an inclusive target on this chain: it must exist
// on this chain now, and its hash is captured from this chain's RPC. A height
// from the other chain is rejected unless it happens to exist here too, and
// then the captured hash still binds it to this chain.
func (r *Recoverer) ResolveTarget(ctx context.Context, height uint64) (string, error) {
	head, err := r.head(ctx)
	if err != nil {
		return "", fmt.Errorf("%s head: %w", r.chain, err)
	}
	if height > head {
		return "", fmt.Errorf("%s target %d is beyond the chain head %d; heights are per chain", r.chain, height, head)
	}
	hash, _, err := r.blockHash(ctx, height)
	return hash, err
}

// Head returns the chain's current height.
func (r *Recoverer) Head(ctx context.Context) (uint64, error) { return r.head(ctx) }

// Begin creates (or resumes) the manifest for [start, target] on this chain.
// trustIndex (N3 only) records EventFilterIndex as the trusted locator.
func (r *Recoverer) Begin(ctx context.Context, start, target uint64, trustIndex bool) error {
	if trustIndex && r.chain != chainN3 {
		return errors.New("the event filter index applies to neo_n3 only")
	}
	if start > target {
		return fmt.Errorf("start %d is after target %d", start, target)
	}
	hash, err := r.ResolveTarget(ctx, target)
	if err != nil {
		return err
	}
	index := ""
	if trustIndex {
		index = EventFilterIndex
	}
	return r.opStore.BeginRecovery(datastore.RecoveryManifest{
		Chain: r.chain, TrustedIndex: index, WatchedContracts: r.contracts, ImplementationRanges: r.implementations(),
		RequiredPaths: datastore.BaseRecoveryPaths(r.chain), StartHeight: start, TargetHeight: target,
		TargetHash: hash, NextHeight: start,
	})
}

// Extend raises the target, e.g. to the live checkpoint at cutover.
func (r *Recoverer) Extend(ctx context.Context, target uint64) error {
	hash, err := r.ResolveTarget(ctx, target)
	if err != nil {
		return err
	}
	return r.opStore.ExtendRecoveryTarget(r.chain, target, hash)
}

func (r *Recoverer) implementations() []string {
	if r.chain == chainNeoX {
		return []string{
			"claimToken implementations 1.0.0 0x1212100000000000000000000000000000000004, 2.0.0 0xb72c01360c20e03e315dccca46804735ab99d3d4, 3.0.0 " + verifiedClaimImplementation.Hex() + " (others stop the range)",
			"native bridge events Gas* before 1,772,382, Native* from it; MessageBridge events only from 4,721,793",
			"message Execution(uint256,(bool,bytes)); Execution(uint256,uint8) stops the range",
		}
	}
	return []string{"N3 bridge notifications decoded by contract hash and event name"}
}

// RecoveryProgress is reported after every committed range.
type RecoveryProgress struct {
	Chain      string
	From, To   uint64
	Target     uint64
	NextHeight uint64
	Inserted   int
	// WithoutDestination: inserted with no destination evidence yet (stored
	// as legacy stuck, lifecycle unknown).
	WithoutDestination int
	LegacyCompleted    int
	Observations       int
	TraceGapBlocks     uint64
	Unresolved         int
	Located            bool // blocks were selected through the event filter
	Elapsed            time.Duration
}

// Run replays from the manifest watermark toward its target, committing one
// range at a time. Cancellation or an error leaves the last committed range
// as the watermark; nothing partial is written.
func (r *Recoverer) Run(ctx context.Context, opts RecoveryRunOptions, report func(RecoveryProgress)) error {
	if opts.Batch == 0 {
		return errors.New("batch size must be positive")
	}
	m, err := r.opStore.GetRecoveryManifest(r.chain)
	if err != nil {
		return fmt.Errorf("recovery manifest %s: %w", r.chain, err)
	}
	if !slices.Equal(m.WatchedContracts, r.contracts) {
		return fmt.Errorf("configured contracts %v differ from manifest %v", r.contracts, m.WatchedContracts)
	}
	// The captured target binds the manifest to this chain and its history.
	hash, _, err := r.blockHash(ctx, m.TargetHeight)
	if err != nil {
		return fmt.Errorf("verify %s target %d: %w", r.chain, m.TargetHeight, err)
	}
	if hash != m.TargetHash {
		return fmt.Errorf("%s target %d hash %s differs from manifest %s: wrong chain or endpoint", r.chain, m.TargetHeight, hash, m.TargetHash)
	}
	useIndex := m.TrustedIndex == EventFilterIndex && r.n3 != nil
	if useIndex && r.filter == nil {
		r.logger.Warn("Manifest trusts the N3 event filter, but no endpoint serves it; scanning every block")
		useIndex = false
	}
	if useIndex {
		if err := r.filter.Verify(ctx, r.contracts); err != nil {
			r.logger.Warn("N3 event filter failed its positive control; scanning every block", "error", err)
			useIndex = false
		}
	}
	if opts.LocatedBatch == 0 {
		opts.LocatedBatch = 10000
	}
	var done uint64
	start := time.Now()
	// Event-only Neo X ranges adapt to the RPC: an eth_getLogs failure halves
	// the range (the attempt commits nothing), successes grow it back.
	logsSpan := opts.Batch
	if opts.MinLogsSpan == 0 {
		opts.MinLogsSpan = 1000
	}
	for m.NextHeight <= m.TargetHeight && (opts.MaxBlocks == 0 || done < opts.MaxBlocks) {
		if err := ctx.Err(); err != nil {
			return err
		}
		from := m.NextHeight
		span := min(opts.Batch, m.TargetHeight-from+1) // no overflow: from <= target
		if r.neox != nil && r.neox.deferClaims {
			span = min(span, logsSpan)
		}
		limit := m.TargetHeight
		if opts.MaxBlocks > 0 {
			span = min(span, opts.MaxBlocks-done)
			limit = min(limit, from+(opts.MaxBlocks-done)-1)
		}
		var located []uint32
		if useIndex {
			if heights, n, ok := r.locate(ctx, from, limit, opts.LocatedBatch); ok {
				span, located = n, heights
			} else if n > 0 {
				span = min(span, n) // full scan up to the index's live window
			}
		}
		to := from + span - 1
		var rng datastore.RecoveryRange
		var batch datastore.RecoveryBatch
		r.recorded.take() // N3 never completes message deposits; keep it empty
		if located != nil {
			rng, batch, err = r.replayLocated(ctx, from, to, located)
		} else {
			rng, batch, err = r.replay(ctx, from, to)
		}
		var logsErr filterLogsError
		if errors.As(err, &logsErr) && r.neox != nil && r.neox.deferClaims && span > opts.MinLogsSpan {
			logsSpan = max(span/2, opts.MinLogsSpan)
			r.logger.Warn("eth_getLogs failed; retrying a smaller range", "from", from, "to", to, "next_span", logsSpan, "error", err)
			continue
		}
		if err != nil {
			return fmt.Errorf("%s recovery %d-%d: %w", r.chain, from, to, err)
		}
		if r.neox != nil && r.neox.deferClaims && logsSpan < opts.Batch {
			logsSpan = min(logsSpan*2, opts.Batch)
		}
		if opts.StuckAfter != nil {
			now := time.Now()
			batch.StaleBefore = map[types.BridgeType]time.Time{}
			for _, bt := range []types.BridgeType{types.BridgeTypeNative, types.BridgeTypeToken, types.BridgeTypeMessage} {
				batch.StaleBefore[bt] = now.Add(-opts.StuckAfter(bt))
			}
		}
		res, err := r.opStore.CommitRecoveryRange(rng, batch)
		if err != nil {
			return fmt.Errorf("%s recovery commit %d-%d: %w", r.chain, from, to, err)
		}
		done += to - from + 1
		if m, err = r.opStore.GetRecoveryManifest(r.chain); err != nil {
			return err
		}
		if report != nil {
			p := RecoveryProgress{Chain: r.chain, From: from, To: to, Target: m.TargetHeight, NextHeight: m.NextHeight,
				Inserted: res.InsertedOperations, WithoutDestination: res.WithoutDestination, LegacyCompleted: res.LegacyCompleted, Observations: len(batch.Observations),
				Unresolved: len(batch.UnresolvedClaims), Located: rng.LocatedBy != "", Elapsed: time.Since(start)}
			for _, g := range batch.TraceGaps {
				p.TraceGapBlocks += g.ToHeight - g.FromHeight + 1
			}
			report(p)
		}
	}
	return nil
}

// locate asks the event filter for the candidate blocks of up to limit
// blocks from from, inside its live-indexed window and at most through
// target. ok reports a located range of n blocks. Otherwise n > 0 means the
// window starts n blocks after from (scan those fully), and n == 0 means the
// index cannot help now. Failures fall back to a full scan.
func (r *Recoverer) locate(ctx context.Context, from, target, limit uint64) (heights []uint32, n uint64, ok bool) {
	st, err := r.filter.Status(ctx)
	if err != nil {
		r.logger.Warn("N3 event filter unavailable; scanning every block", "error", err)
		return nil, 0, false
	}
	if st.LiveIndexedFrom == nil {
		return nil, 0, false
	}
	live, last := uint64(*st.LiveIndexedFrom), uint64(st.LastIndexedBlock)
	if from < live {
		return nil, live - from, false
	}
	if from > last || from == 0 || last > math.MaxUint32 {
		return nil, 0, false
	}
	to := min(target, last, from+limit-1)
	heights, err = r.filter.BlocksWithEvents(ctx, r.contracts, uint32(from), uint32(to))
	if err != nil {
		r.logger.Warn("N3 event filter query failed; scanning every block", "error", err)
		return nil, 0, false
	}
	// The window must still cover the range after the query.
	if after, err := r.filter.Status(ctx); err != nil || !after.LiveCovers(uint32(from), uint32(to)) {
		r.logger.Warn("N3 event filter window changed during the query; scanning every block")
		return nil, 0, false
	}
	// The plugin's node must be on the chain being recovered.
	ours, _, err := r.blockHash(ctx, to)
	if err != nil {
		r.logger.Warn("N3 block hash unavailable; scanning every block", "error", err)
		return nil, 0, false
	}
	if theirs, err := r.filter.BlockHash(ctx, uint32(to)); err != nil || !strings.EqualFold(theirs, strings.TrimPrefix(ours, "0x")) {
		r.logger.Warn("N3 event filter node disagrees on the chain; scanning every block", "height", to, "error", err)
		return nil, 0, false
	}
	if heights == nil {
		heights = []uint32{}
	}
	return heights, to - from + 1, true
}

// replayLocated decodes only the listed blocks of [from, to] with the live
// N3 block decoder; boundary hashes are still fetched for contiguity.
func (r *Recoverer) replayLocated(ctx context.Context, from, to uint64, heights []uint32) (datastore.RecoveryRange, datastore.RecoveryBatch, error) {
	results := make([]n3BlockResult, len(heights))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range max(r.n3Workers, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				state, ops, observations, err := r.n3.processBlockWithLifecycle(ctx, heights[i])
				results[i] = n3BlockResult{height: heights[i], state: state, ops: ops, observations: observations, err: err}
			}
		}()
	}
	for i := range heights {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	var ops []*types.BridgeOperation
	var observations []datastore.LifecycleObservation
	for _, res := range results {
		if res.err != nil {
			return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, res.err
		}
		ops = append(ops, res.ops...)
		observations = append(observations, res.observations...)
	}
	resolved, unresolved, err := r.n3.resolveN3TokenClaims(observations)
	if err != nil {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
	}
	_, parent, err := r.blockHash(ctx, from)
	if err != nil {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
	}
	toHash, _, err := r.blockHash(ctx, to)
	if err != nil {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
	}
	return datastore.RecoveryRange{Chain: r.chain, From: from, To: to, FromParentHash: parent, ToHash: toHash, VerifiedPaths: datastore.BaseRecoveryPaths(r.chain), LocatedBy: EventFilterIndex},
		datastore.RecoveryBatch{Observations: resolved, Operations: ops, UnresolvedClaims: unresolved}, nil
}

func (r *Recoverer) replay(ctx context.Context, from, to uint64) (datastore.RecoveryRange, datastore.RecoveryBatch, error) {
	paths := datastore.BaseRecoveryPaths(r.chain)
	if r.neox != nil {
		r.recorded.take()
		state, ops, observations, gaps, err := r.neox.processBlockRangeWithResult(ctx, from, to)
		completions := r.recorded.take()
		if err != nil {
			return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
		}
		_, parent, err := r.blockHash(ctx, from)
		if err != nil {
			return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
		}
		if !r.neox.deferClaims {
			paths = append(paths, "transactions", "claims")
			if len(gaps) == 0 {
				paths = append(paths, "traces")
			}
		}
		return datastore.RecoveryRange{Chain: r.chain, From: from, To: to, FromParentHash: parent, ToHash: state.LastBlockHash, VerifiedPaths: paths},
			datastore.RecoveryBatch{Observations: observations, Operations: ops, TraceGaps: gaps, LegacyCompletions: completions}, nil
	}
	if to > math.MaxUint32 {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, fmt.Errorf("N3 height %d out of range", to)
	}
	results, err := r.n3.processBlockBatchWithResult(ctx, uint32(from), uint32(to), r.n3Workers)
	if err != nil {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
	}
	var ops []*types.BridgeOperation
	var observations []datastore.LifecycleObservation
	for _, res := range results {
		if res.err != nil {
			return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, res.err
		}
		ops = append(ops, res.ops...)
		observations = append(observations, res.observations...)
	}
	resolved, unresolved, err := r.n3.resolveN3TokenClaims(observations)
	if err != nil {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
	}
	_, parent, err := r.blockHash(ctx, from)
	if err != nil {
		return datastore.RecoveryRange{}, datastore.RecoveryBatch{}, err
	}
	return datastore.RecoveryRange{Chain: r.chain, From: from, To: to, FromParentHash: parent, ToHash: results[len(results)-1].state.LastBlockHash, VerifiedPaths: paths},
		datastore.RecoveryBatch{Observations: resolved, Operations: ops, UnresolvedClaims: unresolved}, nil
}
