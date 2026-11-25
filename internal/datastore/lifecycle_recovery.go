package datastore

import (
	"errors"
	"fmt"
	"slices"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const recoveryPrefix = "lifecycle_recovery:v1:"

// RecoveryTarget records one inclusive target of a recovery manifest. A
// manifest keeps every target it has had, so a cutover extension is auditable.
type RecoveryTarget struct {
	Height uint64    `json:"height"`
	Hash   string    `json:"hash"`
	SetAt  time.Time `json:"set_at"`
}

// RecoveryManifest describes a bounded replay on one destination chain, in
// that chain's own block heights. NextHeight is the first height not yet
// committed; it starts at StartHeight. No migration creates this record on an
// existing database: a missing manifest means coverage is unknown.
type RecoveryManifest struct {
	Chain                string           `json:"chain"`
	WatchedContracts     []string         `json:"watched_contracts"`
	ImplementationRanges []string         `json:"implementation_ranges"`
	RequiredPaths        []string         `json:"required_paths"` // see baseRecoveryPaths
	StartHeight          uint64           `json:"start_height"`
	TargetHeight         uint64           `json:"target_height"`
	TargetHash           string           `json:"target_hash"`
	Targets              []RecoveryTarget `json:"targets,omitempty"`
	NextHeight           uint64           `json:"next_height"`
	ContiguousHash       string           `json:"contiguous_hash,omitempty"`
	LastBatchFrom        uint64           `json:"last_batch_from,omitempty"`
	LastBatchDigest      string           `json:"last_batch_digest,omitempty"`
	Batches              uint64           `json:"batches"`
	Observations         uint64           `json:"observations"`
	RecoveredOperations  uint64           `json:"recovered_operations"`
}

// baseRecoveryPaths are the evidence paths every committed range must have
// verified. Neo X call traces are not among them: a block whose traces were
// unavailable is committed with a claim trace gap, which keeps the interval
// incomplete without stopping standard event recovery. Listing "traces" in a
// manifest makes them mandatory instead.
func baseRecoveryPaths(chain string) []string {
	if chain == "neo_x" {
		return []string{"blocks", "implementations", "logs", "transactions", "receipts", "claims"}
	}
	return []string{"blocks", "implementations", "notifications", "application_logs"}
}

// BaseRecoveryPaths returns the evidence paths a recovery of chain verifies.
func BaseRecoveryPaths(chain string) []string { return baseRecoveryPaths(chain) }

func validRecoveryChain(chain string) error {
	if chain != "neo_n3" && chain != "neo_x" {
		return fmt.Errorf("invalid recovery chain %q", chain)
	}
	return nil
}

func (m RecoveryManifest) valid() error {
	if err := validRecoveryChain(m.Chain); err != nil {
		return err
	}
	if len(m.WatchedContracts) == 0 || len(m.ImplementationRanges) == 0 || len(m.RequiredPaths) == 0 || m.TargetHash == "" || m.StartHeight > m.TargetHeight || m.TargetHeight == ^uint64(0) {
		return errors.New("recovery requires contracts, implementation ranges, evidence paths, and bounded target height/hash")
	}
	for _, path := range baseRecoveryPaths(m.Chain) {
		if !slices.Contains(m.RequiredPaths, path) {
			return fmt.Errorf("missing required recovery evidence path %q", path)
		}
	}
	if m.NextHeight < m.StartHeight || m.NextHeight > m.TargetHeight+1 {
		return errors.New("recovery watermark out of bounds")
	}
	return nil
}

// Contiguous reports that every height from StartHeight through the current
// target was committed and the last one matches the captured target hash. It
// says nothing about unresolved evidence; see RecoveryStatus.
func (m RecoveryManifest) Contiguous() bool {
	return m.valid() == nil && m.NextHeight == m.TargetHeight+1 && m.ContiguousHash == m.TargetHash
}

func (os *OperationStore) GetRecoveryManifest(chain string) (*RecoveryManifest, error) {
	if err := validRecoveryChain(chain); err != nil {
		return nil, err
	}
	var m RecoveryManifest
	if err := os.store.Get(recoveryPrefix+chain, &m); err != nil {
		return nil, err
	}
	if err := m.valid(); err != nil {
		return nil, err
	}
	return &m, nil
}

func readRecoveryManifest(txn *badger.Txn, chain string) (*RecoveryManifest, error) {
	var m RecoveryManifest
	if err := n3Read(txn, recoveryPrefix+chain, &m); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errors.New("recovery manifest missing")
		}
		return nil, err
	}
	if err := m.valid(); err != nil {
		return nil, err
	}
	return &m, nil
}

// BeginRecovery is idempotent for the same manifest, but refuses to replace
// existing bounds or reset an advanced watermark silently. A larger target
// for an existing manifest goes through ExtendRecoveryTarget.
func (os *OperationStore) BeginRecovery(m RecoveryManifest) error {
	if m.NextHeight != m.StartHeight || m.ContiguousHash != "" || m.LastBatchDigest != "" || m.Batches != 0 || len(m.Targets) > 0 {
		return errors.New("new recovery must start with an empty watermark")
	}
	if err := m.valid(); err != nil {
		return err
	}
	m.Targets = []RecoveryTarget{{Height: m.TargetHeight, Hash: m.TargetHash, SetAt: time.Now().UTC()}}
	return retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		key := recoveryPrefix + m.Chain
		var existing RecoveryManifest
		err := n3Read(txn, key, &existing)
		if err == nil {
			if !sameRecoveryBounds(existing, m) {
				return errors.New("recovery bounds changed; use extend for a later target, or an explicit reset and replay")
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		return n3Set(txn, key, m)
	})
}

func sameRecoveryBounds(a, b RecoveryManifest) bool {
	return a.Chain == b.Chain && a.StartHeight == b.StartHeight && a.TargetHeight == b.TargetHeight && a.TargetHash == b.TargetHash && slices.Equal(a.WatchedContracts, b.WatchedContracts) && slices.Equal(a.ImplementationRanges, b.ImplementationRanges) && slices.Equal(a.RequiredPaths, b.RequiredPaths)
}

// ExtendRecoveryTarget raises the inclusive target, keeping all committed
// progress. This is how replay catches up through blocks added after the
// first target was captured, such as those between a database copy and the
// production cutover. A target can never be lowered or re-hashed.
func (os *OperationStore) ExtendRecoveryTarget(chain string, height uint64, hash string) error {
	if hash == "" || height == ^uint64(0) {
		return errors.New("recovery target requires a height and hash")
	}
	return retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		m, err := readRecoveryManifest(txn, chain)
		if err != nil {
			return err
		}
		switch {
		case height == m.TargetHeight && hash == m.TargetHash:
			return nil
		case height <= m.TargetHeight:
			return fmt.Errorf("recovery target %d must exceed current target %d", height, m.TargetHeight)
		}
		m.TargetHeight, m.TargetHash = height, hash
		m.Targets = append(m.Targets, RecoveryTarget{Height: height, Hash: hash, SetAt: time.Now().UTC()})
		return n3Set(txn, recoveryPrefix+chain, m)
	})
}

// RecoveryStatus is the completion and handoff assessment of one chain.
type RecoveryStatus struct {
	Manifest RecoveryManifest `json:"manifest"`
	// LiveCheckpoint is the normal indexer's last committed height, if any.
	LiveCheckpoint *uint64 `json:"live_checkpoint,omitempty"`
	// ClaimTraceGaps are Neo X blocks inside the recovery bounds whose
	// indirect claims were not inspected.
	ClaimTraceGaps      []NeoXClaimTraceGap `json:"claim_trace_gaps,omitempty"`
	ClaimTraceGapBlocks uint64              `json:"claim_trace_gap_blocks"`
	// UnresolvedClaims are N3 claims inside the bounds without a verified pair.
	UnresolvedClaims int  `json:"unresolved_claims"`
	Contiguous       bool `json:"contiguous"`
	// Complete: every height in [start, target] is committed with no
	// unresolved evidence. Only then can absence of an outcome in that
	// interval be read as not executed.
	Complete bool `json:"complete"`
	// HandoffReady: Complete, and the live checkpoint is at or below the
	// target, so the normal indexer resumes inside the verified interval and
	// no block is left uncovered between recovery and live indexing.
	HandoffReady bool     `json:"handoff_ready"`
	Blocking     []string `json:"blocking,omitempty"`
}

// RecoveryStatus derives completion from the manifest and the durable
// unresolved-evidence records; nothing is trusted from a saved flag.
func (os *OperationStore) RecoveryStatus(chain string) (*RecoveryStatus, error) {
	m, err := os.GetRecoveryManifest(chain)
	if err != nil {
		return nil, err
	}
	st := &RecoveryStatus{Manifest: *m, Contiguous: m.Contiguous()}
	if !st.Contiguous {
		st.Blocking = append(st.Blocking, fmt.Sprintf("replay committed through %d of target %d", int64(m.NextHeight)-1, m.TargetHeight))
	}
	if chain == "neo_x" {
		gaps, err := os.store.NeoXClaimCoverageGaps(m.StartHeight, m.TargetHeight)
		if err != nil {
			return nil, err
		}
		for _, g := range gaps {
			from, to := max(g.FromHeight, m.StartHeight), min(g.ToHeight, m.TargetHeight)
			st.ClaimTraceGapBlocks += to - from + 1
		}
		st.ClaimTraceGaps = gaps
		if len(gaps) > 0 {
			st.Blocking = append(st.Blocking, fmt.Sprintf("%d blocks lack claim traces", st.ClaimTraceGapBlocks))
		}
	} else {
		claims, err := os.store.ListUnresolvedN3TokenClaims()
		if err != nil {
			return nil, err
		}
		for _, c := range claims {
			if c.Evidence.BlockHeight >= m.StartHeight && c.Evidence.BlockHeight <= m.TargetHeight {
				st.UnresolvedClaims++
			}
		}
		if st.UnresolvedClaims > 0 {
			st.Blocking = append(st.Blocking, fmt.Sprintf("%d N3 claims lack a verified claimable pair", st.UnresolvedClaims))
		}
	}
	st.Complete = st.Contiguous && st.ClaimTraceGapBlocks == 0 && st.UnresolvedClaims == 0
	live, err := os.store.GetIndexerState(chain)
	switch {
	case err == nil:
		h := live.LastBlockHeight
		st.LiveCheckpoint = &h
		if h > m.TargetHeight {
			st.Blocking = append(st.Blocking, fmt.Sprintf("live checkpoint %d is beyond target %d; extend the target to at least %d", h, m.TargetHeight, h))
		}
	case errors.Is(err, ErrNotFound):
		st.Blocking = append(st.Blocking, "no live checkpoint to hand off to")
	default:
		return nil, err
	}
	st.HandoffReady = st.Complete && st.LiveCheckpoint != nil && *st.LiveCheckpoint <= m.TargetHeight
	return st, nil
}

func retryLifecycleWrite(db *badger.DB, fn func(*badger.Txn) error) error {
	for attempt := 0; attempt < 5; attempt++ {
		err := db.Update(fn)
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	return badger.ErrConflict
}
