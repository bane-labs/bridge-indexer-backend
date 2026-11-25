package datastore

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	badger "github.com/dgraph-io/badger/v4"
)

const recoveryPrefix = "lifecycle_recovery:v1:"

// RecoveryManifest describes a bounded replay on one destination chain.
// NextHeight is the first height not yet verified; it starts at StartHeight.
// No migration sets this record on an existing database: missing means unknown.
type RecoveryManifest struct {
	Chain                string   `json:"chain"`
	WatchedContracts     []string `json:"watched_contracts"`
	ImplementationRanges []string `json:"implementation_ranges"`
	RequiredPaths        []string `json:"required_paths"` // e.g. logs, notifications, calls, traces
	StartHeight          uint64   `json:"start_height"`
	TargetHeight         uint64   `json:"target_height"`
	TargetHash           string   `json:"target_hash"`
	NextHeight           uint64   `json:"next_height"`
	ContiguousHash       string   `json:"contiguous_hash,omitempty"`
	LastBlockDigest      string   `json:"last_block_digest,omitempty"`
	Unresolved           uint64   `json:"unresolved"`
}

func (m RecoveryManifest) valid() error {
	if m.Chain != "neo_n3" && m.Chain != "neo_x" {
		return fmt.Errorf("invalid recovery chain %q", m.Chain)
	}
	if len(m.WatchedContracts) == 0 || len(m.ImplementationRanges) == 0 || len(m.RequiredPaths) == 0 || m.TargetHash == "" || m.StartHeight > m.TargetHeight || m.TargetHeight == ^uint64(0) {
		return errors.New("recovery requires contracts, implementation ranges, evidence paths, and bounded target height/hash")
	}
	paths := []string{"blocks", "implementations"}
	if m.Chain == "neo_x" {
		paths = append(paths, "logs", "transactions", "receipts", "claims", "traces")
	} else {
		paths = append(paths, "notifications", "application_logs")
	}
	for _, path := range paths {
		if !slices.Contains(m.RequiredPaths, path) {
			return fmt.Errorf("missing required recovery evidence path %q", path)
		}
	}
	if m.NextHeight < m.StartHeight || m.NextHeight > m.TargetHeight+1 {
		return errors.New("recovery watermark out of bounds")
	}
	return nil
}

// VerifiedThroughTarget reports only recovery coverage through the captured
// target. Continuous live coverage after cutover requires a separate writer
// and is deliberately not implied by this method.
func (m RecoveryManifest) VerifiedThroughTarget() bool {
	return m.valid() == nil && m.NextHeight == m.TargetHeight+1 && m.ContiguousHash == m.TargetHash && m.Unresolved == 0
}

func (os *OperationStore) GetRecoveryManifest(chain string) (*RecoveryManifest, error) {
	if chain != "neo_n3" && chain != "neo_x" {
		return nil, errors.New("invalid recovery chain")
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

// BeginRecovery is idempotent for the same manifest, but refuses to replace
// existing bounds or reset an advanced watermark silently.
func (os *OperationStore) BeginRecovery(m RecoveryManifest) error {
	if m.NextHeight != m.StartHeight || m.ContiguousHash != "" || m.LastBlockDigest != "" || m.Unresolved != 0 {
		return errors.New("new recovery must start with an empty watermark")
	}
	if err := m.valid(); err != nil {
		return err
	}
	return retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		key := []byte(recoveryPrefix + m.Chain)
		item, err := txn.Get(key)
		if err == nil {
			var existing RecoveryManifest
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &existing) }); err != nil {
				return err
			}
			if !sameRecoveryBounds(existing, m) {
				return errors.New("recovery bounds changed; explicit reset and replay required")
			}
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
}

func sameRecoveryBounds(a, b RecoveryManifest) bool {
	return a.Chain == b.Chain && a.StartHeight == b.StartHeight && a.TargetHeight == b.TargetHeight && a.TargetHash == b.TargetHash && slices.Equal(a.WatchedContracts, b.WatchedContracts) && slices.Equal(a.ImplementationRanges, b.ImplementationRanges) && slices.Equal(a.RequiredPaths, b.RequiredPaths)
}

// RecordRecoveryUnresolved saves the number of unresolved observations at the
// next uncommitted height without moving the watermark. A successful replay
// of that height clears it in CommitRecoveryBlock.
func (os *OperationStore) RecordRecoveryUnresolved(chain string, height, count uint64) error {
	if chain != "neo_n3" && chain != "neo_x" {
		return errors.New("invalid recovery chain")
	}
	return retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		key := []byte(recoveryPrefix + chain)
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var m RecoveryManifest
		if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &m) }); err != nil {
			return err
		}
		if err := m.valid(); err != nil {
			return err
		}
		if height != m.NextHeight || height > m.TargetHeight {
			return errors.New("unresolved count must refer to next recovery height")
		}
		m.Unresolved = count
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
}

// RecoveryBlock is a fully fetched and decoded destination block. The caller
// must verify all required evidence paths and contract implementations before
// advancing it. An unresolved claim or decoder error must not be advanced.
type RecoveryBlock struct {
	Chain         string
	Height        uint64
	Hash          string
	ParentHash    string
	VerifiedPaths []string
	Unresolved    uint64
}

// CommitRecoveryBlock atomically saves the block's observations and the
// contiguous replay watermark. An exact retry of the most recently committed
// block succeeds without advancing it; changed evidence or a skipped block fails.
func (os *OperationStore) CommitRecoveryBlock(block RecoveryBlock, observations []LifecycleObservation) error {
	input, err := json.Marshal(struct {
		Block        RecoveryBlock
		Observations []LifecycleObservation
	}{block, observations})
	if err != nil {
		return err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(input))
	return retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		key := []byte(recoveryPrefix + block.Chain)
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.New("recovery manifest missing")
		}
		if err != nil {
			return err
		}
		var m RecoveryManifest
		if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &m) }); err != nil {
			return err
		}
		if err := m.valid(); err != nil {
			return err
		}
		if m.NextHeight > m.StartHeight && block.Height == m.NextHeight-1 {
			if block.Hash == m.ContiguousHash && digest == m.LastBlockDigest && m.Unresolved == 0 {
				return nil
			}
			return errors.New("conflicting retry of last recovery block")
		}
		if block.Height != m.NextHeight || block.Height > m.TargetHeight || block.Hash == "" {
			return errors.New("noncontiguous or invalid recovery block")
		}
		if block.Height == m.TargetHeight && block.Hash != m.TargetHash {
			return errors.New("recovery target hash mismatch")
		}
		if block.Height > m.StartHeight && (block.ParentHash == "" || block.ParentHash != m.ContiguousHash) {
			return errors.New("recovery parent hash mismatch")
		}
		for _, required := range m.RequiredPaths {
			if !slices.Contains(block.VerifiedPaths, required) {
				return fmt.Errorf("unverified evidence path %q", required)
			}
		}
		if block.Unresolved != 0 {
			return errors.New("unresolved evidence prevents recovery advancement")
		}
		for _, o := range observations {
			if o.Evidence.Chain != block.Chain || o.Evidence.BlockHeight != block.Height || o.Evidence.BlockHash != block.Hash {
				return errors.New("observation outside recovery block")
			}
		}
		if err := writeLifecycleObservations(txn, observations); err != nil {
			return err
		}
		m.NextHeight++
		m.ContiguousHash = block.Hash
		m.LastBlockDigest = digest
		m.Unresolved = block.Unresolved
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
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
