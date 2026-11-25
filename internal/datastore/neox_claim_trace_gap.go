package datastore

import (
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

const neoxClaimTraceGapPrefix = "lifecycle_unresolved:v1:neo_x:claim_trace:"

// NeoXClaimTraceGap marks committed Neo X blocks whose indirect claimToken
// calls were not inspected because call traces were unavailable. Their
// events, receipts and direct claim calls were processed; only indirect claim
// coverage is missing. A gap is not evidence that no claim happened: claim
// outcomes in these blocks stay unknown until the gap is enriched.
type NeoXClaimTraceGap struct {
	FromHeight uint64 `json:"from_height"`
	ToHeight   uint64 `json:"to_height"`
	Reason     string `json:"reason"` // trace_unsupported, trace_unavailable, trace_invalid
}

func neoxClaimTraceGapKey(from uint64) string {
	return fmt.Sprintf("%s%020d", neoxClaimTraceGapPrefix, from)
}

func writeNeoXClaimTraceGaps(txn *badger.Txn, checkpoint uint64, gaps []NeoXClaimTraceGap) error {
	for _, gap := range gaps {
		if gap.FromHeight > gap.ToHeight || gap.ToHeight > checkpoint ||
			(gap.Reason != "trace_unsupported" && gap.Reason != "trace_unavailable" && gap.Reason != "trace_invalid") {
			return fmt.Errorf("invalid Neo X claim trace gap %+v", gap)
		}
		key := neoxClaimTraceGapKey(gap.FromHeight)
		var existing NeoXClaimTraceGap
		if err := n3Read(txn, key, &existing); err != nil && err != ErrNotFound {
			return err
		} else if err == nil {
			if existing != gap {
				return fmt.Errorf("conflicting Neo X claim trace gap %s", key)
			}
			continue
		}
		if err := n3Set(txn, key, gap); err != nil {
			return err
		}
	}
	return nil
}

// ListNeoXClaimTraceGaps returns unresolved claim coverage gaps, lowest first.
func (s *Store) ListNeoXClaimTraceGaps() ([]NeoXClaimTraceGap, error) {
	var gaps []NeoXClaimTraceGap
	err := s.ScanPrefix(neoxClaimTraceGapPrefix, func(_ string, value []byte) error {
		var gap NeoXClaimTraceGap
		if err := json.Unmarshal(value, &gap); err != nil {
			return err
		}
		gaps = append(gaps, gap)
		return nil
	})
	return gaps, err
}

// NeoXClaimCoverageGaps returns the gaps overlapping [from, to]. Any negative
// claim conclusion (not_executed) for that interval requires an empty result
// in addition to verified recovery coverage.
func (s *Store) NeoXClaimCoverageGaps(from, to uint64) ([]NeoXClaimTraceGap, error) {
	gaps, err := s.ListNeoXClaimTraceGaps()
	if err != nil {
		return nil, err
	}
	var overlapping []NeoXClaimTraceGap
	for _, gap := range gaps {
		if gap.FromHeight <= to && gap.ToHeight >= from {
			overlapping = append(overlapping, gap)
		}
	}
	return overlapping, nil
}

// ResolveNeoXClaimTraceGap atomically records the claim observations found by
// enriching the lowest block of a gap and removes that block from the gap. It
// does not touch the live checkpoint. A repeated enrichment of an already
// resolved block returns ErrNotFound without writing.
func (s *Store) ResolveNeoXClaimTraceGap(height uint64, blockHash string, observations []LifecycleObservation) error {
	for _, o := range observations {
		if o.Fact != FactClaim || o.Evidence.Chain != "neo_x" || o.Evidence.BlockHeight != height || o.Evidence.BlockHash != blockHash {
			return errors.New("enrichment observation outside its gap block")
		}
	}
	return retryLifecycleWrite(s.db, func(txn *badger.Txn) error {
		key := neoxClaimTraceGapKey(height)
		var gap NeoXClaimTraceGap
		if err := n3Read(txn, key, &gap); err != nil {
			return err
		}
		if err := writeLifecycleObservations(txn, observations); err != nil {
			return err
		}
		if err := txn.Delete([]byte(key)); err != nil {
			return err
		}
		if gap.ToHeight > height {
			gap.FromHeight = height + 1
			return n3Set(txn, neoxClaimTraceGapKey(gap.FromHeight), gap)
		}
		return nil
	})
}
