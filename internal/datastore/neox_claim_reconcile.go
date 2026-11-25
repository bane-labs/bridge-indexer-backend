package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	badger "github.com/dgraph-io/badger/v4"
)

const unresolvedNeoXTokenClaimPrefix = "lifecycle_unresolved:v1:neo_x:token_claim:"

// UnresolvedNeoXTokenClaim is a Neo X token claimable whose record the
// contract no longer holds, so some transaction claimed it, but that claim
// could not be identified (for example an indirect call without a trace).
// Its claim outcome is unknown, not absent.
type UnresolvedNeoXTokenClaim struct {
	Identity OperationIdentity `json:"identity"`
	// ConsumedAt is the first height at which the claimable record is gone.
	ConsumedAt uint64 `json:"consumed_at"`
	Reason     string `json:"reason"`
}

func unresolvedNeoXTokenClaimKey(id OperationIdentity) string {
	return fmt.Sprintf("%s%s:%s:%d", unresolvedNeoXTokenClaimPrefix, normalizeTokenKeyPart(id.SourceToken), normalizeTokenKeyPart(id.DestinationToken), id.Nonce)
}

// ListUnresolvedNeoXTokenClaims returns claimables whose claim is unknown.
func (s *Store) ListUnresolvedNeoXTokenClaims() ([]UnresolvedNeoXTokenClaim, error) {
	var out []UnresolvedNeoXTokenClaim
	err := s.ScanPrefix(unresolvedNeoXTokenClaimPrefix, func(_ string, v []byte) error {
		var c UnresolvedNeoXTokenClaim
		if err := json.Unmarshal(v, &c); err != nil {
			return err
		}
		out = append(out, c)
		return nil
	})
	return out, err
}

// OpenNeoXTokenClaimable is a Neo X token claimable observed at or below a
// height without a recorded claim.
type OpenNeoXTokenClaimable struct {
	Identity  OperationIdentity
	Claimable LifecycleEvidence
}

// ListOpenNeoXTokenClaimables lists token deposits with Neo X claimable
// evidence at or below through and no claim evidence.
func (os *OperationStore) ListOpenNeoXTokenClaimables(through uint64) ([]OpenNeoXTokenClaimable, error) {
	var out []OpenNeoXTokenClaimable
	err := os.store.ScanPrefix("lifecycle:v1:token:deposit:", func(key string, v []byte) error {
		var l OperationLifecycle
		if err := json.Unmarshal(v, &l); err != nil {
			return err
		}
		if l.Claimable == nil || l.Claim != nil || l.Claimable.Chain != "neo_x" || l.Claimable.BlockHeight > through {
			return nil
		}
		id, err := tokenIdentityFromKey(key)
		if err != nil {
			return err
		}
		out = append(out, OpenNeoXTokenClaimable{Identity: id, Claimable: *l.Claimable})
		return nil
	})
	return out, err
}

func tokenIdentityFromKey(key string) (OperationIdentity, error) {
	// lifecycle:v1:token:<direction>:<nonce>:<source>:<destination>
	parts := strings.Split(key, ":")
	if len(parts) != 7 {
		return OperationIdentity{}, fmt.Errorf("unexpected token lifecycle key %q", key)
	}
	var nonce uint64
	if _, err := fmt.Sscan(parts[4], &nonce); err != nil {
		return OperationIdentity{}, err
	}
	return OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: types.Direction(parts[3]), Nonce: nonce, SourceToken: parts[5], DestinationToken: parts[6]}, nil
}

// ClaimReconciliation is the result of checking every open Neo X token
// claimable against contract state at Through (the last committed height).
type ClaimReconciliation struct {
	Through uint64
	// DeployHeight is the bridge's first height on Neo X. Only a recovery
	// that started there has seen every claimable, which is what lets claim
	// gaps be cleared.
	DeployHeight uint64
	// Claims are claim observations identified in the consuming blocks.
	Claims []LifecycleObservation
	// Outstanding are claimables the contract still held at Through: no
	// claim of them happened through that height.
	Outstanding []OperationIdentity
	// Unresolved were consumed by a claim that could not be identified.
	Unresolved []UnresolvedNeoXTokenClaim
}

// ClaimReconciliationSummary is kept on the Neo X manifest.
type ClaimReconciliationSummary struct {
	Through     uint64 `json:"through"`
	Claims      int    `json:"claims"`
	Outstanding int    `json:"outstanding"`
	Unresolved  int    `json:"unresolved"`
	GapsCleared bool   `json:"gaps_cleared"`
}

// ReconcileNeoXClaims atomically records identified claims (with the legacy
// completion live indexing applies for a claim), replaces the unresolved
// claim records, and, when every claimable up to Through is accounted for
// and the replay started at the bridge's deployment, clears all claim gaps
// in [start, Through].
//
// Why that is sound: in every deployed Bridge version a token claimable is
// created only together with a TokenClaimable log, and claimToken succeeds
// only by deleting a claimable record. A contiguous log replay from the
// deployment therefore knows every claimable, and once each one is either
// still held at Through or matched to its consuming claim, no other claim
// can exist in the interval. Native claims and message executions emit logs.
func (os *OperationStore) ReconcileNeoXClaims(rc ClaimReconciliation) (*ClaimReconciliationSummary, error) {
	for _, c := range rc.Claims {
		if c.Fact != FactClaim || c.Evidence.Chain != "neo_x" || c.Evidence.BlockHeight > rc.Through || c.Identity.BridgeType != types.BridgeTypeToken {
			return nil, errors.New("reconciled claim outside Neo X token claims through the reconciled height")
		}
	}
	var summary ClaimReconciliationSummary
	err := retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		m, err := readRecoveryManifest(txn, "neo_x")
		if err != nil {
			return err
		}
		if m.NextHeight == m.StartHeight || rc.Through != m.NextHeight-1 {
			return fmt.Errorf("reconcile through the committed height %d, not %d", int64(m.NextHeight)-1, rc.Through)
		}
		if err := writeLifecycleObservations(txn, rc.Claims); err != nil {
			return err
		}
		for _, c := range rc.Claims {
			if _, err := legacyCompleteFrom(txn, c); err != nil {
				return err
			}
		}
		// Replace the unresolved records.
		var stale [][]byte
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		prefix := []byte(unresolvedNeoXTokenClaimPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			stale = append(stale, it.Item().KeyCopy(nil))
		}
		it.Close()
		for _, k := range stale {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		accounted := map[string]bool{}
		for _, id := range rc.Outstanding {
			k, err := id.key()
			if err != nil {
				return err
			}
			accounted[k] = true
		}
		for _, u := range rc.Unresolved {
			k, err := u.Identity.key()
			if err != nil {
				return err
			}
			accounted[k] = true
			if err := n3Set(txn, unresolvedNeoXTokenClaimKey(u.Identity), u); err != nil {
				return err
			}
		}
		// Every open claimable must have been checked.
		it = txn.NewIterator(badger.DefaultIteratorOptions)
		lprefix := []byte("lifecycle:v1:token:deposit:")
		var missing []string
		for it.Seek(lprefix); it.ValidForPrefix(lprefix); it.Next() {
			var l OperationLifecycle
			if err := it.Item().Value(func(v []byte) error { return json.Unmarshal(v, &l) }); err != nil {
				it.Close()
				return err
			}
			key := string(it.Item().Key())
			if l.Claimable != nil && l.Claim == nil && l.Claimable.Chain == "neo_x" && l.Claimable.BlockHeight <= rc.Through && !accounted[key] {
				missing = append(missing, key)
			}
		}
		it.Close()
		if len(missing) > 0 {
			return fmt.Errorf("claimables not reconciled: %v", missing)
		}
		summary = ClaimReconciliationSummary{Through: rc.Through, Claims: len(rc.Claims), Outstanding: len(rc.Outstanding), Unresolved: len(rc.Unresolved)}
		if len(rc.Unresolved) == 0 && m.StartHeight <= rc.DeployHeight {
			if err := replaceNeoXClaimTraceGaps(txn, m.StartHeight, rc.Through, nil); err != nil {
				return err
			}
			summary.GapsCleared = true
		}
		m.Claims = &summary
		return n3Set(txn, recoveryPrefix+"neo_x", m)
	})
	if err != nil {
		return nil, err
	}
	return &summary, nil
}
