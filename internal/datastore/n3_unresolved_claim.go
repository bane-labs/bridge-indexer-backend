package datastore

import (
	"encoding/json"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

const unresolvedN3TokenClaimPrefix = "lifecycle_unresolved:v1:neo_n3:token_claim:"

// UnresolvedN3TokenClaim retains a verified committed notification whose
// source token cannot yet be linked to a verified claimable record. It is not
// a lifecycle claim or evidence that any particular operation completed.
// Recovery must reconcile these records before certifying execution coverage.
type UnresolvedN3TokenClaim struct {
	Nonce            uint64            `json:"nonce"`
	DestinationToken string            `json:"destination_token"`
	Evidence         LifecycleEvidence `json:"evidence"`
	Reason           string            `json:"reason"`
}

func unresolvedN3TokenClaimKey(c UnresolvedN3TokenClaim) string {
	return unresolvedN3TokenClaimPrefix + c.Evidence.TxHash + ":" + c.Evidence.Position
}

func writeUnresolvedN3TokenClaims(txn *badger.Txn, claims []UnresolvedN3TokenClaim) error {
	for _, claim := range claims {
		if claim.DestinationToken == "" || normalizeTokenKeyPart(claim.DestinationToken) == "unknown" ||
			(claim.Reason != "missing_claimable_pair" && claim.Reason != "ambiguous_claimable_pair") ||
			claim.Evidence.Kind != "notification" || claim.Evidence.Chain != "neo_n3" {
			return fmt.Errorf("invalid unresolved N3 token claim")
		}
		if err := validateEvidence(claim.Evidence); err != nil {
			return err
		}
		key := unresolvedN3TokenClaimKey(claim)
		var existing UnresolvedN3TokenClaim
		if err := n3Read(txn, key, &existing); err != nil && err != ErrNotFound {
			return err
		} else if err == nil {
			if existing.Nonce != claim.Nonce || normalizeTokenKeyPart(existing.DestinationToken) != normalizeTokenKeyPart(claim.DestinationToken) ||
				existing.Reason != claim.Reason || !sameEvidence(&existing.Evidence, &claim.Evidence) {
				return fmt.Errorf("conflicting unresolved N3 claim %s", key)
			}
			continue
		}
		if err := n3Set(txn, key, claim); err != nil {
			return err
		}
	}
	return nil
}

// ListUnresolvedN3TokenClaims exposes durable live gaps for historical recovery.
func (s *Store) ListUnresolvedN3TokenClaims() ([]UnresolvedN3TokenClaim, error) {
	var claims []UnresolvedN3TokenClaim
	err := s.ScanPrefix(unresolvedN3TokenClaimPrefix, func(_ string, value []byte) error {
		var claim UnresolvedN3TokenClaim
		if err := json.Unmarshal(value, &claim); err != nil {
			return err
		}
		claims = append(claims, claim)
		return nil
	})
	return claims, err
}
