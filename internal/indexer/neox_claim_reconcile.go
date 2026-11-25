package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
)

// tokenClaimablesSlot is the storage slot of the Bridge's
// tokenClaimables[token][nonce] => {to, amount} mapping. It is the same in
// every deployed version (BridgeStorageV1 is append-only) and was verified on
// mainnet: the nonce 108 claimable of 0xc28736dc… is held at this mapping
// from block 6,748,615 and cleared by its claim at 6,757,004; the native
// mapping two slots below holds GasClaimable 958 from 1,270,711.
const tokenClaimablesSlot = 104

// neoxBridgeDeployHeight: the Neo X Bridge is a genesis contract. Claim gaps
// can only be cleared by a recovery that started here.
const neoxBridgeDeployHeight uint64 = 0

// tokenClaimableSlot returns the slot of the claimable's `to` word.
func tokenClaimableSlot(token common.Address, nonce uint64) common.Hash {
	inner := crypto.Keccak256(common.LeftPadBytes(token.Bytes(), 32), common.LeftPadBytes(big.NewInt(tokenClaimablesSlot).Bytes(), 32))
	return crypto.Keccak256Hash(common.LeftPadBytes(new(big.Int).SetUint64(nonce).Bytes(), 32), inner)
}

func (r *Recoverer) claimableHeld(ctx context.Context, slot common.Hash, height uint64) (bool, error) {
	v, err := r.neox.client.StorageAt(ctx, r.neox.processor.tokenBridge, slot, new(big.Int).SetUint64(height))
	if err != nil {
		return false, err
	}
	return new(big.Int).SetBytes(v).Sign() != 0, nil
}

// ReconcileClaims accounts for every open Neo X token claimable through the
// last committed recovery height H, using contract state instead of a
// per-block scan. For each claimable without claim evidence:
//
//   - its record still held at H: no claim of it happened through H;
//   - its record gone: a binary search over heights finds the block that
//     consumed it; that one block is decoded for the direct claimToken call
//     or, failing that, traced. A claim that still cannot be identified is
//     recorded as unresolved, never as unclaimed.
//
// The result is committed atomically. Claim gaps in [start, H] are cleared
// only when the recovery started at the Bridge's deployment and nothing is
// unresolved (see datastore.ReconcileNeoXClaims for why that is sound).
func (r *Recoverer) ReconcileClaims(ctx context.Context) (*datastore.ClaimReconciliationSummary, error) {
	if r.neox == nil {
		return nil, errors.New("claim reconciliation applies to neo_x only")
	}
	if err := r.requireMainnet(ctx); err != nil {
		return nil, err
	}
	m, err := r.opStore.GetRecoveryManifest(chainNeoX)
	if err != nil {
		return nil, err
	}
	if m.NextHeight == m.StartHeight {
		return nil, errors.New("nothing committed yet")
	}
	through := m.NextHeight - 1
	open, err := r.opStore.ListOpenNeoXTokenClaimables(through)
	if err != nil {
		return nil, err
	}
	rc := datastore.ClaimReconciliation{Through: through, DeployHeight: neoxBridgeDeployHeight}
	for _, c := range open {
		token := common.HexToAddress(c.Identity.DestinationToken)
		slot := tokenClaimableSlot(token, c.Identity.Nonce)
		held, err := r.claimableHeld(ctx, slot, through)
		if err != nil {
			return nil, err
		}
		if held {
			rc.Outstanding = append(rc.Outstanding, c.Identity)
			continue
		}
		if ok, err := r.claimableHeld(ctx, slot, c.Claimable.BlockHeight); err != nil {
			return nil, err
		} else if !ok {
			// The record is not where the verified layout puts it.
			rc.Unresolved = append(rc.Unresolved, datastore.UnresolvedNeoXTokenClaim{Identity: c.Identity, Reason: "claimable_record_not_found"})
			continue
		}
		lo, hi := c.Claimable.BlockHeight, through // held at lo, gone at hi
		for hi-lo > 1 {
			mid := lo + (hi-lo)/2
			ok, err := r.claimableHeld(ctx, slot, mid)
			if err != nil {
				return nil, err
			}
			if ok {
				lo = mid
			} else {
				hi = mid
			}
		}
		claim, reason, err := r.identifyClaim(ctx, c.Identity, hi)
		if err != nil {
			return nil, err
		}
		if claim != nil {
			rc.Claims = append(rc.Claims, *claim)
		} else {
			rc.Unresolved = append(rc.Unresolved, datastore.UnresolvedNeoXTokenClaim{Identity: c.Identity, ConsumedAt: hi, Reason: reason})
		}
	}
	return r.opStore.ReconcileNeoXClaims(rc)
}

// identifyClaim finds the claim of id in the block that consumed it: first a
// direct claimToken transaction, then, with a trace, an indirect call.
func (r *Recoverer) identifyClaim(ctx context.Context, id datastore.OperationIdentity, height uint64) (*datastore.LifecycleObservation, string, error) {
	block, err := r.neox.client.BlockByNumber(ctx, new(big.Int).SetUint64(height))
	if err != nil {
		return nil, "", fmt.Errorf("claim block %d: %w", height, err)
	}
	logs, err := r.neox.client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: block.Number(), ToBlock: block.Number(), Addresses: r.neox.contracts})
	if err != nil {
		return nil, "", err
	}
	match := func(claims []datastore.LifecycleObservation) *datastore.LifecycleObservation {
		for i := range claims {
			if claims[i].Identity == id {
				return &claims[i]
			}
		}
		return nil
	}
	claims, err := r.neox.blockClaims(ctx, block, nil, logs, nil)
	if err != nil {
		return nil, "claim_not_verifiable: " + err.Error(), nil
	}
	if c := match(claims); c != nil {
		return c, "", nil
	}
	raw, traceErr := r.neox.client.TraceBlock(ctx, height)
	traces, reason, err := r.neox.checkTraces(ctx, block, raw, traceErr)
	if err != nil {
		return nil, "", err
	}
	if reason != "" {
		return nil, "indirect_claim_" + reason, nil
	}
	if claims, err = r.neox.blockClaims(ctx, block, traces, logs, nil); err != nil {
		return nil, "claim_not_verifiable: " + err.Error(), nil
	}
	if c := match(claims); c != nil {
		return c, "", nil
	}
	return nil, "claim_not_identified", nil
}
