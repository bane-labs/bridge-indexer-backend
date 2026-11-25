package datastore

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	badger "github.com/dgraph-io/badger/v4"
)

// RecoveryRange is a contiguous, fully processed interval of one chain.
// FromParentHash links it to the previous committed range; ToHash is the hash
// of its last block. VerifiedPaths lists the evidence paths the caller fetched
// and decoded for every block in the range.
type RecoveryRange struct {
	Chain          string
	From, To       uint64
	FromParentHash string
	ToHash         string
	VerifiedPaths  []string
	// LocatedBy names the index that selected the blocks fetched in this
	// range; it must be the manifest's TrustedIndex. Empty: every block was
	// fetched and decoded.
	LocatedBy string
}

// RecoveryBatch is what the live decoders produced for a RecoveryRange.
type RecoveryBatch struct {
	Observations []LifecycleObservation
	// Operations are source operations emitted on the replayed chain. They are
	// inserted only when absent; an existing record is never replaced.
	Operations []*types.BridgeOperation
	// TraceGaps (Neo X) are the range's blocks whose call traces were missing.
	TraceGaps []NeoXClaimTraceGap
	// UnresolvedClaims (N3) are claims in the range without a verified pair.
	// They replace any earlier unresolved records inside the range.
	UnresolvedClaims []UnresolvedN3TokenClaim
	// LegacyCompletions are legacy completions the live event processors
	// apply that no lifecycle fact expresses one-to-one: a Neo X
	// NeoToEvmRootUpdate completes only its own nonce, not every stored
	// message of the batch. Identity, height, transaction and time are used.
	LegacyCompletions []LifecycleObservation
	// StaleBefore holds, per bridge type, the cutoff before which a
	// recovered operation without destination evidence is stored as legacy
	// stuck (the live stuck threshold). Not part of the retry digest.
	StaleBefore map[types.BridgeType]time.Time `json:"-"`
}

// RecoveryCommitResult reports what one committed range changed.
type RecoveryCommitResult struct {
	InsertedOperations int
	LegacyCompleted    int
	// WithoutDestination counts recovered operations with no destination
	// evidence yet, stored with legacy status stuck (see below).
	WithoutDestination int
	Retry              bool
}

// CommitRecoveryRange atomically records one replayed range and advances the
// recovery watermark. Either everything below is written, or nothing is:
//
//   - lifecycle observations, with the same monotonic conflict checks as live
//     indexing (a conflicting fact aborts the range);
//   - missing source operations (insert-only);
//   - the legacy completion projection, which only moves pending or stuck
//     operations to completed, using the live indexers' completing events;
//   - directional source and received states, which only rise (a verified
//     received root also replaces synthetic bookkeeping at the same nonce);
//   - Neo X claim trace gaps for exactly this range, and N3 claims that still
//     lack a verified pair;
//   - the watermark.
//
// Recovery never writes the live checkpoint. An exact retry of the last
// committed range succeeds without changes; anything else must start at the
// watermark and stay inside the current target.
func (os *OperationStore) CommitRecoveryRange(r RecoveryRange, b RecoveryBatch) (*RecoveryCommitResult, error) {
	input, err := json.Marshal(struct {
		Range RecoveryRange
		Batch RecoveryBatch
	}{r, b})
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(input))
	if err := validateRecoveryBatch(r, b); err != nil {
		return nil, err
	}
	var result RecoveryCommitResult
	err = retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		result = RecoveryCommitResult{}
		m, err := readRecoveryManifest(txn, r.Chain)
		if err != nil {
			return err
		}
		if m.Batches > 0 && r.From == m.LastBatchFrom && r.To+1 == m.NextHeight {
			if digest == m.LastBatchDigest && r.ToHash == m.ContiguousHash {
				result.Retry = true
				return nil
			}
			return errors.New("conflicting retry of last recovery range")
		}
		if r.From != m.NextHeight || r.To > m.TargetHeight {
			return fmt.Errorf("noncontiguous recovery range %d-%d: next height %d, target %d", r.From, r.To, m.NextHeight, m.TargetHeight)
		}
		if r.To == m.TargetHeight && r.ToHash != m.TargetHash {
			return errors.New("recovery target hash mismatch")
		}
		if r.From > m.StartHeight && r.FromParentHash != m.ContiguousHash {
			return errors.New("recovery parent hash mismatch")
		}
		for _, required := range m.RequiredPaths {
			if !slices.Contains(r.VerifiedPaths, required) {
				return fmt.Errorf("unverified evidence path %q", required)
			}
		}
		if r.LocatedBy != "" && r.LocatedBy != m.TrustedIndex {
			return fmt.Errorf("range located by %q, but the manifest trusts %q", r.LocatedBy, m.TrustedIndex)
		}
		if slices.Contains(m.RequiredPaths, "traces") && len(b.TraceGaps) > 0 {
			return errors.New("manifest requires traces but the range has claim trace gaps")
		}

		if err := writeLifecycleObservations(txn, b.Observations); err != nil {
			return err
		}
		ops := append([]*types.BridgeOperation(nil), b.Operations...)
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].Nonce < ops[j].Nonce })
		var inserted []*types.BridgeOperation
		for _, op := range ops {
			ok, err := insertRecoveredOperation(txn, op)
			if err != nil {
				return err
			}
			if ok {
				inserted = append(inserted, op)
			}
		}
		for _, o := range b.Observations {
			if completesLegacy(o) {
				done, err := legacyCompleteFrom(txn, o)
				if err != nil {
					return err
				}
				if done {
					result.LegacyCompleted++
				}
			}
		}
		for _, o := range b.LegacyCompletions {
			done, err := legacyCompleteFrom(txn, o)
			if err != nil {
				return err
			}
			if done {
				result.LegacyCompleted++
			}
		}
		for _, op := range inserted {
			done, err := completeInsertedFromLifecycle(txn, op)
			if err != nil {
				return err
			}
			if done {
				result.LegacyCompleted++
			} else if marked, err := markRecoveredWithoutDestination(txn, op, b.StaleBefore); err != nil {
				return err
			} else if marked {
				result.WithoutDestination++
			}
			if err := raiseSourceState(txn, op); err != nil {
				return err
			}
		}
		if err := raiseReceivedStates(txn, b.Observations); err != nil {
			return err
		}
		for _, o := range b.Observations {
			if o.Fact == FactClaim && o.Evidence.Chain == "neo_n3" && o.Evidence.Kind == "notification" {
				// A claim now paired with verified claimable evidence is no
				// longer an unresolved live gap.
				if err := txn.Delete([]byte(unresolvedN3TokenClaimPrefix + o.Evidence.TxHash + ":" + o.Evidence.Position)); err != nil {
					return err
				}
			}
		}
		if r.Chain == "neo_n3" {
			if err := replaceUnresolvedN3TokenClaims(txn, r.From, r.To, b.UnresolvedClaims); err != nil {
				return err
			}
		}
		if r.Chain == "neo_x" {
			if err := replaceNeoXClaimTraceGaps(txn, r.From, r.To, b.TraceGaps); err != nil {
				return err
			}
		}
		m.NextHeight = r.To + 1
		m.ContiguousHash = r.ToHash
		m.LastBatchFrom = r.From
		m.LastBatchDigest = digest
		m.Batches++
		m.Observations += uint64(len(b.Observations))
		m.RecoveredOperations += uint64(len(inserted))
		if r.LocatedBy != "" {
			m.LocatedBlocks += r.To - r.From + 1
		}
		result.InsertedOperations = len(inserted)
		return n3Set(txn, recoveryPrefix+r.Chain, m)
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// validateRecoveryBatch keeps every fact inside the committed range and on
// the replayed chain, so one chain's heights can never certify the other's.
func validateRecoveryBatch(r RecoveryRange, b RecoveryBatch) error {
	if err := validRecoveryChain(r.Chain); err != nil {
		return err
	}
	if r.To < r.From || r.ToHash == "" {
		return errors.New("invalid recovery range")
	}
	inRange := func(h uint64) bool { return h >= r.From && h <= r.To }
	for _, o := range b.Observations {
		if o.Evidence.Chain != r.Chain || !inRange(o.Evidence.BlockHeight) {
			return fmt.Errorf("observation at %s:%d outside recovery range %s %d-%d", o.Evidence.Chain, o.Evidence.BlockHeight, r.Chain, r.From, r.To)
		}
	}
	for _, op := range b.Operations {
		if op == nil || op.SourceChain != r.Chain || !inRange(op.SourceBlockHeight) {
			return errors.New("recovered operation outside recovery range")
		}
	}
	for _, g := range b.TraceGaps {
		if r.Chain != "neo_x" || g.FromHeight > g.ToHeight || g.FromHeight < r.From || g.ToHeight > r.To {
			return errors.New("claim trace gap outside recovery range")
		}
		if g.Reason != "trace_unsupported" && g.Reason != "trace_unavailable" && g.Reason != "trace_invalid" {
			return fmt.Errorf("invalid claim trace gap reason %q", g.Reason)
		}
	}
	for _, c := range b.UnresolvedClaims {
		if r.Chain != "neo_n3" || !inRange(c.Evidence.BlockHeight) {
			return errors.New("unresolved claim outside recovery range")
		}
	}
	for _, o := range b.LegacyCompletions {
		if o.Evidence.Chain != r.Chain || !inRange(o.Evidence.BlockHeight) || o.Evidence.TxHash == "" {
			return errors.New("legacy completion outside recovery range")
		}
	}
	return nil
}

// completesLegacy mirrors the facts whose events complete a legacy operation
// in live indexing: N3 distribution, claim and execution (withdrawals); Neo X
// deposit, claimable and claim events (native and token deposits). Message
// deposits complete on a root update's own nonce only, which is passed as
// RecoveryBatch.LegacyCompletions. Settlement alone never completes.
func completesLegacy(o LifecycleObservation) bool {
	switch o.Identity.Direction {
	case types.DirectionWithdrawal:
		return o.Fact == FactDistribution || o.Fact == FactClaim || o.Fact == FactExecution
	case types.DirectionDeposit:
		if o.Identity.BridgeType == types.BridgeTypeMessage {
			return false
		}
		return o.Fact == FactDistribution || o.Fact == FactClaimable || o.Fact == FactClaim
	}
	return false
}

// legacyCompleteFrom applies the shared, non-regressing legacy completion and
// reports whether it moved an operation to completed.
func legacyCompleteFrom(txn *badger.Txn, o LifecycleObservation) (bool, error) {
	before, _ := legacyStatus(txn, o.Identity)
	if err := n3LegacyCompletion(txn, o); err != nil {
		return false, err
	}
	after, _ := legacyStatus(txn, o.Identity)
	return before != types.StatusCompleted && after == types.StatusCompleted && before != "", nil
}

func legacyStatus(txn *badger.Txn, id OperationIdentity) (types.OperationStatus, error) {
	key := operationKey(id.BridgeType, id.Direction, id.Nonce)
	if id.BridgeType == types.BridgeTypeToken {
		key += ":" + normalizeTokenKeyPart(id.SourceToken) + ":" + normalizeTokenKeyPart(id.DestinationToken)
	}
	var op types.BridgeOperation
	err := n3Read(txn, key, &op)
	if errors.Is(err, ErrNotFound) && id.BridgeType == types.BridgeTypeToken {
		// Older databases can hold the same pair under the unscoped key.
		err = n3Read(txn, operationKey(id.BridgeType, id.Direction, id.Nonce), &op)
		if err == nil && (normalizeTokenKeyPart(strVal(op.TokenContract)) != normalizeTokenKeyPart(id.SourceToken) || normalizeTokenKeyPart(strVal(op.DestTokenContract)) != normalizeTokenKeyPart(id.DestinationToken)) {
			return "", ErrNotFound
		}
	}
	if err != nil {
		return "", err
	}
	return op.Status, nil
}

func identityForOperation(op *types.BridgeOperation) OperationIdentity {
	id := OperationIdentity{BridgeType: op.BridgeType, Direction: op.Direction, Nonce: op.Nonce}
	if op.BridgeType == types.BridgeTypeToken {
		id.SourceToken, id.DestinationToken = strVal(op.TokenContract), strVal(op.DestTokenContract)
	}
	return id
}

// insertRecoveredOperation writes op only if neither its scoped key nor a
// legacy unscoped record of the same token pair exists.
func insertRecoveredOperation(txn *badger.Txn, op *types.BridgeOperation) (bool, error) {
	key := operationKeyForOp(op)
	var existing types.BridgeOperation
	if err := n3Read(txn, key, &existing); err == nil {
		return false, nil
	} else if !errors.Is(err, ErrNotFound) {
		return false, err
	}
	if op.BridgeType == types.BridgeTypeToken {
		legacy := operationKey(op.BridgeType, op.Direction, op.Nonce)
		if err := n3Read(txn, legacy, &existing); err == nil {
			if normalizeTokenKeyPart(strVal(existing.TokenContract)) == normalizeTokenKeyPart(strVal(op.TokenContract)) &&
				normalizeTokenKeyPart(strVal(existing.DestTokenContract)) == normalizeTokenKeyPart(strVal(op.DestTokenContract)) {
				return false, nil
			}
		} else if !errors.Is(err, ErrNotFound) {
			return false, err
		}
	}
	if err := n3Set(txn, key, op); err != nil {
		return false, err
	}
	return true, n3Set(txn, statusIndexKeyForOp(op.Status, op), []byte{})
}

// completeInsertedFromLifecycle completes a newly recovered source operation
// whose destination evidence was recorded earlier (by live indexing or the
// other chain's replay). The earliest completing observation is used.
func completeInsertedFromLifecycle(txn *badger.Txn, op *types.BridgeOperation) (bool, error) {
	if op.Status == types.StatusCompleted {
		return false, nil
	}
	id := identityForOperation(op)
	key, err := id.key()
	if err != nil {
		return false, err
	}
	var l OperationLifecycle
	if err := n3Read(txn, key, &l); errors.Is(err, ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	var best *LifecycleObservation
	for _, c := range []struct {
		fact LifecycleFact
		e    *LifecycleEvidence
	}{{FactSettlement, l.Settlement}, {FactDistribution, l.Distribution}, {FactClaimable, l.Claimable}, {FactClaim, l.Claim}, {FactExecution, l.Execution}} {
		o := LifecycleObservation{Identity: id, Fact: c.fact}
		if c.e == nil || !completesLegacy(o) {
			continue
		}
		o.Evidence = *c.e
		if best == nil || o.Evidence.BlockHeight < best.Evidence.BlockHeight {
			best = &o
		}
	}
	if best == nil {
		return completeInsertedFromDeferred(txn, op, id)
	}
	return legacyCompleteFrom(txn, *best)
}

// completeInsertedFromDeferred applies a legacy deferred completion recorded
// before the source operation existed, exactly as live reconciliation would,
// and removes it.
func completeInsertedFromDeferred(txn *badger.Txn, op *types.BridgeOperation, id OperationIdentity) (bool, error) {
	keys := []string{deferredCompletionKey(op.BridgeType, op.Direction, op.Nonce, op.TokenContract, op.DestTokenContract)}
	if op.BridgeType == types.BridgeTypeToken {
		keys = append(keys, deferredCompletionKey(op.BridgeType, op.Direction, op.Nonce, nil, op.DestTokenContract),
			deferredCompletionKey(op.BridgeType, op.Direction, op.Nonce, nil, nil))
	}
	for _, key := range keys {
		var d deferredCompletion
		if err := n3Read(txn, key, &d); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return false, err
		}
		if err := txn.Delete([]byte(key)); err != nil {
			return false, err
		}
		return legacyCompleteFrom(txn, LifecycleObservation{Identity: id, Evidence: LifecycleEvidence{BlockHeight: d.DestBlock, TxHash: d.DestTxHash, ObservedAt: d.CompletionTime}})
	}
	return false, nil
}

// markRecoveredWithoutDestination stores a recovered, still pending
// operation older than the live stuck threshold as legacy stuck. The stuck
// detector would otherwise move it from pending to stuck on its next pass and
// send one stale alert per historical operation; the stored legacy state is
// the same. Younger operations stay pending for the detector. The lifecycle
// stays unknown, and destination evidence found later still completes the
// operation. Neo X to N3 message withdrawals stay pending, as in live
// indexing, where they are never considered stuck.
func markRecoveredWithoutDestination(txn *badger.Txn, op *types.BridgeOperation, staleBefore map[types.BridgeType]time.Time) (bool, error) {
	key := operationKeyForOp(op)
	var current types.BridgeOperation
	if err := n3Read(txn, key, &current); err != nil {
		return false, err
	}
	cutoff, ok := staleBefore[current.BridgeType]
	if current.Status != types.StatusPending || current.BridgeType == types.BridgeTypeMessage && current.Direction == types.DirectionWithdrawal || !ok || !current.Timestamp.Before(cutoff) {
		return false, nil
	}
	if err := txn.Delete([]byte(statusIndexKeyForOp(types.StatusPending, &current))); err != nil {
		return false, err
	}
	current.Status = types.StatusStuck
	if err := n3Set(txn, key, &current); err != nil {
		return false, err
	}
	return true, n3Set(txn, statusIndexKeyForOp(types.StatusStuck, &current), []byte{})
}

func ensureBridgeMetadata(txn *badger.Txn, meta types.DirectionalBridgeState) error {
	var existing types.DirectionalBridgeState
	if err := n3Read(txn, bridgeStateKey(meta.InstanceID), &existing); errors.Is(err, ErrNotFound) {
		return n3Set(txn, bridgeStateKey(meta.InstanceID), meta)
	} else {
		return err
	}
}

// raiseChainState writes next only when it is newer than the stored state
// (or replaces synthetic bookkeeping at the same nonce, if allowed).
func raiseChainState(txn *badger.Txn, key string, next *types.ChainState, replaceSynthetic bool) error {
	var current types.ChainState
	if err := n3Read(txn, key, &current); errors.Is(err, ErrNotFound) {
		return n3Set(txn, key, next)
	} else if err != nil {
		return err
	}
	if shouldReplaceChainState(&current, next) || replaceSynthetic && current.Synthetic && !next.Synthetic && current.Nonce == next.Nonce {
		return n3Set(txn, key, next)
	}
	return nil
}

func raiseSourceState(txn *badger.Txn, op *types.BridgeOperation) error {
	instance := InstanceIDFromOperation(op)
	if err := ensureBridgeMetadata(txn, types.DirectionalBridgeState{InstanceID: instance, BridgeType: op.BridgeType, SrcChain: op.SourceChain, DstChain: op.DestinationChain, SrcToken: op.TokenContract, DstToken: op.DestTokenContract}); err != nil {
		return err
	}
	if err := raiseChainState(txn, bridgeSrcPrefix+instance, BuildSourceChainState(op), false); err != nil {
		return err
	}
	// STORE_ONLY/RESULT Neo X messages keep the live synthetic destination.
	if messageNeoXWithdrawalCompletedNeedsSourceState(op) {
		dst := BuildDestChainState(op.Nonce, op.SourceBlockHeight, op.SourceTxHash, op.Timestamp, op.Root)
		dst.Synthetic = true
		return raiseChainState(txn, bridgeDstPrefix+instance, dst, false)
	}
	return nil
}

// raiseReceivedStates applies, per directional instance, the highest-nonce
// settlement in the batch. That settlement carries its own received root,
// whereas a lower nonce relayed in the same batch carries the batch root.
// Settlements without a root (Neo X message Store evidence) are skipped.
func raiseReceivedStates(txn *badger.Txn, observations []LifecycleObservation) error {
	best := map[string]LifecycleObservation{}
	var order []string
	for _, o := range observations {
		if o.Fact != FactSettlement {
			continue
		}
		var payload struct {
			Root string `json:"root"`
		}
		if json.Unmarshal(o.Evidence.Payload, &payload) != nil || payload.Root == "" {
			continue
		}
		instance := receivedInstance(o.Identity)
		if prev, ok := best[instance]; !ok || o.Identity.Nonce > prev.Identity.Nonce {
			if !ok {
				order = append(order, instance)
			}
			best[instance] = o
		}
	}
	for _, instance := range order {
		o := best[instance]
		var payload struct {
			Root string `json:"root"`
		}
		_ = json.Unmarshal(o.Evidence.Payload, &payload)
		id := o.Identity
		meta := types.DirectionalBridgeState{InstanceID: instance, BridgeType: id.BridgeType}
		meta.SrcChain, meta.DstChain = directionChains(id.Direction)
		if id.BridgeType == types.BridgeTypeToken {
			src, dst := id.SourceToken, id.DestinationToken
			meta.SrcToken, meta.DstToken = &src, &dst
		}
		if err := ensureBridgeMetadata(txn, meta); err != nil {
			return err
		}
		next := BuildDestChainState(id.Nonce, o.Evidence.BlockHeight, o.Evidence.TxHash, o.Evidence.ObservedAt, &payload.Root)
		if err := raiseChainState(txn, bridgeDstPrefix+instance, next, true); err != nil {
			return err
		}
	}
	return nil
}

func directionChains(d types.Direction) (string, string) {
	if d == types.DirectionWithdrawal {
		return "neo_x", "neo_n3"
	}
	return "neo_n3", "neo_x"
}

func receivedInstance(id OperationIdentity) string {
	src, dst := directionChains(id.Direction)
	if id.BridgeType != types.BridgeTypeToken {
		return BuildInstanceID(string(id.BridgeType), src, dst, nil, nil)
	}
	s, d := id.SourceToken, id.DestinationToken
	return BuildInstanceID(string(id.BridgeType), src, dst, &s, &d)
}

// replaceUnresolvedN3TokenClaims makes [from, to] carry exactly the given
// unresolved claims. A replay re-derives every claim in the range, so an
// earlier record (for example with a reason that changed as more claimables
// became known) is superseded rather than conflicting.
func replaceUnresolvedN3TokenClaims(txn *badger.Txn, from, to uint64, claims []UnresolvedN3TokenClaim) error {
	var stale [][]byte
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	prefix := []byte(unresolvedN3TokenClaimPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var c UnresolvedN3TokenClaim
		if err := it.Item().Value(func(v []byte) error { return json.Unmarshal(v, &c) }); err != nil {
			it.Close()
			return err
		}
		if c.Evidence.BlockHeight >= from && c.Evidence.BlockHeight <= to {
			stale = append(stale, it.Item().KeyCopy(nil))
		}
	}
	it.Close()
	for _, key := range stale {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return writeUnresolvedN3TokenClaims(txn, claims)
}

// replaceNeoXClaimTraceGaps makes [from, to] carry exactly the given gaps:
// a traced replay of a block resolves any earlier gap for it, because its
// claims were re-derived and written in the same transaction. Gap parts
// outside the range are kept.
func replaceNeoXClaimTraceGaps(txn *badger.Txn, from, to uint64, gaps []NeoXClaimTraceGap) error {
	var overlapping []NeoXClaimTraceGap
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	prefix := []byte(neoxClaimTraceGapPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var g NeoXClaimTraceGap
		if err := it.Item().Value(func(v []byte) error { return json.Unmarshal(v, &g) }); err != nil {
			it.Close()
			return err
		}
		if g.FromHeight <= to && g.ToHeight >= from {
			overlapping = append(overlapping, g)
		}
	}
	it.Close()
	for _, g := range overlapping {
		if err := txn.Delete([]byte(neoxClaimTraceGapKey(g.FromHeight))); err != nil {
			return err
		}
		if g.FromHeight < from {
			left := g
			left.ToHeight = from - 1
			if err := n3Set(txn, neoxClaimTraceGapKey(left.FromHeight), left); err != nil {
				return err
			}
		}
		if g.ToHeight > to {
			right := g
			right.FromHeight = to + 1
			if err := n3Set(txn, neoxClaimTraceGapKey(right.FromHeight), right); err != nil {
				return err
			}
		}
	}
	for _, g := range gaps {
		if err := n3Set(txn, neoxClaimTraceGapKey(g.FromHeight), g); err != nil {
			return err
		}
	}
	return nil
}
