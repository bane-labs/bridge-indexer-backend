package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	badger "github.com/dgraph-io/badger/v4"
)

// Lifecycle data lives beside legacy op:* records. A missing observation is
// unknown, not a negative lifecycle state. In particular, neither legacy
// completion nor a synthetic directional state is settlement evidence.
type OperationIdentity struct {
	BridgeType       types.BridgeType `json:"bridge_type"`
	Direction        types.Direction  `json:"direction"`
	Nonce            uint64           `json:"nonce"`
	SourceToken      string           `json:"source_token,omitempty"`
	DestinationToken string           `json:"destination_token,omitempty"`
}

func (id OperationIdentity) key() (string, error) {
	if id.Direction != types.DirectionDeposit && id.Direction != types.DirectionWithdrawal {
		return "", fmt.Errorf("invalid direction %q", id.Direction)
	}
	if id.BridgeType != types.BridgeTypeNative && id.BridgeType != types.BridgeTypeToken && id.BridgeType != types.BridgeTypeMessage {
		return "", fmt.Errorf("invalid bridge type %q", id.BridgeType)
	}
	base := fmt.Sprintf("lifecycle:v1:%s:%s:%d", id.BridgeType, id.Direction, id.Nonce)
	if id.BridgeType == types.BridgeTypeToken {
		if strings.ContainsAny(id.SourceToken, ":\x00") || strings.ContainsAny(id.DestinationToken, ":\x00") {
			return "", errors.New("token lifecycle contracts contain an invalid key separator")
		}
		if normalizeTokenKeyPart(id.SourceToken) == "unknown" || normalizeTokenKeyPart(id.DestinationToken) == "unknown" {
			return "", errors.New("token lifecycle requires both token contracts")
		}
		return base + ":" + normalizeTokenKeyPart(id.SourceToken) + ":" + normalizeTokenKeyPart(id.DestinationToken), nil
	}
	if id.SourceToken != "" || id.DestinationToken != "" {
		return "", errors.New("non-token lifecycle cannot include token contracts")
	}
	return base, nil
}

// LifecycleEvidence records the original, verifiable chain position. Payload
// can retain a typed execution result or raw event/call details without
// imposing an ABI on the datastore. Call positions can identify silent claims.
type LifecycleEvidence struct {
	Kind           string          `json:"kind"` // root, event, notification, call, trace
	Chain          string          `json:"chain"`
	Contract       string          `json:"contract"`
	Implementation string          `json:"implementation,omitempty"`
	BlockHeight    uint64          `json:"block_height"`
	BlockHash      string          `json:"block_hash"`
	TxHash         string          `json:"tx_hash"`
	ObservedAt     time.Time       `json:"observed_at"`
	Position       string          `json:"position"` // log index, notification index, or call path
	Payload        json.RawMessage `json:"payload,omitempty"`
}

// OperationLifecycle records independent positive observations. Claimable is
// historical evidence and remains present after a later claim. Execution is
// the committed action, even if a message call result reports failure.
type OperationLifecycle struct {
	Settlement          *LifecycleEvidence `json:"settlement,omitempty"`
	Distribution        *LifecycleEvidence `json:"distribution,omitempty"`
	Claimable           *LifecycleEvidence `json:"claimable,omitempty"`
	Claim               *LifecycleEvidence `json:"claim,omitempty"`
	Execution           *LifecycleEvidence `json:"execution,omitempty"`
	MessageType         *string            `json:"message_type,omitempty"` // execute, store, return
	MessageTypeEvidence *LifecycleEvidence `json:"message_type_evidence,omitempty"`
}

type LifecycleFact string

const (
	FactSettlement   LifecycleFact = "settlement"
	FactDistribution LifecycleFact = "distribution"
	FactClaimable    LifecycleFact = "claimable"
	FactClaim        LifecycleFact = "claim"
	FactExecution    LifecycleFact = "execution"
	FactMessageType  LifecycleFact = "message_type"
)

type LifecycleObservation struct {
	Identity    OperationIdentity
	Fact        LifecycleFact
	Evidence    LifecycleEvidence
	MessageType string // only for FactMessageType
}

// JSON persistence does not retain a time.Time's Location pointer. Compare
// instants so a replay of the same chain observation remains idempotent.
func sameEvidence(a, b *LifecycleEvidence) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Kind == b.Kind && a.Chain == b.Chain && a.Contract == b.Contract &&
		a.Implementation == b.Implementation && a.BlockHeight == b.BlockHeight &&
		a.BlockHash == b.BlockHash && a.TxHash == b.TxHash &&
		a.ObservedAt.Equal(b.ObservedAt) && a.Position == b.Position &&
		bytes.Equal(a.Payload, b.Payload)
}

func validateEvidence(e LifecycleEvidence) error {
	switch e.Kind {
	case "root", "event", "notification", "call", "trace":
	default:
		return fmt.Errorf("invalid evidence kind %q", e.Kind)
	}
	if (e.Chain != "neo_n3" && e.Chain != "neo_x") || e.Contract == "" || e.BlockHash == "" || e.TxHash == "" || e.ObservedAt.IsZero() || e.Position == "" {
		return errors.New("evidence requires chain, contract, block hash, transaction hash, timestamp, and position")
	}
	if len(e.Payload) > 0 && !json.Valid(e.Payload) {
		return errors.New("invalid evidence payload JSON")
	}
	return nil
}

func applyObservation(l *OperationLifecycle, o LifecycleObservation) error {
	if err := validateEvidence(o.Evidence); err != nil {
		return err
	}
	if o.Fact != FactMessageType {
		destination := "neo_x"
		if o.Identity.Direction == types.DirectionWithdrawal {
			destination = "neo_n3"
		}
		if o.Evidence.Chain != destination {
			return errors.New("destination lifecycle evidence is on the wrong chain")
		}
	} else {
		source := "neo_n3"
		if o.Identity.Direction == types.DirectionWithdrawal {
			source = "neo_x"
		}
		if o.Evidence.Chain != source {
			return errors.New("message type evidence is on the wrong source chain")
		}
	}
	var dst **LifecycleEvidence
	switch o.Fact {
	case FactSettlement:
		dst = &l.Settlement
	case FactDistribution:
		if o.Identity.BridgeType == types.BridgeTypeMessage {
			return errors.New("message operation cannot have asset distribution")
		}
		dst = &l.Distribution
	case FactClaimable:
		if o.Identity.BridgeType == types.BridgeTypeMessage {
			return errors.New("message operation cannot be claimable")
		}
		dst = &l.Claimable
	case FactClaim:
		if o.Identity.BridgeType == types.BridgeTypeMessage {
			return errors.New("message operation cannot have a token/native claim")
		}
		dst = &l.Claim
	case FactExecution:
		if o.Identity.BridgeType == types.BridgeTypeMessage && l.MessageType != nil && *l.MessageType != "execute" {
			return errors.New("non-executable message cannot execute")
		}
		dst = &l.Execution
	case FactMessageType:
		if o.Identity.BridgeType != types.BridgeTypeMessage || (o.MessageType != "execute" && o.MessageType != "store" && o.MessageType != "return") {
			return errors.New("invalid message type observation")
		}
		if l.MessageType != nil && (*l.MessageType != o.MessageType || !sameEvidence(l.MessageTypeEvidence, &o.Evidence)) {
			return errors.New("conflicting message type observation")
		}
		if o.MessageType != "execute" && l.Execution != nil {
			return errors.New("executed message cannot be store-only or result")
		}
		t := o.MessageType
		l.MessageType, l.MessageTypeEvidence = &t, &o.Evidence
		return nil
	default:
		return fmt.Errorf("invalid lifecycle fact %q", o.Fact)
	}
	if o.MessageType != "" {
		return errors.New("message type only applies to message type observation")
	}
	if *dst != nil && !sameEvidence(*dst, &o.Evidence) {
		return fmt.Errorf("conflicting %s evidence", o.Fact)
	}
	*dst = &o.Evidence
	return nil
}

// GetLifecycle does not infer anything from op:* or a missing sidecar.
func (os *OperationStore) GetLifecycle(id OperationIdentity) (*OperationLifecycle, error) {
	key, err := id.key()
	if err != nil {
		return nil, err
	}
	var result OperationLifecycle
	if err := os.store.Get(key, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// FindClaimableTokenPair only resolves a claim when one previously observed
// claimable identifies both sides of its token pair. Historical gaps stay unknown.
func (os *OperationStore) FindClaimableTokenPair(nonce uint64, destinationToken string) (*OperationIdentity, error) {
	return os.FindClaimableTokenPairForDirection(types.DirectionDeposit, nonce, destinationToken)
}

// FindClaimableTokenPairForDirection only uses verified claimable evidence.
func (os *OperationStore) FindClaimableTokenPairForDirection(direction types.Direction, nonce uint64, destinationToken string) (*OperationIdentity, error) {
	if direction != types.DirectionDeposit && direction != types.DirectionWithdrawal {
		return nil, fmt.Errorf("invalid claim direction %q", direction)
	}
	prefix := fmt.Sprintf("lifecycle:v1:token:%s:%d:", direction, nonce)
	var found *OperationIdentity
	err := os.store.ScanPrefix(prefix, func(key string, value []byte) error {
		parts := strings.Split(strings.TrimPrefix(key, prefix), ":")
		if len(parts) != 2 || parts[1] != normalizeTokenKeyPart(destinationToken) {
			return nil
		}
		var l OperationLifecycle
		if err := json.Unmarshal(value, &l); err != nil {
			return err
		}
		if l.Claimable == nil {
			return nil
		}
		if found != nil {
			return errors.New("ambiguous token pair for claim")
		}
		found = &OperationIdentity{BridgeType: types.BridgeTypeToken, Direction: direction,
			Nonce: nonce, SourceToken: parts[0], DestinationToken: parts[1]}
		return nil
	})
	return found, err
}

// ApplyLifecycleObservations is atomic and serializes competing writers via
// Badger's conflict detection. A destination observation can precede op:*.
// Failed or reverted attempts must never be passed as positive observations.
func (os *OperationStore) ApplyLifecycleObservations(observations []LifecycleObservation) error {
	if len(observations) == 0 {
		return nil
	}
	return retryLifecycleWrite(os.store.db, func(txn *badger.Txn) error {
		return writeLifecycleObservations(txn, observations)
	})
}

func writeLifecycleObservations(txn *badger.Txn, observations []LifecycleObservation) error {
	changed := make(map[string]*OperationLifecycle)
	for _, o := range observations {
		key, err := o.Identity.key()
		if err != nil {
			return err
		}
		l := changed[key]
		if l == nil {
			l = &OperationLifecycle{}
			item, err := txn.Get([]byte(key))
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if err == nil {
				if err := item.Value(func(v []byte) error { return json.Unmarshal(v, l) }); err != nil {
					return err
				}
			}
			changed[key] = l
		}
		if err := applyObservation(l, o); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	for key, l := range changed {
		data, err := json.Marshal(l)
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(key), data); err != nil {
			return err
		}
	}
	return nil
}
