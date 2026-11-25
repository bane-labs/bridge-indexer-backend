package indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/datastore"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// n3Notification includes the position within the committed application log.
type n3Notification struct {
	event    state.NotificationEvent
	position string
}

func n3Identity(bt types.BridgeType, nonce uint64, source, dest string) datastore.OperationIdentity {
	return datastore.OperationIdentity{BridgeType: bt, Direction: types.DirectionWithdrawal, Nonce: nonce, SourceToken: source, DestinationToken: dest}
}

func n3Evidence(event state.NotificationEvent, position, blockHash, txHash string, height uint32, timestamp time.Time, payload any) (datastore.LifecycleEvidence, error) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return datastore.LifecycleEvidence{}, err
		}
		raw = b
	}
	return datastore.LifecycleEvidence{Kind: "notification", Chain: chainN3, Contract: event.ScriptHash.StringLE(), BlockHeight: uint64(height), BlockHash: blockHash, TxHash: txHash, ObservedAt: timestamp, Position: position, Payload: raw}, nil
}

func n3Items(e state.NotificationEvent, length int) ([]stackitem.Item, error) {
	items, err := getArrayItems(e.Item)
	if err != nil {
		return nil, err
	}
	if len(items) != length {
		return nil, fmt.Errorf("%s: expected %d fields, got %d", e.Name, length, len(items))
	}
	return items, nil
}
func n3Root(items []stackitem.Item, at int) (string, error) {
	b, err := items[at].TryBytes()
	if err != nil || len(b) != 32 {
		return "", fmt.Errorf("invalid received root: %v, length %d", err, len(b))
	}
	return fmt.Sprintf("%x", b), nil
}
func n3Token(items []stackitem.Item, at int) (string, error) {
	b, err := items[at].TryBytes()
	if err != nil || len(b) != 20 {
		return "", fmt.Errorf("invalid token hash: %v, length %d", err, len(b))
	}
	h, err := util.Uint160DecodeBytesBE(b)
	if err != nil {
		return "", err
	}
	return h.StringLE(), nil
}
func n3MessageType(data []byte) (string, error) {
	item, err := stackitem.Deserialize(data)
	if err != nil {
		return "", err
	}
	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) < 1 {
		return "", fmt.Errorf("invalid serialized message metadata")
	}
	t, err := itemToUint64(arr[0])
	if err != nil {
		return "", err
	}
	switch t {
	case 0:
		return "execute", nil
	case 1:
		return "store", nil
	case 2:
		return "return", nil
	default:
		return "", fmt.Errorf("unsupported message type %d", t)
	}
}

type n3RootObservation struct {
	id       datastore.OperationIdentity
	nonce    uint64
	position int
	evidence datastore.LifecycleEvidence
}
type n3ExecutionGroup struct {
	count     uint64
	first     datastore.LifecycleEvidence
	chunks    map[uint64][]byte
	positions map[uint64]string
	single    stackitem.Item
}

// decodeN3LifecycleTransaction consumes all notifications from a HALT transaction.
// A root is independent of the eventual outcome, including in batched relays.
func decodeN3LifecycleTransaction(notifications []n3Notification, tokenBridge, messageBridge util.Uint160, height uint32, blockHash, txHash string, timestamp time.Time) ([]datastore.LifecycleObservation, error) {
	var observations []datastore.LifecycleObservation
	var roots []n3RootObservation
	groups := make(map[uint64]*n3ExecutionGroup)
	add := func(id datastore.OperationIdentity, fact datastore.LifecycleFact, e datastore.LifecycleEvidence, typ string) {
		observations = append(observations, datastore.LifecycleObservation{Identity: id, Fact: fact, Evidence: e, MessageType: typ})
	}
	for i, n := range notifications {
		e := n.event
		isToken := e.ScriptHash.Equals(tokenBridge)
		isMessage := e.ScriptHash.Equals(messageBridge)
		if !isToken && !isMessage {
			continue
		}
		rootType := isToken && (e.Name == "NativeWithdrawalRootUpdate" || e.Name == "TokenWithdrawalRootUpdate") || isMessage && e.Name == "EvmToNeoRootUpdate"
		if !rootType {
			continue
		}
		length := 2
		nonceAt := 0
		rootAt := 1
		bt := types.BridgeTypeNative
		src, dst := "", ""
		if e.Name == "TokenWithdrawalRootUpdate" {
			length = 4
			nonceAt = 2
			rootAt = 3
			bt = types.BridgeTypeToken
		}
		if isMessage {
			bt = types.BridgeTypeMessage
		}
		items, err := n3Items(e, length)
		if err != nil {
			return nil, err
		}
		if bt == types.BridgeTypeToken {
			dst, err = n3Token(items, 0)
			if err != nil {
				return nil, err
			}
			src, err = n3Token(items, 1)
			if err != nil {
				return nil, err
			}
		}
		nonce, err := itemToUint64(items[nonceAt])
		if err != nil {
			return nil, err
		}
		root, err := n3Root(items, rootAt)
		if err != nil {
			return nil, err
		}
		evidence, err := n3Evidence(e, n.position, blockHash, txHash, height, timestamp, map[string]string{"root": root})
		if err != nil {
			return nil, err
		}
		id := n3Identity(bt, nonce, src, dst)
		roots = append(roots, n3RootObservation{id, nonce, i, evidence})
		add(id, datastore.FactSettlement, evidence, "")
	}
	findRoot := func(bt types.BridgeType, nonce uint64, dst string, position int) *n3RootObservation {
		var matched *n3RootObservation
		for i := range roots {
			r := &roots[i]
			if r.position < position && r.id.BridgeType == bt && r.nonce >= nonce && (bt != types.BridgeTypeToken || r.id.DestinationToken == dst) {
				matched = r
			}
		}
		return matched
	}
	for i, n := range notifications {
		e := n.event
		isToken := e.ScriptHash.Equals(tokenBridge)
		isMessage := e.ScriptHash.Equals(messageBridge)
		if !isToken && !isMessage {
			continue
		}
		switch {
		case isToken && (e.Name == "NativeWithdrawal" || e.Name == "NativeClaimable" || e.Name == "NativeClaim" || e.Name == "TokenWithdrawal" || e.Name == "TokenClaimable" || e.Name == "TokenClaim"):
			bt := types.BridgeTypeNative
			length := 3
			nonceAt := 0
			dst := ""
			var err error
			if e.Name == "TokenWithdrawal" || e.Name == "TokenClaimable" || e.Name == "TokenClaim" {
				bt = types.BridgeTypeToken
				length = 4
				nonceAt = 1
			}
			items, err := n3Items(e, length)
			if err != nil {
				return nil, err
			}
			if bt == types.BridgeTypeToken {
				dst, err = n3Token(items, 0)
				if err != nil {
					return nil, err
				}
			}
			nonce, err := itemToUint64(items[nonceAt])
			if err != nil {
				return nil, err
			}
			recipient, err := n3Token(items, nonceAt+1)
			if err != nil {
				return nil, err
			}
			amount, err := itemToBigIntString(items[nonceAt+2])
			if err != nil {
				return nil, err
			}
			id := n3Identity(bt, nonce, "", dst)
			isClaim := e.Name == "NativeClaim" || e.Name == "TokenClaim"
			r := findRoot(bt, nonce, dst, i)
			if !isClaim && r == nil {
				return nil, fmt.Errorf("%s nonce %d without received root in %s", e.Name, nonce, txHash)
			}
			if r != nil {
				id.SourceToken = r.id.SourceToken
				id.DestinationToken = r.id.DestinationToken
			}
			// A claim can occur in a later transaction. Its pair is resolved from
			// already persisted claimable evidence by the indexer before committing.
			evidence, err := n3Evidence(e, n.position, blockHash, txHash, height, timestamp, map[string]string{"recipient": recipient, "amount": amount})
			if err != nil {
				return nil, err
			}
			fact := datastore.FactDistribution
			if e.Name == "NativeClaimable" || e.Name == "TokenClaimable" {
				fact = datastore.FactClaimable
			}
			if isClaim {
				fact = datastore.FactClaim
			} else if nonce != r.nonce {
				add(id, datastore.FactSettlement, r.evidence, "")
			}
			add(id, fact, evidence, "")
		case isMessage && e.Name == "Store":
			items, err := n3Items(e, 2)
			if err != nil {
				return nil, err
			}
			nonce, err := itemToUint64(items[0])
			if err != nil {
				return nil, err
			}
			b, err := items[1].TryBytes()
			if err != nil {
				return nil, err
			}
			// N3 metadata is StdLib-serialized, never EVM ABI encoded.
			if _, err = n3MessageType(b); err != nil {
				return nil, fmt.Errorf("Store metadata: %w", err)
			}
			r := findRoot(types.BridgeTypeMessage, nonce, "", i)
			if r == nil {
				return nil, fmt.Errorf("Store nonce %d without received root", nonce)
			}
			id := n3Identity(types.BridgeTypeMessage, nonce, "", "")
			var received struct {
				Root string `json:"root"`
			}
			if err := json.Unmarshal(r.evidence.Payload, &received); err != nil {
				return nil, err
			}
			perMessage := r.evidence
			perMessage.Payload, err = json.Marshal(map[string]any{"root": received.Root, "store": map[string]string{"metadata": fmt.Sprintf("%x", b), "position": n.position, "contract": e.ScriptHash.StringLE()}})
			if err != nil {
				return nil, err
			}
			if nonce != r.nonce {
				add(id, datastore.FactSettlement, perMessage, "")
			} else {
				// Upgrade the received-root evidence with this message's verified
				// Store metadata while keeping its root transaction and position.
				found := false
				for j := range observations {
					if observations[j].Fact == datastore.FactSettlement && observations[j].Identity == id {
						if found {
							return nil, fmt.Errorf("duplicate Store nonce %d", nonce)
						}
						observations[j].Evidence = perMessage
						found = true
					}
				}
				if !found {
					return nil, fmt.Errorf("missing root evidence for Store nonce %d", nonce)
				}
			}
		case isMessage && e.Name == "Execution":
			items, err := n3Items(e, 4)
			if err != nil {
				return nil, err
			}
			nonce, err := itemToUint64(items[0])
			if err != nil {
				return nil, err
			}
			count, err := itemToUint64(items[1])
			if err != nil {
				return nil, err
			}
			index, err := itemToUint64(items[2])
			if err != nil {
				return nil, err
			}
			if count == 0 || index >= count || count > 100000 {
				return nil, fmt.Errorf("invalid Execution chunk %d/%d", index, count)
			}
			ev, err := n3Evidence(e, n.position, blockHash, txHash, height, timestamp, nil)
			if err != nil {
				return nil, err
			}
			g := groups[nonce]
			if g == nil {
				g = &n3ExecutionGroup{count: count, first: ev, chunks: make(map[uint64][]byte), positions: make(map[uint64]string)}
				groups[nonce] = g
			}
			if g.count != count {
				return nil, fmt.Errorf("inconsistent Execution chunk counts nonce %d", nonce)
			}
			if count == 1 {
				if g.single != nil {
					previous, err := stackitem.ToJSONWithTypes(g.single)
					if err != nil {
						return nil, err
					}
					current, err := stackitem.ToJSONWithTypes(items[3])
					if err != nil {
						return nil, err
					}
					if g.first.Position != n.position || !bytes.Equal(previous, current) {
						return nil, fmt.Errorf("conflicting single Execution nonce %d", nonce)
					}
					continue
				}
				g.single = items[3]
				continue
			}
			b, ok := items[3].(*stackitem.ByteArray)
			if !ok {
				return nil, fmt.Errorf("Execution chunk %d must be byte string", index)
			}
			if previous, exists := g.chunks[index]; exists {
				if !bytes.Equal(previous, []byte(*b)) || g.positions[index] != n.position {
					return nil, fmt.Errorf("conflicting Execution chunk %d", index)
				}
				continue
			}
			g.chunks[index] = append([]byte(nil), (*b)...)
			g.positions[index] = n.position
		case isMessage && e.Name == "MessageSend":
			items, err := n3Items(e, 5)
			if err != nil {
				return nil, err
			}
			nonce, err := itemToUint64(items[0])
			if err != nil {
				return nil, err
			}
			metadata, err := items[1].TryBytes()
			if err != nil {
				return nil, err
			}
			typ, err := n3MessageType(metadata)
			if err != nil {
				return nil, fmt.Errorf("MessageSend metadata: %w", err)
			}
			evidence, err := n3Evidence(e, n.position, blockHash, txHash, height, timestamp, map[string]string{"metadata": fmt.Sprintf("%x", metadata)})
			if err != nil {
				return nil, err
			}
			id := datastore.OperationIdentity{BridgeType: types.BridgeTypeMessage, Direction: types.DirectionDeposit, Nonce: nonce}
			add(id, datastore.FactMessageType, evidence, typ)
		}
	}
	keys := make([]uint64, 0, len(groups))
	for nonce := range groups {
		keys = append(keys, nonce)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, nonce := range keys {
		g := groups[nonce]
		var result stackitem.Item
		var slices []map[string]any
		if g.count == 1 {
			result = g.single
		} else {
			var serialized []byte
			if uint64(len(g.chunks)) != g.count {
				return nil, fmt.Errorf("incomplete Execution nonce %d: %d/%d", nonce, len(g.chunks), g.count)
			}
			for index := uint64(0); index < g.count; index++ {
				b := g.chunks[index]
				serialized = append(serialized, b...)
				slices = append(slices, map[string]any{"index": index, "position": g.positions[index], "bytes": fmt.Sprintf("%x", b)})
			}
			var err error
			result, err = stackitem.Deserialize(serialized)
			if err != nil {
				return nil, fmt.Errorf("deserialize Execution nonce %d: %w", nonce, err)
			}
		}
		typed, err := stackitem.ToJSONWithTypes(result)
		if err != nil {
			return nil, err
		}
		payload, err := json.Marshal(map[string]any{"result": json.RawMessage(typed), "chunks": slices})
		if err != nil {
			return nil, err
		}
		ev := g.first
		ev.Payload = payload
		id := n3Identity(types.BridgeTypeMessage, nonce, "", "")
		add(id, datastore.FactExecution, ev, "")
	}
	return observations, nil
}
