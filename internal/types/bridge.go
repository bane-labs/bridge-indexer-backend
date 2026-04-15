package types

import "time"

// BridgeType represents the three bridge types (PD §2).
type BridgeType string

const (
	BridgeTypeNative  BridgeType = "native"
	BridgeTypeToken   BridgeType = "token"
	BridgeTypeMessage BridgeType = "message"
)

// Direction represents transfer direction (PD §3).
type Direction string

const (
	DirectionDeposit    Direction = "deposit"    // N3 → NeoX
	DirectionWithdrawal Direction = "withdrawal" // NeoX → N3
)

// OperationStatus represents the lifecycle state of a bridge operation.
type OperationStatus string

const (
	StatusPending   OperationStatus = "pending"
	StatusCompleted OperationStatus = "completed"
	StatusStuck     OperationStatus = "stuck"
)

// AMB message kinds from NeoX MessageSend encodedMetadata (AMBTypes.MessageType).
const (
	AMBMessageTypeExecutable = "executable"
	AMBMessageTypeStoreOnly  = "store_only"
	AMBMessageTypeResult     = "result"
)

// MessageAMBTypeSkipsNeoXWithdrawalDest returns true when this NeoX→N3 message
// does not expect a matching N3 Execution completion (STORE_ONLY / RESULT).
func MessageAMBTypeSkipsNeoXWithdrawalDest(t *string) bool {
	if t == nil {
		return false
	}
	switch *t {
	case AMBMessageTypeStoreOnly, AMBMessageTypeResult:
		return true
	default:
		return false
	}
}

// BridgeOperation represents a single bridge operation with all 15 PD §4.2
// indexed fields plus Status.
type BridgeOperation struct {
	BridgeType          BridgeType      `json:"bridge_type"`
	Direction           Direction       `json:"direction"`
	Nonce               uint64          `json:"nonce"`
	SourceChain         string          `json:"source_chain"`      // "neo_n3" or "neo_x"
	DestinationChain    string          `json:"destination_chain"` // "neo_n3" or "neo_x"
	SourceBlockHeight   uint64          `json:"source_block_height"`
	DestBlockHeight     *uint64         `json:"dest_block_height,omitempty"`
	SourceTxHash        string          `json:"source_tx_hash"`
	DestTxHash          *string         `json:"dest_tx_hash,omitempty"`
	TokenContract       *string         `json:"token_contract,omitempty"`      // source chain token; nil for native bridge
	DestTokenContract   *string         `json:"dest_token_contract,omitempty"` // destination chain token; nil for native bridge
	Root                *string         `json:"root,omitempty"`                // deposit/withdrawal root hash from event
	Amount              *string         `json:"amount,omitempty"`              // string to avoid big.Int JSON issues
	FromAddress         string          `json:"from_address"`
	ToAddress           string          `json:"to_address"`
	Timestamp           time.Time       `json:"timestamp"`                      // source event time
	CompletionTimestamp *time.Time      `json:"completion_timestamp,omitempty"` // destination event time
	Status              OperationStatus `json:"status"`
	// AMBMessageType is set for NeoX MessageSend events (executable | store_only | result).
	// Omitted when unknown or for non-message operations.
	AMBMessageType *string `json:"amb_message_type,omitempty"`
}

// ChainState represents the latest observed state on one side of a directional bridge.
type ChainState struct {
	Nonce       uint64    `json:"nonce"`
	Root        *string   `json:"root,omitempty"`
	BlockNumber uint64    `json:"block_number"`
	TxHash      string    `json:"tx_hash"`
	ObservedAt  time.Time `json:"observed_at"` // block timestamp
	IndexedAt   time.Time `json:"indexed_at"`  // backend time
}

// DirectionalBridgeState represents the aggregate latest state for a specific
// bridge type and direction. For token bridges, src/dst token addresses are
// included to distinguish between different token pairs.
type DirectionalBridgeState struct {
	InstanceID  string      `json:"instance_id"`
	BridgeType  BridgeType  `json:"bridge_type"`
	SrcChain    string      `json:"src_chain"`
	DstChain    string      `json:"dst_chain"`
	SrcToken    *string     `json:"src_token,omitempty"`
	DstToken    *string     `json:"dst_token,omitempty"`
	SourceState *ChainState `json:"source_state,omitempty"`
	DestState   *ChainState `json:"dest_state,omitempty"`
}
