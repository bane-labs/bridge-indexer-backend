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
	TokenContract       *string         `json:"token_contract,omitempty"` // nil for native bridge
	Amount              *string         `json:"amount,omitempty"`         // string to avoid big.Int JSON issues
	FromAddress         string          `json:"from_address"`
	ToAddress           string          `json:"to_address"`
	Timestamp           time.Time       `json:"timestamp"`                      // source event time
	CompletionTimestamp *time.Time      `json:"completion_timestamp,omitempty"` // destination event time
	Status              OperationStatus `json:"status"`
}
