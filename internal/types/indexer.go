package types

import "time"

// IndexerState tracks the indexing progress for a blockchain (PD §4.1).
type IndexerState struct {
	Chain           string    `json:"chain"` // "neo_n3" or "neo_x"
	LastBlockHeight uint64    `json:"last_block_height"`
	LastBlockHash   string    `json:"last_block_hash"` // for reorg detection
	IsSyncing       bool      `json:"is_syncing"`
	UpdatedAt       time.Time `json:"updated_at"`
}
