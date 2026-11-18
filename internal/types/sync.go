package types

import "time"

// DirectionalSyncStatus describes source/destination sync state per directional instance.
type DirectionalSyncStatus string

const (
	DirectionalSyncStatusSynced       DirectionalSyncStatus = "synced"
	DirectionalSyncStatusSourceAhead  DirectionalSyncStatus = "source_ahead"
	DirectionalSyncStatusRootMismatch DirectionalSyncStatus = "root_mismatch"
	DirectionalSyncStatusUnknown      DirectionalSyncStatus = "unknown"
)

// DirectionalSyncReason provides machine-readable reasons for status derivation.
type DirectionalSyncReason string

const (
	DirectionalSyncReasonSynced                   DirectionalSyncReason = "SYNCED"
	DirectionalSyncReasonSourceNonceGreater       DirectionalSyncReason = "SOURCE_NONCE_GREATER_THAN_DESTINATION"
	DirectionalSyncReasonRootMismatchAtEqualNonce DirectionalSyncReason = "ROOT_MISMATCH_AT_EQUAL_NONCE"
	DirectionalSyncReasonMissingState             DirectionalSyncReason = "MISSING_STATE"
	DirectionalSyncReasonInvariantViolation       DirectionalSyncReason = "INVARIANT_VIOLATION"
)

// DirectionalInstanceSync contains derived sync for a single directional bridge instance.
type DirectionalInstanceSync struct {
	ID         string     `json:"id"`
	BridgeType BridgeType `json:"bridge_type"`
	SrcChain   string     `json:"src_chain"`
	DstChain   string     `json:"dst_chain"`
	SrcToken   *string    `json:"src_token,omitempty"`
	DstToken   *string    `json:"dst_token,omitempty"`
	// Present for token bridge instances when the server can resolve symbol() via RPC.
	SrcTokenSymbol *string               `json:"src_token_symbol,omitempty"`
	DstTokenSymbol *string               `json:"dst_token_symbol,omitempty"`
	Status         DirectionalSyncStatus `json:"status"`
	DeltaNonce     *int64                `json:"delta_nonce,omitempty"`
	SrcNonce       *uint64               `json:"src_nonce,omitempty"`
	DstNonce       *uint64               `json:"dst_nonce,omitempty"`
	SrcRoot        *string               `json:"src_root,omitempty"`
	DstRoot        *string               `json:"dst_root,omitempty"`
	// Side metadata is present only when the corresponding state has been observed.
	SrcBlockNumber *uint64               `json:"src_block_number,omitempty"`
	DstBlockNumber *uint64               `json:"dst_block_number,omitempty"`
	SrcObservedAt  *time.Time            `json:"src_observed_at,omitempty"`
	DstObservedAt  *time.Time            `json:"dst_observed_at,omitempty"`
	Reason         DirectionalSyncReason `json:"reason"`
}
