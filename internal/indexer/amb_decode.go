package indexer

import (
	"fmt"
	"math/big"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// readFirstDynamicBytes returns the first ABI-encoded `bytes` argument in data.
// data is the non-indexed ABI tail for MessageSend(uint256,address,bytes,bytes,bytes32,bytes32).
func readFirstDynamicBytes(data []byte) ([]byte, error) {
	if len(data) < 64 {
		return nil, fmt.Errorf("MessageSend data too short")
	}
	off := new(big.Int).SetBytes(data[0:32]).Uint64()
	if int(off)+32 > len(data) {
		return nil, fmt.Errorf("invalid MessageSend offset")
	}
	length := new(big.Int).SetBytes(data[off : off+32]).Uint64()
	start := off + 32
	end := start + length
	if end > uint64(len(data)) {
		return nil, fmt.Errorf("invalid MessageSend bytes length")
	}
	return data[start:end], nil
}

// readAMBMessageType reads AMBTypes.MessageType from the first word of ABI-encoded metadata.
func readAMBMessageType(encodedMetadata []byte) (uint8, error) {
	if len(encodedMetadata) < 32 {
		return 0, fmt.Errorf("metadata too short")
	}
	v := new(big.Int).SetBytes(encodedMetadata[0:32]).Uint64()
	if v > 2 {
		return 0, fmt.Errorf("invalid message type %d", v)
	}
	return uint8(v), nil
}

func ambMessageTypeString(t uint8) string {
	switch t {
	case 0:
		return types.AMBMessageTypeExecutable
	case 1:
		return types.AMBMessageTypeStoreOnly
	case 2:
		return types.AMBMessageTypeResult
	default:
		return ""
	}
}

func ambTypePtrFromMessageSendData(data []byte) *string {
	enc, err := readFirstDynamicBytes(data)
	if err != nil {
		return nil
	}
	t, err := readAMBMessageType(enc)
	if err != nil {
		return nil
	}
	s := ambMessageTypeString(t)
	if s == "" {
		return nil
	}
	return &s
}
