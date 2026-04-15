package indexer

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestReadFirstDynamicBytes_MessageSend_ABI(t *testing.T) {
	meta := common.LeftPadBytes(new(big.Int).SetUint64(2).Bytes(), 32) // RESULT
	data := buildMessageSendLogData(meta, []byte{1, 2, 3}, common.HexToHash("0xaa"), common.HexToHash("0xbb"))
	got, err := readFirstDynamicBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, meta) {
		t.Fatalf("metadata mismatch: got %x want %x", got, meta)
	}
	t.Run("type", func(t *testing.T) {
		ty, err := readAMBMessageType(got)
		if err != nil {
			t.Fatal(err)
		}
		if ty != 2 {
			t.Fatalf("type %d, want 2 (RESULT)", ty)
		}
	})
}
