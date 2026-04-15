package n3

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	neio "github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"

	"github.com/bane-labs/bridge-indexer-backend/pkg/blockcache"
)

func newTestBlockCache(t *testing.T) *blockcache.Cache {
	t.Helper()
	c, err := blockcache.New(t.TempDir(), slog.Default())
	if err != nil {
		t.Fatalf("blockcache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestBlockKeyRoundTrip(t *testing.T) {
	key := n3BlockKey(12345)
	want := "n3:blk:12345"
	if string(key) != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestAppLogKeyRoundTrip(t *testing.T) {
	hash, _ := util.Uint256DecodeStringLE("0000000000000000000000000000000000000000000000000000000000000001")
	key := n3AppLogKey(hash)
	want := "n3:applog:" + hash.StringLE()
	if string(key) != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestBlockCacheBinaryRoundTrip(t *testing.T) {
	bc := newTestBlockCache(t)

	blk := block.New(false)
	w := neio.NewBufBinWriter()
	blk.EncodeBinary(w.BinWriter)
	if w.Err != nil {
		t.Fatalf("encode: %v", w.Err)
	}
	data := w.Bytes()

	key := n3BlockKey(100)

	if _, ok := bc.Get(key); ok {
		t.Fatal("expected miss")
	}

	if err := bc.Put(key, data); err != nil {
		t.Fatalf("put: %v", err)
	}
	bc.Flush()

	got, ok := bc.Get(key)
	if !ok {
		t.Fatal("expected hit")
	}

	decoded := block.New(false)
	r := neio.NewBinReaderFromBuf(got)
	decoded.DecodeBinary(r)
	if r.Err != nil {
		t.Fatalf("decode cached block: %v", r.Err)
	}
}

func TestAppLogCacheHitMiss(t *testing.T) {
	bc := newTestBlockCache(t)

	appLog := &result.ApplicationLog{}
	data, err := json.Marshal(appLog)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	hash, _ := util.Uint256DecodeStringLE("0000000000000000000000000000000000000000000000000000000000000042")
	key := n3AppLogKey(hash)

	if _, ok := bc.Get(key); ok {
		t.Fatal("expected miss")
	}

	if err := bc.Put(key, data); err != nil {
		t.Fatalf("put: %v", err)
	}
	bc.Flush()

	got, ok := bc.Get(key)
	if !ok {
		t.Fatal("expected hit")
	}

	var decoded result.ApplicationLog
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal cached applog: %v", err)
	}
}
