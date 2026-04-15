package neox

import (
	"encoding/json"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"

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

func TestHeaderKeyRoundTrip(t *testing.T) {
	key := neoxHeaderKey(67890)
	want := "neox:hdr:67890"
	if string(key) != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestHeaderCacheHitMiss(t *testing.T) {
	bc := newTestBlockCache(t)

	hdr := &types.Header{
		Number: big.NewInt(500),
		Time:   1700000000,
	}
	data, err := rlp.EncodeToBytes(hdr)
	if err != nil {
		t.Fatalf("rlp encode: %v", err)
	}

	key := neoxHeaderKey(500)

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

	var decoded types.Header
	if err := rlp.DecodeBytes(got, &decoded); err != nil {
		t.Fatalf("rlp decode: %v", err)
	}
	if decoded.Number.Uint64() != 500 {
		t.Fatalf("expected block 500, got %d", decoded.Number.Uint64())
	}
	if decoded.Time != 1700000000 {
		t.Fatalf("expected time 1700000000, got %d", decoded.Time)
	}
}

func TestFilterLogsCaching(t *testing.T) {
	bc := newTestBlockCache(t)

	c := &Client{cache: bc, logger: slog.Default()}

	logs := []types.Log{
		{BlockNumber: 10, TxHash: common.HexToHash("0x01"), Address: common.HexToAddress("0xAA"), Topics: []common.Hash{}},
		{BlockNumber: 10, TxHash: common.HexToHash("0x02"), Address: common.HexToAddress("0xAA"), Topics: []common.Hash{}},
		{BlockNumber: 12, TxHash: common.HexToHash("0x03"), Address: common.HexToAddress("0xBB"), Topics: []common.Hash{}},
	}

	// Cache logs for range [10, 15]
	c.cacheFilterLogs(10, 15, logs)
	bc.Flush()

	// Watermark should be >= 15
	wm := bc.GetWatermark(neoxWatermarkKey)
	if wm < 15 {
		t.Fatalf("expected watermark >= 15, got %d", wm)
	}
	if gotFrom := bc.GetWatermark(neoxWatermarkFromKey); gotFrom != 10 {
		t.Fatalf("expected watermark-from 10, got %d", gotFrom)
	}

	// Verify JSON round-trip for block 10 logs
	{
		data, ok := bc.Get(neoxLogsKey(10))
		if !ok {
			t.Fatal("block 10 missing from cache")
		}
		var roundTrip []types.Log
		if err := json.Unmarshal(data, &roundTrip); err != nil {
			t.Fatalf("json round-trip failed for block 10: %v", err)
		}
		if len(roundTrip) != 2 {
			t.Fatalf("expected 2 logs in block 10, got %d", len(roundTrip))
		}
	}

	// Read back from cache
	cached, ok := c.logsFromCache(10, 15)
	if !ok {
		t.Fatal("expected cache hit for [10,15]")
	}
	if len(cached) != 3 {
		t.Fatalf("expected 3 logs, got %d", len(cached))
	}

	// Empty blocks should NOT have cache entries (watermark-only approach)
	{
		_, ok := bc.Get(neoxLogsKey(11))
		if ok {
			t.Fatal("expected no cache entry for empty block 11 (watermark handles it)")
		}
	}
}

func TestCacheRangeCoveredRequiresLowerBoundMetadata(t *testing.T) {
	bc := newTestBlockCache(t)
	c := &Client{cache: bc, logger: slog.Default(), cacheReads: true}

	// Simulate legacy cache containing only the upper watermark metadata.
	if err := bc.SetWatermark(neoxWatermarkKey, 200); err != nil {
		t.Fatalf("set wm: %v", err)
	}
	bc.Flush()

	if c.cacheRangeCovered(0, 100) {
		t.Fatal("expected cache range not covered without lower-bound metadata")
	}

	// Once lower-bound watermark exists, covered ranges become readable.
	if err := bc.SetWatermark(neoxWatermarkFromKey, 0); err != nil {
		t.Fatalf("set wm_from: %v", err)
	}
	bc.Flush()

	if !c.cacheRangeCovered(0, 100) {
		t.Fatal("expected cache range covered with both watermarks")
	}
	if c.cacheRangeCovered(0, 250) {
		t.Fatal("expected range beyond upper watermark to be uncovered")
	}
}

func TestLogsFromCacheEmptyRange(t *testing.T) {
	bc := newTestBlockCache(t)

	c := &Client{cache: bc, logger: slog.Default()}

	// Cache range [10, 12] with no events
	c.cacheFilterLogs(10, 12, nil)
	bc.Flush()

	// logsFromCache returns empty slice for range with no events
	cached, ok := c.logsFromCache(10, 12)
	if !ok {
		t.Fatal("expected cache hit for range within watermark")
	}
	if len(cached) != 0 {
		t.Fatalf("expected 0 logs for empty range, got %d", len(cached))
	}
}

func TestWatermarkProgression(t *testing.T) {
	bc := newTestBlockCache(t)

	c := &Client{cache: bc, logger: slog.Default()}

	c.cacheFilterLogs(0, 100, nil)
	bc.Flush()
	if wm := bc.GetWatermark(neoxWatermarkKey); wm != 100 {
		t.Fatalf("expected watermark 100, got %d", wm)
	}

	c.cacheFilterLogs(101, 200, nil)
	bc.Flush()
	if wm := bc.GetWatermark(neoxWatermarkKey); wm != 200 {
		t.Fatalf("expected watermark 200, got %d", wm)
	}

	// Earlier range should not regress watermark
	c.cacheFilterLogs(50, 80, nil)
	bc.Flush()
	if wm := bc.GetWatermark(neoxWatermarkKey); wm != 200 {
		t.Fatalf("expected watermark to stay 200, got %d", wm)
	}
}
