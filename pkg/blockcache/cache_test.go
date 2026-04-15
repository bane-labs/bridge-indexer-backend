package blockcache

import (
	"log/slog"
	"testing"
)

func newTestCache(t *testing.T) *Cache {
	t.Helper()
	dir := t.TempDir()
	c, err := New(dir, slog.Default())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestPutGet(t *testing.T) {
	c := newTestCache(t)

	key := []byte("test:key:1")
	val := []byte("hello world")

	if _, ok := c.Get(key); ok {
		t.Fatal("expected miss on empty cache")
	}

	if err := c.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	c.Flush()

	got, ok := c.Get(key)
	if !ok {
		t.Fatal("expected hit after Put")
	}
	if string(got) != string(val) {
		t.Fatalf("got %q, want %q", got, val)
	}
}

func TestPutBatch(t *testing.T) {
	c := newTestCache(t)

	entries := []Entry{
		{Key: []byte("batch:1"), Value: []byte("a")},
		{Key: []byte("batch:2"), Value: []byte("b")},
		{Key: []byte("batch:3"), Value: []byte("c")},
	}

	if err := c.PutBatch(entries); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	c.Flush()

	for _, e := range entries {
		got, ok := c.Get(e.Key)
		if !ok {
			t.Fatalf("expected hit for %s", e.Key)
		}
		if string(got) != string(e.Value) {
			t.Fatalf("got %q, want %q", got, e.Value)
		}
	}
}

func TestWatermark(t *testing.T) {
	c := newTestCache(t)

	key := []byte("wm:test")

	if v := c.GetWatermark(key); v != 0 {
		t.Fatalf("expected 0 for missing watermark, got %d", v)
	}

	if err := c.SetWatermark(key, 42); err != nil {
		t.Fatalf("SetWatermark: %v", err)
	}
	c.Flush()

	if v := c.GetWatermark(key); v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	if err := c.SetWatermark(key, 999); err != nil {
		t.Fatalf("SetWatermark: %v", err)
	}
	c.Flush()

	if v := c.GetWatermark(key); v != 999 {
		t.Fatalf("expected 999, got %d", v)
	}
}

func TestGetMiss(t *testing.T) {
	c := newTestCache(t)

	_, ok := c.Get([]byte("nonexistent"))
	if ok {
		t.Fatal("expected miss")
	}
}
