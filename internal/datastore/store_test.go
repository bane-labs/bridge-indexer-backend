package datastore

import (
	"log/slog"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return newStoreWithDB(db, slog.Default())
}

func TestStore_GetSetDelete(t *testing.T) {
	s := newTestStore(t)

	type item struct {
		Name string `json:"name"`
	}

	if err := s.Set("k1", item{Name: "hello"}); err != nil {
		t.Fatalf("set: %v", err)
	}

	var got item
	if err := s.Get("k1", &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "hello" {
		t.Fatalf("expected hello, got %s", got.Name)
	}

	if err := s.Delete("k1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := s.Get("k1", &got); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_GetNotFound(t *testing.T) {
	s := newTestStore(t)
	var v string
	if err := s.Get("nonexistent", &v); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_BatchWrite(t *testing.T) {
	s := newTestStore(t)

	err := s.BatchWrite([]WriteOp{
		{Key: "a", Value: "val_a"},
		{Key: "b", Value: "val_b"},
		{Key: "c", Value: "val_c"},
	})
	if err != nil {
		t.Fatalf("batch write: %v", err)
	}

	var v string
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Get(k, &v); err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
	}

	// Batch with delete
	err = s.BatchWrite([]WriteOp{
		{Key: "b", Value: nil},
	})
	if err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	if err := s.Get("b", &v); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after batch delete, got %v", err)
	}
}

func TestStore_ScanPrefix(t *testing.T) {
	s := newTestStore(t)

	_ = s.Set("pfx:1", "one")
	_ = s.Set("pfx:2", "two")
	_ = s.Set("pfx:3", "three")
	_ = s.Set("other:1", "other")

	var keys []string
	err := s.ScanPrefix("pfx:", func(key string, _ []byte) error {
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestStore_HasKey(t *testing.T) {
	s := newTestStore(t)
	_ = s.Set("exists", "yes")

	ok, err := s.HasKey("exists")
	if err != nil || !ok {
		t.Fatalf("expected true, got %v %v", ok, err)
	}

	ok, err = s.HasKey("nope")
	if err != nil || ok {
		t.Fatalf("expected false, got %v %v", ok, err)
	}
}
