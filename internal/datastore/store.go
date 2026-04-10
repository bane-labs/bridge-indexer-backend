package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	badger "github.com/dgraph-io/badger/v4"
)

// ErrNotFound is returned when a key does not exist in the store.
var ErrNotFound = errors.New("key not found")

// WriteOp represents a single write operation for batch writes.
// If Value is nil the key is deleted.
type WriteOp struct {
	Key   string
	Value interface{} // nil means delete
}

// Store wraps BadgerDB v4 with JSON serialization and prefix-scan support.
type Store struct {
	db     *badger.DB
	logger *slog.Logger
}

// NewStore opens a BadgerDB database at the given path.
func NewStore(path string, logger *slog.Logger) (*Store, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // suppress badger internal logging
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}
	return &Store{db: db, logger: logger}, nil
}

// NewReadOnlyStore opens a BadgerDB database in read-only mode.
// Useful for status/reporting commands that run alongside the main process.
func NewReadOnlyStore(path string, logger *slog.Logger) (*Store, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // suppress badger internal logging
	opts.ReadOnly = true
	opts.BypassLockGuard = true
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db (read-only): %w", err)
	}
	return &Store{db: db, logger: logger}, nil
}

// newStoreWithDB creates a Store from an already-opened badger.DB (internal tests).
func newStoreWithDB(db *badger.DB, logger *slog.Logger) *Store {
	return &Store{db: db, logger: logger}
}

// NewStoreFromDB creates a Store from an already-opened badger.DB (cross-package tests).
func NewStoreFromDB(db *badger.DB, logger *slog.Logger) *Store {
	return &Store{db: db, logger: logger}
}

// Get retrieves a JSON-encoded value by key. Returns ErrNotFound when absent.
func (s *Store) Get(key string, dest interface{}) error {
	return s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, dest)
		})
	})
}

// Set stores a JSON-encoded value under the given key.
func (s *Store) Set(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// Delete removes a key from the store.
func (s *Store) Delete(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// BatchWrite atomically writes multiple operations.
func (s *Store) BatchWrite(ops []WriteOp) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, op := range ops {
		if op.Value == nil {
			if err := wb.Delete([]byte(op.Key)); err != nil {
				return err
			}
		} else {
			data, err := json.Marshal(op.Value)
			if err != nil {
				return fmt.Errorf("failed to marshal value for key %s: %w", op.Key, err)
			}
			if err := wb.Set([]byte(op.Key), data); err != nil {
				return err
			}
		}
	}
	return wb.Flush()
}

// ScanPrefix iterates over all keys with the given prefix, calling fn for each.
func (s *Store) ScanPrefix(prefix string, fn func(key string, value []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		pfx := []byte(prefix)
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			item := it.Item()
			k := string(item.Key())
			if err := item.Value(func(val []byte) error {
				return fn(k, val)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// HasKey returns true if the key exists.
func (s *Store) HasKey(key string) (bool, error) {
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DB returns the underlying badger.DB for advanced operations.
func (s *Store) DB() *badger.DB {
	return s.db
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}
