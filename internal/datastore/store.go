package datastore

import (
	"log/slog"
	"sync"

	"github.com/philippgille/gokv"
	"github.com/philippgille/gokv/badgerdb"
	"github.com/philippgille/gokv/encoding"
)

// DataStore wraps gokv.Store with thread safety
type DataStore struct {
	mu    sync.RWMutex
	store gokv.Store
}

// NewDataStore creates a new DataStore instance
func NewDataStore(store gokv.Store) *DataStore {
	return &DataStore{
		mu:    sync.RWMutex{},
		store: store,
	}
}

// getValue retrieves a value from the store
func (ds *DataStore) getValue(key string, value any) (bool, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.store.Get(key, value)
}

// setValue stores a value in the store
func (ds *DataStore) setValue(key string, value any) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.store.Set(key, value)
}

// Close closes the underlying store
func (ds *DataStore) Close() error {
	return ds.store.Close()
}

// prefixedDataStore provides key prefixing functionality
type prefixedDataStore struct {
	prefix    string
	dataStore *DataStore
}

// NewDbOptions creates BadgerDB options with the given path
func NewDbOptions(path *string) badgerdb.Options {
	return badgerdb.Options{
		Dir:   *path,
		Codec: encoding.JSON,
	}
}

// NewBadgerDbStore creates a new BadgerDB store
func NewBadgerDbStore(opts badgerdb.Options, logger *slog.Logger) (gokv.Store, error) {
	logger.Info("Creating new BadgerDB store")
	store, err := badgerdb.NewStore(opts)
	if err != nil {
		logger.Error("Failed to create new BadgerDB store", "error", err)
		return nil, err
	}
	return store, nil
}
