package blockcache

import (
	"encoding/binary"
	"log/slog"
	"os"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

// Entry is a key-value pair for batch writes.
type Entry struct {
	Key   []byte
	Value []byte
}

const writeChBuf = 4096

// Cache is a BadgerDB-backed byte cache for RPC responses.
// Writes are buffered and flushed in a background goroutine so they
// never block the caller.
type Cache struct {
	db      *badger.DB
	logger  *slog.Logger
	writeCh chan any // accepts Entry or flushBarrier
	wg      sync.WaitGroup
}

type flushBarrier chan struct{}

// New opens (or creates) a BadgerDB at dir for block/log caching.
func New(dir string, logger *slog.Logger) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	opts.CompactL0OnClose = true
	opts.ValueLogFileSize = 256 << 20 // 256 MB value log files

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	c := &Cache{
		db:      db,
		logger:  logger,
		writeCh: make(chan any, writeChBuf),
	}
	c.wg.Add(1)
	go c.flushLoop()

	logger.Info("Block cache opened", "dir", dir)
	return c, nil
}

// flushLoop drains writeCh in batches and writes them to BadgerDB.
func (c *Cache) flushLoop() {
	defer c.wg.Done()

	const maxBatch = 512
	buf := make([]Entry, 0, maxBatch)
	var barriers []flushBarrier

	drain := func() bool {
		for len(buf) < maxBatch {
			select {
			case msg, ok := <-c.writeCh:
				if !ok {
					return false
				}
				switch v := msg.(type) {
				case Entry:
					buf = append(buf, v)
				case flushBarrier:
					barriers = append(barriers, v)
				}
			default:
				return true
			}
		}
		return true
	}

	flushAll := func() {
		if len(buf) > 0 {
			c.writeBatch(buf)
			buf = buf[:0]
		}
		for _, b := range barriers {
			close(b)
		}
		barriers = barriers[:0]
	}

	for msg := range c.writeCh {
		switch v := msg.(type) {
		case Entry:
			buf = append(buf, v)
		case flushBarrier:
			barriers = append(barriers, v)
		}
		if !drain() {
			flushAll()
			return
		}
		flushAll()
	}
}

func (c *Cache) writeBatch(entries []Entry) {
	wb := c.db.NewWriteBatch()
	for _, e := range entries {
		if err := wb.Set(e.Key, e.Value); err != nil {
			c.logger.Warn("Block cache batch set error", "error", err)
			wb.Cancel()
			return
		}
	}
	if err := wb.Flush(); err != nil {
		c.logger.Warn("Block cache batch flush error", "error", err)
	}
}

// Get returns the cached value for key, or nil and false on miss.
func (c *Cache) Get(key []byte) ([]byte, bool) {
	var val []byte
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, false
	}
	return val, true
}

// Put enqueues a single key-value pair for async writing.
func (c *Cache) Put(key, value []byte) error {
	select {
	case c.writeCh <- Entry{Key: key, Value: value}:
	default:
	}
	return nil
}

// PutBatch enqueues multiple entries for async writing.
func (c *Cache) PutBatch(entries []Entry) error {
	for _, e := range entries {
		select {
		case c.writeCh <- e:
		default:
		}
	}
	return nil
}

// Flush blocks until all currently enqueued writes have been committed.
func (c *Cache) Flush() {
	b := make(flushBarrier)
	c.writeCh <- any(b)
	<-b
}

// GetWatermark reads a uint64 watermark stored at key. Returns 0 on miss.
func (c *Cache) GetWatermark(key []byte) uint64 {
	val, ok := c.Get(key)
	if !ok || len(val) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(val)
}

// SetWatermark stores a uint64 watermark at key.
func (c *Cache) SetWatermark(key []byte, v uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return c.Put(key, buf)
}

// HasPrefix returns true if at least one key with the given prefix exists.
// Uses a key-only iterator, so it is very cheap even on large databases.
func (c *Cache) HasPrefix(prefix []byte) bool {
	var found bool
	_ = c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         prefix,
		})
		defer it.Close()
		it.Seek(prefix)
		found = it.Valid()
		return nil
	})
	return found
}

// Close drains all pending writes and closes the underlying BadgerDB.
// Callers must ensure no new Put/PutBatch calls are made after Close.
func (c *Cache) Close() error {
	c.logger.Info("Closing block cache")
	close(c.writeCh)
	c.wg.Wait()
	return c.db.Close()
}
