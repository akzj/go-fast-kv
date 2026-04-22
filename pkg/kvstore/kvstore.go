// Package kvstore provides a high-performance embedded key-value store
// with MVCC, WAL, checkpoint, and crash recovery.
//
// Key Features
//
//   - MVCC concurrency control with snapshot isolation
//   - Write-Ahead Log (WAL) with configurable fsync modes
//   - Automatic checkpoint and crash recovery
//   - Bulk loading of pre-sorted key-value pairs
//   - Automatic vacuum for reclaiming stale MVCC versions
//
// Quick Start
//
//	cfg := kvstore.Config{Dir: "/path/to/db"}
//	store, err := kvstore.Open(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer store.Close()
//
//	// Put a key-value pair
//	err = store.Put([]byte("key"), []byte("value"))
//
//	// Get the value back
//	value, err := store.Get([]byte("key"))
//
//	// Scan keys in a range
//	iter := store.Scan([]byte("a"), []byte("z"))
//	for iter.Next() {
//	    fmt.Printf("key=%s value=%s\n", iter.Key(), iter.Value())
//	}
//
// Bulk Writes
//
// For bulk writes, use WriteBatch to amortize WAL fsync cost:
//
//	batch := store.NewWriteBatch()
//	batch.Put([]byte("k1"), []byte("v1"))
//	batch.Put([]byte("k2"), []byte("v2"))
//	err = batch.Commit()
//
// Bulk Loading
//
// For loading large datasets, use BulkLoad for O(n) import of sorted pairs:
//
//	pairs := []kvstore.KVPair{
//	    {Key: []byte("a"), Value: []byte("1")},
//	    {Key: []byte("b"), Value: []byte("2")},
//	}
//	err := store.BulkLoad(pairs)
//
// SQL Layer
//
// For SQL capabilities, use the gosql package:
//
//	import "github.com/akzj/go-fast-kv/pkg/gosql"
//
//	cfg := kvstore.Config{Dir: "/path/to/db"}
//	store, err := kvstore.Open(cfg)
//	db, err := gosql.Open(store)
//	defer db.Close()
//
//	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
//	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice')")
//
// Thread Safety
//
// Store is safe for concurrent use by multiple goroutines.
// WriteBatch is NOT safe for concurrent use — create one per goroutine.
//
// Module: github.com/akzj/go-fast-kv
package kvstore

import (
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// Re-export types from internal/kvstore/api

// Config holds configuration for opening a KVStore.
type Config = kvstoreapi.Config

// Store is the main key-value store interface.
type Store = kvstoreapi.Store

// Iterator provides forward iteration over key-value pairs.
type Iterator = kvstoreapi.Iterator

// WriteBatch groups multiple Put/Delete operations into a single atomic batch.
type WriteBatch = kvstoreapi.WriteBatch

// ScanParams contains optional parameters for ScanWithParams.
type ScanParams = kvstoreapi.ScanParams

// SyncMode controls WAL fsync behavior.
type SyncMode = kvstoreapi.SyncMode

// VacuumStats reports the results of a vacuum run.
type VacuumStats = kvstoreapi.VacuumStats

// Error re-exports

// ErrKeyNotFound is returned when Get cannot find the requested key.
var ErrKeyNotFound = kvstoreapi.ErrKeyNotFound

// ErrKeyTooLarge is returned when a key exceeds MaxKeySize.
var ErrKeyTooLarge = kvstoreapi.ErrKeyTooLarge

// ErrClosed is returned when operating on a closed store.
var ErrClosed = kvstoreapi.ErrClosed

// ErrBatchCommitted is returned when operating on a committed or discarded batch.
var ErrBatchCommitted = kvstoreapi.ErrBatchCommitted

// ErrNotImplemented is returned for features not yet implemented.
var ErrNotImplemented = kvstoreapi.ErrNotImplemented

// Sync mode constants

const (
	// SyncAlways fsyncs the WAL after every write batch.
	// Maximum durability — no data loss on crash.
	SyncAlways SyncMode = kvstoreapi.SyncAlways

	// SyncNone does not fsync the WAL per write.
	// WAL data is written to OS page cache but not fsynced.
	// On crash, recent writes since the last Checkpoint may be lost.
	// Segment data is still fsynced at Checkpoint time.
	// Close() always fsyncs regardless of this setting.
	SyncNone SyncMode = kvstoreapi.SyncNone
)

// Open creates and opens a KVStore at the given directory.
func Open(cfg Config) (Store, error) {
	return kvstore.Open(cfg)
}

// KVPair represents a key-value pair for bulk loading.
type KVPair = btreeapi.KVPair