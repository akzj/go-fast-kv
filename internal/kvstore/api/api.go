// Package kvstoreapi defines the public interface for go-fast-kv.
//
// KVStore is the top-level user-facing API that integrates all internal
// modules: B-tree, PageStore, BlobStore, WAL, SegmentManager, and TxnManager.
//
// Design reference: docs/DESIGN.md §1, §2, §7
package kvstoreapi

import (
	"errors"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrKeyNotFound is returned when Get cannot find the requested key.
	ErrKeyNotFound = errors.New("kvstore: key not found")

	// ErrKeyTooLarge is returned when a key exceeds MaxKeySize.
	ErrKeyTooLarge = errors.New("kvstore: key too large")

	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("kvstore: closed")
)

// ─── Iterator ───────────────────────────────────────────────────────

// Iterator provides forward iteration over key-value pairs.
//
// Usage:
//
//	iter := store.Scan(startKey, endKey)
//	defer iter.Close()
//	for iter.Next() {
//	    key := iter.Key()
//	    value := iter.Value()
//	}
//	if err := iter.Err(); err != nil { ... }
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
	Close()
}

// ─── Store ──────────────────────────────────────────────────────────

// Store is the main key-value store interface.
//
// Every Put/Get/Delete/Scan operates in auto-commit mode:
// each operation is wrapped in its own transaction (BeginTxn + Commit).
// This means every read sees the latest committed state.
//
// Large values (> 256 bytes) are transparently stored in BlobStore.
//
// Thread safety: Store must be safe for concurrent use.
//
// Design reference: docs/DESIGN.md §1, §3.9.10
type Store interface {
	// Put inserts or updates a key-value pair.
	//
	// Auto-commit: internally wraps in BeginTxn + Commit.
	// Large values are transparently stored in BlobStore.
	Put(key, value []byte) error

	// Get retrieves the value for a key.
	//
	// Returns ErrKeyNotFound if the key does not exist.
	Get(key []byte) ([]byte, error)

	// Delete removes a key.
	//
	// MVCC: marks the entry as deleted (logical delete).
	// Physical removal is done by Vacuum.
	// Returns ErrKeyNotFound if the key does not exist.
	Delete(key []byte) error

	// Scan returns an iterator over keys in [start, end).
	//
	// The iterator sees a consistent snapshot (auto-commit read txn).
	// Each key appears at most once (latest committed version).
	Scan(start, end []byte) Iterator

	// Checkpoint writes a full snapshot of the current state to disk.
	//
	// This includes: PageStore mapping, BlobStore mapping, CLOG state,
	// next XIDs, and root PageID. After checkpoint, old WAL entries
	// can be truncated.
	Checkpoint() error

	// Close flushes all data and closes the store.
	// After Close, all operations return ErrClosed.
	Close() error
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for opening a KVStore.
type Config struct {
	// Dir is the root directory for all store data.
	// Subdirectories are created automatically:
	//   {Dir}/page_segments/   — page segment files
	//   {Dir}/blob_segments/   — blob segment files
	//   {Dir}/wal/             — WAL file
	//   {Dir}/checkpoint       — checkpoint file
	Dir string

	// MaxSegmentSize is the maximum size of a segment file in bytes.
	// Defaults to 64MB if zero.
	MaxSegmentSize int64

	// InlineThreshold is the max value size for inline storage in B-tree.
	// Values larger than this are stored in BlobStore.
	// Defaults to 256 if zero.
	InlineThreshold int
}
