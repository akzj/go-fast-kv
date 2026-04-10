// Package kvstoreapi defines the public interface for go-fast-kv.
//
// KVStore is the top-level user-facing API that integrates all internal
// modules: B-tree, PageStore, BlobStore, WAL, SegmentManager, and TxnManager.
//
// Design reference: docs/DESIGN.md §1, §2, §7
package kvstoreapi

import (
	"errors"

	vacuumapi "github.com/akzj/go-fast-kv/internal/vacuum/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrKeyNotFound is returned when Get cannot find the requested key.
	ErrKeyNotFound = errors.New("kvstore: key not found")

	// ErrKeyTooLarge is returned when a key exceeds MaxKeySize.
	ErrKeyTooLarge = errors.New("kvstore: key too large")

	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("kvstore: closed")

	// ErrBatchCommitted is returned when operating on a committed or discarded batch.
	ErrBatchCommitted = errors.New("kvstore: batch already committed or discarded")
)

// VacuumStats reports the results of a vacuum run.
// Re-exported from the vacuum module for user convenience.
type VacuumStats = vacuumapi.VacuumStats

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

// ─── WriteBatch ─────────────────────────────────────────────────────

// WriteBatch groups multiple Put/Delete operations into a single atomic batch.
// All operations share one transaction and one WAL fsync, dramatically
// reducing per-operation overhead for bulk writes.
//
// Usage:
//
//	batch := store.NewWriteBatch()
//	batch.Put(key1, value1)
//	batch.Put(key2, value2)
//	batch.Delete(key3)
//	err := batch.Commit()
//
// Thread safety: WriteBatch is NOT safe for concurrent use.
// Create one WriteBatch per goroutine.
type WriteBatch interface {
	// Put stages a key-value pair for writing.
	// The write is not visible until Commit is called.
	Put(key, value []byte) error

	// Delete stages a key for deletion.
	// The delete is not visible until Commit is called.
	Delete(key []byte) error

	// Commit atomically applies all staged operations.
	// All operations share a single transaction and a single WAL fsync.
	// After Commit, the batch cannot be reused.
	Commit() error

	// Discard releases resources without committing.
	// Safe to call multiple times. After Discard, the batch cannot be reused.
	Discard()
}

// ─── Store ──────────────────────────────────────────────────────────

// Store is the main key-value store interface.
//
// Every Put/Get/Delete/Scan operates in auto-commit mode:
// each operation is wrapped in its own transaction (BeginTxn + Commit).
// This means every read sees the latest committed state.
//
// For bulk writes, use NewWriteBatch to group multiple operations
// into a single atomic batch with one WAL fsync.
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

	// NewWriteBatch creates a new write batch for grouping operations.
	// Multiple Put/Delete calls are staged and applied atomically on Commit.
	// This amortizes WAL fsync cost across all operations in the batch.
	NewWriteBatch() WriteBatch

	// Checkpoint writes a full snapshot of the current state to disk.
	//
	// This includes: PageStore mapping, BlobStore mapping, CLOG state,
	// next XIDs, and root PageID. After checkpoint, old WAL entries
	// can be truncated.
	Checkpoint() error

	// RunVacuum performs a single vacuum pass, cleaning up old MVCC
	// versions from B-tree leaf nodes that are no longer visible to
	// any active transaction.
	//
	// Two cleanup cases:
	//   1. Committed delete/overwrite: entry.TxnMax < safeXID and committed
	//   2. Aborted creator: entry.TxnMin was aborted
	//
	// This is an on-demand operation — call it periodically or after
	// a batch of deletes/updates to reclaim space.
	//
	// Thread safety: RunVacuum acquires per-page write locks individually,
	// so it can run concurrently with Put/Get/Delete/Scan. It does NOT
	// block the entire store.
	RunVacuum() (*VacuumStats, error)

	// Close flushes all data and closes the store.
	// After Close, all operations return ErrClosed.
	Close() error
}

// ─── SyncMode ───────────────────────────────────────────────────────

// SyncMode controls WAL fsync behavior, trading durability for performance.
type SyncMode int

const (
	// SyncAlways fsyncs the WAL after every write batch.
	// Maximum durability — no data loss on crash.
	// This is the default (zero value).
	SyncAlways SyncMode = iota

	// SyncNone does not fsync the WAL per write.
	// WAL data is written to OS page cache but not fsynced.
	// On crash, recent writes since the last Checkpoint may be lost.
	// Segment data is still fsynced at Checkpoint time.
	// Close() always fsyncs regardless of this setting.
	// Equivalent to Badger's SyncWrites=false.
	SyncNone
)

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

	// SyncMode controls WAL fsync behavior.
	// SyncAlways (default): fsync after every write — maximum durability.
	// SyncNone: no per-write fsync — faster writes, risk of data loss on crash.
	// Close() and Checkpoint() always fsync regardless of this setting.
	SyncMode SyncMode

	// AutoVacuumThreshold is the minimum number of Put+Delete operations
	// before an automatic vacuum pass is triggered.
	// Default: 1000. Set to 0 to disable auto-vacuum.
	AutoVacuumThreshold int

	// AutoVacuumRatio is the fraction of live entries that, when exceeded by
	// dead tuple operations, triggers an automatic vacuum pass.
	// Default: 0.1 (10%). Set to 0 to disable ratio-based triggering.
	// The effective threshold is max(AutoVacuumThreshold, totalEntries * AutoVacuumRatio).
	AutoVacuumRatio float64

	// PageCacheSize is the maximum number of B-tree page entries to keep in
	// the LRU page cache. Each entry is ~4KB (one page). Default: 8192 (32MB).
	// Larger values improve read performance for workloads that exceed the
	// default 8MB working set. Memory usage is proportional to this setting.
	PageCacheSize int
}
