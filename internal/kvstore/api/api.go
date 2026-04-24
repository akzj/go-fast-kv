// Package kvstoreapi defines the public interface for go-fast-kv.
//
// KVStore is the top-level user-facing API that integrates all internal
// modules: B-tree, PageStore, BlobStore, WAL, SegmentManager, and TxnManager.
//
// Design reference: docs/DESIGN.md §1, §2, §7
package kvstoreapi

import (
	"errors"
	"time"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	vacuumapi "github.com/akzj/go-fast-kv/internal/vacuum/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
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

	// ErrNotImplemented is returned for features not yet implemented.
	ErrNotImplemented = errors.New("kvstore: not implemented")
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

// ScanParams contains optional parameters for ScanWithParams.
type ScanParams struct {
	// Limit restricts the number of keys returned.
	// If 0, no limit is applied.
	Limit int

	// Offset skips the first Offset keys.
	// If 0, no offset is applied.
	Offset int
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

	// PutWithXID writes a key-value pair with a specific transaction ID.
	// Unlike Put which allocates a fresh XID, this writes directly to the
	// btree with the given txnID. Used by SQL executor for deferred-write
	// transactions where writes should share the transaction's XID.
	PutWithXID(key, value []byte, txnID uint64) error

	// DeleteWithXID marks a key as deleted with a specific transaction ID.
	// Unlike Delete which allocates a fresh XID, this marks txnMax=txnID
	// directly in the btree. Used by SQL executor for deferred-write
	// transactions to enable rollback via self-delete.
	DeleteWithXID(key []byte, txnID uint64) error

	// CommitWithXID atomically applies all operations staged with PutWithXID/DeleteWithXID
	// under the given transaction ID. All operations share one WAL fsync.
	// Used by SQL executor to commit deferred-write transactions.
	CommitWithXID(xid uint64) error
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

	// ScanWithParams returns an iterator over keys in [start, end) with optional LIMIT/OFFSET.
	//
	// If params.Limit > 0, at most params.Limit keys are returned.
	// If params.Offset > 0, the first params.Offset keys are skipped.
	// Combining offset and limit: returns keys [offset, offset+limit).
	//
	// This enables push-down optimization: instead of scanning all keys and filtering
	// in the SQL executor, the storage layer stops early after returning the needed rows.
	//
	// The iterator sees a consistent snapshot (auto-commit read txn).
	ScanWithParams(start, end []byte, params ScanParams) Iterator

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

	// DeleteRange removes all keys in [start, end).
	// Uses WriteBatch internally for efficiency.
	// Returns the number of keys deleted.
	DeleteRange(start, end []byte) (int, error)

	// BulkLoad performs a fast bulk import of pre-sorted key-value pairs.
	// Entries should be sorted by key for best performance.
	//
	// This bypasses the normal O(log n) insert path, achieving O(n) complexity.
	// All entries are loaded with TxnMin=0, TxnMax=MaxUint64 (visible to all readers).
	//
	// Thread safety: BulkLoad holds a write lock during the operation.
	// Get/Scan can run concurrently (snapshot semantics).
	//
	// WAL: A single root-change WAL entry is written for crash recovery.
	// Individual page writes are NOT logged (performance optimization).
	BulkLoad(pairs []btreeapi.KVPair) error

	// BulkLoadMVCC performs a bulk import with MVCC versioning.
	// All loaded entries have TxnMin=startTxnID.
	// Use this when loading historical data that needs version tracking.
	//
	// Thread safety: BulkLoadMVCC holds a write lock during the operation.
	// Get/Scan can run concurrently (snapshot semantics).
	//
	// WAL: A single root-change WAL entry is written for crash recovery.
	BulkLoadMVCC(pairs []btreeapi.KVPair, startTxnID uint64) error

	// Close flushes all data and closes the store.
	// After Close, all operations return ErrClosed.
	Close() error

	// SetTTL sets a key-value pair with expiration time.
	// The key will be automatically deleted after duration.
	// Returns error if key does not exist (for update, use Put with TTL).
	SetTTL(key []byte, ttl time.Duration) error

	// TTL returns the remaining time until key expires.
	// Returns 0 if key has no expiration, negative if expired.
	TTL(key []byte) (time.Duration, error)

	// TxnManager returns the underlying transaction manager.
	// Used by the SQL layer to create TxnContext for BEGIN...COMMIT transactions.
	TxnManager() txnapi.TxnContextFactory

	// RegisterSnapshot registers a transaction's read snapshot for the duration
	// of the transaction. All Get/Scan calls with txnXID as readTxnID will use
	// this snapshot for visibility checks.
	// Caller is responsible for calling UnregisterSnapshot when the transaction ends.
	RegisterSnapshot(txnXID uint64, snap *txnapi.Snapshot)

	// UnregisterSnapshot removes a transaction's snapshot from readSnaps.
	// Called when the transaction commits or rolls back.
	UnregisterSnapshot(txnXID uint64)

	// SetActiveTxnContext registers a goroutine-local active transaction context.
	// Used by the SQL layer to enable store.Get/Scan to use the txnCtx snapshot
	// for own-write visibility within a transaction.
	SetActiveTxnContext(txnCtx txnapi.TxnContext)

	// ClearActiveTxnContext removes the goroutine-local active transaction context.
	// Called when the transaction commits or rolls back.
	ClearActiveTxnContext()

	// PutWithXID writes a key-value pair with a specific transaction ID.
	// Unlike Put which allocates a fresh XID via BeginTxn+Commit, this writes
	// directly to the btree with the given txnID. Used by the SQL executor for
	// deferred-write transactions so that all writes share the transaction's
	// XID (enabling own-write visibility via txnMin==s.XID rule) and rollback
	// via self-delete (txnMax==txnXID).
	PutWithXID(key, value []byte, txnID uint64) error

	// DeleteWithXID marks a key as deleted with a specific transaction ID.
	// Unlike Delete which allocates a fresh XID, this marks txnMax=txnID
	// directly. Used for SQL rollback: a self-delete (txnMax==txnXID) makes
	// the entry invisible without restoring the original value.
	DeleteWithXID(key []byte, txnID uint64) error

	// CommitWithXID finalizes a SQL transaction by flushing pending WAL entries
	// to disk and updating CLOG. Called by the SQL layer at COMMIT time.
	// PutWithXID/DeleteWithXID register collectors that capture page/blob entries.
	// CommitWithXID collects those entries, writes them to WAL (fsync), and
	// updates CLOG. This ensures SQL transaction writes survive crashes.
	CommitWithXID(xid uint64) error

	// AbortWithXID rolls back a SQL transaction by writing a TxnAbort WAL record.
	// Called by the SQL layer at ROLLBACK time.
	AbortWithXID(xid uint64) error

	// GetMetrics returns current operational metrics.
	// Zero blocking — all fields are populated atomically.
	// Latency percentiles are calculated from a fixed-size ring buffer.
	GetMetrics() *Metrics

	// Backup creates a consistent point-in-time backup of the store to destDir.
	// The backup runs while the store is fully operational (zero downtime).
	// It includes: checkpoint file, all WAL segments needed for recovery,
	// all SSTable files, page_segments, and blob_segments.
	// A backup manifest (backup.json) is written with metadata and SHA256 checksums.
	//
	// Thread safety: Backup holds a read lock internally. Get/Put/Scan can
	// run concurrently (they see their own snapshots). Checkpoint runs
	// concurrently (lock-free async). Close cannot run concurrently.
	//
	// Restore is a package-level function: kvstore.Restore(backupDir, targetDir).
	Backup(destDir string) error
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

// IsolationLevel defines the transaction isolation level.
type IsolationLevel int

const (
	// IsolationAutoCommit uses per-operation auto-commit transactions.
	// Each Put/Delete operates in its own transaction.
	// This is the default.
	IsolationAutoCommit IsolationLevel = iota
)

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
	// Defaults to 512MB if zero.
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

	// IsolationLevel sets the transaction isolation mode.
	// Defaults to IsolationAutoCommit (per-operation transactions).
	IsolationLevel IsolationLevel

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

	// LockTimeoutMs is the timeout in milliseconds for acquiring row locks
	// in SQL transactions (SELECT FOR UPDATE, etc.). Default: 5000 (5 seconds).
	// Set to 0 for no timeout (wait indefinitely).
	LockTimeoutMs int
}
