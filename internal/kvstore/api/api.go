// Package kvstore provides the top-level public API for the KV store.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - KVStore wraps B-link-tree index + ExternalValueStore
//   - All operations are thread-safe for concurrent use
//   - Operations are atomic — either fully applied or not at all
//   - Scan iterator is point-in-time consistent (snapshot semantics)
//   - Single-writer/multi-reader: exclusive write access for mutations
//
// Module boundaries:
//   - Import VAddr/PageID from vaddr package (DO NOT re-define)
//   - Import NodeFormat from blinktree for internal operations
//   - All internal/ implementation details are private
//
// Why KVStore as top-level?
//   Users interact only with this interface. Lower layers are implementation details.
//   BlinkTree and ExternalValueStore are wrapped, not exposed.
package kvstore

import (
	"errors"
	"time"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Type Aliases — Do NOT re-define, import from vaddr package
// =============================================================================

// VAddr is the physical address type (16 bytes: SegmentID[8] + Offset[8]).
// Defined in: vaddr package
// type VAddr = vaddr.VAddr

// PageID is the logical identifier for a page (uint64).
// Defined in: vaddr package
// type PageID = vaddr.PageID

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultMaxKeySize is the maximum key length in bytes.
	DefaultMaxKeySize = 1024 // 1 KB

	// DefaultMaxValueSize is the maximum value length in bytes.
	// This is the limit for the external value store.
	DefaultMaxValueSize = vaddr.DefaultMaxValueSize // 64 MB from vaddr package

	// DefaultNodeSize is the default B-link tree node size.
	DefaultNodeSize = 64 * 1024 // 64 KB

	// DefaultCacheSizeMB is the default cache size.
	DefaultCacheSizeMB = 256 // 256 MB

	// DefaultSyncWrites enables synchronous writes for durability.
	DefaultSyncWrites = true
)

// Isolation levels for transactions.
const (
	// IsolationSnapshot provides read-committed-at-begin semantics.
	// Reads see the state at transaction start; writes are isolated.
	IsolationSnapshot IsolationLevel = iota
)

// =============================================================================
// Error Types
// =============================================================================

var (
	// ErrKeyNotFound is returned when the requested key does not exist.
	// Returned by: Get, Delete, Transaction.Get, Transaction.Delete
	//
	// Why not return zero value from Get?
	// Zero value is ambiguous — caller can't distinguish missing key from empty value.
	ErrKeyNotFound = errors.New("kvstore: key not found")

	// ErrStoreClosed is returned when operations are attempted on a closed store.
	// Returned by: all operations after Close()
	//
	// Why check before operation?
	// Prevents goroutine leaks and use-after-free scenarios.
	ErrStoreClosed = errors.New("kvstore: store is closed")

	// ErrTransactionAborted is returned when transaction was aborted.
	// Returned by: Transaction.Commit (if Rollback was called)
	ErrTransactionAborted = errors.New("kvstore: transaction aborted")

	// ErrStoreFull is returned when storage capacity has been reached.
	// Returned by: Put, Transaction.Put
	//
	// Why not auto-expand?
	// Append-only storage may require compaction before new allocations.
	ErrStoreFull = errors.New("kvstore: store is full")

	// ErrKeyTooLarge is returned when key exceeds maximum allowed size.
	// Returned by: Put, Transaction.Put
	// Maximum key size: DefaultMaxKeySize (1024 bytes).
	ErrKeyTooLarge = errors.New("kvstore: key too large")

	// ErrValueTooLarge is returned when value exceeds maximum allowed size.
	// Returned by: Put, Transaction.Put
	// Maximum value size: DefaultMaxValueSize (64 MB).
	ErrValueTooLarge = errors.New("kvstore: value too large")

	// ErrWriteLocked is returned when a write operation is attempted
	// while another write is in progress (single-writer violation).
	ErrWriteLocked = errors.New("kvstore: write operation in progress")

	// ErrReadOnly is returned when write operations are attempted on read-only store.
	ErrReadOnly = errors.New("kvstore: store is read-only")

	// ErrTransactionFull is returned when transaction limit is reached.
	ErrTransactionFull = errors.New("kvstore: too many transactions")

	// ErrBatchCommitted is returned when operations are queued on committed batch.
	ErrBatchCommitted = errors.New("kvstore: batch already committed")
)

// =============================================================================
// Interface: KVStore
// =============================================================================

// KVStore is the primary interface for key-value operations.
// Thread-safe: all operations are safe for concurrent use.
//
// Invariant: Operations are atomic — either fully applied or not at all.
// Invariant: After a successful Put/Delete, subsequent Get reflects the change.
// Invariant: Scan iterator is point-in-time consistent (snapshot semantics).
//
// Why snapshot semantics for Scan?
// B-link-tree nodes are immutable; snapshot is just the root's VAddr.
// Iterator captures root VAddr at creation, traverses at that point-in-time.
type KVStore interface {
	// Get retrieves the value for key.
	// Returns (value, nil) if found, (nil, ErrKeyNotFound) if absent.
	//
	// Why return []byte, not (value, found bool)?
	// ErrKeyNotFound is self-describing; found is redundant.
	// Callers check error, not a bool.
	Get(key []byte) ([]byte, error)

	// Put stores a key-value pair.
	// If key exists, its value is overwritten.
	// Returns nil on success, error otherwise.
	//
	// Invariant: Put is durable when this method returns nil (if SyncWrites enabled).
	// Invariant: Put with value > 48 bytes stores externally per vaddr.ExternalThreshold.
	//
	// Why not return previous value?
	// Append-only storage; previous value may be in sealed segment.
	// If caller needs it, they should Get before Put.
	Put(key, value []byte) error

	// Delete removes a key-value pair.
	// Returns nil if deleted, ErrKeyNotFound if key was absent.
	//
	// Invariant: After Delete returns nil, subsequent Get returns ErrKeyNotFound.
	// Invariant: Delete is idempotent — calling twice when key exists returns ErrKeyNotFound on second call.
	//
	// Why idempotent? Simplifies error handling in callers.
	// Append-only tombstone is indistinguishable from "key never existed" for reads.
	Delete(key []byte) error

	// Scan returns an iterator over keys in range [start, end).
	// If end is nil, scan continues to the last key.
	// If start is nil, scan begins at the first key.
	//
	// Invariant: Iterator reflects state at time of Scan() call (snapshot semantics).
	// Invariant: Iterator is safe to use after concurrent modifications to store.
	//
	// Iterator lifecycle:
	//   iter := store.Scan(start, end)
	//   for iter.Next() {
	//       key, value := iter.Key(), iter.Value()
	//       // process
	//   }
	//   if err := iter.Error(); err != nil {
	//       // handle error
	//   }
	//   iter.Close()
	//
	// Why not return slice of key-value pairs?
	// Memory efficiency for large ranges; lazy evaluation.
	// Iterator is the standard pattern for range queries in storage systems.
	Scan(start, end []byte) (Iterator, error)

	// Close releases resources held by the store.
	// After Close, all operations return ErrStoreClosed.
	//
	// Invariant: Close is idempotent; subsequent calls return nil.
	Close() error
}

// =============================================================================
// Interface: KVStoreWithTransactions
// =============================================================================

// KVStoreWithTransactions extends KVStore with transaction support.
// Not all implementations may support transactions.
//
// Why separate interface?
// - Simpler implementations can implement only KVStore
// - Transaction support may have additional resource costs
// - Clear capability indication for callers
type KVStoreWithTransactions interface {
	KVStore

	// Begin starts a new transaction.
	// Returns ErrStoreFull if transaction limit is reached.
	//
	// Why not auto-detect need?
	// Explicit Begin makes transaction boundaries clear.
	// Callers know when they enter/exit transactional mode.
	Begin() (Transaction, error)

	// BeginWithOptions starts a transaction with configuration.
	// Options control isolation level, timeout, etc.
	BeginWithOptions(opts TransactionOptions) (Transaction, error)
}

// =============================================================================
// Interface: Iterator
// =============================================================================

// Iterator provides sequential access to a range of key-value pairs.
// Created by KVStore.Scan() or Transaction.Scan().
//
// Invariant: Iterator is point-in-time consistent (snapshot semantics).
// Invariant: Iterator is independent of concurrent store modifications.
// Invariant: Iterator must be closed to release resources.
//
// Why B-link sibling chain matters for iterators:
// B-link trees allow concurrent splits. During iteration:
//   1. Current node may split while iterating
//   2. Sibling chain links connect old node → new right node
//   3. Iterator follows sibling chain to continue past split
//
// This is why we expose Next() as a loop, not a single-pass method.
type Iterator interface {
	// Next advances the iterator to the next key-value pair.
	// Returns true if positioned at a valid entry, false if exhausted.
	// Returns false when:
	//   - Iteration reached end bound
	//   - Iteration encountered an error (check Error())
	//   - Iterator was closed
	//
	// Why return bool, not error?
	// Exhaustion is normal; error is exceptional.
	// Caller pattern: for iter.Next() { process } is cleaner.
	Next() bool

	// Key returns the key at the current position.
	// Valid only after Next() returns true.
	//
	// Why return copy?
	// Iterator may advance or close, invalidating underlying reference.
	// Caller receives independent copy.
	Key() []byte

	// Value returns the value at the current position.
	// Valid only after Next() returns true.
	//
	// Why return copy?
	// Same as Key() — iterator state may change.
	Value() []byte

	// Error returns any error encountered during iteration.
	// Returns nil if iteration completed successfully.
	//
	// Why separate from Next()?
	// Next() returning false could mean exhaustion OR error.
	// Error() disambiguates — caller can distinguish.
	Error() error

	// Close releases resources held by the iterator.
	// Idempotent — safe to call multiple times.
	Close()
}

// =============================================================================
// Interface: Transaction
// =============================================================================

// Transaction provides atomic multi-key operations.
// Transactions are optional — single-key operations work without them.
//
// Invariant: Transaction commits atomically or not at all.
// Invariant: Uncommitted changes are invisible to other readers.
// Invariant: Committed changes are immediately visible to new readers.
//
// Transaction lifecycle:
//   tx, err := store.Begin()
//   if err != nil { /* handle */ }
//   // ... perform operations
//   if err := tx.Commit(); err != nil {
//       tx.Rollback()  // cleanup
//   }
//
// Why not auto-commit?
// Explicit commit gives caller control over durability.
// Implicit commit on close would hide errors.
type Transaction interface {
	// Get retrieves value within this transaction.
	// Sees uncommitted changes from this transaction.
	// Does not see uncommitted changes from other transactions.
	Get(key []byte) ([]byte, error)

	// Put stores value within this transaction.
	// Change is not visible until Commit succeeds.
	Put(key, value []byte) error

	// Delete removes key within this transaction.
	// Change is not visible until Commit succeeds.
	Delete(key []byte) error

	// Scan returns an iterator within this transaction.
	// Iterator is consistent with uncommitted changes.
	Scan(start, end []byte) (Iterator, error)

	// Commit makes all changes in this transaction visible to other operations.
	// After Commit, transaction is closed.
	// Returns ErrTransactionAborted if transaction was already aborted.
	// Returns error if commit fails (transaction remains usable for rollback).
	Commit() error

	// Rollback cancels all changes in this transaction.
	// After Rollback, transaction is closed.
	// Idempotent — safe to call even after Commit (which becomes no-op).
	Rollback()

	// TxID returns the transaction's unique identifier.
	// Used for debugging and transaction ordering.
	TxID() uint64
}

// =============================================================================
// Interface: Batch
// =============================================================================

// Batch performs multiple operations atomically.
// Batch is simpler than Transaction but less flexible.
//
// Invariant: Batch commits atomically or not at all.
// Invariant: Batch applies operations in order (not parallelized).
// Invariant: All operations in a batch succeed or all fail.
//
// Batch lifecycle:
//   batch := store.NewBatch()
//   batch.Put(key1, value1)
//   batch.Put(key2, value2)
//   batch.Delete(key3)
//   if err := batch.Commit(); err != nil {
//       // all changes rolled back
//   }
//
// Why Batch AND Transaction?
// - Batch: simpler, lower overhead, no TxID needed
// - Transaction: explicit Begin/Commit, can span time, can rollback
// - Transaction supports reads; Batch is write-only
type Batch interface {
	// Put queues a put operation.
	// Panics if batch was already committed.
	//
	// Why panic?
	// Commit/Reset clears the batch; subsequent Put is a programming error.
	// Panic is fail-fast — bugs are caught early.
	Put(key, value []byte)

	// Delete queues a delete operation.
	// Panics if batch was already committed.
	Delete(key []byte)

	// Commit applies all queued operations atomically.
	// After Commit, batch is closed.
	// Returns error if commit fails (batch remains usable for retry).
	Commit() error

	// Reset clears queued operations without committing.
	// Batch can be reused after Reset.
	Reset()
}

// =============================================================================
// Interface: BatchCreator
// =============================================================================

// BatchCreator creates batches for atomic multi-operation.
//
// Why separate interface?
// - KVStore may not support batching (simpler implementations)
// - Allows type assertion to check capability
type BatchCreator interface {
	// NewBatch creates a new batch for atomic multi-operation.
	NewBatch() Batch
}

// =============================================================================
// Supporting Types
// =============================================================================

// IsolationLevel controls transaction isolation.
// Currently: Snapshot isolation only.
type IsolationLevel int

// TransactionOptions controls transaction behavior.
type TransactionOptions struct {
	// IsolationLevel (not yet implemented — reserved for future).
	// Default: Snapshot isolation.
	IsolationLevel IsolationLevel

	// Timeout sets maximum duration for commit.
	// If 0, uses implementation default.
	// If negative, no timeout.
	Timeout time.Duration

	// ReadOnly prevents writes within this transaction.
	// Implementation may optimize for read-only workloads.
	ReadOnly bool
}

// Config holds KVStore initialization parameters.
// All fields are optional; defaults are applied for zero values.
type Config struct {
	// Directory is the path to storage directory.
	// Required unless Open is called with nil Config.
	Directory string

	// MaxKeySize is the maximum key length in bytes.
	// If 0, uses DefaultMaxKeySize.
	MaxKeySize uint32

	// MaxValueSize is the maximum value length in bytes.
	// If 0, uses DefaultMaxValueSize.
	MaxValueSize uint64

	// ReadOnly opens the store in read-only mode.
	// Useful for read replicas or recovery verification.
	ReadOnly bool

	// SyncWrites controls durability vs performance trade-off.
	// If true, writes are synced to storage before returning.
	// If false, relies on OS page cache (faster, less durable).
	// Default: true (durable).
	SyncWrites bool

	// CacheSizeMB is the size of in-memory cache in megabytes.
	// If 0, uses DefaultCacheSizeMB.
	CacheSizeMB uint32

	// BLinkTreeNodeSize is the size of B-link tree nodes.
	// If 0, uses DefaultNodeSize (aligned with PageSize).
	BLinkTreeNodeSize uint32
}

// =============================================================================
// Factory Functions
// =============================================================================

// Open creates or opens a KVStore at the given directory.
// Applies default configuration for nil Config.
//
// Why a factory function instead of constructor?
// - Directory-based initialization is common
// - Allows deferred configuration
// - Future: could add OpenWithOptions for more control
func Open(directory string, config *Config) (KVStore, error) {
	panic("TODO: implementation provided by branch")
}

// OpenWithTransactions creates a store with transaction support.
func OpenWithTransactions(directory string, config *Config) (KVStoreWithTransactions, error) {
	panic("TODO: implementation provided by branch")
}

// Destroy removes all storage files in directory.
// Use with caution — this is irreversible.
//
// Why not a method?
// Easier to call without holding a reference.
// Destruction is often done during cleanup, not during operation.
func Destroy(directory string) error {
	panic("TODO: implementation provided by branch")
}

// =============================================================================
// Helper Functions
// =============================================================================

// IsNotFound is a helper for checking ErrKeyNotFound.
//
// Why a helper?
// Common pattern; avoids importing errors package directly.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}

// IsClosed is a helper for checking ErrStoreClosed.
func IsClosed(err error) bool {
	return errors.Is(err, ErrStoreClosed)
}
