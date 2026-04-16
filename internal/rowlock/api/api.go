// Package api defines the interfaces and types for row-level locking.
//
// Row locks use data keys (tableID:rowID) to coordinate concurrent access
// to database rows, distinct from page-level B-tree locks (internal/lock/)
// which operate on PageIDs.
//
// Lock ordering: To prevent deadlocks, all lock acquisitions for a transaction
// must be acquired in sorted rowKey order. The LockManager does not enforce
// this automatically — callers must sort rowKeys before calling Acquire.
//
// CLOG cleanup: Stale locks may accumulate if transactions abort without
// releasing locks. A background cleanup goroutine can call ReleaseAll(txnID)
// for known-aborted transactions. LockStats() provides metrics for monitoring.
package api

import (
	"time"
)

// TxnID is a transaction identifier.
type TxnID = uint64

// LockMode represents the type of lock held on a row.
type LockMode uint8

const (
	// LockShared allows multiple transactions to hold shared locks simultaneously.
	// Used for read operations.
	LockShared LockMode = iota

	// LockExclusive prevents any other transaction from holding a lock on the row.
	// Used for write operations, FOR UPDATE clauses, and any modification.
	LockExclusive
)

// String returns a human-readable name for the lock mode.
func (m LockMode) String() string {
	switch m {
	case LockShared:
		return "Shared"
	case LockExclusive:
		return "Exclusive"
	default:
		return "Unknown"
	}
}

// LockContext contains configuration for lock acquisition.
type LockContext struct {
	// TxnID is the transaction ID requesting the lock.
	TxnID TxnID

	// TimeoutMs is the maximum time to wait for lock acquisition.
	// A value of 0 means no timeout (wait indefinitely).
	// Future: support for NOWAIT (return immediately if contested) and
	// SKIP LOCKED (skip over locked rows) via negative/negative values.
	TimeoutMs int64

	// Nowait, if true, returns immediately if the lock cannot be acquired.
	// This is equivalent to setting TimeoutMs to 0 with immediate failure.
	Nowait bool
}

// Timeout returns the lock acquisition timeout as a time.Duration.
// Returns 0 for no timeout (wait forever).
func (c *LockContext) Timeout() time.Duration {
	if c.TimeoutMs <= 0 {
		return 0 // No timeout (wait indefinitely)
	}
	return time.Duration(c.TimeoutMs) * time.Millisecond
}

// LockEntry represents a single lock held on a row.
type LockEntry struct {
	// RowKey identifies the locked row (format: tableID:rowID).
	RowKey string

	// Mode is the lock mode (Shared or Exclusive).
	Mode LockMode

	// TxnID is the transaction holding this lock.
	TxnID TxnID

	// AcquiredAt is when the lock was acquired.
	AcquiredAt time.Time
}

// LockStats contains statistics about the lock manager's state.
type LockStats struct {
	// TotalLocks is the total number of locks currently held.
	TotalLocks int64

	// ShardStats contains per-shard lock counts.
	ShardStats []ShardStat
}

// ShardStat contains statistics for a single shard.
type ShardStat struct {
	ShardID  int
	Locks    int
	Waiters  int
}

// LockManager manages row-level locks with sharding to reduce contention.
//
// Thread safety: All methods are safe for concurrent use.
//
// Deadlock prevention: Callers must acquire locks in sorted rowKey order
// to prevent deadlocks. The manager does not enforce ordering internally.
type LockManager interface {
	// Acquire attempts to acquire a lock on rowKey for the given transaction.
	// If the lock is already held by another transaction in exclusive mode,
	// or if a shared lock exists and exclusive is requested, this blocks
	// until the lock becomes available or the timeout is reached.
	//
	// ctx.TimeoutMs controls the maximum wait time:
	//   0 = wait indefinitely
	//   >0 = wait up to TimeoutMs milliseconds
	//
	// Returns true if the lock was acquired, false if timeout occurred.
	Acquire(rowKey string, ctx LockContext, mode LockMode) bool

	// TryAcquire attempts to acquire a lock without blocking.
	// Returns immediately with true if the lock was acquired,
	// or false if the lock is already held by another transaction.
	//
	// This is equivalent to Acquire with Nowait=true.
	TryAcquire(rowKey string, txnID TxnID, mode LockMode) bool

	// Release releases the lock on rowKey held by the given transaction.
	// If the transaction doesn't hold the lock, this is a no-op.
	Release(rowKey string, txnID TxnID)

	// ReleaseAll releases all locks held by the given transaction.
	// This is used for transaction commit/abort and for cleaning up
	// stale locks from aborted transactions.
	ReleaseAll(txnID TxnID)

	// IsLocked returns true if rowKey is currently locked by any transaction.
	IsLocked(rowKey string) bool

	// IsLockedByTxn returns true if rowKey is locked by the specified transaction.
	IsLockedByTxn(rowKey string, txnID TxnID) bool

	// GetLockMode returns the current lock mode for rowKey, or -1 if unlocked.
	GetLockMode(rowKey string) LockMode

	// LockStats returns current statistics about the lock manager.
	LockStats() LockStats

	// Close releases all resources held by the lock manager.
	// After Close, the manager should not be used.
	Close()
}
