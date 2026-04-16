// Package rowlock provides row-level locking for database operations.
//
// This is a new package for row-level locking (by data key tableID:rowID),
// distinct from internal/lock/internal/ which provides page-level B-tree
// locks (by PageID).
//
// Key features:
//   - 16 shards to reduce contention
//   - Shared and Exclusive lock modes
//   - Blocking acquire with timeout support
//   - Non-blocking TryAcquire
//   - Deadlock prevention via sorted rowKey ordering (caller responsibility)
//
// Example usage:
//
//	manager := rowlock.New()
//	ctx := rowlock.LockContext{TxnID: 1, TimeoutMs: 5000}
//	if manager.Acquire("table1:row1", ctx, rowlock.LockExclusive) {
//	    // Do work
//	    manager.Release("table1:row1", 1)
//	}
//	manager.Close()
package rowlock

import (
	"github.com/akzj/go-fast-kv/internal/rowlock/api"
	"github.com/akzj/go-fast-kv/internal/rowlock/internal"
)

// Re-export types from api
type (
	LockMode    = api.LockMode
	LockContext = api.LockContext
	LockEntry   = api.LockEntry
	LockStats   = api.LockStats
	ShardStat   = api.ShardStat
	TxnID       = api.TxnID
)

// Lock modes
const (
	LockShared    = api.LockShared
	LockExclusive = api.LockExclusive
)

// LockManager is the row lock manager interface.
type LockManager = api.LockManager

// New creates a new RowLockManager with 16 shards.
func New() *internal.RowLockManager {
	return internal.New()
}
