// Package txnapi defines the interfaces for the MVCC transaction layer.
//
// Five components:
//   - XIDManager: transaction ID allocation + active transaction set
//   - CLOG: commit log (transaction status table)
//   - Snapshot: consistent read snapshot with visibility boundaries
//   - Visibility: version visibility rules (IsVisible)
//   - BeginTxn: atomic Alloc + Take (combines XIDManager + Snapshot)
//
// Design reference: docs/DESIGN.md §3.9.2-§3.9.5
package txnapi

import (
	"errors"
	"math"

	rowlockapi "github.com/akzj/go-fast-kv/internal/rowlock/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrTxnNotFound is returned when querying a transaction ID that
	// does not exist in the CLOG.
	ErrTxnNotFound = errors.New("txn: transaction not found")
)

// ─── Constants ──────────────────────────────────────────────────────

const (
	// TxnMaxInfinity is the sentinel value for "not deleted" in LeafEntry.TxnMax.
	// Real transaction IDs must be < TxnMaxInfinity.
	TxnMaxInfinity = math.MaxUint64

	// MaxXID is the maximum valid transaction ID.
	// XIDManager must never allocate an XID >= TxnMaxInfinity.
	MaxXID = math.MaxUint64 - 1
)

// ─── TxnStatus ──────────────────────────────────────────────────────

// TxnStatus represents the state of a transaction in the CLOG.
type TxnStatus uint8

const (
	// TxnInProgress means the transaction is still running.
	TxnInProgress TxnStatus = 0

	// TxnCommitted means the transaction has been committed.
	// Its written data is visible to subsequent snapshots.
	TxnCommitted TxnStatus = 1

	// TxnAborted means the transaction has been aborted.
	// Its written data is never visible.
	TxnAborted TxnStatus = 2
)

// String returns a human-readable name for the status.
func (s TxnStatus) String() string {
	switch s {
	case TxnInProgress:
		return "InProgress"
	case TxnCommitted:
		return "Committed"
	case TxnAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// ─── XIDManager ─────────────────────────────────────────────────────

// XIDManager allocates transaction IDs and tracks active transactions.
//
// Thread safety: all methods must be safe for concurrent use.
//
// Design reference: docs/DESIGN.md §3.9.2
type XIDManager interface {
	// Alloc allocates a new XID and adds it to the active set.
	// The returned XID is always < TxnMaxInfinity.
	Alloc() uint64

	// Remove removes an XID from the active set (called after Commit/Abort).
	Remove(xid uint64)

	// GetActive returns a copy of the current active transaction set.
	// Used by Snapshot.Take to capture the active set at snapshot time.
	GetActive() map[uint64]struct{}

	// GetMinActive returns the smallest XID in the active set.
	// Returns TxnMaxInfinity if no active transactions.
	// Used by Vacuum to determine the safe cleanup boundary.
	GetMinActive() uint64

	// NextXID returns the next XID that will be allocated.
	// Used for checkpoint serialization and snapshot xmax.
	NextXID() uint64

	// SetNextXID sets the next allocatable XID (used during recovery).
	SetNextXID(next uint64)

	// Lock/Unlock expose the internal mutex for BeginTxn atomicity.
	// BeginTxn needs to hold the lock while doing Alloc + Take.
	Lock()
	Unlock()
}

// ─── CLOG ───────────────────────────────────────────────────────────

// CLOG (Commit Log) records the final status of each transaction.
//
// After a transaction is removed from the active set, other transactions
// still need to know whether it Committed or Aborted — CLOG is the
// sole authority for this information.
//
// Thread safety: all methods must be safe for concurrent use.
//
// Design reference: docs/DESIGN.md §3.9.3
type CLOG interface {
	// Set records the status of a transaction.
	Set(xid uint64, status TxnStatus)

	// Get returns the status of a transaction.
	// Returns TxnInProgress for unknown XIDs (default).
	Get(xid uint64) TxnStatus

	// Truncate removes all entries with XID < belowXID.
	// Called during Vacuum to reclaim CLOG memory.
	Truncate(belowXID uint64)

	// Entries returns all CLOG entries (for checkpoint serialization).
	Entries() map[uint64]TxnStatus

	// EntriesUpTo returns all CLOG entries with XID < xmax.
	// Used by checkpoint goroutine for consistent snapshot.
	EntriesUpTo(xmax uint64) map[uint64]TxnStatus

	// LoadEntries bulk-loads CLOG entries (for recovery from checkpoint).
	LoadEntries(entries map[uint64]TxnStatus)
}

// ─── Snapshot ───────────────────────────────────────────────────────

// Snapshot represents a consistent read view at a point in time.
//
// Three boundary values define visibility:
//   - XID: this transaction's own ID
//   - Xmin: oldest active XID at snapshot time (anything < xmin has finished)
//   - Xmax: next XID to be allocated at snapshot time (anything >= xmax is future)
//   - ActiveXIDs: transactions in [xmin, xmax) that were still running
//   - ExcludeXID: for write transactions, XID to exclude from ActiveXIDs
//
// Design reference: docs/DESIGN.md §3.9.4
type Snapshot struct {
	XID        uint64             // this transaction's ID
	Xmin       uint64             // oldest active XID at snapshot time
	Xmax       uint64             // next-to-allocate XID at snapshot time
	ActiveXIDs map[uint64]struct{} // active transactions at snapshot time (shared ref)
	ExcludeXID uint64             // XID to exclude from visibility checks (self for write txns)
}

// isXidActive checks if an XID is in the active set.
// If the XID matches ExcludeXID, it's considered NOT active.
func (s *Snapshot) isXidActive(xid uint64) bool {
	if xid == s.ExcludeXID {
		return false
	}
	_, active := s.ActiveXIDs[xid]
	return active
}

// IsVisible determines whether a data version (txnMin, txnMax) is
// visible to this snapshot.
//
// This implements the full PostgreSQL-style MVCC visibility rules.
//
// Design reference: docs/DESIGN.md §3.9.5
func (s *Snapshot) IsVisible(txnMin, txnMax uint64, clog CLOG) bool {
	// Own writes are always visible (unless we deleted it ourselves)
	if txnMin == s.XID {
		return txnMax == TxnMaxInfinity
	}

	// Created after snapshot → not visible
	if txnMin >= s.Xmax {
		return false
	}

	// Creator was still running at snapshot time → not visible
	// (Excludes self via ExcludeXID check)
	if s.isXidActive(txnMin) {
		return false
	}

	// Creator aborted → not visible
	if clog.Get(txnMin) == TxnAborted {
		return false
	}

	// At this point, txnMin is committed and visible to this snapshot.
	// Now check if the entry has been deleted/superseded.

	// Not deleted → visible
	if txnMax == TxnMaxInfinity {
		return true
	}

	// Deleted by ourselves → not visible
	if txnMax == s.XID {
		return false
	}

	// Deleter started after snapshot → visible (deletion hasn't happened yet)
	if txnMax >= s.Xmax {
		return true
	}

	// Deleter was still running at snapshot time → visible (deletion not committed)
	// (Excludes self via ExcludeXID check)
	if s.isXidActive(txnMax) {
		return true
	}

	// Deleter committed → not visible (deletion is effective)
	if clog.Get(txnMax) == TxnCommitted {
		return false
	}

	// Deleter aborted → visible (deletion was rolled back)
	return true
}

// ─── BeginTxn ───────────────────────────────────────────────────────

// BeginTxn atomically allocates an XID and takes a snapshot.
// This must be done under the XIDManager's lock to prevent race conditions.
//
// Design reference: docs/DESIGN.md §3.9.4 (BeginTxn), §3.9.7
//
// Usage:
//
//	xid, snap := txnapi.BeginTxn(xidMgr)
//	defer func() {
//	    if success { Commit(xid, clog, xidMgr) }
//	    else       { Abort(xid, clog, xidMgr) }
//	}()
func BeginTxn(xidMgr XIDManager) (uint64, *Snapshot) {
	xidMgr.Lock()
	defer xidMgr.Unlock()

	xid := xidMgr.NextXID()
	xidMgr.SetNextXID(xid + 1)

	// Add to active set (while still under lock)
	// Note: we need direct access, so XIDManager.Alloc is not used here.
	// Instead, the active set manipulation is done by the implementation.
	// This function is a template — the real implementation handles this
	// inside the locked section.

	// For now, we define the contract: the implementation must provide
	// a BeginTxn that does alloc + snapshot atomically.
	return xid, nil // placeholder — real implementation below
}

// ─── Transaction ─────────────────────────────────────────────────────



// ─── TxnManager combines XIDManager + CLOG + Snapshot management
// into a single interface for convenience.
//
// This is the primary interface used by KVStore.
type TxnManager interface {
	// BeginTxn atomically allocates an XID and creates a snapshot.
	// Returns the XID and a Snapshot for visibility checks.
	// Use for write transactions (Put, Delete, WriteBatch).
	BeginTxn() (uint64, *Snapshot)



	// BeginTxnContext starts a new SQL transaction with row-level locking.
	// Used by the SQL engine for BEGIN/COMMIT/ROLLBACK semantics.
	BeginTxnContext() TxnContext

	// ReadSnapshot creates a read-only snapshot WITHOUT allocating a XID.
	// Returns a logical readID (equal to nextXID at snapshot time) and a
	// frozen Snapshot. The readID is NOT added to the active set and NO
	// CLOG entry is created. No Abort() call is needed for cleanup.
	//
	// Use for read-only operations (Get, Scan) to avoid inflating the
	// CLOG and active set under high read load.
	ReadSnapshot() (uint64, *Snapshot)

	// Commit marks a transaction as committed.
	// Order: WAL.Append(TxnCommit) → clog.Set(Committed) → xidMgr.Remove(xid)
	//
	// Returns a WAL entry for the caller to include in their WAL batch.
	Commit(xid uint64) WALEntry

	// Abort marks a transaction as aborted.
	// Order: WAL.Append(TxnAbort) → clog.Set(Aborted) → xidMgr.Remove(xid)
	//
	// Returns a WAL entry for the caller to include in their WAL batch.
	Abort(xid uint64) WALEntry

	// IsVisible checks if a version (txnMin, txnMax) is visible to a snapshot.
	IsVisible(snap *Snapshot, txnMin, txnMax uint64) bool

	// GetMinActive returns the smallest active XID (for Vacuum).
	GetMinActive() uint64

	// NextXID returns the next XID to be allocated (for checkpoint).
	NextXID() uint64

	// CLOG returns the underlying CLOG (for checkpoint/recovery).
	CLOG() CLOG

	// Recovery methods:

	// SetNextXID sets the next allocatable XID (recovery).
	SetNextXID(next uint64)

	// LoadCLOG bulk-loads CLOG entries from checkpoint.
	LoadCLOG(entries map[uint64]TxnStatus)

	// MarkInProgressAsAborted marks all InProgress transactions as Aborted.
	// Called during crash recovery — any transaction that was in progress
	// at crash time is considered aborted.
	MarkInProgressAsAborted()
}

// WALEntry represents a WAL record for transaction state changes.
type WALEntry struct {
	Type uint8  // walapi.RecordTxnCommit (7) or walapi.RecordTxnAbort (8)
	ID   uint64 // XID
}

// ─── TxnContext (SQL Layer) ─────────────────────────────────────────

// TxnContext represents an active SQL transaction with row-level locking.
// Created by TxnManager.BeginTxnContext().
type TxnContext interface {
	XID() uint64
	Snapshot() *Snapshot
	LockManager() LockManager
	AddLock(rowKey string, mode LockMode) bool
	Commit() error
	Rollback()
	IsActive() bool
	// AddPendingWrite records a key modified within this transaction.
	// Used by SQL executor to track writes for rollback.
	AddPendingWrite(key []byte)
	// GetPendingWrites returns all keys modified within this transaction.
	// Used by Tx.Rollback() to call store.DeleteWithXID for each.
	GetPendingWrites() [][]byte
}

// TxnContextFactory creates TxnContext instances.
type TxnContextFactory interface {
	BeginTxnContext() TxnContext
}

// LockManager is the row lock manager interface (re-exported from rowlock).
type LockManager = rowlockapi.LockManager

// LockMode represents the type of lock (re-exported from rowlock).
type LockMode = rowlockapi.LockMode

// LockShared is a shared lock allowing multiple readers.
const LockShared = rowlockapi.LockShared

// LockExclusive is an exclusive lock preventing other readers/writers.
const LockExclusive = rowlockapi.LockExclusive

// LockContext contains configuration for lock acquisition.
type LockContext = rowlockapi.LockContext

// TxnID is a transaction identifier.
type TxnID = rowlockapi.TxnID

// LockStats contains statistics about the lock manager.
type LockStats = rowlockapi.LockStats

// ShardStat contains statistics for a single shard.
type ShardStat = rowlockapi.ShardStat
