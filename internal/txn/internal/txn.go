// Package txn implements the MVCC transaction layer.
//
// It provides transaction ID allocation, commit log (CLOG),
// snapshot management, and visibility checking.
//
// Design reference: docs/DESIGN.md §3.9.2-§3.9.5
package internal

import (
	"sync"

	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

// ─── CLOG implementation ────────────────────────────────────────────

// commitLog implements txnapi.CLOG.
type commitLog struct {
	mu       sync.RWMutex
	statuses map[uint64]txnapi.TxnStatus
}

func newCommitLog() *commitLog {
	return &commitLog{
		statuses: make(map[uint64]txnapi.TxnStatus),
	}
}

func (c *commitLog) Set(xid uint64, status txnapi.TxnStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statuses[xid] = status
}

func (c *commitLog) Get(xid uint64) txnapi.TxnStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	status, ok := c.statuses[xid]
	if !ok {
		return txnapi.TxnInProgress // default for unknown XIDs
	}
	return status
}

func (c *commitLog) Truncate(belowXID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for xid := range c.statuses {
		if xid < belowXID {
			delete(c.statuses, xid)
		}
	}
}

func (c *commitLog) Entries() map[uint64]txnapi.TxnStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[uint64]txnapi.TxnStatus, len(c.statuses))
	for k, v := range c.statuses {
		result[k] = v
	}
	return result
}

func (c *commitLog) LoadEntries(entries map[uint64]txnapi.TxnStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range entries {
		c.statuses[k] = v
	}
}

// ─── TxnManager implementation ─────────────────────────────────────

// txnManager implements txnapi.TxnManager.
type txnManager struct {
	mu      sync.Mutex
	nextXID uint64
	active  map[uint64]struct{}
	clog    *commitLog
}

// New creates a new TxnManager.
// nextXID starts at 1 (0 is reserved as invalid).
func New() txnapi.TxnManager {
	return &txnManager{
		nextXID: 1,
		active:  make(map[uint64]struct{}),
		clog:    newCommitLog(),
	}
}

// BeginTxn atomically allocates an XID and creates a snapshot.
//
// The snapshot's ActiveXIDs excludes the new transaction itself,
// because self-visibility is handled by the IsVisible check
// (txnMin == snap.XID branch).
func (tm *txnManager) BeginTxn() (uint64, *txnapi.Snapshot) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Allocate XID
	xid := tm.nextXID
	tm.nextXID++

	// Add to active set
	tm.active[xid] = struct{}{}

	// Create snapshot (under the same lock — atomic with allocation)
	// ActiveXIDs: copy of active set, excluding self
	activeXIDs := make(map[uint64]struct{}, len(tm.active)-1)
	for id := range tm.active {
		if id != xid {
			activeXIDs[id] = struct{}{}
		}
	}

	// Xmin: smallest active XID (excluding self)
	xmin := tm.nextXID // default if no other active txns
	for id := range activeXIDs {
		if id < xmin {
			xmin = id
		}
	}

	snap := &txnapi.Snapshot{
		XID:        xid,
		Xmin:       xmin,
		Xmax:       tm.nextXID,
		ActiveXIDs: activeXIDs,
	}

	return xid, snap
}

// Commit marks a transaction as committed.
// Returns a WAL entry for the caller to batch.
//
// Both the CLOG update and active-set removal are performed under tm.mu
// to prevent a snapshot race: without this, a BeginTxn between the CLOG
// set and the active-set removal would see the xid as both committed
// (in CLOG) and still active (in ActiveXIDs), causing IsVisible to
// incorrectly return false — a transient phantom read.
//
// Lock ordering: tm.mu → clog.mu (safe — no reverse path exists).
func (tm *txnManager) Commit(xid uint64) txnapi.WALEntry {
	tm.mu.Lock()
	tm.clog.Set(xid, txnapi.TxnCommitted)
	delete(tm.active, xid)
	tm.mu.Unlock()

	return txnapi.WALEntry{Type: 7, ID: xid} // RecordTxnCommit
}

// Abort marks a transaction as aborted.
// Returns a WAL entry for the caller to batch.
//
// Both the CLOG update and active-set removal are performed under tm.mu
// for the same reason as Commit (see above). For Abort the impact is
// smaller (both paths return "not visible"), but consistency matters.
func (tm *txnManager) Abort(xid uint64) txnapi.WALEntry {
	tm.mu.Lock()
	tm.clog.Set(xid, txnapi.TxnAborted)
	delete(tm.active, xid)
	tm.mu.Unlock()

	return txnapi.WALEntry{Type: 8, ID: xid} // RecordTxnAbort
}

// IsVisible checks if a version (txnMin, txnMax) is visible to a snapshot.
func (tm *txnManager) IsVisible(snap *txnapi.Snapshot, txnMin, txnMax uint64) bool {
	return snap.IsVisible(txnMin, txnMax, tm.clog)
}

// ReadSnapshot creates a read-only snapshot WITHOUT allocating a XID.
// Returns a logical readID (equal to nextXID at snapshot time) and a
// frozen Snapshot. The readID is NOT added to the active set and NO
// CLOG entry is created. No Abort() call is needed for cleanup.
//
// This avoids inflating the CLOG and active set under high read load.
// The snapshot captures the same consistent view as BeginTxn but without
// the write-transaction overhead.
func (tm *txnManager) ReadSnapshot() (uint64, *txnapi.Snapshot) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Use nextXID as the logical readID — it's guaranteed to be higher
	// than any currently allocated XID, so it won't conflict with any
	// real transaction. We do NOT increment nextXID or add to active set.
	readID := tm.nextXID

	// Freeze the active set (same as BeginTxn)
	activeXIDs := make(map[uint64]struct{}, len(tm.active))
	for id := range tm.active {
		activeXIDs[id] = struct{}{}
	}

	// Xmin: smallest active XID
	xmin := tm.nextXID // default if no active txns
	for id := range activeXIDs {
		if id < xmin {
			xmin = id
		}
	}

	snap := &txnapi.Snapshot{
		XID:        readID,
		Xmin:       xmin,
		Xmax:       tm.nextXID,
		ActiveXIDs: activeXIDs,
	}

	return readID, snap
}

// GetMinActive returns the smallest active XID.
// Returns TxnMaxInfinity if no active transactions.
func (tm *txnManager) GetMinActive() uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var minXID uint64 = txnapi.TxnMaxInfinity
	for xid := range tm.active {
		if xid < minXID {
			minXID = xid
		}
	}
	return minXID
}

// NextXID returns the next XID to be allocated.
func (tm *txnManager) NextXID() uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.nextXID
}

// CLOG returns the underlying CLOG.
func (tm *txnManager) CLOG() txnapi.CLOG {
	return tm.clog
}

// SetNextXID sets the next allocatable XID (recovery).
func (tm *txnManager) SetNextXID(next uint64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.nextXID = next
}

// LoadCLOG bulk-loads CLOG entries from checkpoint.
func (tm *txnManager) LoadCLOG(entries map[uint64]txnapi.TxnStatus) {
	tm.clog.LoadEntries(entries)
}

// MarkInProgressAsAborted marks all InProgress transactions as Aborted.
// Called during crash recovery.
func (tm *txnManager) MarkInProgressAsAborted() {
	tm.clog.mu.Lock()
	defer tm.clog.mu.Unlock()
	for xid, status := range tm.clog.statuses {
		if status == txnapi.TxnInProgress {
			tm.clog.statuses[xid] = txnapi.TxnAborted
		}
	}
}
