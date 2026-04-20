// Package internal implements the MVCC transaction layer.
//
// It provides transaction ID allocation, commit log (CLOG),
// snapshot management, and visibility checking.
//
// Design reference: docs/DESIGN.md §3.9.2-§3.9.5
package internal

import (
	"fmt"
	"sync"

	"github.com/akzj/go-fast-kv/internal/rowlock"
	rowlockapi "github.com/akzj/go-fast-kv/internal/rowlock/api"
	api "github.com/akzj/go-fast-kv/internal/txn/api"
)

// ─── CLOG implementation ────────────────────────────────────────────

// commitLog implements api.CLOG.
type commitLog struct {
	mu       sync.RWMutex
	statuses map[uint64]api.TxnStatus
}

func newCommitLog() *commitLog {
	return &commitLog{
		statuses: make(map[uint64]api.TxnStatus),
	}
}

func (c *commitLog) Set(xid uint64, status api.TxnStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statuses[xid] = status
}

func (c *commitLog) Get(xid uint64) api.TxnStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	status, ok := c.statuses[xid]
	if !ok {
		return api.TxnInProgress // default for unknown XIDs
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

func (c *commitLog) Entries() map[uint64]api.TxnStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[uint64]api.TxnStatus, len(c.statuses))
	for k, v := range c.statuses {
		result[k] = v
	}
	return result
}

func (c *commitLog) LoadEntries(entries map[uint64]api.TxnStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range entries {
		c.statuses[k] = v
	}
}

// ─── TxnManager implementation ─────────────────────────────────────

// TxnManager implements api.TxnManager.
// Exported as type alias for use in kvstore.
type TxnManager struct {
	mu      sync.Mutex
	nextXID uint64
	active  map[uint64]struct{}
	clog    *commitLog

	// Shared row lock manager for all transactions from this TxnManager
	lockMgr rowlock.LockManager
}

// New creates a new TxnManager.
// nextXID starts at 1 (0 is reserved as invalid).
func New() api.TxnManager {
	return &TxnManager{
		nextXID: 1,
		active:  make(map[uint64]struct{}),
		clog:    newCommitLog(),
		lockMgr: rowlock.New(),
	}
}

// BeginTxn atomically allocates an XID and creates a snapshot.
func (tm *TxnManager) BeginTxn() (uint64, *api.Snapshot) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	xid := tm.nextXID
	tm.nextXID++
	tm.active[xid] = struct{}{}

	activeXIDs := make(map[uint64]struct{}, len(tm.active)-1)
	for id := range tm.active {
		if id != xid {
			activeXIDs[id] = struct{}{}
		}
	}

	xmin := tm.nextXID
	for id := range activeXIDs {
		if id < xmin {
			xmin = id
		}
	}

	snap := &api.Snapshot{
		XID:        xid,
		Xmin:       xmin,
		Xmax:       tm.nextXID,
		ActiveXIDs: activeXIDs,
	}

	return xid, snap
}

// Commit marks a transaction as committed.
func (tm *TxnManager) Commit(xid uint64) api.WALEntry {
	tm.mu.Lock()
	tm.clog.Set(xid, api.TxnCommitted)
	delete(tm.active, xid)
	tm.mu.Unlock()

	return api.WALEntry{Type: 7, ID: xid}
}

// Abort marks a transaction as aborted.
func (tm *TxnManager) Abort(xid uint64) api.WALEntry {
	tm.mu.Lock()
	tm.clog.Set(xid, api.TxnAborted)
	delete(tm.active, xid)
	tm.mu.Unlock()

	return api.WALEntry{Type: 8, ID: xid}
}

// IsVisible checks if a version is visible to a snapshot.
func (tm *TxnManager) IsVisible(snap *api.Snapshot, txnMin, txnMax uint64) bool {
	return snap.IsVisible(txnMin, txnMax, tm.clog)
}

// ReadSnapshot creates a read-only snapshot WITHOUT allocating a XID.
func (tm *TxnManager) ReadSnapshot() (uint64, *api.Snapshot) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	readID := tm.nextXID

	activeXIDs := make(map[uint64]struct{}, len(tm.active))
	for id := range tm.active {
		activeXIDs[id] = struct{}{}
	}

	xmin := tm.nextXID
	for id := range activeXIDs {
		if id < xmin {
			xmin = id
		}
	}

	snap := &api.Snapshot{
		XID:        readID,
		Xmin:       xmin,
		Xmax:       tm.nextXID,
		ActiveXIDs: activeXIDs,
	}

	return readID, snap
}

// GetMinActive returns the smallest active XID.
func (tm *TxnManager) GetMinActive() uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var minXID uint64 = api.TxnMaxInfinity
	for xid := range tm.active {
		if xid < minXID {
			minXID = xid
		}
	}
	return minXID
}

// NextXID returns the next XID to be allocated.
func (tm *TxnManager) NextXID() uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.nextXID
}

// CLOG returns the underlying CLOG.
func (tm *TxnManager) CLOG() api.CLOG {
	return tm.clog
}

// SetNextXID sets the next allocatable XID (recovery).
func (tm *TxnManager) SetNextXID(next uint64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.nextXID = next
}

// LoadCLOG bulk-loads CLOG entries from checkpoint.
func (tm *TxnManager) LoadCLOG(entries map[uint64]api.TxnStatus) {
	tm.clog.LoadEntries(entries)
}

// MarkInProgressAsAborted marks all InProgress transactions as Aborted.
func (tm *TxnManager) MarkInProgressAsAborted() {
	tm.clog.mu.Lock()
	defer tm.clog.mu.Unlock()
	for xid, status := range tm.clog.statuses {
		if status == api.TxnInProgress {
			tm.clog.statuses[xid] = api.TxnAborted
		}
	}
}

// ─── TxnContext Implementation ──────────────────────────────────────

// txnContext implements api.TxnContext for SQL-layer transactions.
type txnContext struct {
	txnManager   *TxnManager
	xid          uint64
	snap         *api.Snapshot
	lockMgr      rowlock.LockManager
	active       bool
	pendingWrites [][]byte // keys modified within this transaction, for rollback
}

func (tc *txnContext) XID() uint64 {
	return tc.xid
}

func (tc *txnContext) Snapshot() *api.Snapshot {
	return tc.snap
}

func (tc *txnContext) LockManager() rowlock.LockManager {
	return tc.lockMgr
}

func (tc *txnContext) AddLock(rowKey string, mode rowlockapi.LockMode) bool {
	ctx := rowlockapi.LockContext{
		TxnID:     tc.xid,
		TimeoutMs: 5000, // 5 second default timeout
	}
	return tc.lockMgr.Acquire(rowKey, ctx, mode)
}

func (tc *txnContext) Commit() error {
	if !tc.active {
		return fmt.Errorf("txn: transaction already closed")
	}
	tc.active = false
	tc.lockMgr.ReleaseAll(tc.xid)
	tc.txnManager.Commit(tc.xid)
	return nil
}

func (tc *txnContext) Rollback() {
	if !tc.active {
		return
	}
	tc.active = false
	tc.lockMgr.ReleaseAll(tc.xid)
	tc.txnManager.Abort(tc.xid)
}

func (tc *txnContext) IsActive() bool {
	return tc.active
}

// AddPendingWrite records a key that was modified within this transaction.
// Used by the SQL executor to track writes for potential rollback.
// Keys are stored in insertion order (first write first).
func (tc *txnContext) AddPendingWrite(key []byte) {
	tc.pendingWrites = append(tc.pendingWrites, append([]byte(nil), key...))
}

// GetPendingWrites returns a copy of all keys modified within this transaction.
// Used by Tx.Rollback() to iterate pending keys and call store.DeleteWithXID.
func (tc *txnContext) GetPendingWrites() [][]byte {
	result := make([][]byte, len(tc.pendingWrites))
	copy(result, tc.pendingWrites)
	return result
}

// BeginTxnContext starts a new SQL transaction with row-level locking.
func (tm *TxnManager) BeginTxnContext() api.TxnContext {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	xid := tm.nextXID
	tm.nextXID++
	tm.active[xid] = struct{}{}

	activeXIDs := make(map[uint64]struct{}, len(tm.active)-1)
	for id := range tm.active {
		if id != xid {
			activeXIDs[id] = struct{}{}
		}
	}

	xmin := tm.nextXID
	for id := range activeXIDs {
		if id < xmin {
			xmin = id
		}
	}

	snap := &api.Snapshot{
		XID:        xid,
		Xmin:       xmin,
		Xmax:       tm.nextXID,
		ActiveXIDs: activeXIDs,
	}

	return &txnContext{
		txnManager: tm,
		xid:        xid,
		snap:       snap,
		lockMgr:    tm.lockMgr, // SHARED across all TxnContexts from same TxnManager
		active:     true,
	}
}
