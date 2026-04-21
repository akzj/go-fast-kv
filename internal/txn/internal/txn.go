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

// EntriesUpTo returns all CLOG entries with XID < xmax.
func (c *commitLog) EntriesUpTo(xmax uint64) map[uint64]api.TxnStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[uint64]api.TxnStatus)
	for xid, status := range c.statuses {
		if xid < xmax {
			result[xid] = status
		}
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

	// Default lock timeout in milliseconds for row locks
	lockTimeoutMs int64
}

// New creates a new TxnManager.
// nextXID starts at 1 (0 is reserved as invalid).
// Default lock timeout is 5000ms (backward compatible).
func New() api.TxnManager {
	return &TxnManager{
		nextXID:       1,
		active:        make(map[uint64]struct{}),
		clog:          newCommitLog(),
		lockMgr:       rowlock.New(),
		lockTimeoutMs: 5000, // default 5 second lock timeout
	}
}

// NewWithLockTimeout creates a new TxnManager with a custom lock timeout.
// lockTimeoutMs: timeout in milliseconds for row lock acquisition.
// Set to 0 for no timeout (wait indefinitely).
func NewWithLockTimeout(lockTimeoutMs int64) api.TxnManager {
	return &TxnManager{
		nextXID:       1,
		active:        make(map[uint64]struct{}),
		clog:          newCommitLog(),
		lockMgr:       rowlock.New(),
		lockTimeoutMs: lockTimeoutMs,
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

// ─── Savepoint ─────────────────────────────────────────────────────

// pendingWrite stores a key modified in this transaction, along with its
// pre-write value for savepoint rollback.
// preValue is non-nil for UPDATE/DELETE (the encoded row before modification).
// nil for INSERT (nothing to restore — just delete on rollback).
type pendingWrite struct {
	key      []byte
	preValue []byte // nil for INSERT, non-nil for UPDATE/DELETE
}

// savepoint stores the transaction state at a point in time.
// It is used to implement nested transactions via SAVEPOINT/ROLLBACK TO.
type savepoint struct {
	Name          string
	Snap          *api.Snapshot    // snapshot at savepoint creation
	PendingWrites []pendingWrite   // keys written since this savepoint
}

// ─── TxnContext Implementation ──────────────────────────────────────

// txnContext implements api.TxnContext for SQL-layer transactions.
type txnContext struct {
	txnManager    *TxnManager
	xid           uint64
	snap          *api.Snapshot
	lockMgr       rowlock.LockManager
	lockTimeoutMs int64
	active        bool
	pendingWrites []pendingWrite // writes within this transaction, for rollback

	// Savepoint stack for nested transactions.
	// Savepoints are ordered: earlier savepoints are at lower indices.
	// RollbackToSavepoint pops everything after the named savepoint.
	savepoints []savepoint
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
	timeoutMs := tc.lockTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = 5000 // default for backward compatibility
	}
	ctx := rowlockapi.LockContext{
		TxnID:     tc.xid,
		TimeoutMs: timeoutMs,
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
// preValue is nil for INSERT (nothing to restore on rollback).
// preValue is non-nil for UPDATE/DELETE (the encoded old row to restore on rollback).
// Keys are stored in insertion order (first write first).
func (tc *txnContext) AddPendingWrite(key []byte, preValue []byte) {
	pw := pendingWrite{
		key: append([]byte(nil), key...),
	}
	if preValue != nil {
		pw.preValue = append([]byte(nil), preValue...)
	}
	tc.pendingWrites = append(tc.pendingWrites, pw)
}

// GetPendingWrites returns a copy of all keys modified within this transaction.
// Used by Tx.Rollback() to iterate pending keys and call store.DeleteWithXID.
func (tc *txnContext) GetPendingWrites() [][]byte {
	result := make([][]byte, len(tc.pendingWrites))
	for i, pw := range tc.pendingWrites {
		result[i] = pw.key
	}
	return result
}

// GetSavepoints returns the current savepoint stack (for testing/debugging).
func (tc *txnContext) GetSavepoints() []string {
	names := make([]string, len(tc.savepoints))
	for i, sp := range tc.savepoints {
		names[i] = sp.Name
	}
	return names
}

// CreateSavepoint creates a new named savepoint.
// The snapshot and pending writes at this point are saved.
func (tc *txnContext) CreateSavepoint(name string) error {
	if !tc.active {
		return fmt.Errorf("txn: cannot create savepoint on inactive transaction")
	}
	// Deep copy the snapshot
	snapCopy := &api.Snapshot{
		XID:        tc.snap.XID,
		Xmin:       tc.snap.Xmin,
		Xmax:       tc.snap.Xmax,
		ActiveXIDs: make(map[uint64]struct{}, len(tc.snap.ActiveXIDs)),
	}
	for k, v := range tc.snap.ActiveXIDs {
		snapCopy.ActiveXIDs[k] = v
	}
	// Deep copy pending writes at this point
	pwCopy := make([]pendingWrite, len(tc.pendingWrites))
	for i, w := range tc.pendingWrites {
		pwCopy[i] = pendingWrite{
			key:      append([]byte(nil), w.key...),
			preValue: append([]byte(nil), w.preValue...),
		}
	}
	tc.savepoints = append(tc.savepoints, savepoint{
		Name:          name,
		Snap:          snapCopy,
		PendingWrites: pwCopy,
	})
	return nil
}

// RollbackToSavepoint rolls back to a named savepoint.
// All writes made after the savepoint are undone:
//   - For INSERT (preValue==nil): key is deleted with this XID (invisible).
//   - For UPDATE/DELETE (preValue!=nil): key is restored to preValue.
// The savepoint itself is NOT removed (can be rolled back to again).
func (tc *txnContext) RollbackToSavepoint(name string, store interface {
	DeleteWithXID(key []byte, xid uint64) error
	PutWithXID(key, value []byte, xid uint64) error
}) error {
	if !tc.active {
		return fmt.Errorf("tx: cannot rollback to savepoint on inactive transaction")
	}
	// Find the savepoint by name
	idx := -1
	for i, sp := range tc.savepoints {
		if sp.Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("tx: savepoint %q not found", name)
	}
	sp := tc.savepoints[idx]
	// Undo writes made after this savepoint (in reverse order).
	// Iterate from the end of pendingWrites down to sp.PendingWrites length.
	for i := len(tc.pendingWrites) - 1; i >= len(sp.PendingWrites); i-- {
		pw := tc.pendingWrites[i]
		if pw.preValue == nil {
			// INSERT: just delete the key (it shouldn't exist in the pre-savepoint state)
			store.(interface {
				DeleteWithXID(key []byte, xid uint64) error
			}).DeleteWithXID(pw.key, tc.xid)
		} else {
			// UPDATE or DELETE: restore the pre-value (the old row/image)
			// For DELETE: preValue==the pre-deleted value → restores the row.
			// For UPDATE: preValue==the old row value → restores the previous version.
			store.(interface {
				PutWithXID(key, value []byte, xid uint64) error
			}).PutWithXID(pw.key, pw.preValue, tc.xid)
		}
	}
	// Restore pending writes to savepoint state
	tc.pendingWrites = make([]pendingWrite, len(sp.PendingWrites))
	copy(tc.pendingWrites, sp.PendingWrites)
	return nil
}

// ReleaseSavepoint removes a named savepoint.
// Writes made within the savepoint are kept (committed to the transaction).
// An error is returned if the savepoint does not exist.
func (tc *txnContext) ReleaseSavepoint(name string) error {
	if !tc.active {
		return fmt.Errorf("tx: cannot release savepoint on inactive transaction")
	}
	// Find and remove the savepoint
	newList := make([]savepoint, 0, len(tc.savepoints))
	found := false
	for _, sp := range tc.savepoints {
		if sp.Name == name {
			found = true
			continue // skip this one (remove it)
		}
		newList = append(newList, sp)
	}
	if !found {
		return fmt.Errorf("tx: savepoint %q not found", name)
	}
	tc.savepoints = newList
	return nil
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
		txnManager:    tm,
		xid:           xid,
		snap:          snap,
		lockMgr:       tm.lockMgr, // SHARED across all TxnContexts from same TxnManager
		lockTimeoutMs: tm.lockTimeoutMs,
		active:        true,
	}
}
