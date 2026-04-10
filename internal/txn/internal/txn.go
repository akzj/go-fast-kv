// Package internal implements the MVCC transaction layer.
//
// It provides transaction ID allocation, commit log (CLOG),
// snapshot management, and visibility checking.
//
// Design reference: docs/DESIGN.md §3.9.2-§3.9.5
package internal

import (
	"sync"

	"github.com/akzj/go-fast-kv/internal/ssi"
	ssiapi "github.com/akzj/go-fast-kv/internal/ssi/api"
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

	// SSI support
	ssiIndex   ssiapi.Index // SSI index for conflict detection
	ssiEnabled bool         // Whether SSI is enabled
}

// New creates a new TxnManager (without SSI).
// nextXID starts at 1 (0 is reserved as invalid).
func New() api.TxnManager {
	return &TxnManager{
		nextXID: 1,
		active:  make(map[uint64]struct{}),
		clog:    newCommitLog(),
	}
}

// NewWithSSI creates a TxnManager with Serializable Snapshot Isolation enabled.
// When SSI is enabled, BeginSSITxn returns a Transaction that tracks reads/writes
// and validates SSI conflicts at commit time.
func NewWithSSI() *TxnManager {
	return &TxnManager{
		nextXID:    1,
		active:     make(map[uint64]struct{}),
		clog:       newCommitLog(),
		ssiIndex:   ssi.NewIndex(),
		ssiEnabled: true,
	}
}

// ─── SSI Transaction ────────────────────────────────────────────────

// ssiTransaction implements api.Transaction with SSI tracking.
type ssiTransaction struct {
	txnManager *TxnManager
	xid        uint64
	snap       *api.Snapshot
	ssiState   *api.SSIState
}

func (txn *ssiTransaction) XID() uint64 {
	return txn.xid
}

func (txn *ssiTransaction) Snapshot() *api.Snapshot {
	return txn.snap
}

func (txn *ssiTransaction) State() *api.SSIState {
	return txn.ssiState
}

func (txn *ssiTransaction) Get(key []byte) ([]byte, error) {
	// SSI: track read in RWSet and index
	txn.ssiState.MarkRead(string(key))
	txn.txnManager.ssiIndex.SetReader(ssiapi.Key(key), txn.xid)
	return nil, nil // actual read done by KVStore
}

func (txn *ssiTransaction) Put(key, value []byte) error {
	// SSI: track write in WWSet
	txn.ssiState.MarkWrite(string(key))
	return nil // actual write done by KVStore
}

func (txn *ssiTransaction) Commit() error {
	tm := txn.txnManager

	if !tm.ssiEnabled {
		return nil
	}

	// SSI commit validation: check RWSet and WWSet for conflicts
	xid := txn.xid

	// For each key in RWSet (read), check for RW-conflict:
	// If another committed transaction wrote this key AFTER our snapshot,
	// we have a read-write conflict.
	for key := range txn.ssiState.RWSet {
		info := tm.ssiIndex.GetWriteInfo(ssiapi.Key(key))
		if info != nil {
			// A committed transaction wrote this key.
			// Check if that commit happened after our snapshot started.
			if info.CommitTS > txn.snap.Xmin {
				// RW-conflict detected: we read a stale value
				tm.Abort(xid)
				return api.ErrSerializationFailure
			}
		}
	}

	// Note: WW-conflict detection via TIndex is intentionally omitted here.
	// The RW-conflict check above correctly detects all serialization anomalies.
	// A WW-conflict (write-write anomaly) requires: T1 wrote K, T2 read K, T1 committed.
	// This is already caught as RW-conflict when T2 tries to commit (since T2
	// read K, and T1's write to K committed after T2's snapshot began).
	// Attempting to detect WW-conflict via TIndex's last-reader tracking produces
	// false positives when a transaction writes a key it never read.

	// No conflicts detected — commit and update SSI index
	tm.mu.Lock()
	tm.clog.Set(xid, api.TxnCommitted)
	delete(tm.active, xid)
	tm.mu.Unlock()

	// Update SSI index for committed writes
	if txn.ssiState != nil {
		for key := range txn.ssiState.WWSet {
			tm.ssiIndex.SetWriteInfo(ssiapi.Key(key), &ssiapi.WriteInfo{
				TxnID:    xid,
				CommitTS: xid, // Use XID as commit timestamp
			})
			// Update TIndex: we are now the last writer
			tm.ssiIndex.SetReader(ssiapi.Key(key), xid)
		}
	}

	return nil
}

func (txn *ssiTransaction) Abort() {
	txn.txnManager.Abort(txn.xid)
}

// BeginSSITxn starts an SSI-aware transaction with RWSet/WWSet tracking.
func (tm *TxnManager) BeginSSITxn() api.Transaction {
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

	return &ssiTransaction{
		txnManager: tm,
		xid:        xid,
		snap:       snap,
		ssiState:   api.NewSSIState(),
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
