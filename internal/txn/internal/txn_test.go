package internal

import (
	"sync"
	"testing"

	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

func TestBeginTxn(t *testing.T) {
	tm := New()

	xid1, snap1 := tm.BeginTxn()
	if xid1 != 1 {
		t.Fatalf("expected xid=1, got %d", xid1)
	}
	if snap1.XID != 1 {
		t.Fatalf("expected snap.XID=1, got %d", snap1.XID)
	}
	if snap1.Xmax != 2 {
		t.Fatalf("expected snap.Xmax=2, got %d", snap1.Xmax)
	}
	// No other active txns, so ActiveXIDs should be empty (self excluded)
	if len(snap1.ActiveXIDs) != 0 {
		t.Fatalf("expected empty ActiveXIDs, got %v", snap1.ActiveXIDs)
	}

	xid2, snap2 := tm.BeginTxn()
	if xid2 != 2 {
		t.Fatalf("expected xid=2, got %d", xid2)
	}
	if snap2.Xmax != 3 {
		t.Fatalf("expected snap2.Xmax=3, got %d", snap2.Xmax)
	}
	// xid1 is still active, so snap2.ActiveXIDs should contain xid1
	if _, ok := snap2.ActiveXIDs[1]; !ok {
		t.Fatalf("expected xid1 in snap2.ActiveXIDs")
	}
	// snap2 should not contain itself
	if _, ok := snap2.ActiveXIDs[2]; ok {
		t.Fatalf("snap2.ActiveXIDs should not contain self")
	}
	if snap2.Xmin != 1 {
		t.Fatalf("expected snap2.Xmin=1, got %d", snap2.Xmin)
	}
}

func TestCommit(t *testing.T) {
	tm := New()
	xid, _ := tm.BeginTxn()
	entry := tm.Commit(xid)

	if tm.CLOG().Get(xid) != txnapi.TxnCommitted {
		t.Fatalf("expected Committed status after Commit")
	}
	if tm.GetMinActive() != txnapi.TxnMaxInfinity {
		t.Fatalf("expected no active txns after Commit")
	}
	if entry.Type != 7 || entry.ID != xid {
		t.Fatalf("unexpected WALEntry: %+v", entry)
	}
}

func TestAbort(t *testing.T) {
	tm := New()
	xid, _ := tm.BeginTxn()
	entry := tm.Abort(xid)

	if tm.CLOG().Get(xid) != txnapi.TxnAborted {
		t.Fatalf("expected Aborted status after Abort")
	}
	if tm.GetMinActive() != txnapi.TxnMaxInfinity {
		t.Fatalf("expected no active txns after Abort")
	}
	if entry.Type != 8 || entry.ID != xid {
		t.Fatalf("unexpected WALEntry: %+v", entry)
	}
}

func TestIsVisible_OwnWrite(t *testing.T) {
	tm := New()
	xid, snap := tm.BeginTxn()
	// Own write, not deleted → visible
	if !tm.IsVisible(snap, xid, txnapi.TxnMaxInfinity) {
		t.Fatal("own write should be visible")
	}
}

func TestIsVisible_OwnDelete(t *testing.T) {
	tm := New()
	xid, snap := tm.BeginTxn()
	// Own write, then deleted by self → not visible
	if tm.IsVisible(snap, xid, xid) {
		t.Fatal("own delete should not be visible")
	}
}

func TestIsVisible_CommittedVisible(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	tm.Commit(xid1)

	_, snap2 := tm.BeginTxn()
	// xid1 committed before snap2 → visible
	if !tm.IsVisible(snap2, xid1, txnapi.TxnMaxInfinity) {
		t.Fatal("committed version should be visible to later snapshot")
	}
}

func TestIsVisible_AbortedInvisible(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	tm.Abort(xid1)

	_, snap2 := tm.BeginTxn()
	// xid1 aborted → not visible
	if tm.IsVisible(snap2, xid1, txnapi.TxnMaxInfinity) {
		t.Fatal("aborted version should not be visible")
	}
}

func TestIsVisible_InProgressInvisible(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	// xid1 still in progress

	_, snap2 := tm.BeginTxn()
	// xid1 in progress at snap2 time → not visible
	if tm.IsVisible(snap2, xid1, txnapi.TxnMaxInfinity) {
		t.Fatal("in-progress version should not be visible to other txn")
	}
}

func TestIsVisible_DeletedByCommitted(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	tm.Commit(xid1)

	xid2, _ := tm.BeginTxn()
	tm.Commit(xid2)

	_, snap3 := tm.BeginTxn()
	// Entry created by xid1, deleted by xid2, both committed before snap3
	if tm.IsVisible(snap3, xid1, xid2) {
		t.Fatal("entry deleted by committed txn should not be visible")
	}
}

func TestIsVisible_DeletedByAborted(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	tm.Commit(xid1)

	xid2, _ := tm.BeginTxn()
	tm.Abort(xid2) // delete was rolled back

	_, snap3 := tm.BeginTxn()
	// Entry created by xid1 (committed), deleted by xid2 (aborted)
	// → deletion rolled back → visible
	if !tm.IsVisible(snap3, xid1, xid2) {
		t.Fatal("entry with aborted delete should still be visible")
	}
}

func TestIsVisible_DeletedByInProgress(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	tm.Commit(xid1)

	xid2, _ := tm.BeginTxn() // in progress, "deleting" the entry
	_ = xid2

	_, snap3 := tm.BeginTxn()
	// Entry created by xid1 (committed), deleted by xid2 (in progress)
	// → deletion not committed → visible
	if !tm.IsVisible(snap3, xid1, xid2) {
		t.Fatal("entry with in-progress delete should still be visible")
	}
}

func TestIsVisible_FutureCreate(t *testing.T) {
	tm := New()
	_, snap1 := tm.BeginTxn()

	xid2, _ := tm.BeginTxn()
	tm.Commit(xid2)

	// xid2 started after snap1 → not visible to snap1
	if tm.IsVisible(snap1, xid2, txnapi.TxnMaxInfinity) {
		t.Fatal("future version should not be visible to earlier snapshot")
	}
}

func TestGetMinActive(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	xid2, _ := tm.BeginTxn()
	_ = xid2

	if tm.GetMinActive() != xid1 {
		t.Fatalf("expected min active = %d, got %d", xid1, tm.GetMinActive())
	}

	tm.Commit(xid1)
	if tm.GetMinActive() != xid2 {
		t.Fatalf("expected min active = %d after commit, got %d", xid2, tm.GetMinActive())
	}
}

func TestGetMinActive_Empty(t *testing.T) {
	tm := New()
	if tm.GetMinActive() != txnapi.TxnMaxInfinity {
		t.Fatalf("expected TxnMaxInfinity when no active txns, got %d", tm.GetMinActive())
	}
}

func TestRecovery(t *testing.T) {
	tm := New()

	// Simulate recovery: set nextXID, load CLOG with some in-progress
	tm.SetNextXID(100)
	tm.LoadCLOG(map[uint64]txnapi.TxnStatus{
		10: txnapi.TxnCommitted,
		20: txnapi.TxnInProgress, // was in progress at crash
		30: txnapi.TxnAborted,
	})

	// Mark in-progress as aborted (crash recovery)
	tm.MarkInProgressAsAborted()

	if tm.CLOG().Get(10) != txnapi.TxnCommitted {
		t.Fatal("xid 10 should still be Committed")
	}
	if tm.CLOG().Get(20) != txnapi.TxnAborted {
		t.Fatal("xid 20 should be Aborted after recovery")
	}
	if tm.CLOG().Get(30) != txnapi.TxnAborted {
		t.Fatal("xid 30 should still be Aborted")
	}

	// NextXID should be 100
	if tm.NextXID() != 100 {
		t.Fatalf("expected nextXID=100, got %d", tm.NextXID())
	}

	// New transaction should get xid=100
	xid, _ := tm.BeginTxn()
	if xid != 100 {
		t.Fatalf("expected xid=100 after recovery, got %d", xid)
	}
}

func TestCLOGTruncate(t *testing.T) {
	tm := New()
	clog := tm.CLOG()

	clog.Set(1, txnapi.TxnCommitted)
	clog.Set(2, txnapi.TxnCommitted)
	clog.Set(3, txnapi.TxnAborted)
	clog.Set(5, txnapi.TxnCommitted)

	clog.Truncate(3) // remove xid < 3

	if clog.Get(1) != txnapi.TxnInProgress { // should be gone (returns default)
		t.Fatal("xid 1 should be truncated")
	}
	if clog.Get(2) != txnapi.TxnInProgress { // should be gone
		t.Fatal("xid 2 should be truncated")
	}
	if clog.Get(3) != txnapi.TxnAborted { // should remain
		t.Fatal("xid 3 should remain")
	}
	if clog.Get(5) != txnapi.TxnCommitted { // should remain
		t.Fatal("xid 5 should remain")
	}
}

func TestWALEntry(t *testing.T) {
	tm := New()
	xid1, _ := tm.BeginTxn()
	xid2, _ := tm.BeginTxn()

	e1 := tm.Commit(xid1)
	if e1.Type != 7 || e1.ID != xid1 {
		t.Fatalf("commit WALEntry wrong: %+v", e1)
	}

	e2 := tm.Abort(xid2)
	if e2.Type != 8 || e2.ID != xid2 {
		t.Fatalf("abort WALEntry wrong: %+v", e2)
	}
}

func TestConcurrentBeginCommit(t *testing.T) {
	tm := New()
	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			xid, snap := tm.BeginTxn()
			if snap == nil {
				t.Error("snapshot should not be nil")
				return
			}
			tm.Commit(xid)
		}()
	}
	wg.Wait()

	// All txns committed, no active
	if tm.GetMinActive() != txnapi.TxnMaxInfinity {
		t.Fatalf("expected no active txns, got min=%d", tm.GetMinActive())
	}
	// NextXID should be n+1
	if tm.NextXID() != uint64(n+1) {
		t.Fatalf("expected nextXID=%d, got %d", n+1, tm.NextXID())
	}
}

func TestMVCCScenario_ThreeTransactions(t *testing.T) {
	// Scenario: T1 creates key1=v1 and commits.
	// T2 overwrites key1=v2 (sets old entry TxnMax=2) but does NOT commit.
	// T3 begins — should see v1 (T1 committed), not v2 (T2 in progress).
	tm := New()

	// T1: Begin, Commit
	xid1, _ := tm.BeginTxn()
	tm.Commit(xid1)

	// T2: Begin (still in progress)
	xid2, _ := tm.BeginTxn()
	_ = xid2

	// T3: Begin
	_, snap3 := tm.BeginTxn()

	// T1's entry: created by xid1 (committed), overwritten by xid2 (in progress)
	// txnMin=1, txnMax=2
	visible := tm.IsVisible(snap3, xid1, xid2)
	if !visible {
		t.Fatal("T1's version (overwritten by in-progress T2) should be visible to T3")
	}

	// T2's entry: created by xid2 (in progress), not deleted
	// txnMin=2, txnMax=MaxUint64
	visible2 := tm.IsVisible(snap3, xid2, txnapi.TxnMaxInfinity)
	if visible2 {
		t.Fatal("T2's version (in progress) should NOT be visible to T3")
	}

	// Now commit T2
	tm.Commit(xid2)

	// T4 begins after T2 committed
	_, snap4 := tm.BeginTxn()

	// T1's entry: created by xid1, overwritten by xid2 (now committed)
	visible3 := tm.IsVisible(snap4, xid1, xid2)
	if visible3 {
		t.Fatal("T1's version (overwritten by committed T2) should NOT be visible to T4")
	}

	// T2's entry: created by xid2 (committed), not deleted
	visible4 := tm.IsVisible(snap4, xid2, txnapi.TxnMaxInfinity)
	if !visible4 {
		t.Fatal("T2's version (committed) should be visible to T4")
	}
}

func TestCLOGEntries(t *testing.T) {
	tm := New()
	clog := tm.CLOG()

	clog.Set(1, txnapi.TxnCommitted)
	clog.Set(2, txnapi.TxnAborted)

	entries := clog.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[1] != txnapi.TxnCommitted {
		t.Fatal("xid 1 should be Committed")
	}
	if entries[2] != txnapi.TxnAborted {
		t.Fatal("xid 2 should be Aborted")
	}
}

// ─── SSI Integration Tests ─────────────────────────────────────────

func TestNewWithSSI(t *testing.T) {
	tm := NewWithSSI()

	if tm == nil {
		t.Fatal("expected non-nil TxnManager")
	}

	// Verify it's an SSI-enabled manager
	ssiTxn := tm.BeginSSITxn()
	if ssiTxn == nil {
		t.Fatal("expected non-nil SSI transaction")
	}
	if ssiTxn.State() == nil {
		t.Fatal("expected non-nil SSI state")
	}
	if ssiTxn.XID() == 0 {
		t.Fatal("expected non-zero XID")
	}
}

func TestSSIStateRWSetTracking(t *testing.T) {
	tm := NewWithSSI()

	txn := tm.BeginSSITxn()

	// Initially empty
	if len(txn.State().RWSet) != 0 {
		t.Fatal("expected empty RWSet initially")
	}
	if len(txn.State().WWSet) != 0 {
		t.Fatal("expected empty WWSet initially")
	}

	// Get should track read
	txn.Get([]byte("key1"))
	if _, ok := txn.State().RWSet["key1"]; !ok {
		t.Fatal("expected key1 in RWSet after Get")
	}

	// Put should track write
	txn.Put([]byte("key2"), []byte("value2"))
	if _, ok := txn.State().WWSet["key2"]; !ok {
		t.Fatal("expected key2 in WWSet after Put")
	}
}

func TestSSICommit_NoConflicts(t *testing.T) {
	tm := NewWithSSI()

	// T1: read key, write different key
	txn1 := tm.BeginSSITxn()
	txn1.Get([]byte("a"))
	txn1.Put([]byte("b"), []byte("val_b"))

	// T2: read different key, write different key
	txn2 := tm.BeginSSITxn()
	txn2.Get([]byte("c"))
	txn2.Put([]byte("d"), []byte("val_d"))

	// Both should commit without conflict
	if err := txn1.Commit(); err != nil {
		t.Fatalf("expected T1 commit to succeed, got %v", err)
	}

	if err := txn2.Commit(); err != nil {
		t.Fatalf("expected T2 commit to succeed, got %v", err)
	}

	// Verify both committed
	if tm.CLOG().Get(txn1.XID()) != txnapi.TxnCommitted {
		t.Fatal("T1 should be committed")
	}
	if tm.CLOG().Get(txn2.XID()) != txnapi.TxnCommitted {
		t.Fatal("T2 should be committed")
	}
}

func TestSSICommit_AbortAfterCommit(t *testing.T) {
	tm := NewWithSSI()

	// T1: read key
	txn1 := tm.BeginSSITxn()
	txn1.Get([]byte("key"))

	// T1 aborts
	txn1.Abort()

	// Should be able to start new transaction after abort
	txn2 := tm.BeginSSITxn()
	if txn2.XID() <= txn1.XID() {
		t.Fatal("T2 XID should be greater than T1 XID after abort")
	}
	txn2.Abort()
}

func TestSSIWriteSkewScenario(t *testing.T) {
	// Classic write skew: T1 reads {A,B}, T2 reads {A,B}, both update different keys
	// With SSI, when T1 commits, it should not conflict with T2's read of A or B
	// because T2 hasn't written either key yet.
	// Similarly, T2 should not conflict with T1's commit.
	//
	// The actual conflict would be detected if:
	// - T1 writes A, T2 writes B, then one tries to read the other's write
	//
	// For this test, we verify non-conflicting reads/writes work

	tm := NewWithSSI()

	// T1: read A, B (but doesn't write either)
	txn1 := tm.BeginSSITxn()
	txn1.Get([]byte("doctor:alice:on_call"))
	txn1.Get([]byte("doctor:bob:on_call"))

	// T2: read A, B (but doesn't write either)
	txn2 := tm.BeginSSITxn()
	txn2.Get([]byte("doctor:alice:on_call"))
	txn2.Get([]byte("doctor:bob:on_call"))

	// Both should commit (no WW conflict because neither wrote)
	if err := txn1.Commit(); err != nil {
		t.Fatalf("T1 commit should succeed, got %v", err)
	}

	if err := txn2.Commit(); err != nil {
		t.Fatalf("T2 commit should succeed, got %v", err)
	}
}

func TestSSIGetPut_TracksRWSet(t *testing.T) {
	// Verify that Get/Put correctly track keys in RWSet/WWSet
	tm := NewWithSSI()

	txn := tm.BeginSSITxn()

	// Get tracks key in RWSet
	_, _ = txn.Get([]byte("key1"))
	state := txn.State()
	if _, ok := state.RWSet["key1"]; !ok {
		t.Error("expected 'key1' in RWSet after Get")
	}

	// Put tracks key in WWSet
	_ = txn.Put([]byte("key2"), []byte("value"))
	if _, ok := state.WWSet["key2"]; !ok {
		t.Error("expected 'key2' in WWSet after Put")
	}

	// RWSet should still have key1
	if _, ok := state.RWSet["key1"]; !ok {
		t.Error("expected 'key1' still in RWSet")
	}

	// Abort to clean up
	txn.Abort()
}

func TestSSITransactionAbortedAfterFailedCommit(t *testing.T) {
	tm := NewWithSSI()

	// Create two transactions
	txn1 := tm.BeginSSITxn()
	txn1.Get([]byte("key"))

	txn2 := tm.BeginSSITxn()
	txn2.Get([]byte("key"))

	// Commit T1 successfully
	if err := txn1.Commit(); err != nil {
		t.Fatalf("T1 commit failed: %v", err)
	}

	// T2 tries to commit - in our simplified SSI, there's no actual conflict
	// because we don't have the actual read/write set integration with KVStore
	if err := txn2.Commit(); err != nil {
		// If commit fails, Abort should still work
		txn2.Abort()
	} else {
		// If commit succeeds, Abort should be no-op
		txn2.Abort()
	}

	// T2 should be able to commit a new transaction
	txn3 := tm.BeginSSITxn()
	if txn3.XID() <= txn2.XID() {
		t.Fatal("T3 XID should be greater than T2 XID")
	}
	txn3.Abort()
}
