package internal

import (
	"testing"

	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

func TestBeginTxnContext(t *testing.T) {
	tm := New()

	// Begin a transaction
	txn := tm.BeginTxnContext()
	if txn == nil {
		t.Fatal("BeginTxnContext() returned nil")
	}
	if !txn.IsActive() {
		t.Error("txn.IsActive() = false, want true")
	}
	if txn.XID() == 0 {
		t.Error("txn.XID() = 0, want non-zero")
	}
	if txn.Snapshot() == nil {
		t.Error("txn.Snapshot() = nil, want non-nil")
	}
	if txn.LockManager() == nil {
		t.Error("txn.LockManager() = nil, want non-nil")
	}

	// Commit the transaction
	err := txn.Commit()
	if err != nil {
		t.Fatalf("txn.Commit() error: %v", err)
	}
	if txn.IsActive() {
		t.Error("txn.IsActive() = true after Commit, want false")
	}
}

func TestBeginTxnContext_Multiple(t *testing.T) {
	tm := New()

	// Begin multiple transactions
	txn1 := tm.BeginTxnContext()
	xid1 := txn1.XID()

	txn2 := tm.BeginTxnContext()
	xid2 := txn2.XID()

	// Each should have unique XID
	if xid1 == xid2 {
		t.Errorf("txn1.XID() = %d, txn2.XID() = %d, want unique IDs", xid1, xid2)
	}

	// Clean up
	txn1.Rollback()
	txn2.Rollback()
}

func TestTxnContext_Commit(t *testing.T) {
	tm := New()
	txn := tm.BeginTxnContext()

	// Should not be able to commit twice
	err := txn.Commit()
	if err != nil {
		t.Fatalf("txn.Commit() error: %v", err)
	}

	err = txn.Commit()
	if err == nil {
		t.Error("txn.Commit() second time should return error")
	}
}

func TestTxnContext_Rollback(t *testing.T) {
	tm := New()
	txn := tm.BeginTxnContext()

	if !txn.IsActive() {
		t.Fatal("txn.IsActive() = false before Rollback")
	}

	txn.Rollback()

	if txn.IsActive() {
		t.Error("txn.IsActive() = true after Rollback, want false")
	}

	// Should be safe to call Rollback multiple times
	txn.Rollback()
}

func TestTxnContext_AddLock(t *testing.T) {
	tm := New()
	txn := tm.BeginTxnContext()

	// Acquire a lock
	ok := txn.AddLock("table1:row1", txnapi.LockExclusive)
	if !ok {
		t.Error("txn.AddLock() returned false, want true")
	}

	// Check that lock is held
	lm := txn.LockManager()
	if lm == nil {
		t.Fatal("txn.LockManager() = nil")
	}

	if !lm.IsLockedByTxn("table1:row1", txn.XID()) {
		t.Error("LockManager.IsLockedByTxn() = false, want true")
	}

	// Clean up
	txn.Rollback()

	if lm.IsLocked("table1:row1") {
		t.Error("LockManager.IsLocked() = true after Rollback, want false")
	}
}

func TestTxnContext_ReleaseAll(t *testing.T) {
	tm := New()
	txn := tm.BeginTxnContext()
	lm := txn.LockManager()

	// Acquire multiple locks
	txn.AddLock("table1:row1", txnapi.LockExclusive)
	txn.AddLock("table1:row2", txnapi.LockExclusive)
	txn.AddLock("table2:row1", txnapi.LockShared)

	// Verify locks are held
	if !lm.IsLocked("table1:row1") || !lm.IsLocked("table1:row2") || !lm.IsLocked("table2:row1") {
		t.Fatal("Locks not held before Commit")
	}

	// Commit should release all locks
	txn.Commit()

	if lm.IsLocked("table1:row1") || lm.IsLocked("table1:row2") || lm.IsLocked("table2:row1") {
		t.Error("Locks still held after Commit")
	}
}

func TestTxnContext_Rollback_ReleasesLocks(t *testing.T) {
	tm := New()
	txn := tm.BeginTxnContext()
	lm := txn.LockManager()

	// Acquire locks
	txn.AddLock("table1:row1", txnapi.LockExclusive)
	txn.AddLock("table1:row2", txnapi.LockExclusive)

	// Rollback should release all locks
	txn.Rollback()

	if lm.IsLocked("table1:row1") || lm.IsLocked("table1:row2") {
		t.Error("Locks still held after Rollback")
	}
}
