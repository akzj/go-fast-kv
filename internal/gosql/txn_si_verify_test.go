package gosql

import (
	"database/sql"
	"testing"
)

// TestOwnWritesVisibleAndCommitted verifies that reads within a transaction
// see their own uncommitted writes (via MVCC txnMin == readTxnID visibility rule).
func TestOwnWritesVisibleAndCommitted(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 10)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// UPDATE within txn
	_, err = tx.Exec("UPDATE t SET x = $1 WHERE id = $2", 20, 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("UPDATE: %v", err)
	}

	// Read within transaction — should see x=20 (own uncommitted write)
	var x int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	tx.Rollback()

	if x != 20 {
		t.Errorf("own write not visible: x=%d (expected 20)", x)
	}
}

// TestUpdatePersistenceAfterCommit verifies COMMIT persists writes.
func TestUpdatePersistenceAfterCommit(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 10)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	_, err = tx.Exec("UPDATE t SET x = $1 WHERE id = $2", 20, 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("UPDATE: %v", err)
	}

	tx.Commit()

	// After commit, should see x=20
	var x int
	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if x != 20 {
		t.Errorf("expected x=20 after commit, got x=%d", x)
	}
}
