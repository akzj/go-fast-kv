package gosql

import (
	"database/sql"
	"testing"
)

// TestRollbackUndoesInsert verifies that ROLLBACK undoes an INSERT.
// BEGIN → INSERT → ROLLBACK → SELECT row NOT found.
func TestRollbackUndoesInsert(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	_, err = tx.Exec("INSERT INTO t VALUES ($1, $2)", 1, 100)
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT: %v", err)
	}

	// Read within txn — should see own uncommitted write
	var x int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != nil {
		tx.Rollback()
		t.Fatalf("SELECT within txn: %v", err)
	}
	if x != 100 {
		t.Errorf("expected x=100 in txn, got x=%d", x)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// After rollback, row should NOT exist
	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != sql.ErrNoRows {
		if err == nil {
			t.Errorf("row still visible after rollback: x=%d (expected no rows)", x)
		} else {
			t.Errorf("SELECT after rollback: %v", err)
		}
	}
	// sql.ErrNoRows is the expected result — test passes
}

// TestRollbackUndoesUpdate verifies that ROLLBACK undoes an UPDATE.
// BEGIN → UPDATE → ROLLBACK → SELECT original value.
func TestRollbackUndoesUpdate(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")
	db.Exec("INSERT INTO t VALUES ($1, $2)", 1, 10)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	_, err = tx.Exec("UPDATE t SET x = $1 WHERE id = $2", 999, 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("UPDATE: %v", err)
	}

	// Read within txn — should see own uncommitted write
	var x int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != nil {
		tx.Rollback()
		t.Fatalf("SELECT within txn: %v", err)
	}
	if x != 999 {
		t.Errorf("expected x=999 in txn, got x=%d", x)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// After rollback, should see original value x=10
	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != nil {
		t.Fatalf("SELECT after rollback: %v", err)
	}
	if x != 10 {
		t.Errorf("expected x=10 after rollback, got x=%d", x)
	}
}

// TestRollbackOnDelete verifies the MVCC fundamental limitation:
// DELETEs cannot be rolled back (we cannot resurrect deleted rows).
// BEGIN → DELETE → ROLLBACK → row IS still visible (delete was a self-delete).
func TestRollbackOnDelete(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")
	db.Exec("INSERT INTO t VALUES ($1, $2)", 1, 10)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Delete within transaction
	_, err = tx.Exec("DELETE FROM t WHERE id = $1", 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("DELETE: %v", err)
	}

	// Read within txn — should see nothing (self-delete visible to txn itself)
	var x int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != sql.ErrNoRows {
		tx.Rollback()
		if err == nil {
			t.Errorf("expected no rows in txn, got x=%d", x)
		} else {
			t.Errorf("SELECT within txn: %v", err)
		}
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// After rollback, row IS still visible — this is the fundamental MVCC limitation.
	// We cannot restore the pre-delete state because txnMax=txnXID was set during delete.
	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != nil {
		t.Fatalf("expected row visible after delete+rollback, got error: %v", err)
	}
	if x != 10 {
		t.Errorf("expected x=10 after delete+rollback, got x=%d", x)
	}
}

// TestRollbackMultipleWrites verifies rollback with multiple writes.
func TestRollbackMultipleWrites(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")
	db.Exec("INSERT INTO t VALUES ($1, $2)", 1, 10)
	db.Exec("INSERT INTO t VALUES ($1, $2)", 2, 20)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Multiple writes: INSERT, UPDATE, DELETE
	_, err = tx.Exec("INSERT INTO t VALUES ($1, $2)", 3, 30)
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT: %v", err)
	}

	_, err = tx.Exec("UPDATE t SET x = $1 WHERE id = $2", 999, 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("UPDATE: %v", err)
	}

	_, err = tx.Exec("DELETE FROM t WHERE id = $1", 2)
	if err != nil {
		tx.Rollback()
		t.Fatalf("DELETE: %v", err)
	}

	// Rollback all
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Original rows unchanged
	var x int
	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	if err != nil {
		t.Fatalf("SELECT id=1: %v", err)
	}
	if x != 10 {
		t.Errorf("id=1: expected x=10, got x=%d", x)
	}

	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 2).Scan(&x)
	if err != nil {
		t.Fatalf("SELECT id=2: %v", err)
	}
	if x != 20 {
		t.Errorf("id=2: expected x=20, got x=%d", x)
	}

	// New row should not exist
	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 3).Scan(&x)
	if err != sql.ErrNoRows {
		if err == nil {
			t.Errorf("id=3 should not exist after rollback")
		} else {
			t.Errorf("SELECT id=3: %v", err)
		}
	}
}
