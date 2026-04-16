package gosql

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec("CREATE TABLE t (id INTEGER, x INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec("INSERT INTO t VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// BEGIN
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	fmt.Println("After Begin()")

	// First read using QueryRow
	var x1 int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x1)
	if err != nil {
		t.Fatalf("first QueryRow: %v", err)
	}
	fmt.Printf("First QueryRow: x = %d\n", x1)

	// UPDATE x=20 (outside txn, auto-commits)
	_, err = db.Exec("UPDATE t SET x = $1 WHERE id = $2", 20, 1)
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	fmt.Println("After UPDATE x=20")

	// Second read — should STILL see x=10
	var x2 int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x2)
	if err != nil {
		fmt.Printf("second QueryRow FAILED: %v\n", err)
		t.Fatalf("second QueryRow: %v", err)
	}
	fmt.Printf("Second QueryRow: x = %d\n", x2)

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if x1 != x2 {
		t.Errorf("SI violation: x1=%d, x2=%d", x1, x2)
	}
}
