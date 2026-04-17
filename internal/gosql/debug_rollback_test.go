package gosql

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestDebugRollback(t *testing.T) {
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

	fmt.Printf("DEBUG: After INSERT\n")

	// Read within txn
	var x int
	err = tx.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	fmt.Printf("DEBUG: Within txn, x=%d, err=%v\n", x, err)

	err = tx.Rollback()
	fmt.Printf("DEBUG: After Rollback err=%v\n", err)

	err = db.QueryRow("SELECT x FROM t WHERE id = $1", 1).Scan(&x)
	fmt.Printf("DEBUG: After rollback SELECT, x=%d, err=%v\n", x, err)
}
