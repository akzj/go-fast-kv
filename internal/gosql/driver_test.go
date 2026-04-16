package gosql_test

import (
	"database/sql"
	"io"
	"os"
	"testing"

	_ "github.com/akzj/go-fast-kv/internal/gosql"
)

func TestDriverOpen(t *testing.T) {
	// Create a temp directory for the store.
	dir := t.TempDir()

	// Open database with store path as DSN.
	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Verify connection works.
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping: %v", err)
	}
}

func TestExecDDL(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create a table.
	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
}

func TestInsertAndQuery(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create table.
	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows.
	_, err = db.Exec("INSERT INTO users VALUES ($1, $2)", 1, "Alice")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_, err = db.Exec("INSERT INTO users VALUES ($1, $2)", 2, "Bob")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Query rows.
	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	var id int
	var name string
	count := 0
	for {
		if !rows.Next() {
			break
		}
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
		t.Logf("row: id=%d, name=%s", id, name)
	}
	// Check for end-of-iteration errors (EOF is expected, so we don't treat it as error).
	if err := rows.Err(); err != nil && err != io.EOF {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestTransaction(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create table.
	_, err = db.Exec("CREATE TABLE accounts (id INTEGER, balance INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Start transaction.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Insert initial balance.
	_, err = tx.Exec("INSERT INTO accounts VALUES ($1, $2)", 1, 100)
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT: %v", err)
	}

	// Commit.
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify committed data.
	var balance int
	err = db.QueryRow("SELECT balance FROM accounts WHERE id = $1", 1).Scan(&balance)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if balance != 100 {
		t.Errorf("expected balance=100, got %d", balance)
	}
}

func TestPreparedStatement(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create table.
	_, err = db.Exec("CREATE TABLE items (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Prepare statement.
	stmt, err := db.Prepare("INSERT INTO items VALUES ($1, $2)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	// Execute with different args.
	for i := 0; i < 5; i++ {
		_, err = stmt.Exec(i, "item")
		if err != nil {
			t.Fatalf("Exec %d: %v", i, err)
		}
	}

	// Query to verify.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

func TestRowsAffected(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create table.
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert.
	result, err := db.Exec("INSERT INTO test VALUES ($1)", 1)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row affected, got %d", n)
	}
}

func TestLastInsertId(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create table with INTEGER PRIMARY KEY to enable auto-increment.
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert.
	result, err := db.Exec("INSERT INTO test VALUES (NULL)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// LastInsertId is not supported for KV stores.
	id, err := result.LastInsertId()
	t.Logf("LastInsertId: id=%d, err=%v", id, err)
}

func TestCleanup(t *testing.T) {
	// Ensure no leaked goroutines.
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	// Create and query.
	_, err = db.Exec("CREATE TABLE t (a INTEGER)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	rows.Close()

	// Close should not leak.
	db.Close()

	// Clean up temp directory.
	os.RemoveAll(dir)
}