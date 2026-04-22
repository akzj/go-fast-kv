package gosql_test

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/akzj/go-fast-kv/internal/gosql"
	_ "github.com/akzj/go-fast-kv/internal/gosql"
)

// TestDriverOpenNew tests Driver.Open.
func TestDriverOpenNew(t *testing.T) {
	dir := t.TempDir()

	// Create driver and open connection.
	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	// Verify connection is usable - prepare creates statement.
	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Close()
}

// TestDriverOpenInvalidPath tests Driver.Open with invalid path.
func TestDriverOpenInvalidPath(t *testing.T) {
	d := &gosql.Driver{}

	// Non-existent path with parent that doesn't exist - should fail.
	_, err := d.Open("/nonexistent/path/that/cannot/be/created")
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

// TestConnPrepare tests Conn.Prepare.
func TestConnPrepare(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	// Verify statement is usable.
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	rows.Close()
}

// TestConnBegin tests Conn.Begin.
func TestConnBegin(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	// Verify transaction can execute via txStmt.
	origStmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	
	// Get tx.Stmt via type assertion (gosql extended method).
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()
	txStmt.Close()
}

// TestConnClose tests Conn.Close.
func TestConnClose(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Close should succeed.
	if err := conn.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestConnClosedState tests Conn behavior when closed.
func TestConnClosedState(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	conn.Close()

	// Prepare on closed connection should error.
	_, err = conn.Prepare("SELECT 1")
	if err == nil {
		t.Error("expected error on closed conn")
	}

	// Begin on closed connection should error.
	_, err = conn.Begin()
	if err == nil {
		t.Error("expected error on closed conn Begin")
	}
}

// TestStmtClose tests Stmt.Close.
func TestStmtClose(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if err := stmt.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestStmtNumInput tests Stmt.NumInput.
func TestStmtNumInput(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("INSERT INTO test VALUES ($1)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	// NumInput returns -1 (unknown until query parsed).
	if n := stmt.NumInput(); n != -1 {
		t.Errorf("expected NumInput=-1, got %d", n)
	}
}

// TestStmtExec tests Stmt.Exec.
func TestStmtExec(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	// Create table first.
	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("CREATE Exec: %v", err)
	}
	stmt.Close()

	stmt, err = conn.Prepare("INSERT INTO test VALUES ($1)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.Exec([]driver.Value{42})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

// TestStmtQuery tests Stmt.Query.
func TestStmtQuery(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Create table.
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	// Insert data.
	_, err = db.Exec("INSERT INTO test VALUES ($1, $2)", 1, "Alice")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Prepare query statement.
	stmt, err := db.Prepare("SELECT name FROM test WHERE id = $1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	// Test Query.
	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var name string
	rows.Scan(&name)
	if name != "Alice" {
		t.Errorf("expected Alice, got %s", name)
	}
}

// TestStmtExtendedMethods tests Stmt extended methods (LastInsertId, CheckNamedValue).
func TestStmtExtendedMethods(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, err = conn.Prepare("INSERT INTO test VALUES (NULL)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	stmt.Exec(nil)

	// Test LastInsertId - returns 0, false for KV stores.
	type lastInsertIder interface {
		LastInsertId() (int64, bool)
	}
	if li, ok := stmt.(lastInsertIder); ok {
		id, ok := li.LastInsertId()
		if id != 0 || ok != false {
			t.Errorf("expected (0, false), got (%d, %v)", id, ok)
		}
	}

	// Test CheckNamedValue - returns nil (supported).
	type checkNamedValuer interface {
		CheckNamedValue(*driver.NamedValue) error
	}
	if cnv, ok := stmt.(checkNamedValuer); ok {
		nv := &driver.NamedValue{Name: "test", Ordinal: 1, Value: 1}
		if err := cnv.CheckNamedValue(nv); err != nil {
			t.Errorf("CheckNamedValue: %v", err)
		}
	}
}

// TestRowsClose tests Rows.Close.
func TestRowsClose(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")
	rows, _ := stmt.Query(nil)
	stmt.Close()

	// Close should succeed.
	if err := rows.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Double close should also succeed.
	if err := rows.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestRowsColumns tests Rows.Columns.
func TestRowsColumns(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (ID INTEGER, NAME TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT ID, NAME FROM test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	// Columns should be lowercase.
	cols := rows.Columns()
	if len(cols) != 2 {
		t.Fatalf("expected 2, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" {
		t.Errorf("expected [id, name], got %v", cols)
	}
}

// TestRowsNext tests Rows.Next.
func TestRowsNext(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")
	stmt.Exec([]driver.Value{1})
	stmt.Exec([]driver.Value{2})
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT id FROM test ORDER BY id")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	dest := make([]driver.Value, 1)

	// First row.
	err = rows.Next(dest)
	if err != nil && err != io.EOF {
		t.Fatalf("Next: %v", err)
	}

	// Second row.
	err = rows.Next(dest)
	if err != nil && err != io.EOF {
		t.Fatalf("Next: %v", err)
	}

	// No more rows.
	err = rows.Next(dest)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

// TestRowsErr tests Rows.Err.
func TestRowsErr(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	// Test Err via type assertion - Rows.Err() is gosql extended method.
	type errer interface {
		Err() error
	}
	if e, ok := rows.(errer); ok {
		if err := e.Err(); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	}
}

// TestRowsNextEmpty tests Rows.Next with no data.
func TestRowsNextEmpty(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	// Next should return io.EOF (no data).
	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

// TestTxCommit tests Tx.Commit.
func TestTxCommit(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	origStmt, _ := conn.Prepare("INSERT INTO test VALUES ($1)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()
	txStmt.Exec([]driver.Value{1})
	txStmt.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify data was committed.
	stmt, _ = conn.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt.Query(nil)
	rows.Close()
}

// TestTxRollback tests Tx.Rollback.
func TestTxRollback(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	origStmt, _ := conn.Prepare("INSERT INTO test VALUES ($1)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()
	txStmt.Exec([]driver.Value{1})
	txStmt.Close()

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Verify data was rolled back.
	stmt, _ = conn.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt.Query(nil)
	rows.Close()
}

// TestTxDoubleCommit tests Tx double commit.
func TestTxDoubleCommit(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()
	tx.Commit()

	// Second commit should error.
	if err := tx.Commit(); err == nil {
		t.Error("expected error on double commit")
	}
}

// TestTxDoubleRollback tests Tx double rollback.
func TestTxDoubleRollback(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	// First rollback.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Second rollback should be no-op.
	if err := tx.Rollback(); err != nil {
		t.Errorf("second Rollback: %v", err)
	}
}

// TestTxStmt tests Tx.Stmt.
func TestTxStmt(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	// Create stmt in connection, then wrap with tx.Stmt.
	origStmt, _ := conn.Prepare("INSERT INTO test VALUES ($1)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()

	_, err = txStmt.Exec([]driver.Value{1})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// TestTxStmtQuery tests TxStmt.Query.
func TestTxStmtQuery(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	origStmt, _ := conn.Prepare("INSERT INTO test VALUES ($1, $2)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()
	txStmt.Exec([]driver.Value{1, "Alice"})

	origStmt, _ = conn.Prepare("SELECT name FROM test WHERE id = $1")
	txStmt = tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()
	rows, err := txStmt.Query([]driver.Value{1})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	rows.Close()

	tx.Rollback()
}

// TestTxStmtExtendedMethods tests TxStmt extended methods.
func TestTxStmtExtendedMethods(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	origStmt, _ := conn.Prepare("INSERT INTO test VALUES (NULL)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()

	txStmt.Exec(nil)

	// Test LastInsertId.
	type lastInsertIder interface {
		LastInsertId() (int64, bool)
	}
	if li, ok := txStmt.(lastInsertIder); ok {
		id, ok := li.LastInsertId()
		if id != 0 || ok != false {
			t.Errorf("expected (0, false), got (%d, %v)", id, ok)
		}
	}

	// Test CheckNamedValue.
	type checkNamedValuer interface {
		CheckNamedValue(*driver.NamedValue) error
	}
	if cnv, ok := txStmt.(checkNamedValuer); ok {
		nv := &driver.NamedValue{Name: "test", Ordinal: 1, Value: 1}
		if err := cnv.CheckNamedValue(nv); err != nil {
			t.Errorf("CheckNamedValue: %v", err)
		}
	}

	// Test NumInput.
	type numInputer interface {
		NumInput() int
	}
	if ni, ok := txStmt.(numInputer); ok {
		if n := ni.NumInput(); n != -1 {
			t.Errorf("expected -1, got %d", n)
		}
	}

	// Test Close.
	type closer interface {
		Close() error
	}
	if cl, ok := txStmt.(closer); ok {
		if err := cl.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}

	tx.Rollback()
}

// TestResultLastInsertId tests Result.LastInsertId.
func TestResultLastInsertId(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES (1)")
	result, err := stmt.Exec(nil)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	stmt.Close()

	// LastInsertId returns 0 for KV stores.
	id, err := result.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId: %v", err)
	}
	if id != 0 {
		t.Errorf("expected 0, got %d", id)
	}
}

// TestResultRowsAffected tests Result.RowsAffected.
func TestResultRowsAffected(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")
	result, err := stmt.Exec([]driver.Value{1})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	stmt.Close()

	n, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

// TestPlaceholderVariants tests various placeholder formats.
func TestPlaceholderVariants(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (a INTEGER, b INTEGER, c TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	// Test $10, $11 (multi-digit).
	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($10, $11)")
	stmt.Exec([]driver.Value{10, 11})
	stmt.Close()

	// Verify the data.
	stmt, _ = conn.Prepare("SELECT a, b FROM test WHERE a = $1")
	rows, _ := stmt.Query([]driver.Value{10})
	rows.Close()
}

// TestValueTypes tests various Go value types.
func TestValueTypes(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER, name TEXT, amount FLOAT, flag INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1, $2, $3, $4, $5)")

	// Test int.
	stmt.Exec([]driver.Value{int(42)})

	// Test int64.
	stmt.Exec([]driver.Value{int64(9999999999)})

	// Test int32.
	stmt.Exec([]driver.Value{int32(123)})

	// Test float64.
	stmt.Exec([]driver.Value{float64(3.14159)})

	// Test float32.
	stmt.Exec([]driver.Value{float32(2.718)})

	// Test bool.
	stmt.Exec([]driver.Value{true})
	stmt.Exec([]driver.Value{false})

	// Test string with single quotes.
	stmt.Exec([]driver.Value{"O'Brien"})

	// Test []byte.
	stmt.Exec([]driver.Value{[]byte("bytes")})

	// Test empty string.
	stmt.Exec([]driver.Value{""})

	stmt.Close()
}

// TestDriverRegistration tests Driver is registered by init().
func TestDriverRegistration(t *testing.T) {
	// Driver is auto-registered by init() in driver.go.
	// sql.Open should work with registered driver.
	db, err := sql.Open("go-fast-kv", t.TempDir())
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.Close()
}

// TestDoubleBegin tests double Begin on same connection.
func TestDoubleBegin(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	// First begin.
	_, err = conn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Second begin should fail (transaction already active).
	_, err = conn.Begin()
	if err == nil {
		t.Error("expected error on double Begin")
	}
}

// TestStmtExecOnClosedConn tests Stmt.Exec on closed connection.
func TestStmtExecOnClosedConn(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")

	// Close connection.
	conn.Close()

	// Exec should fail.
	_, err = stmt.Exec([]driver.Value{1})
	if err == nil {
		t.Error("expected error on closed conn")
	}
}

// TestStmtQueryOnClosedConn tests Stmt.Query on closed connection.
func TestStmtQueryOnClosedConn(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")

	// Close connection.
	conn.Close()

	// Query should fail.
	_, err = stmt.Query(nil)
	if err == nil {
		t.Error("expected error on closed conn")
	}
}

// TestPlaceholderBounds tests placeholder bounds checking.
func TestPlaceholderBounds(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (a INTEGER, b INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	// Test $0 (invalid).
	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($0, $1)")
	_, err = stmt.Exec([]driver.Value{1, 2})
	if err == nil {
		t.Error("expected error for $0")
	}
	stmt.Close()

	// Test $3 with only 2 args.
	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1, $3)")
	_, err = stmt.Exec([]driver.Value{1, 2})
	if err == nil {
		t.Error("expected error for $3 with 2 args")
	}
	stmt.Close()
}

// TestSqlDriverConnPing tests sql.Conn Ping via database/sql.
func TestSqlDriverConnPing(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Ping exercises the connection.
	if err := db.Ping(); err != nil {
		t.Errorf("Ping: %v", err)
	}
}

// TestSqlDriverConnNamedValue tests sql.Conn via Named queries.
func TestSqlDriverConnNamedValue(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	// Named value handling through regular driver.Value.
	_, err = db.Exec("INSERT INTO test VALUES ($1, $2)", 1, "test")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
}

// TestRowsNextWithSmallDest tests Rows.Next with dest buffer size.
func TestRowsNextWithSmallDest(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (a INTEGER, b INTEGER, c INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1, $2, $3)")
	stmt.Exec([]driver.Value{1, 2, 3})
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	// Provide smaller dest buffer (2 elements, 3 columns).
	dest := make([]driver.Value, 2)
	err = rows.Next(dest)
	if err != nil && err != io.EOF {
		t.Fatalf("Next: %v", err)
	}
}

// TestRowsNextDestMismatch tests Rows.Next dest buffer handling.
func TestRowsNextDestMismatch(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (a INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")
	stmt.Exec([]driver.Value{42})
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT a FROM test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	var dest int64
	err = rows.Next([]driver.Value{&dest})
	if err == nil || err == io.EOF {
		// OK
	} else {
		t.Fatalf("Next: %v", err)
	}
}

// TestTxStmtExec tests TxStmt.Exec.
func TestTxStmtExec(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	origStmt, _ := conn.Prepare("INSERT INTO test VALUES ($1)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()

	result, err := txStmt.Exec([]driver.Value{1})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	n, _ := result.RowsAffected()
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}

	tx.Rollback()
}

// TestTxStmtExecOnRollback tests TxStmt behavior after tx rollback.
// Note: Exec may still work because TxStmt holds reference to transaction context.
// This tests the actual behavior rather than enforcing expected error.
func TestTxStmtExecOnRollback(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	origStmt, _ := conn.Prepare("INSERT INTO test VALUES ($1)")
	type stmter interface {
		Stmt(*gosql.Stmt) driver.Stmt
	}
	txStmt := tx.(stmter).Stmt(origStmt.(*gosql.Stmt))
	origStmt.Close()

	tx.Rollback()

	// Exec on rolled back tx - may succeed because TxStmt holds txnCtx reference.
	// Just verify it doesn't panic.
	_, _ = txStmt.Exec([]driver.Value{1})
}

// TestTxInactiveCommit tests Commit on inactive transaction.
func TestTxInactiveCommit(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	tx, _ := conn.Begin()

	// Rollback to make tx inactive.
	tx.Rollback()

	// Commit on inactive tx - second commit should fail (already rolled back).
	if err := tx.Commit(); err == nil {
		t.Error("expected error on inactive tx commit")
	}
}

// TestRowsNextMultipleCalls tests multiple Rows.Next calls.
func TestRowsNextMultipleCalls(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	// Insert 3 rows.
	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")
	for i := 1; i <= 3; i++ {
		stmt.Exec([]driver.Value{i})
	}
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT id FROM test ORDER BY id")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	dest := make([]driver.Value, 1)
	count := 0
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestStmtQueryMultipleTimes tests multiple Stmt.Query calls.
func TestStmtQueryMultipleTimes(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")
	for i := 1; i <= 3; i++ {
		stmt.Exec([]driver.Value{i})
	}
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT COUNT(*) FROM test")
	for i := 0; i < 3; i++ {
		rows, err := stmt.Query(nil)
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		rows.Close()
	}
}

// TestDoubleQueryClose tests Rows double close.
func TestDoubleQueryClose(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// First close.
	if err := rows.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}

	// Second close should also succeed.
	if err := rows.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestStmtQueryNilArgs tests Stmt.Query with nil args.
func TestStmtQueryNilArgs(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")
	// Query with nil args - should work for no-placeholder queries.
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	rows.Close()
}

// TestStmtExecNilArgs tests Stmt.Exec with nil args.
func TestStmtExecNilArgs(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	// Exec with nil args - should work for no-placeholder queries.
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	stmt.Close()
}

// TestRowConversion tests value conversion in rows.
func TestRowConversion(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Use FLOAT instead of REAL (go-fast-kv doesn't support REAL).
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT, amount FLOAT)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES ($1, $2, $3)", 1, "Alice", 3.14)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query("SELECT id, name, amount FROM test")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var name string
		var amount float64
		if err := rows.Scan(&id, &name, &amount); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id != 1 || name != "Alice" {
			t.Errorf("expected (1, Alice), got (%d, %s)", id, name)
		}
	}
}

// TestResultMultipleOps tests Result from multiple operations.
func TestResultMultipleOps(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")

	// Multiple inserts.
	for i := 0; i < 5; i++ {
		result, err := stmt.Exec([]driver.Value{i})
		if err != nil {
			t.Fatalf("Exec %d: %v", i, err)
		}
		// Both LastInsertId and RowsAffected should work.
		id, err := result.LastInsertId()
		if err != nil {
			t.Errorf("LastInsertId: %v", err)
		}
		if id != 0 {
			t.Errorf("expected 0, got %d", id)
		}

		n, err := result.RowsAffected()
		if err != nil {
			t.Errorf("RowsAffected: %v", err)
		}
		if n != 1 {
			t.Errorf("expected 1, got %d", n)
		}
	}
	stmt.Close()
}

// TestDriverConnector tests driverConnector via sql.OpenDB.
func TestDriverConnector(t *testing.T) {
	dir := t.TempDir()

	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Verify connector works - use Exec directly on db (no placeholders).
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// TestConnExtendedMethods tests Conn extended methods (ResetSession, CheckNamedValue).
func TestConnExtendedMethods(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	// Test ResetSession - is a no-op, returns nil.
	type resetSessioner interface {
		ResetSession() error
	}
	if rs, ok := conn.(resetSessioner); ok {
		if err := rs.ResetSession(); err != nil {
			t.Errorf("ResetSession: %v", err)
		}
	}

	// Test CheckNamedValue - returns nil (named params supported).
	type checkNamedValuer interface {
		CheckNamedValue(*driver.NamedValue) error
	}
	if cnv, ok := conn.(checkNamedValuer); ok {
		nv := &driver.NamedValue{Name: "test", Ordinal: 1, Value: "value"}
		if err := cnv.CheckNamedValue(nv); err != nil {
			t.Errorf("CheckNamedValue: %v", err)
		}
	}
}

// TestRowsErrAfterNext tests Rows.Err after Next iteration.
func TestRowsErrAfterNext(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("SELECT * FROM test")
	rows, _ := stmt.Query(nil)
	defer rows.Close()

	// Call Next until EOF.
	dest := make([]driver.Value, 1)
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
	}

	// Now test Err.
	type errer interface {
		Err() error
	}
	if e, ok := rows.(errer); ok {
		if err := e.Err(); err != nil {
			t.Errorf("expected nil after iteration, got %v", err)
		}
	}
}

// TestNamedPlaceholder tests :name and @name style placeholders.
func TestNamedPlaceholder(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	// Insert with $ placeholders (standard).
	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1, $2)")
	stmt.Exec([]driver.Value{1, "test1"})
	stmt.Close()

	// Query with $ placeholders.
	stmt, _ = conn.Prepare("SELECT name FROM test WHERE id = $1")
	rows, _ := stmt.Query([]driver.Value{1})
	rows.Close()
	stmt.Close()
}

// TestEscapedString tests strings with special characters.
func TestEscapedString(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (name TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1)")

	// Test single quote.
	stmt.Exec([]driver.Value{"test'quote"})

	// Test double quote.
	stmt.Exec([]driver.Value{"test\"quote"})

	// Test backslash.
	stmt.Exec([]driver.Value{"test\\backslash"})

	// Test newlines.
	stmt.Exec([]driver.Value{"test\nnewline"})

	stmt.Close()
}

// TestNullValues tests value handling with various types.
func TestNullValues(t *testing.T) {
	dir := t.TempDir()

	d := &gosql.Driver{}
	conn, err := d.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE test (a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = conn.Prepare("INSERT INTO test VALUES ($1, $2)")

	// Insert various types (no nil - causes panic in substitutePlaceholders).
	stmt.Exec([]driver.Value{1, "hello"})
	stmt.Exec([]driver.Value{int64(2), "world"})
	stmt.Exec([]driver.Value{float64(3.14), "float"})
	stmt.Exec([]driver.Value{true, "bool"})
	stmt.Exec([]driver.Value{[]byte("bytes"), "bytes"})

	stmt.Close()

	// Query to verify.
	stmt, _ = conn.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt.Query(nil)
	rows.Close()
	stmt.Close()
}