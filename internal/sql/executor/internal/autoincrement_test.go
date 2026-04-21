package internal

import (
	"testing"
)

// TestAutoIncrementBasic tests basic AUTOINCREMENT functionality.
func TestAutoIncrementBasic(t *testing.T) {
	env := newTestEnv(t)

	// Create table with AUTOINCREMENT PRIMARY KEY
	env.execSQL(t, "CREATE TABLE users (id INT AUTOINCREMENT PRIMARY KEY, name TEXT)")

	// Insert without specifying ID (implicit column list: name only)
	result := env.execSQL(t, "INSERT INTO users (name) VALUES ('Alice')")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify auto-generated ID
	result = env.execSQL(t, "SELECT id, name FROM users WHERE name = 'Alice'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	row := result.Rows[0]
	if row[0].IsNull || row[0].Type != 1 { // TypeInt = 1
		t.Errorf("expected non-null INT id, got %+v", row[0])
	}
	if row[0].Int != 1 {
		t.Errorf("expected id=1, got %d", row[0].Int)
	}

	// Insert another row
	env.execSQL(t, "INSERT INTO users (name) VALUES ('Bob')")

	// Verify second auto-generated ID
	result = env.execSQL(t, "SELECT id FROM users WHERE name = 'Bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for Bob, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int != 2 {
		t.Errorf("expected id=2 for Bob, got %d", result.Rows[0][0].Int)
	}
}

// TestAutoIncrementWithExplicitID tests that explicitly providing ID still works.
func TestAutoIncrementWithExplicitID(t *testing.T) {
	env := newTestEnv(t)

	// Create table with AUTOINCREMENT
	env.execSQL(t, "CREATE TABLE items (id INT AUTOINCREMENT PRIMARY KEY, name TEXT)")

	// Insert with explicit ID
	result := env.execSQL(t, "INSERT INTO items VALUES (100, 'Item1')")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify explicit ID was used
	result = env.execSQL(t, "SELECT id FROM items WHERE name = 'Item1'")
	if result.Rows[0][0].Int != 100 {
		t.Errorf("expected id=100, got %d", result.Rows[0][0].Int)
	}

	// Next auto-generated ID should continue from the max of existing IDs
	env.execSQL(t, "INSERT INTO items (name) VALUES ('Item2')")
	result = env.execSQL(t, "SELECT id FROM items WHERE name = 'Item2'")
	// Should be 101 (next after explicit 100)
	if result.Rows[0][0].Int != 101 {
		t.Errorf("expected id=101 for Item2, got %d", result.Rows[0][0].Int)
	}
}

// TestAutoIncrementSerialAlias tests that SERIAL keyword works as an alias.
func TestAutoIncrementSerialAlias(t *testing.T) {
	env := newTestEnv(t)

	// Create table with SERIAL (PostgreSQL alias for AUTOINCREMENT)
	env.execSQL(t, "CREATE TABLE seq (id INT SERIAL PRIMARY KEY, value TEXT)")

	// Insert without ID
	result := env.execSQL(t, "INSERT INTO seq (value) VALUES ('test')")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify auto-generated ID
	result = env.execSQL(t, "SELECT id FROM seq")
	if result.Rows[0][0].Int != 1 {
		t.Errorf("expected id=1, got %d", result.Rows[0][0].Int)
	}
}

// TestAutoIncrementMultipleColumns tests AUTOINCREMENT with other columns.
func TestAutoIncrementMultipleColumns(t *testing.T) {
	env := newTestEnv(t)

	// Create table with AUTOINCREMENT and other columns
	env.execSQL(t, "CREATE TABLE products (id INT AUTOINCREMENT PRIMARY KEY, name TEXT, price FLOAT)")

	// Insert all columns except ID
	env.execSQL(t, "INSERT INTO products (name, price) VALUES ('Widget', 9.99)")

	// Verify
	result := env.execSQL(t, "SELECT id, name, price FROM products")
	row := result.Rows[0]
	if row[0].Int != 1 {
		t.Errorf("expected id=1, got %d", row[0].Int)
	}
	if row[1].Text != "Widget" {
		t.Errorf("expected name='Widget', got %s", row[1].Text)
	}
	if row[2].Float != 9.99 {
		t.Errorf("expected price=9.99, got %f", row[2].Float)
	}
}

// TestAutoIncrementNotNull tests that AUTOINCREMENT columns are implicitly NOT NULL.
func TestAutoIncrementNotNull(t *testing.T) {
	env := newTestEnv(t)

	// Create table with AUTOINCREMENT
	env.execSQL(t, "CREATE TABLE logs (id INT AUTOINCREMENT PRIMARY KEY, msg TEXT)")

	// Insert without ID - should auto-generate (NOT NULL satisfied)
	env.execSQL(t, "INSERT INTO logs (msg) VALUES ('hello')")

	// Verify
	result := env.execSQL(t, "SELECT id FROM logs")
	if result.Rows[0][0].Int != 1 {
		t.Errorf("expected id=1, got %d", result.Rows[0][0].Int)
	}
}

// TestAutoIncrementMultipleRows tests multiple rows in a single INSERT.
func TestAutoIncrementMultipleRows(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, val TEXT)")

	// Insert multiple rows
	env.execSQL(t, "INSERT INTO t (val) VALUES ('a'), ('b'), ('c')")

	// Verify all IDs are sequential
	result := env.execSQL(t, "SELECT id, val FROM t ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		expected := int64(i + 1)
		if row[0].Int != expected {
			t.Errorf("row %d: expected id=%d, got %d", i, expected, row[0].Int)
		}
	}
}

// TestAutoIncrementExplicitZero tests inserting 0 to AUTOINCREMENT column.
func TestAutoIncrementExplicitZero(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, val TEXT)")

	// Insert with explicit 0
	result := env.execSQL(t, "INSERT INTO t VALUES (0, 'zero')")
	// Should succeed - explicit ID overrides auto-increment
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify ID=0 was stored
	result = env.execSQL(t, "SELECT id FROM t WHERE val='zero'")
	if result.Rows[0][0].Int != 0 {
		t.Errorf("expected id=0, got %d", result.Rows[0][0].Int)
	}

	// Next auto should continue from 1
	env.execSQL(t, "INSERT INTO t (val) VALUES ('auto')")
	result = env.execSQL(t, "SELECT id FROM t WHERE val='auto'")
	if result.Rows[0][0].Int != 1 {
		t.Errorf("expected id=1 for auto, got %d", result.Rows[0][0].Int)
	}
}

// TestAutoIncrementUpdate tests that AUTOINCREMENT columns can be updated.
func TestAutoIncrementUpdate(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, name TEXT)")

	// Insert a row
	env.execSQL(t, "INSERT INTO t (name) VALUES ('Alice')")

	// Update the name - should still have auto-generated ID
	result := env.execSQL(t, "SELECT id, name FROM t")
	if result.Rows[0][0].Int != 1 {
		t.Errorf("expected id=1, got %d", result.Rows[0][0].Int)
	}
	if result.Rows[0][1].Text != "Alice" {
		t.Errorf("expected name=Alice, got %s", result.Rows[0][1].Text)
	}

	// Update name
	env.execSQL(t, "UPDATE t SET name = 'Bob' WHERE id = 1")
	result = env.execSQL(t, "SELECT id, name FROM t")
	if result.Rows[0][0].Int != 1 {
		t.Errorf("id should be unchanged after update, got %d", result.Rows[0][0].Int)
	}
	if result.Rows[0][1].Text != "Bob" {
		t.Errorf("expected name=Bob, got %s", result.Rows[0][1].Text)
	}
}

// TestAutoIncrementWithDefault tests AUTOINCREMENT with other columns having DEFAULT.
func TestAutoIncrementWithDefault(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, name TEXT, score INT DEFAULT 0)")

	// Insert only id and name, score should use default
	result := env.execSQL(t, "INSERT INTO t (name) VALUES ('Alice')")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row, got %d", result.RowsAffected)
	}

	// Verify
	result = env.execSQL(t, "SELECT id, name, score FROM t")
	row := result.Rows[0]
	if row[0].Int != 1 {
		t.Errorf("expected id=1, got %d", row[0].Int)
	}
	if row[1].Text != "Alice" {
		t.Errorf("expected name=Alice, got %s", row[1].Text)
	}
	if row[2].Int != 0 {
		t.Errorf("expected score=0 (default), got %d", row[2].Int)
	}
}

// TestAutoIncrementInTransaction tests that auto-IDs work correctly in transactions.
func TestAutoIncrementInTransaction(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, val TEXT)")

	// Simulate transaction by executing multiple inserts
	env.execSQL(t, "INSERT INTO t (val) VALUES ('txn1')")
	env.execSQL(t, "INSERT INTO t (val) VALUES ('txn2')")
	env.execSQL(t, "INSERT INTO t (val) VALUES ('txn3')")

	// Verify IDs are sequential
	result := env.execSQL(t, "SELECT id, val FROM t ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		expected := int64(i + 1)
		if row[0].Int != expected {
			t.Errorf("row %d: expected id=%d, got %d", i, expected, row[0].Int)
		}
	}
}
