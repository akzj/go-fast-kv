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
