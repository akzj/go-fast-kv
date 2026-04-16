package internal

import "testing"

// TestExec_UpdateOnIndexedColumn verifies UPDATE correctly maintains index entries.
func TestExec_UpdateOnIndexedColumn(t *testing.T) {
	env := newTestEnv(t)

	// Create table with indexed column
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")

	// Insert rows
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	// Update indexed column: age 30 -> 40
	env.execSQL(t, "UPDATE users SET age = 40 WHERE id = 1")

	// Index scan for age=40 should find Alice
	result := env.execSQL(t, "SELECT * FROM users WHERE age = 40")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for age=40, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Alice" {
		t.Errorf("expected Alice, got %s", result.Rows[0][1].Text)
	}

	// Index scan for age=30 should return nothing (old value)
	result = env.execSQL(t, "SELECT * FROM users WHERE age = 30")
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows for age=30, got %d", len(result.Rows))
	}

	// Index scan for age=25 should still find Bob
	result = env.execSQL(t, "SELECT * FROM users WHERE age = 25")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for age=25, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Bob" {
		t.Errorf("expected Bob, got %s", result.Rows[0][1].Text)
	}

	t.Logf("UPDATE on indexed column: old entry removed, new entry inserted correctly")
}

// TestExec_UpdateSameValue verifies UPDATE that doesn't change indexed column value.
func TestExec_UpdateSameValue(t *testing.T) {
	env := newTestEnv(t)

	// Create table with indexed column
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")

	// Insert rows
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	// UPDATE that sets age to the same value
	env.execSQL(t, "UPDATE users SET age = 30 WHERE id = 1")

	// Index scan for age=30 should still find Alice
	result := env.execSQL(t, "SELECT * FROM users WHERE age = 30")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for age=30, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Alice" {
		t.Errorf("expected Alice, got %s", result.Rows[0][1].Text)
	}

	// Index scan for age=25 should still find Bob
	result = env.execSQL(t, "SELECT * FROM users WHERE age = 25")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for age=25, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Bob" {
		t.Errorf("expected Bob, got %s", result.Rows[0][1].Text)
	}

	t.Logf("UPDATE with same value: index correctly unchanged")
}
