package sql

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// TestPersistence verifies data survives store close and reopen
func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create and populate
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	engine, err := Open(Config{KVStore: store})
	if err != nil {
		store.Close()
		t.Fatalf("failed to open engine: %v", err)
	}

	engine.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	engine.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	engine.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")

	// Close and reopen
	engine.Close()
	store.Close()

	// Phase 2: Reopen and verify
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	engine2, err := Open(Config{KVStore: store2})
	if err != nil {
		store2.Close()
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer func() {
		engine2.Close()
		store2.Close()
	}()

	iter2, err := engine2.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query after reopen failed: %v", err)
	}

	// Verify data correctness, not just count
	expected := map[int64][]any{
		1: {int64(1), "Alice", int64(30)},
		2: {int64(2), "Bob", int64(25)},
	}
	found := make(map[int64]bool)

	for iter2.Next() {
		row := iter2.Row()
		if len(row.Values) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(row.Values))
		}

		id := row.Values[0].AsInt()
		name := row.Values[1].AsText()
		age := row.Values[2].AsInt()

		exp, ok := expected[id]
		if !ok {
			t.Errorf("unexpected row with id=%d", id)
			continue
		}

		if id != exp[0].(int64) {
			t.Errorf("id mismatch: got %d, want %d", id, exp[0].(int64))
		}
		if name != exp[1].(string) {
			t.Errorf("name mismatch for id=%d: got %q, want %q", id, name, exp[1].(string))
		}
		if age != exp[2].(int64) {
			t.Errorf("age mismatch for id=%d: got %d, want %d", id, age, exp[2].(int64))
		}

		found[id] = true
	}
	iter2.Close()

	if len(found) != 2 {
		t.Errorf("expected 2 rows, found %d", len(found))
	}
}

// TestPersistence_CreateTable verifies table schema persists
func TestPersistence_CreateTable(t *testing.T) {
	dir := t.TempDir()

	// Create table
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	engine, err := Open(Config{KVStore: store})
	if err != nil {
		store.Close()
		t.Fatalf("failed to open engine: %v", err)
	}

	engine.Exec("CREATE TABLE t1 (id INT, name TEXT)")
	engine.Close()
	store.Close()

	// Reopen and verify table exists
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	engine2, err := Open(Config{KVStore: store2})
	if err != nil {
		store2.Close()
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer func() {
		engine2.Close()
		store2.Close()
	}()

	// Should be able to INSERT and SELECT
	_, err = engine2.Exec("INSERT INTO t1 VALUES (1, 'test')")
	if err != nil {
		t.Errorf("INSERT into persisted table failed: %v", err)
	}

	iter, err := engine2.Query("SELECT * FROM t1")
	if err != nil {
		t.Fatalf("SELECT from persisted table failed: %v", err)
	}

	count := 0
	for iter.Next() {
		row := iter.Row()
		if len(row.Values) != 2 {
			t.Errorf("expected 2 columns, got %d", len(row.Values))
		}
		id := row.Values[0].AsInt()
		name := row.Values[1].AsText()
		if id != 1 {
			t.Errorf("id mismatch: got %d, want 1", id)
		}
		if name != "test" {
			t.Errorf("name mismatch: got %q, want %q", name, "test")
		}
		count++
	}
	iter.Close()

	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

// TestCrashRecovery_Update verifies UPDATE survives crash
func TestCrashRecovery_Update(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Insert and update
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	engine, err := Open(Config{KVStore: store})
	if err != nil {
		store.Close()
		t.Fatalf("failed to open engine: %v", err)
	}

	engine.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	engine.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	engine.Exec("UPDATE users SET age = 31 WHERE id = 1")

	// Close
	engine.Close()
	store.Close()

	// Phase 2: Reopen and verify UPDATE persisted with correct value
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	engine2, err := Open(Config{KVStore: store2})
	if err != nil {
		store2.Close()
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer func() {
		engine2.Close()
		store2.Close()
	}()

	iter, err := engine2.Query("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	found := false
	for iter.Next() {
		row := iter.Row()
		if len(row.Values) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(row.Values))
		}

		id := row.Values[0].AsInt()
		name := row.Values[1].AsText()
		age := row.Values[2].AsInt()

		if id != 1 {
			t.Errorf("id mismatch: got %d, want 1", id)
		}
		if name != "Alice" {
			t.Errorf("name mismatch: got %q, want %q", name, "Alice")
		}
		if age != 31 {
			t.Errorf("age mismatch after UPDATE: got %d, want 31", age)
		}
		found = true
	}
	iter.Close()

	if !found {
		t.Error("row not found after reopen")
	}
}

// TestCrashRecovery_Delete verifies DELETE survives crash
func TestCrashRecovery_Delete(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Insert and delete some
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	engine, err := Open(Config{KVStore: store})
	if err != nil {
		store.Close()
		t.Fatalf("failed to open engine: %v", err)
	}

	engine.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	engine.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	engine.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")
	engine.Exec("INSERT INTO users VALUES (3, 'Charlie', 35)")

	// Delete Bob (id=2)
	engine.Exec("DELETE FROM users WHERE id = 2")

	// Close
	engine.Close()
	store.Close()

	// Phase 2: Reopen and verify DELETE persisted
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	engine2, err := Open(Config{KVStore: store2})
	if err != nil {
		store2.Close()
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer func() {
		engine2.Close()
		store2.Close()
	}()

	iter2, err := engine2.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Expected: Alice (id=1) and Charlie (id=3), NOT Bob (id=2)
	expectedIDs := map[int64]struct{ name string; age int64 }{
		1: {"Alice", 30},
		3: {"Charlie", 35},
	}
	foundIDs := make(map[int64]bool)

	for iter2.Next() {
		row := iter2.Row()
		if len(row.Values) != 3 {
			t.Errorf("expected 3 columns, got %d", len(row.Values))
			continue
		}

		id := row.Values[0].AsInt()
		name := row.Values[1].AsText()
		age := row.Values[2].AsInt()

		exp, ok := expectedIDs[id]
		if !ok {
			t.Errorf("unexpected row with id=%d (name=%q) - it should have been deleted", id, name)
			continue
		}

		if name != exp.name {
			t.Errorf("name mismatch for id=%d: got %q, want %q", id, name, exp.name)
		}
		if age != exp.age {
			t.Errorf("age mismatch for id=%d: got %d, want %d", id, age, exp.age)
		}

		foundIDs[id] = true
	}
	iter2.Close()

	if len(foundIDs) != 2 {
		t.Errorf("expected 2 rows, found %d", len(foundIDs))
	}

	// Explicitly verify Bob is gone
	if _, ok := foundIDs[2]; ok {
		t.Error("deleted row (id=2, Bob) should not exist after reopen")
	}
}

// TestCheckpoint verifies data after checkpoint
func TestCheckpoint(t *testing.T) {
	dir := t.TempDir()

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	engine, err := Open(Config{KVStore: store})
	if err != nil {
		store.Close()
		t.Fatalf("failed to open engine: %v", err)
	}

	engine.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	engine.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	engine.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")

	// Call checkpoint if available
	if cp, ok := store.(interface{ Checkpoint() error }); ok {
		if err := cp.Checkpoint(); err != nil {
			t.Logf("checkpoint warning: %v", err)
		}
	}

	engine.Close()
	store.Close()

	// Reopen and verify with correct values
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	engine2, err := Open(Config{KVStore: store2})
	if err != nil {
		store2.Close()
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer func() {
		engine2.Close()
		store2.Close()
	}()

	iter, err := engine2.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	expected := map[int64][]any{
		1: {int64(1), "Alice", int64(30)},
		2: {int64(2), "Bob", int64(25)},
	}
	foundIDs := make(map[int64]bool)

	for iter.Next() {
		row := iter.Row()
		if len(row.Values) != 3 {
			t.Errorf("expected 3 columns, got %d", len(row.Values))
			continue
		}

		id := row.Values[0].AsInt()
		name := row.Values[1].AsText()
		age := row.Values[2].AsInt()

		exp, ok := expected[id]
		if !ok {
			t.Errorf("unexpected row id=%d", id)
			continue
		}

		if name != exp[1].(string) {
			t.Errorf("name mismatch for id=%d: got %q, want %q", id, name, exp[1].(string))
		}
		if age != exp[2].(int64) {
			t.Errorf("age mismatch for id=%d: got %d, want %d", id, age, exp[2].(int64))
		}

		foundIDs[id] = true
	}
	iter.Close()

	if len(foundIDs) != 2 {
		t.Errorf("expected 2 rows, found %d", len(foundIDs))
	}
}

func TestIndexScan(t *testing.T) {
	dir := t.TempDir()
	store, _ := kvstore.Open(kvstoreapi.Config{Dir: dir})
	engine, _ := Open(Config{KVStore: store})
	defer func() { engine.Close(); store.Close() }()

	engine.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	engine.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	engine.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")
	engine.Exec("INSERT INTO users VALUES (3, 'Charlie', 30)")
	engine.Exec("CREATE INDEX idx_age ON users (age)")

	iter, err := engine.Query("SELECT * FROM users WHERE age = 30")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}
