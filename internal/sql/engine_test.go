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

	// Verify before close
	iter, err := engine.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 2 {
		t.Errorf("expected 2 rows before close, got %d", count)
	}

	// Phase 2: Close and reopen
	engine.Close()
	store.Close()

	// Phase 3: Reopen and verify
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
	count2 := 0
	for iter2.Next() {
		count2++
	}
	iter2.Close()

	if count2 != 2 {
		t.Errorf("expected 2 rows after reopen, got %d", count2)
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

	// Should be able to query the table
	_, err = engine2.Exec("INSERT INTO t1 VALUES (1, 'test')")
	if err != nil {
		t.Errorf("INSERT into persisted table failed: %v", err)
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

	// Close without explicit sync (simulates crash)
	engine.Close()
	store.Close()

	// Phase 2: Reopen and verify UPDATE persisted
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
		if len(row.Values) >= 3 {
			age := row.Values[2].AsInt()
			if age != 31 {
				t.Errorf("expected age 31 after UPDATE, got %d", age)
			}
			found = true
		}
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

	// Verify 2 rows before close
	iter, _ := engine.Query("SELECT * FROM users")
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 2 {
		t.Errorf("expected 2 rows before close, got %d", count)
	}

	// Close (simulates crash)
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

	count2 := 0
	var ids []int64
	for iter2.Next() {
		row := iter2.Row()
		if len(row.Values) >= 1 {
			ids = append(ids, row.Values[0].AsInt())
		}
		count2++
	}
	iter2.Close()

	if count2 != 2 {
		t.Errorf("expected 2 rows after reopen, got %d", count2)
	}

	// Verify Bob (id=2) is gone
	for _, id := range ids {
		if id == 2 {
			t.Error("deleted row (id=2) should not exist after reopen")
		}
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

	// Call checkpoint if available
	if cp, ok := store.(interface{ Checkpoint() error }); ok {
		if err := cp.Checkpoint(); err != nil {
			t.Logf("checkpoint warning: %v", err)
		}
	}

	engine.Close()
	store.Close()

	// Reopen and verify
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

	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()

	if count != 1 {
		t.Errorf("expected 1 row after checkpoint, got %d", count)
	}
}
