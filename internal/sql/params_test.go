package sql_test

import (
	"testing"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// TestNamedParams tests $1, $2 positional parameter support.
func TestNamedParams(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with $1, $2
	_, err = db.ExecParams("INSERT INTO users VALUES ($1, $2, $3)", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
		{Type: catalogapi.TypeText, Text: "Alice"},
		{Type: catalogapi.TypeInt, Int: 30},
	})
	if err != nil {
		t.Fatalf("INSERT with params failed: %v", err)
	}

	// Query with $1
	result, err := db.QueryParams("SELECT * FROM users WHERE id = $1", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
	})
	if err != nil {
		t.Fatalf("SELECT with params failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Alice" {
		t.Fatalf("expected name 'Alice', got %q", result.Rows[0][1].Text)
	}

	// Update with $1, $2
	_, err = db.ExecParams("UPDATE users SET age = $1 WHERE id = $2", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 35},
		{Type: catalogapi.TypeInt, Int: 1},
	})
	if err != nil {
		t.Fatalf("UPDATE with params failed: %v", err)
	}

	// Verify update
	result, err = db.QueryParams("SELECT age FROM users WHERE id = $1", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
	})
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if result.Rows[0][0].Int != 35 {
		t.Fatalf("expected age 35, got %d", result.Rows[0][0].Int)
	}

	// Delete with $1
	_, err = db.ExecParams("DELETE FROM users WHERE id = $1", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
	})
	if err != nil {
		t.Fatalf("DELETE with params failed: %v", err)
	}

	// Verify delete
	result, err = db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("COUNT after DELETE failed: %v", err)
	}
	if result.Rows[0][0].Int != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", result.Rows[0][0].Int)
	}
}

// TestNamedParamsOutOfRange tests error handling for out-of-range parameters.
func TestNamedParamsOutOfRange(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	_, err := db.Exec("CREATE TABLE t (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert a row so SELECT matches something
	_, err = db.Exec("INSERT INTO t VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test $2 when only 1 param provided (error because $2 is out of range)
	_, err = db.QueryParams("SELECT * FROM t WHERE id = $2", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
	})
	if err == nil {
		t.Fatal("expected error for out-of-range param index")
	}
	t.Logf("Got expected error: %v", err)
}

// TestQuestionMarkParam tests ? placeholder (treated as $1).
func TestQuestionMarkParam(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	_, err := db.Exec("CREATE TABLE t (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with ? placeholder
	_, err = db.ExecParams("INSERT INTO t VALUES (?, ?)", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
		{Type: catalogapi.TypeText, Text: "Bob"},
	})
	if err != nil {
		t.Fatalf("INSERT with ? failed: %v", err)
	}

	// Query with ? placeholder
	result, err := db.QueryParams("SELECT * FROM t WHERE id = ?", []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 1},
	})
	if err != nil {
		t.Fatalf("SELECT with ? failed: %v", err)
	}
	if result.Rows[0][1].Text != "Bob" {
		t.Fatalf("expected 'Bob', got %q", result.Rows[0][1].Text)
	}
}
