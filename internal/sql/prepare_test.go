package sql_test

import (
	"testing"

	gosql "github.com/akzj/go-fast-kv/internal/sql"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

func TestPrepare_Basic(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert some data
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")
	if err != nil {
		t.Fatal(err)
	}

	// Prepare a query
	stmt, err := db.Prepare("SELECT * FROM users WHERE age > $1")
	if err != nil {
		t.Fatal(err)
	}

	// Execute with different parameters
	result1, err := stmt.Query(catalogapi.Value{Type: catalogapi.TypeInt, Int: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(result1.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result1.Rows))
	}

	result2, err := stmt.Query(catalogapi.Value{Type: catalogapi.TypeInt, Int: 28})
	if err != nil {
		t.Fatal(err)
	}
	if len(result2.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result2.Rows))
	}
	if result2.Rows[0][1].Text != "Alice" {
		t.Errorf("expected Alice, got %v", result2.Rows[0][1])
	}
}

func TestPrepare_CacheHit(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Prepare same query twice - should get cache hit
	stmt1, err := db.Prepare("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}

	stmt2, err := db.Prepare("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}

	// Should be the same pointer (cache hit)
	if stmt1 != stmt2 {
		t.Error("expected cache hit to return same pointer")
	}
}

func TestPrepare_WhitespaceNormalization(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Prepare with whitespace
	stmt1, err := db.Prepare("  SELECT 1  ")
	if err != nil {
		t.Fatal(err)
	}

	// Prepare with same SQL normalized
	stmt2, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	// Should be the same pointer (cache hit due to normalization)
	if stmt1 != stmt2 {
		t.Error("expected cache hit for normalized whitespace")
	}
}

func TestPrepare_Exec(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Prepare INSERT
	stmt, err := db.Prepare("INSERT INTO users VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}

	// Execute INSERT
	result, err := stmt.Exec(catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}, catalogapi.Value{Type: catalogapi.TypeText, Text: "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Execute another INSERT
	result, err = stmt.Exec(catalogapi.Value{Type: catalogapi.TypeInt, Int: 2}, catalogapi.Value{Type: catalogapi.TypeText, Text: "Bob"})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify data
	result, err = db.Query("SELECT * FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestPrepare_Update(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatal(err)
	}

	// Prepare UPDATE
	stmt, err := db.Prepare("UPDATE users SET age = $1 WHERE name = $2")
	if err != nil {
		t.Fatal(err)
	}

	// Execute UPDATE
	result, err := stmt.Exec(catalogapi.Value{Type: catalogapi.TypeInt, Int: 31}, catalogapi.Value{Type: catalogapi.TypeText, Text: "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify update
	result, err = db.Query("SELECT age FROM users WHERE name = 'Alice'")
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0][0].Int != 31 {
		t.Errorf("expected age 31, got %d", result.Rows[0][0].Int)
	}
}

func TestPrepare_Delete(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (2, 'Bob')")
	if err != nil {
		t.Fatal(err)
	}

	// Prepare DELETE
	stmt, err := db.Prepare("DELETE FROM users WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}

	// Execute DELETE
	result, err := stmt.Exec(catalogapi.Value{Type: catalogapi.TypeInt, Int: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify delete
	result, err = db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0][0].Int != 1 {
		t.Errorf("expected 1 row, got %d", result.Rows[0][0].Int)
	}
}

func TestPrepare_ClosedDatabase(t *testing.T) {
	_, store := openTestDB(t)
	defer store.Close()

	db := gosql.Open(store)
	db.Close()

	// Prepare on closed database should fail
	_, err := db.Prepare("SELECT 1")
	if err == nil {
		t.Error("expected error on closed database")
	}
}

func TestPrepare_ParseError(t *testing.T) {
	db, store := openTestDB(t)
	defer store.Close()

	// Prepare invalid SQL should fail
	_, err := db.Prepare("INVALID SQL SYNTAX")
	if err == nil {
		t.Error("expected parse error")
	}
}
