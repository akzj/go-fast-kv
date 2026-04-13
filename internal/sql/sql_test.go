package sql_test

import (
	"os"
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	gosql "github.com/akzj/go-fast-kv/internal/sql"
)

func openTestDB(t *testing.T) (*gosql.DB, kvstoreapi.Store) {
	t.Helper()
	dir, err := os.MkdirTemp("", "sql-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	db := gosql.Open(store)
	t.Cleanup(func() { db.Close() })

	return db, store
}

func TestEndToEnd_CreateInsertSelectDeleteUpdate(t *testing.T) {
	db, _ := openTestDB(t)

	// CREATE TABLE
	res, err := db.Exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("CREATE TABLE: expected 0 rows affected, got %d", res.RowsAffected)
	}

	// INSERT
	for _, q := range []string{
		"INSERT INTO users VALUES (1, 'Alice', 30)",
		"INSERT INTO users VALUES (2, 'Bob', 25)",
		"INSERT INTO users VALUES (3, 'Charlie', 35)",
	} {
		res, err = db.Exec(q)
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("INSERT: expected 1 row affected, got %d", res.RowsAffected)
		}
	}

	// SELECT *
	res, err = db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("SELECT *: expected 3 rows, got %d", len(res.Rows))
	}

	// SELECT with WHERE
	res, err = db.Query("SELECT name, age FROM users WHERE age > 28")
	if err != nil {
		t.Fatalf("SELECT WHERE: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("SELECT WHERE: expected 2 rows (Alice+Charlie), got %d", len(res.Rows))
	}

	// SELECT with ORDER BY and LIMIT
	res, err = db.Query("SELECT name FROM users ORDER BY age DESC LIMIT 2")
	if err != nil {
		t.Fatalf("SELECT ORDER BY LIMIT: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0].Text != "Charlie" {
		t.Fatalf("expected Charlie first (oldest), got %v", res.Rows[0][0])
	}

	// UPDATE
	res, err = db.Exec("UPDATE users SET age = 31 WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("UPDATE: expected 1 row affected, got %d", res.RowsAffected)
	}

	// Verify UPDATE
	res, err = db.Query("SELECT age FROM users WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("SELECT after UPDATE: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 31 {
		t.Fatalf("expected Alice age=31, got %v", res.Rows)
	}

	// DELETE
	res, err = db.Exec("DELETE FROM users WHERE name = 'Bob'")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("DELETE: expected 1 row affected, got %d", res.RowsAffected)
	}

	// Verify DELETE
	res, err = db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT after DELETE: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows after delete, got %d", len(res.Rows))
	}
}

func TestEndToEnd_CreateIndex(t *testing.T) {
	db, _ := openTestDB(t)

	db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)")
	db.Exec("INSERT INTO products VALUES (1, 'Apple', 100)")
	db.Exec("INSERT INTO products VALUES (2, 'Banana', 50)")
	db.Exec("INSERT INTO products VALUES (3, 'Cherry', 200)")

	// CREATE INDEX
	res, err := db.Exec("CREATE INDEX idx_price ON products (price)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	_ = res

	// Query that can use the index
	res, err = db.Query("SELECT name FROM products WHERE price > 80")
	if err != nil {
		t.Fatalf("SELECT with index: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows (Apple+Cherry), got %d", len(res.Rows))
	}
}

func TestEndToEnd_DropTable(t *testing.T) {
	db, _ := openTestDB(t)

	db.Exec("CREATE TABLE temp (x INTEGER)")
	db.Exec("INSERT INTO temp VALUES (1)")

	_, err := db.Exec("DROP TABLE temp")
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	// Table should not exist
	_, err = db.Query("SELECT * FROM temp")
	if err == nil {
		t.Fatal("expected error querying dropped table")
	}
}

func TestEndToEnd_NullHandling(t *testing.T) {
	db, _ := openTestDB(t)

	db.Exec("CREATE TABLE nullable (id INTEGER, val TEXT)")
	db.Exec("INSERT INTO nullable VALUES (1, NULL)")
	db.Exec("INSERT INTO nullable VALUES (2, 'hello')")

	// IS NULL
	res, err := db.Query("SELECT id FROM nullable WHERE val IS NULL")
	if err != nil {
		t.Fatalf("IS NULL: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 1 {
		t.Fatalf("expected id=1 for NULL val, got %v", res.Rows)
	}

	// IS NOT NULL
	res, err = db.Query("SELECT id FROM nullable WHERE val IS NOT NULL")
	if err != nil {
		t.Fatalf("IS NOT NULL: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 2 {
		t.Fatalf("expected id=2 for non-NULL val, got %v", res.Rows)
	}
}
