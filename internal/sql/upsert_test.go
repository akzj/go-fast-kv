package sql_test

import (
	"os"
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	gosql "github.com/akzj/go-fast-kv/internal/sql"
)

func openTestDBForUpsert(t *testing.T) *gosql.DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "sql-upsert-*")
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
	return db
}

func TestUpsertBasic(t *testing.T) {
	db := openTestDBForUpsert(t)

	// Create table with PRIMARY KEY
	_, err := db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert first row
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatal(err)
	}

	// ON CONFLICT DO NOTHING - id=1 already exists, should be ignored
	res, err := db.Exec("INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT(id) DO NOTHING")
	if err != nil {
		t.Fatalf("UPSERT DO NOTHING failed: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("DO NOTHING should affect 0 rows, got %d", res.RowsAffected)
	}

	// Verify original value unchanged
	res, err = db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	name := res.Rows[0][0].Text
	if name != "alice" {
		t.Errorf("expected 'alice', got %q", name)
	}

	// ON CONFLICT DO UPDATE - should update
	res, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT(id) DO UPDATE SET name='bob'")
	if err != nil {
		t.Fatalf("UPSERT DO UPDATE failed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("DO UPDATE should affect 1 row, got %d", res.RowsAffected)
	}

	// Verify updated value
	res, err = db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	name = res.Rows[0][0].Text
	if name != "bob" {
		t.Errorf("expected 'bob', got %q", name)
	}

	// Insert new row (no conflict)
	res, err = db.Exec("INSERT INTO users (id, name) VALUES (2, 'charlie') ON CONFLICT(id) DO UPDATE SET name='charlie'")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("INSERT should affect 1 row, got %d", res.RowsAffected)
	}

	// Verify new row
	res, err = db.Query("SELECT name FROM users WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	name = res.Rows[0][0].Text
	if name != "charlie" {
		t.Errorf("expected 'charlie', got %q", name)
	}
}

func TestUpsertMultipleRows(t *testing.T) {
	db := openTestDBForUpsert(t)

	_, err := db.Exec("CREATE TABLE t (k INT PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Batch insert with UPSERT
	res, err := db.Exec("INSERT INTO t (k, v) VALUES (1, 'a'), (2, 'b'), (1, 'a2') ON CONFLICT(k) DO UPDATE SET v='updated'")
	if err != nil {
		t.Fatalf("Batch UPSERT failed: %v", err)
	}
	// Should insert 2 new rows + update 1 = 3
	if res.RowsAffected != 3 {
		t.Errorf("expected 3 affected rows, got %d", res.RowsAffected)
	}

	// Verify updated
	res, err = db.Query("SELECT v FROM t WHERE k = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	v := res.Rows[0][0].Text
	if v != "updated" {
		t.Errorf("expected 'updated', got %q", v)
	}
}
