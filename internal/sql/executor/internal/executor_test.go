package internal

import (
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	catalog "github.com/akzj/go-fast-kv/internal/sql/catalog"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	engine "github.com/akzj/go-fast-kv/internal/sql/engine"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parser "github.com/akzj/go-fast-kv/internal/sql/parser"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	planner "github.com/akzj/go-fast-kv/internal/sql/planner"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// ─── Test Helpers ───────────────────────────────────────────────────

type testEnv struct {
	store   kvstoreapi.Store
	cat     catalogapi.CatalogManager
	parser  parserapi.Parser
	planner plannerapi.Planner
	exec    executorapi.Executor
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cat := catalog.New(store)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)
	p := parser.New()
	pl := planner.New(cat)
	ex := New(store, cat, tbl, idx)

	return &testEnv{
		store:   store,
		cat:     cat,
		parser:  p,
		planner: pl,
		exec:    ex,
	}
}

// execSQL parses, plans, and executes a SQL statement.
func (env *testEnv) execSQL(t *testing.T, sql string) *executorapi.Result {
	t.Helper()
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	plan, err := env.planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan %q: %v", sql, err)
	}
	result, err := env.exec.Execute(plan)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return result
}

// execSQLErr parses, plans, and executes — expects no parse/plan error but may return exec error.
func (env *testEnv) execSQLErr(t *testing.T, sql string) (*executorapi.Result, error) {
	t.Helper()
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	plan, err := env.planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan %q: %v", sql, err)
	}
	return env.exec.Execute(plan)
}

// ─── Tests ──────────────────────────────────────────────────────────

func TestExec_CreateTable(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

	tbl, err := env.cat.GetTable("users")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if tbl.Name != "USERS" {
		t.Errorf("table name = %q, want %q", tbl.Name, "USERS")
	}
	if len(tbl.Columns) != 3 {
		t.Errorf("columns = %d, want 3", len(tbl.Columns))
	}
	if tbl.TableID == 0 {
		t.Error("TableID should be assigned (non-zero)")
	}
	if tbl.PrimaryKey != "ID" {
		t.Errorf("PrimaryKey = %q, want %q", tbl.PrimaryKey, "ID")
	}
}

func TestExec_CreateTableIfNotExists(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")

	// Second create with IF NOT EXISTS should succeed silently
	env.execSQL(t, "CREATE TABLE IF NOT EXISTS users (id INT, name TEXT)")
}

func TestExec_DropTable(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")

	// Verify it exists
	_, err := env.cat.GetTable("users")
	if err != nil {
		t.Fatalf("table should exist: %v", err)
	}

	env.execSQL(t, "DROP TABLE users")

	// Verify it's gone
	_, err = env.cat.GetTable("users")
	if err != catalogapi.ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}

	// DROP IF EXISTS on non-existent should succeed
	env.execSQL(t, "DROP TABLE IF EXISTS users")
}

func TestExec_Insert(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

	result := env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	// Verify via SELECT
	sel := env.execSQL(t, "SELECT * FROM users")
	if len(sel.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(sel.Rows))
	}
	if sel.Rows[0][0].Int != 1 {
		t.Errorf("id = %d, want 1", sel.Rows[0][0].Int)
	}
	if sel.Rows[0][1].Text != "Alice" {
		t.Errorf("name = %q, want %q", sel.Rows[0][1].Text, "Alice")
	}
	if sel.Rows[0][2].Int != 30 {
		t.Errorf("age = %d, want 30", sel.Rows[0][2].Int)
	}
}

func TestExec_InsertMultipleRows(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	if result.RowsAffected != 3 {
		t.Errorf("RowsAffected = %d, want 3", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM users")
	if len(sel.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(sel.Rows))
	}
}

func TestExec_SelectStar(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	result := env.execSQL(t, "SELECT * FROM users")
	if len(result.Columns) != 3 {
		t.Errorf("columns = %d, want 3", len(result.Columns))
	}
	if result.Columns[0] != "ID" || result.Columns[1] != "NAME" || result.Columns[2] != "AGE" {
		t.Errorf("columns = %v, want [ID NAME AGE]", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
}

func TestExec_SelectWhere(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := env.execSQL(t, "SELECT * FROM users WHERE age > 28")
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2 (Alice=30, Charlie=35)", len(result.Rows))
	}

	// SELECT with equality
	result = env.execSQL(t, "SELECT name FROM users WHERE id = 2")
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if result.Rows[0][0].Text != "Bob" {
		t.Errorf("name = %q, want %q", result.Rows[0][0].Text, "Bob")
	}
}

func TestExec_SelectOrderByLimit(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	// ORDER BY age ASC, LIMIT 2
	result := env.execSQL(t, "SELECT * FROM users ORDER BY age LIMIT 2")
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	// Youngest first: Bob(25), Alice(30)
	if result.Rows[0][2].Int != 25 {
		t.Errorf("first row age = %d, want 25", result.Rows[0][2].Int)
	}
	if result.Rows[1][2].Int != 30 {
		t.Errorf("second row age = %d, want 30", result.Rows[1][2].Int)
	}

	// ORDER BY age DESC
	result = env.execSQL(t, "SELECT * FROM users ORDER BY age DESC")
	if result.Rows[0][2].Int != 35 {
		t.Errorf("first row age = %d, want 35", result.Rows[0][2].Int)
	}
}

func TestExec_Delete(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := env.execSQL(t, "DELETE FROM users WHERE age < 30")
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM users")
	if len(sel.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(sel.Rows))
	}
}

func TestExec_DeleteAll(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob')")

	result := env.execSQL(t, "DELETE FROM users")
	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM users")
	if len(sel.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(sel.Rows))
	}
}

func TestExec_Update(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	result := env.execSQL(t, "UPDATE users SET age = 31 WHERE id = 1")
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM users WHERE id = 1")
	if len(sel.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(sel.Rows))
	}
	if sel.Rows[0][2].Int != 31 {
		t.Errorf("age = %d, want 31", sel.Rows[0][2].Int)
	}
}

func TestExec_CreateIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")

	idx, err := env.cat.GetIndex("users", "idx_age")
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if idx.Column != "AGE" {
		t.Errorf("column = %q, want %q", idx.Column, "AGE")
	}
	if idx.IndexID == 0 {
		t.Error("IndexID should be assigned (non-zero)")
	}

	// IF NOT EXISTS should succeed silently
	env.execSQL(t, "CREATE INDEX IF NOT EXISTS idx_age ON users (age)")
}

func TestExec_IndexScan(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	// This query should use the index on age
	result := env.execSQL(t, "SELECT * FROM users WHERE age = 30")
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Alice" {
		t.Errorf("name = %q, want %q", result.Rows[0][1].Text, "Alice")
	}
}

func TestExec_NullHandling(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	env.execSQL(t, "INSERT INTO data VALUES (1, NULL)")
	env.execSQL(t, "INSERT INTO data VALUES (2, 42)")
	env.execSQL(t, "INSERT INTO data VALUES (3, NULL)")

	// IS NULL
	result := env.execSQL(t, "SELECT * FROM data WHERE val IS NULL")
	if len(result.Rows) != 2 {
		t.Fatalf("IS NULL rows = %d, want 2", len(result.Rows))
	}

	// IS NOT NULL
	result = env.execSQL(t, "SELECT * FROM data WHERE val IS NOT NULL")
	if len(result.Rows) != 1 {
		t.Fatalf("IS NOT NULL rows = %d, want 1", len(result.Rows))
	}
	if result.Rows[0][1].Int != 42 {
		t.Errorf("val = %d, want 42", result.Rows[0][1].Int)
	}
}

func TestExec_FullLifecycle(t *testing.T) {
	env := newTestEnv(t)

	// CREATE TABLE
	env.execSQL(t, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")

	// INSERT
	env.execSQL(t, "INSERT INTO products VALUES (1, 'Widget', 100)")
	env.execSQL(t, "INSERT INTO products VALUES (2, 'Gadget', 200)")
	env.execSQL(t, "INSERT INTO products VALUES (3, 'Doohickey', 150)")

	// SELECT
	result := env.execSQL(t, "SELECT * FROM products")
	if len(result.Rows) != 3 {
		t.Fatalf("after insert: rows = %d, want 3", len(result.Rows))
	}

	// UPDATE
	upd := env.execSQL(t, "UPDATE products SET price = 250 WHERE id = 2")
	if upd.RowsAffected != 1 {
		t.Errorf("update RowsAffected = %d, want 1", upd.RowsAffected)
	}

	// Verify update
	sel := env.execSQL(t, "SELECT * FROM products WHERE id = 2")
	if len(sel.Rows) != 1 {
		t.Fatalf("after update: rows = %d, want 1", len(sel.Rows))
	}
	if sel.Rows[0][2].Int != 250 {
		t.Errorf("price = %d, want 250", sel.Rows[0][2].Int)
	}

	// DELETE
	del := env.execSQL(t, "DELETE FROM products WHERE price < 150")
	if del.RowsAffected != 1 {
		t.Errorf("delete RowsAffected = %d, want 1", del.RowsAffected)
	}

	// Verify delete
	result = env.execSQL(t, "SELECT * FROM products")
	if len(result.Rows) != 2 {
		t.Fatalf("after delete: rows = %d, want 2", len(result.Rows))
	}

	// DROP TABLE
	env.execSQL(t, "DROP TABLE products")
	_, err := env.cat.GetTable("products")
	if err != catalogapi.ErrTableNotFound {
		t.Fatalf("after drop: expected ErrTableNotFound, got %v", err)
	}
}


func TestExec_Like(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 'bob@work.org')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Carol', 'carol@example.com')")

	// LIKE with % suffix
	result := env.execSQL(t, "SELECT name FROM users WHERE email LIKE '%@example.com'")
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}

	// LIKE with exact pattern
	result = env.execSQL(t, "SELECT name FROM users WHERE name LIKE 'Bob'")
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}

	// LIKE with % prefix
	result = env.execSQL(t, "SELECT name FROM users WHERE name LIKE '%ice'")
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (Alice)", len(result.Rows))
	}

	// LIKE no match
	result = env.execSQL(t, "SELECT name FROM users WHERE email LIKE '%.gov'")
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(result.Rows))
	}

	// LIKE with _ wildcard (Carol matches Ca__)
	result = env.execSQL(t, "SELECT name FROM users WHERE name LIKE 'Ca___'")
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (Carol)", len(result.Rows))
	}
}

func TestExec_Between(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
	env.execSQL(t, "INSERT INTO products VALUES (1, 'Apple', 50)")
	env.execSQL(t, "INSERT INTO products VALUES (2, 'Banana', 30)")
	env.execSQL(t, "INSERT INTO products VALUES (3, 'Cherry', 70)")
	env.execSQL(t, "INSERT INTO products VALUES (4, 'Date', 90)")

	t.Run("between_in_range", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE price BETWEEN 40 AND 70")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (Apple, Cherry)", len(result.Rows))
		}
	})

	t.Run("between_boundary_inclusive", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE price BETWEEN 30 AND 70")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3 (Apple, Banana, Cherry)", len(result.Rows))
		}
	})

	t.Run("between_reversed_bounds", func(t *testing.T) {
		// Standard SQL: BETWEEN with reversed bounds returns 0 rows
		result := env.execSQL(t, "SELECT name FROM products WHERE price BETWEEN 70 AND 30")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0", len(result.Rows))
		}
	})

	t.Run("between_text", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE name BETWEEN 'A' AND 'C'")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (Apple, Banana, Cherry)", len(result.Rows))
		}
	})
}

func TestExec_In(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
	env.execSQL(t, "INSERT INTO products VALUES (1, 'Apple', 50)")
	env.execSQL(t, "INSERT INTO products VALUES (2, 'Banana', 30)")
	env.execSQL(t, "INSERT INTO products VALUES (3, 'Cherry', 70)")
	env.execSQL(t, "INSERT INTO products VALUES (4, 'Date', 90)")

	t.Run("in_int_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE id IN (1, 3)")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (Apple, Cherry)", len(result.Rows))
		}
	})

	t.Run("in_text_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE name IN ('Banana', 'Date')")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (Banana, Date)", len(result.Rows))
		}
	})

	t.Run("in_single_value", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE price IN (50)")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (Apple)", len(result.Rows))
		}
	})

	t.Run("in_no_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE id IN (99, 100)")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0", len(result.Rows))
		}
	})

	t.Run("in_mixed_types", func(t *testing.T) {
		// Mixed types: int id IN text values — pragmatically evaluates to 0 rows
		result := env.execSQL(t, "SELECT name FROM products WHERE id IN ('a', 'b')")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0 (type mismatch)", len(result.Rows))
		}
	})
}
