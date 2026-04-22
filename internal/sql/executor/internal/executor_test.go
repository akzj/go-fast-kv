package internal

import (
	"strconv"
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	catalog "github.com/akzj/go-fast-kv/internal/sql/catalog"
	engine "github.com/akzj/go-fast-kv/internal/sql/engine"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	sqlerrors "github.com/akzj/go-fast-kv/internal/sql/errors"
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
	enc     encodingapi.KeyEncoder // exposed for testing index cleanup
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
	ex := New(store, cat, tbl, idx, pl, p)

	return &testEnv{
		store:   store,
		cat:     cat,
		parser:  p,
		planner: pl,
		exec:    ex,
		enc:     enc,
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
		return nil, err
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

func TestExec_DropTableWithIndexes(t *testing.T) {
	env := newTestEnv(t)

	// Create table with UNIQUE columns (auto-creates indexes)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, age INT UNIQUE)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'alice@example.com', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'bob@example.com', 25)")

	// Verify indexes exist (index names use format uq_<table>_<column>)
	idxEmail, err := env.cat.GetIndex("users", "uq_users_email")
	if err != nil {
		t.Fatalf("email index should exist: %v", err)
	}
	idxAge, err := env.cat.GetIndex("users", "uq_users_age")
	if err != nil {
		t.Fatalf("age index should exist: %v", err)
	}

	// Build prefixes to scan for orphaned index entries
	emailPrefix := env.enc.EncodeIndexPrefix(1, idxEmail.IndexID)
	emailPrefixEnd := env.enc.EncodeIndexPrefixEnd(1, idxEmail.IndexID)
	agePrefix := env.enc.EncodeIndexPrefix(1, idxAge.IndexID)
	agePrefixEnd := env.enc.EncodeIndexPrefixEnd(1, idxAge.IndexID)

	// Verify index data exists before DROP
	iter := env.store.Scan(emailPrefix, emailPrefixEnd)
	emailCountBefore := 0
	for iter.Next() {
		emailCountBefore++
	}
	if emailCountBefore == 0 {
		t.Fatal("email index should have entries before DROP")
	}

	iter = env.store.Scan(agePrefix, agePrefixEnd)
	ageCountBefore := 0
	for iter.Next() {
		ageCountBefore++
	}
	if ageCountBefore == 0 {
		t.Fatal("age index should have entries before DROP")
	}

	// Drop the table
	env.execSQL(t, "DROP TABLE users")

	// Verify table is gone
	_, err = env.cat.GetTable("users")
	if err != catalogapi.ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound after DROP, got %v", err)
	}

	// Verify NO orphaned index entries remain
	iter = env.store.Scan(emailPrefix, emailPrefixEnd)
	emailCountAfter := 0
	for iter.Next() {
		emailCountAfter++
	}
	if emailCountAfter != 0 {
		t.Errorf("email index: expected 0 orphaned entries after DROP, got %d", emailCountAfter)
	}

	iter = env.store.Scan(agePrefix, agePrefixEnd)
	ageCountAfter := 0
	for iter.Next() {
		ageCountAfter++
	}
	if ageCountAfter != 0 {
		t.Errorf("age index: expected 0 orphaned entries after DROP, got %d", ageCountAfter)
	}
}

func TestExec_AlterTable(t *testing.T) {
	env := newTestEnv(t)

	t.Run("AddColumn_Int", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)")
		env.execSQL(t, "ALTER TABLE t1 ADD COLUMN age INT")

		schema, err := env.cat.GetTable("t1")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		if len(schema.Columns) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(schema.Columns))
		}
		if schema.Columns[2].Name != "AGE" {
			t.Errorf("column name: expected AGE, got %s", schema.Columns[2].Name)
		}
		if schema.Columns[2].Type != catalogapi.TypeInt {
			t.Errorf("column type: expected TypeInt, got %v", schema.Columns[2].Type)
		}

		// Verify INSERT/SELECT works with new column
		env.execSQL(t, "INSERT INTO t1 VALUES (1, 'Alice', 30)")
		sel := env.execSQL(t, "SELECT * FROM t1")
		if len(sel.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(sel.Rows))
		}
		if sel.Rows[0][2].Int != 30 {
			t.Errorf("age = %d, want 30", sel.Rows[0][2].Int)
		}
	})

	t.Run("AddColumn_Text", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t2 (id INT PRIMARY KEY)")
		env.execSQL(t, "ALTER TABLE t2 ADD COLUMN email TEXT")

		schema, err := env.cat.GetTable("t2")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		if schema.Columns[len(schema.Columns)-1].Name != "EMAIL" {
			t.Errorf("column name: expected EMAIL, got %s", schema.Columns[len(schema.Columns)-1].Name)
		}
		if schema.Columns[len(schema.Columns)-1].Type != catalogapi.TypeText {
			t.Errorf("column type: expected TypeText, got %v", schema.Columns[len(schema.Columns)-1].Type)
		}
	})

	t.Run("AddColumn_Float", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t3 (id INT PRIMARY KEY)")
		env.execSQL(t, "ALTER TABLE t3 ADD COLUMN score FLOAT")

		schema, err := env.cat.GetTable("t3")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		if schema.Columns[len(schema.Columns)-1].Type != catalogapi.TypeFloat {
			t.Errorf("column type: expected TypeFloat, got %v", schema.Columns[len(schema.Columns)-1].Type)
		}
	})

	t.Run("AddColumn_BLOB", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t4 (id INT PRIMARY KEY)")
		env.execSQL(t, "ALTER TABLE t4 ADD COLUMN data BLOB")

		schema, err := env.cat.GetTable("t4")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		if schema.Columns[len(schema.Columns)-1].Type != catalogapi.TypeBlob {
			t.Errorf("column type: expected TypeBlob, got %v", schema.Columns[len(schema.Columns)-1].Type)
		}
	})

	t.Run("AddColumn_NotNull", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t5 (id INT PRIMARY KEY)")
		env.execSQL(t, "ALTER TABLE t5 ADD COLUMN nick TEXT NOT NULL")

		schema, err := env.cat.GetTable("t5")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		col := schema.Columns[len(schema.Columns)-1]
		if col.Name != "NICK" {
			t.Errorf("column name: expected NICK, got %s", col.Name)
		}
		if !col.NotNull {
			t.Error("expected NotNull=true")
		}
	})

	t.Run("DropColumn", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t6 (id INT PRIMARY KEY, name TEXT, age INT)")
		env.execSQL(t, "ALTER TABLE t6 DROP COLUMN age")

		schema, err := env.cat.GetTable("t6")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		if len(schema.Columns) != 2 {
			t.Fatalf("expected 2 columns after DROP, got %d", len(schema.Columns))
		}
		for _, col := range schema.Columns {
			if col.Name == "AGE" {
				t.Error("AGE column should have been dropped")
			}
		}
	})

	t.Run("RenameColumn", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t7 (id INT PRIMARY KEY, name TEXT)")
		env.execSQL(t, "ALTER TABLE t7 RENAME COLUMN name TO full_name")

		schema, err := env.cat.GetTable("t7")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		found := false
		for _, col := range schema.Columns {
			if col.Name == "FULL_NAME" {
				found = true
				break
			}
		}
		if !found {
			t.Error("NAME should have been renamed to FULL_NAME")
		}
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		_, err := env.execSQLErr(t, "ALTER TABLE nope ADD COLUMN col INT")
		if err == nil {
			t.Error("expected error for non-existent table")
		}
	})

	t.Run("DuplicateColumn", func(t *testing.T) {
		_, err := env.execSQLErr(t, "ALTER TABLE users ADD COLUMN id INT")
		if err == nil {
			t.Error("expected error for duplicate column")
		}
	})

	t.Run("RenameTo", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE old_name (id INT PRIMARY KEY, name TEXT)")
		env.execSQL(t, "ALTER TABLE old_name RENAME TO new_name")

		// New table name should exist
		schema, err := env.cat.GetTable("new_name")
		if err != nil {
			t.Fatalf("GetTable new_name failed: %v", err)
		}
		if schema.Name != "NEW_NAME" {
			t.Errorf("table name: expected NEW_NAME, got %s", schema.Name)
		}

		// Old table name should not exist
		_, err = env.cat.GetTable("old_name")
		if err != catalogapi.ErrTableNotFound {
			t.Errorf("old_name should not exist: got %v", err)
		}

		// INSERT/SELECT should work with new table name
		env.execSQL(t, "INSERT INTO new_name VALUES (1, 'Alice')")
		sel := env.execSQL(t, "SELECT * FROM new_name")
		if len(sel.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(sel.Rows))
		}
		if sel.Rows[0][1].Text != "Alice" {
			t.Errorf("name = %q, want %q", sel.Rows[0][1].Text, "Alice")
		}
	})

	t.Run("RenameTo_Duplicate", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE dup1 (id INT PRIMARY KEY)")
		env.execSQL(t, "CREATE TABLE dup2 (id INT PRIMARY KEY)")
		_, err := env.execSQLErr(t, "ALTER TABLE dup1 RENAME TO dup2")
		if err == nil {
			t.Error("expected error for duplicate table name")
		}
	})

	t.Run("RenameTo_NonExistent", func(t *testing.T) {
		_, err := env.execSQLErr(t, "ALTER TABLE nonexistent RENAME TO new_name")
		if err == nil {
			t.Error("expected error for non-existent table")
		}
	})
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

func TestExec_UniqueConstraint(t *testing.T) {
	env := newTestEnv(t)

	t.Run("insert_duplicate_unique_column", func(t *testing.T) {
		// Create table with UNIQUE column
		env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)")

		// First insert should succeed
		env.execSQL(t, "INSERT INTO users VALUES (1, 'alice@example.com')")

		// Second insert with same email should fail
		_, err := env.execSQLErr(t, "INSERT INTO users VALUES (2, 'alice@example.com')")
		if err == nil {
			t.Fatal("expected unique constraint violation error, got nil")
		}
		// Verify SQLSTATE is 23505 (unique violation)
		if sqle, ok := err.(*sqlerrors.SQLError); ok {
			if sqle.SQLState != "23505" {
				t.Errorf("SQLState = %s, want 23505", sqle.SQLState)
			}
		} else {
			t.Fatalf("expected SQLError, got %T", err)
		}

		// Verify first row still exists
		sel := env.execSQL(t, "SELECT * FROM users")
		if len(sel.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(sel.Rows))
		}
	})

	t.Run("insert_different_unique_values", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE)")

		// Insert with different values should succeed
		env.execSQL(t, "INSERT INTO items VALUES (1, 'CODE1')")
		env.execSQL(t, "INSERT INTO items VALUES (2, 'CODE2')")

		sel := env.execSQL(t, "SELECT * FROM items")
		if len(sel.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(sel.Rows))
		}
	})

	t.Run("insert_null_unique", func(t *testing.T) {
		// NULL values don't violate UNIQUE constraint
		env.execSQL(t, "CREATE TABLE logs (id INT PRIMARY KEY, ref TEXT UNIQUE)")

		env.execSQL(t, "INSERT INTO logs VALUES (1, NULL)")
		env.execSQL(t, "INSERT INTO logs VALUES (2, NULL)") // Should succeed

		sel := env.execSQL(t, "SELECT * FROM logs")
		if len(sel.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (NULLs allowed multiple times)", len(sel.Rows))
		}
	})

	t.Run("update_violates_unique", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE products (id INT PRIMARY KEY, sku TEXT UNIQUE)")
		env.execSQL(t, "INSERT INTO products VALUES (1, 'SKU-A')")
		env.execSQL(t, "INSERT INTO products VALUES (2, 'SKU-B')")

		// Update row 2 to have same SKU as row 1
		_, err := env.execSQLErr(t, "UPDATE products SET sku = 'SKU-A' WHERE id = 2")
		if err == nil {
			t.Fatal("expected unique constraint violation error on update, got nil")
		}
		if sqle, ok := err.(*sqlerrors.SQLError); ok {
			if sqle.SQLState != "23505" {
				t.Errorf("SQLState = %s, want 23505", sqle.SQLState)
			}
		}
	})
}

func TestExec_SelectStar(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	t.Run("insert_set_syntax", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE t (a INT, b TEXT)")
		env.execSQL(t, "INSERT INTO t SET a = 1, b = 'hello'")
		sel := env.execSQL(t, "SELECT * FROM t")
		if len(sel.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(sel.Rows))
		}
		if sel.Rows[0][0].Int != 1 {
			t.Errorf("a = %d, want 1", sel.Rows[0][0].Int)
		}
		if sel.Rows[0][1].Text != "hello" {
			t.Errorf("b = %q, want hello", sel.Rows[0][1].Text)
		}
	})

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

func TestExec_OrderByMultipleColumns(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

	// Insert test data with same age but different names
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 30)") // Same age as Alice
	env.execSQL(t, "INSERT INTO users VALUES (4, 'Diana', 25)")  // Same age as Bob

	// ORDER BY age ASC, name ASC
	// Expected: Bob(25), Diana(25), Alice(30), Charlie(30)
	// (Bob < Diana alphabetically, Alice < Charlie)
	result := env.execSQL(t, "SELECT name, age FROM users ORDER BY age, name")
	if len(result.Rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(result.Rows))
	}

	// Verify the order: Bob(25), Diana(25), Alice(30), Charlie(30)
	expected := []struct {
		name string
		age  int64
	}{
		{"Bob", 25},
		{"Diana", 25},
		{"Alice", 30},
		{"Charlie", 30},
	}

	for i, exp := range expected {
		if result.Rows[i][0].Text != exp.name {
			t.Errorf("row[%d].name = %q, want %q", i, result.Rows[i][0].Text, exp.name)
		}
		if result.Rows[i][1].Int != exp.age {
			t.Errorf("row[%d].age = %d, want %d", i, result.Rows[i][1].Int, exp.age)
		}
	}

	// ORDER BY age DESC, name ASC (secondary sort ascending)
	// Expected: Charlie(30), Alice(30), Diana(25), Bob(25)
	// (Age DESC first, then name ASC within same age)
	result = env.execSQL(t, "SELECT name, age FROM users ORDER BY age DESC, name")
	if len(result.Rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(result.Rows))
	}

	expectedDesc := []struct {
		name string
		age  int64
	}{
		{"Alice", 30},   // 30s: Alice < Charlie
		{"Charlie", 30},
		{"Bob", 25},     // 25s: Bob < Diana
		{"Diana", 25},
	}

	for i, exp := range expectedDesc {
		if result.Rows[i][0].Text != exp.name {
			t.Errorf("row[%d].name = %q, want %q", i, result.Rows[i][0].Text, exp.name)
		}
		if result.Rows[i][1].Int != exp.age {
			t.Errorf("row[%d].age = %d, want %d", i, result.Rows[i][1].Int, exp.age)
		}
	}

	// ORDER BY name, age (both ASC - lexicographic on combined)
	// Expected: Alice(30), Bob(25), Charlie(30), Diana(25)
	result = env.execSQL(t, "SELECT name, age FROM users ORDER BY name, age")
	if len(result.Rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(result.Rows))
	}

	expectedNameAge := []string{"Alice", "Bob", "Charlie", "Diana"}
	for i, exp := range expectedNameAge {
		if result.Rows[i][0].Text != exp {
			t.Errorf("row[%d].name = %q, want %q", i, result.Rows[i][0].Text, exp)
		}
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

func TestExec_DeleteAll_LargeTable(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE big (id INT PRIMARY KEY, val TEXT)")

	// Insert 2500 rows
	for i := 1; i <= 2500; i++ {
		env.execSQL(t, "INSERT INTO big VALUES ("+strconv.Itoa(i)+", 'row"+strconv.Itoa(i)+"')")
	}

	// DELETE all without WHERE
	result := env.execSQL(t, "DELETE FROM big")
	if result.RowsAffected != 2500 {
		t.Errorf("RowsAffected = %d, want 2500", result.RowsAffected)
	}

	// Verify no rows remain
	sel := env.execSQL(t, "SELECT COUNT(*) FROM big")
	if sel.Rows[0][0].Int != 0 {
		t.Errorf("COUNT(*) = %d, want 0", sel.Rows[0][0].Int)
	}
}

func TestExec_Truncate(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie')")

	// TRUNCATE the table
	result := env.execSQL(t, "TRUNCATE TABLE users")
	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// Verify table is empty
	sel := env.execSQL(t, "SELECT * FROM users")
	if len(sel.Rows) != 0 {
		t.Errorf("rows = %d, want 0", len(sel.Rows))
	}

	// Verify table structure still exists (can re-insert)
	env.execSQL(t, "INSERT INTO users VALUES (10, 'Dave')")
	sel = env.execSQL(t, "SELECT * FROM users")
	if len(sel.Rows) != 1 {
		t.Errorf("rows = %d, want 1 after re-insert", len(sel.Rows))
	}
	if sel.Rows[0][0].Int != 10 {
		t.Errorf("id = %d, want 10", sel.Rows[0][0].Int)
	}
}

func TestExec_Truncate_LargeTable(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE big (id INT PRIMARY KEY, val TEXT)")

	// Insert 1000 rows
	for i := 1; i <= 1000; i++ {
		env.execSQL(t, "INSERT INTO big VALUES ("+strconv.Itoa(i)+", 'row"+strconv.Itoa(i)+"')")
	}

	// TRUNCATE the table
	result := env.execSQL(t, "TRUNCATE TABLE big")
	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// Verify no rows remain
	sel := env.execSQL(t, "SELECT COUNT(*) FROM big")
	if sel.Rows[0][0].Int != 0 {
		t.Errorf("COUNT(*) = %d, want 0", sel.Rows[0][0].Int)
	}

	// Verify table structure still exists
	env.execSQL(t, "INSERT INTO big VALUES (9999, 'new')")
	sel = env.execSQL(t, "SELECT COUNT(*) FROM big")
	if sel.Rows[0][0].Int != 1 {
		t.Errorf("COUNT(*) = %d, want 1 after re-insert", sel.Rows[0][0].Int)
	}
}

func TestExec_Truncate_NonExistentTable(t *testing.T) {
	env := newTestEnv(t)
	_, err := env.execSQLErr(t, "TRUNCATE TABLE nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent table")
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

func TestExec_CreateIndex_Expression(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")

	// Create expression index with LOWER function
	env.execSQL(t, "CREATE INDEX idx_lower_email ON users (LOWER(email))")

	idx, err := env.cat.GetIndex("users", "idx_lower_email")
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if idx.Column != "EMAIL" {
		t.Errorf("column = %q, want %q", idx.Column, "EMAIL")
	}
	if idx.ExprSQL == "" {
		t.Error("ExprSQL should be set for expression index")
	}
	if idx.IndexID == 0 {
		t.Error("IndexID should be assigned (non-zero)")
	}

	// Insert data - should populate the index
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Test@Example.COM')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Another@TEST.com')")

	// Query that could use the expression index
	result := env.execSQL(t, "SELECT id FROM users WHERE LOWER(email) = 'test@example.com'")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestExec_DropIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	// Verify index exists
	_, err := env.cat.GetIndex("users", "idx_age")
	if err != nil {
		t.Fatalf("GetIndex before drop: %v", err)
	}

	// Drop the index
	env.execSQL(t, "DROP INDEX idx_age ON users")

	// Verify index is gone
	_, err = env.cat.GetIndex("users", "idx_age")
	if err != catalogapi.ErrIndexNotFound {
		t.Errorf("GetIndex after drop: err = %v, want ErrIndexNotFound", err)
	}

	// Queries should still work (table scan)
	result := env.execSQL(t, "SELECT * FROM users WHERE age = 30")
	if len(result.Rows) != 1 {
		t.Fatalf("rows after index drop = %d, want 1", len(result.Rows))
	}
}

func TestExec_DropIndex_IfExists(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

	// DROP INDEX IF EXISTS with non-existent index should succeed silently
	env.execSQL(t, "DROP INDEX IF EXISTS idx_age ON users")

	// No error means test passes
}

func TestExec_DropIndex_NotExists(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

	// DROP INDEX without IF EXISTS on non-existent index should fail
	_, err := env.execSQLErr(t, "DROP INDEX idx_age ON users")
	if err == nil {
		t.Fatal("expected error for non-existent index")
	}
}

func TestExec_DropIndex_CascadeToData(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")

	// Drop the index
	env.execSQL(t, "DROP INDEX idx_age ON users")

	// Table data should still exist
	result := env.execSQL(t, "SELECT COUNT(*) FROM users")
	if result.Rows[0][0].Int != 2 {
		t.Errorf("count = %d, want 2", result.Rows[0][0].Int)
	}
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

func TestExec_IndexOnlyScan(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "CREATE INDEX idx_age ON users (age)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	// SELECT age FROM users WHERE age = 30
	// Should use index-only scan (covering index) — no table access needed.
	// The index contains the age column value, so we can satisfy the query
	// entirely from the index without reading table pages.
	result := env.execSQL(t, "SELECT age FROM users WHERE age = 30")

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int != 30 {
		t.Errorf("age = %v, want 30", result.Rows[0][0].Int)
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
	t.Run("not_between_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE price NOT BETWEEN 30 AND 70")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (Date only)", len(result.Rows))
		}
	})

	t.Run("not_between_no_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE price NOT BETWEEN 20 AND 100")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0", len(result.Rows))
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

	// NOT IN tests (verify UnaryNot(InExpr) chain works correctly)
	t.Run("not_in_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE id NOT IN (1, 3)")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (Banana, Date)", len(result.Rows))
		}
	})

	t.Run("not_in_no_match", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name FROM products WHERE id NOT IN (1, 2, 3, 4)")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0", len(result.Rows))
		}
	})

}

func TestExec_Subquery(t *testing.T) {
	env := newTestEnv(t)
	// Set up two tables
	env.execSQL(t, "CREATE TABLE t1 (id INT, name TEXT)")
	env.execSQL(t, "CREATE TABLE t2 (id INT, val TEXT)")
	env.execSQL(t, "INSERT INTO t1 VALUES (1, 'one')")
	env.execSQL(t, "INSERT INTO t1 VALUES (2, 'two')")
	env.execSQL(t, "INSERT INTO t1 VALUES (3, 'three')")
	env.execSQL(t, "INSERT INTO t1 VALUES (4, 'four')")
	env.execSQL(t, "INSERT INTO t2 VALUES (1, 'a')")
	env.execSQL(t, "INSERT INTO t2 VALUES (3, 'c')")
	env.execSQL(t, "INSERT INTO t2 VALUES (5, 'e')")

	t.Run("in_with_subquery", func(t *testing.T) {
		result := env.execSQL(t, "SELECT id, name FROM t1 WHERE id IN (SELECT id FROM t2)")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (id 1, id 3)", len(result.Rows))
		}
		if result.Rows[0][0].Int != 1 {
			t.Errorf("row[0].id = %d, want 1", result.Rows[0][0].Int)
		}
		if result.Rows[1][0].Int != 3 {
			t.Errorf("row[1].id = %d, want 3", result.Rows[1][0].Int)
		}
	})

	t.Run("not_in_with_subquery", func(t *testing.T) {
		result := env.execSQL(t, "SELECT id, name FROM t1 WHERE id NOT IN (SELECT id FROM t2)")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2 (id 2, id 4)", len(result.Rows))
		}
		if result.Rows[0][0].Int != 2 {
			t.Errorf("row[0].id = %d, want 2", result.Rows[0][0].Int)
		}
		if result.Rows[1][0].Int != 4 {
			t.Errorf("row[1].id = %d, want 4", result.Rows[1][0].Int)
		}
	})

	t.Run("in_subquery_no_matches", func(t *testing.T) {
		result := env.execSQL(t, "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id < 0)")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0 (subquery returns empty)", len(result.Rows))
		}
	})

	t.Run("in_subquery_text_column", func(t *testing.T) {
		result := env.execSQL(t, "SELECT id FROM t1 WHERE name IN (SELECT val FROM t2)")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0 (no text overlap)", len(result.Rows))
		}
	})

	t.Run("having_without_groupby", func(t *testing.T) {
		// HAVING without GROUP BY should return an error
		env := newTestEnv(t)
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		_, err := env.execSQLErr(t, "SELECT COUNT(*) FROM orders HAVING COUNT(*) > 0")
		if err == nil {
			t.Error("expected error for HAVING without GROUP BY")
		}
	})
}

func TestExec_ScalarSubquery(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t1 (id INT, salary INT)")
	env.execSQL(t, "CREATE TABLE t2 (id INT, dept TEXT, max_sal INT)")
	env.execSQL(t, "INSERT INTO t1 VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO t1 VALUES (2, 200)")
	env.execSQL(t, "INSERT INTO t1 VALUES (3, 300)")
	env.execSQL(t, "INSERT INTO t1 VALUES (4, 400)")
	env.execSQL(t, "INSERT INTO t2 VALUES (1, 'eng', 400)")
	env.execSQL(t, "INSERT INTO t2 VALUES (2, 'sales', 200)")

	t.Run("scalar_gt_subquery", func(t *testing.T) {
		// t1 rows where salary > (SELECT MAX(salary) FROM t2 WHERE dept='eng') = 400
		// Expected: id 3 (300), id 4 (400) — both > 400? No, 300 is NOT > 400.
		// Actually MAX(eng) = 400, so only id 4 (400) > 400? No. Let's be precise.
		// MAX salary for 'eng' dept: only row (1, 'eng', 400) → max = 400
		// salary > 400 → only rows with salary > 400 → none (max in t1 is 400)
		// salary >= 400 → id 4 (400) only
		result := env.execSQL(t, "SELECT id, salary FROM t1 WHERE salary >= (SELECT max_sal FROM t2 WHERE dept='eng')")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (id 4 with salary 400 >= 400)", len(result.Rows))
		}
		if result.Rows[0][0].Int != 4 {
			t.Errorf("row[0].id = %d, want 4", result.Rows[0][0].Int)
		}
	})

	t.Run("scalar_eq_subquery", func(t *testing.T) {
		// t1 rows where salary = (SELECT MAX(salary) FROM t1 ORDER BY 1 LIMIT 1) — using ORDER/LIMIT instead of aggregate
		// Use a simpler approach: salary = (SELECT salary FROM t1 ORDER BY salary DESC LIMIT 1) = 400
		result := env.execSQL(t, "SELECT id FROM t1 WHERE salary = (SELECT salary FROM t1 ORDER BY salary DESC LIMIT 1)")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (id 4 with max salary 400)", len(result.Rows))
		}
		if result.Rows[0][0].Int != 4 {
			t.Errorf("row[0].id = %d, want 4", result.Rows[0][0].Int)
		}
	})

	t.Run("scalar_lt_subquery", func(t *testing.T) {
		// t1 rows where salary < (SELECT salary FROM t1 WHERE id=2 ORDER BY id LIMIT 1) = 200
		// Rows with salary < 200: id 1 (100) only
		result := env.execSQL(t, "SELECT id FROM t1 WHERE salary < (SELECT salary FROM t1 WHERE id=2 ORDER BY id LIMIT 1)")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (id 1 with salary 100 < 200)", len(result.Rows))
		}
		if result.Rows[0][0].Int != 1 {
			t.Errorf("row[0].id = %d, want 1", result.Rows[0][0].Int)
		}
	})

	t.Run("scalar_empty_subquery", func(t *testing.T) {
		// t1 rows where salary > (SELECT max_sal FROM t2 WHERE dept='unknown') → empty subquery → NULL → 0 rows
		result := env.execSQL(t, "SELECT id FROM t1 WHERE salary > (SELECT max_sal FROM t2 WHERE dept='unknown')")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0 (empty subquery → NULL, comparison yields false)", len(result.Rows))
		}
	})

	t.Run("scalar_comparison_text", func(t *testing.T) {
		// t2 rows where dept = (SELECT dept FROM t2 WHERE id=1) = 'eng'
		result := env.execSQL(t, "SELECT id FROM t2 WHERE dept = (SELECT dept FROM t2 WHERE id=1)")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (dept='eng')", len(result.Rows))
		}
		if result.Rows[0][0].Int != 1 {
			t.Errorf("row[0].id = %d, want 1", result.Rows[0][0].Int)
		}
	})

	t.Run("multi_row_scalar_subquery_error", func(t *testing.T) {
		// Subquery returns 2 rows — should error per SQL standard
		_, err := env.execSQLErr(t, "SELECT name FROM users WHERE id = (SELECT user_id FROM orders WHERE amount > 100)")
		if err == nil {
			t.Error("expected error for multi-row scalar subquery")
		}
	})
}

func TestExec_Join(t *testing.T) {
	env := newTestEnv(t)

	// Setup two tables
	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")
	env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'carol')")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
	env.execSQL(t, "INSERT INTO orders VALUES (3, 50)")
	// user_id=2 has no orders (should NOT appear in INNER JOIN)

	t.Run("inner_join_basic", func(t *testing.T) {
		result := env.execSQL(t, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id")
		// Expected: alice/100, alice/200, carol/50 (3 rows, no bob)
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		// Check column names include both tables
		if len(result.Columns) != 2 {
			t.Fatalf("columns = %d, want 2", len(result.Columns))
		}
	})

	t.Run("inner_join_with_where", func(t *testing.T) {
		// WHERE after JOIN — use SELECT * to avoid projection complexity
		result := env.execSQL(t, "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		// Result has all 4 columns: users.id, users.name, orders.user_id, orders.amount
		// Column order: id(0), name(1), user_id(2), amount(3)
		if result.Rows[0][3].Int != 100 {
			t.Errorf("amount[0] = %d, want 100", result.Rows[0][3].Int)
		}
	})

	t.Run("inner_join_no_match", func(t *testing.T) {
		// bob (id=2) has no orders — should not appear
		result := env.execSQL(t, "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id")
		ids := make(map[int64]bool)
		for _, row := range result.Rows {
			ids[row[0].Int] = true
		}
		if ids[2] {
			t.Errorf("bob (id=2) should NOT be in results — he has no orders")
		}
	})

	t.Run("single_table_still_works", func(t *testing.T) {
		// Verify backward compat — single table queries unchanged
		result := env.execSQL(t, "SELECT name FROM users WHERE id = 1")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Text != "alice" {
			t.Errorf("name = %s, want alice", result.Rows[0][0].Text)
		}
	})

	t.Run("left_join", func(t *testing.T) {
		// LEFT JOIN with SELECT *: all users, bob (id=2) has no orders → NULL for order columns
		result := env.execSQL(t, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
		// users: id(0), name(1); orders: user_id(2), amount(3)
		// Expected: alice/100, alice/200, bob/NULL, carol/50 (4 rows)
		if len(result.Rows) != 4 {
			t.Fatalf("rows = %d, want 4", len(result.Rows))
		}
		// Find bob's row (id=2) — should have NULL for orders columns (indices 2,3)
		var bobRow []catalogapi.Value
		for _, row := range result.Rows {
			if row[0].Int == 2 {
				bobRow = row
				break
			}
		}
		if bobRow == nil {
			t.Fatalf("bob (id=2) not found in results")
		}
		// orders columns (user_id=2, amount=3) should be NULL
		if !bobRow[2].IsNull {
			t.Errorf("bob's user_id should be NULL, got %v", bobRow[2])
		}
		if !bobRow[3].IsNull {
			t.Errorf("bob's amount should be NULL, got %v", bobRow[3])
		}
		// Carol's row (id=3) — should have amount=50
		var carolRow []catalogapi.Value
		for _, row := range result.Rows {
			if row[0].Int == 3 {
				carolRow = row
				break
			}
		}
		if carolRow == nil {
			t.Fatalf("carol (id=3) not found")
		}
		if carolRow[3].Int != 50 {
			t.Errorf("carol's amount = %d, want 50", carolRow[3].Int)
		}
	})

	t.Run("right_join", func(t *testing.T) {
		// RIGHT JOIN with SELECT *: all orders, with user info
		// All orders have matching users, so no NULLs on user side
		result := env.execSQL(t, "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id")
		// users: id(0), name(1); orders: user_id(2), amount(3)
		// All 3 orders have matching users → 3 rows, no NULLs on user side
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		// No NULLs in user columns (all orders have matching users)
		for i, row := range result.Rows {
			if row[0].IsNull {
				t.Errorf("row %d: user id should NOT be NULL", i)
			}
			if row[1].IsNull {
				t.Errorf("row %d: user name should NOT be NULL", i)
			}
		}
	})

	t.Run("triple_join", func(t *testing.T) {
		// Add a third table that joins on users.id
		env.execSQL(t, "CREATE TABLE ages (user_id INT, age INT)")
		env.execSQL(t, "INSERT INTO ages VALUES (1, 30)")
		env.execSQL(t, "INSERT INTO ages VALUES (2, 25)")
		env.execSQL(t, "INSERT INTO ages VALUES (3, 35)")

		// users (3 rows) → orders (3 rows) → ages (3 rows)
		// users JOIN orders ON users.id = orders.user_id → alice has 2 orders, carol has 1
		// Then JOIN ages ON users.id = ages.user_id → each user matched with their age
		// Expected: alice/100/30, alice/200/30, carol/50/35 (3 rows, no bob)
		result := env.execSQL(t, "SELECT users.name, orders.amount, ages.age FROM users JOIN orders ON users.id = orders.user_id JOIN ages ON users.id = ages.user_id")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		// Verify column values: alice has 2 orders (100, 200), both with age 30
		// carol has 1 order (50) with age 35
		aliceCount := 0
		carolCount := 0
		for _, row := range result.Rows {
			if row[0].Text == "alice" {
				aliceCount++
				if row[2].Int != 30 {
					t.Errorf("alice age = %d, want 30", row[2].Int)
				}
			}
			if row[0].Text == "carol" {
				carolCount++
				if row[2].Int != 35 {
					t.Errorf("carol age = %d, want 35", row[2].Int)
				}
			}
		}
		if aliceCount != 2 {
			t.Errorf("alice rows = %d, want 2", aliceCount)
		}
		if carolCount != 1 {
			t.Errorf("carol rows = %d, want 1", carolCount)
		}
	})


	t.Run("triple_join_middle_table_on", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE IF NOT EXISTS ages (user_id INT, age INT)")
		env.execSQL(t, "DELETE FROM ages")
		env.execSQL(t, "INSERT INTO ages VALUES (1, 30)")
		env.execSQL(t, "INSERT INTO ages VALUES (2, 25)")
		env.execSQL(t, "INSERT INTO ages VALUES (3, 35)")
		// B1: second ON references middle table (orders), not leftmost (users)
		result := env.execSQL(t, "SELECT users.name, orders.amount, ages.age FROM users JOIN orders ON users.id = orders.user_id JOIN ages ON orders.user_id = ages.user_id")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
	})

		t.Run("join_order_by", func(t *testing.T) {
		result := env.execSQL(t, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.name")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		if result.Rows[0][0].Text != "alice" {
			t.Errorf("row[0].name = %q, want alice", result.Rows[0][0].Text)
		}
		if result.Rows[1][0].Text != "alice" {
			t.Errorf("row[1].name = %q, want alice", result.Rows[1][0].Text)
		}
		if result.Rows[2][0].Text != "carol" {
			t.Errorf("row[2].name = %q, want carol", result.Rows[2][0].Text)
		}
	})

	t.Run("join_limit", func(t *testing.T) {
		result := env.execSQL(t, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id LIMIT 2")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
	})

	t.Run("join_group_by", func(t *testing.T) {
		// GROUP BY on JOIN: users.name grouped by, COUNT(*) per group
		// alice has 2 orders, carol has 1, bob has 0 (excluded from inner join)
		result := env.execSQL(t, "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		// Results should be sorted by name: alice first
		if result.Rows[0][0].Text != "alice" {
			t.Errorf("row[0].name = %q, want alice", result.Rows[0][0].Text)
		}
		if result.Rows[0][1].Int != 2 {
			t.Errorf("alice count = %d, want 2", result.Rows[0][1].Int)
		}
	})

	t.Run("join_where_scalar_subquery", func(t *testing.T) {
		// B3-1: WHERE subquery in JOIN query should be precomputed
		// Subquery (SELECT MIN(id) FROM users) returns 1, so id > 1 excludes alice
		result := env.execSQL(t, "SELECT * FROM users WHERE id > (SELECT MIN(id) FROM users)")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		// Should return bob (id=2) and carol (id=3)
		for _, row := range result.Rows {
			if row[0].Int == 1 {
				t.Errorf("alice (id=1) should NOT be in results")
			}
		}
	})

}

// ─── GROUP BY Tests ──────────────────────────────────────────────────


// ─── GROUP BY Tests ──────────────────────────────────────────────────

func TestExec_GroupBy(t *testing.T) {
	t.Run("basic_group_by", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.store.Close()
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 50)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 75)")
		result := env.execSQL(t, "SELECT user_id, COUNT(*), SUM(amount) FROM orders GROUP BY user_id")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		for _, row := range result.Rows {
			if row[0].Int == 1 {
				if row[1].Int != 2 {
					t.Errorf("COUNT(*) for user 1 = %d, want 2", row[1].Int)
				}
				if row[2].Int != 300 {
					t.Errorf("SUM(amount) for user 1 = %d, want 300", row[2].Int)
				}
			}
		}
	})

	t.Run("group_by_with_where", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.store.Close()
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 50)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 75)")
		result := env.execSQL(t, "SELECT user_id, COUNT(*) FROM orders WHERE amount > 50 GROUP BY user_id")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		for _, row := range result.Rows {
			if row[0].Int == 1 && row[1].Int != 2 {
				t.Errorf("COUNT(*) for user 1 = %d, want 2", row[1].Int)
			}
			if row[0].Int == 2 && row[1].Int != 1 {
				t.Errorf("COUNT(*) for user 2 = %d, want 1", row[1].Int)
			}
		}
	})

	t.Run("group_by_single_group", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.store.Close()
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 50)")
		result := env.execSQL(t, "SELECT COUNT(*), SUM(amount) FROM orders GROUP BY user_id")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 3 {
			t.Errorf("COUNT(*) = %d, want 3", result.Rows[0][0].Int)
		}
		if result.Rows[0][1].Int != 350 {
			t.Errorf("SUM(amount) = %d, want 350", result.Rows[0][1].Int)
		}
	})

	t.Run("avg_min_max", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.store.Close()
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 50)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 75)")
		result := env.execSQL(t, "SELECT user_id, AVG(amount), MIN(amount), MAX(amount) FROM orders GROUP BY user_id")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		for _, row := range result.Rows {
			if row[0].Int == 1 {
				if row[1].Float != 150.0 {
					t.Errorf("AVG(amount) for user 1 = %v, want 150.0", row[1].Float)
				}
				if row[2].Int != 100 {
					t.Errorf("MIN(amount) for user 1 = %d, want 100", row[2].Int)
				}
				if row[3].Int != 200 {
					t.Errorf("MAX(amount) for user 1 = %d, want 200", row[3].Int)
				}
			}
		}
	})

	t.Run("coalesce_in_group_by", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.store.Close()
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, NULL)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, NULL)")
		env.execSQL(t, "INSERT INTO orders VALUES (3, 50)")

		// Test: SELECT COALESCE(amount, 0) FROM orders GROUP BY amount
		// Groups: {100}, {200}, {NULL (coalesced to 0)}, {50} = 4 groups
		result := env.execSQL(t, "SELECT COALESCE(amount, 0) FROM orders GROUP BY amount")

		if len(result.Rows) != 4 {
			t.Fatalf("rows = %d, want 4", len(result.Rows))
		}

		// Find the coalesced NULL row (should be 0)
		foundZero := false
		for _, row := range result.Rows {
			if row[0].Int == 0 {
				foundZero = true
				break
			}
		}
		if !foundZero {
			t.Error("expected a row with COALESCE(amount, 0) = 0 for NULL amounts")
		}
	})

	t.Run("coalesce_in_group_by_with_aggregate", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.store.Close()
		env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, NULL)")
		env.execSQL(t, "INSERT INTO orders VALUES (2, NULL)")

		// Test: SELECT user_id, COALESCE(amount, 0), COUNT(*) FROM orders GROUP BY user_id, amount
		result := env.execSQL(t, "SELECT user_id, COALESCE(amount, 0), COUNT(*) FROM orders GROUP BY user_id, amount")

		// Should have 3 groups: (1, 100, 1), (1, 0, 1), (2, 0, 1)
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
	})
}

func TestExec_ScalarAggregate(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT, salary INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 200)")

	t.Run("count_in_scalar_subquery", func(t *testing.T) {
		// COUNT(*) = 2, so id > 2 returns no rows
		result := env.execSQL(t, "SELECT * FROM users WHERE id > (SELECT COUNT(*) FROM users)")
		if len(result.Rows) != 0 {
			t.Fatalf("rows = %d, want 0", len(result.Rows))
		}
	})

	t.Run("max_in_scalar_subquery", func(t *testing.T) {
		// MAX(salary) = 200, so salary < 200 returns user with salary 100
		result := env.execSQL(t, "SELECT * FROM users WHERE salary < (SELECT MAX(salary) FROM users)")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][1].Int != 100 {
			t.Errorf("salary = %d, want 100", result.Rows[0][1].Int)
		}
	})

}

func TestExec_SelectWithoutFrom(t *testing.T) {
	env := newTestEnv(t)

	t.Run("select_integer", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
	})

	t.Run("select_string", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 'hello'")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
	})

	t.Run("select_arithmetic_add", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1+1")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 2 {
			t.Errorf("SELECT 1+1 = %d, want 2", result.Rows[0][0].Int)
		}
	})

	t.Run("select_arithmetic_sub", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 5-3")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 2 {
			t.Errorf("SELECT 5-3 = %d, want 2", result.Rows[0][0].Int)
		}
	})

	t.Run("select_arithmetic_from_table", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
		env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
		result := env.execSQL(t, "SELECT 1+1, name FROM users")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		// First column: 1+1 = 2
		if result.Rows[0][0].Int != 2 {
			t.Errorf("SELECT 1+1, name = %d, want 2", result.Rows[0][0].Int)
		}
		// Second column: name = 'Alice'
		if result.Rows[0][1].Text != "Alice" {
			t.Errorf("name = %q, want 'Alice'", result.Rows[0][1].Text)
		}
	})
}

func TestExec_Distinct(t *testing.T) {
	env := newTestEnv(t)

	t.Run("distinct_removes_duplicates", func(t *testing.T) {
		env.execSQL(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
		// Insert duplicate names
		env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
		env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
		env.execSQL(t, "INSERT INTO users VALUES (3, 'Alice', 35)")
		env.execSQL(t, "INSERT INTO users VALUES (4, 'Bob', 40)")
		env.execSQL(t, "INSERT INTO users VALUES (5, 'Charlie', 28)")

		// DISTINCT on name should return 3 unique names
		result := env.execSQL(t, "SELECT DISTINCT name FROM users")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3 distinct names", len(result.Rows))
		}
		// Verify the names are unique
		names := make(map[string]bool)
		for _, row := range result.Rows {
			if names[row[0].Text] {
				t.Errorf("duplicate name found: %s", row[0].Text)
			}
			names[row[0].Text] = true
		}
	})

	t.Run("distinct_with_where", func(t *testing.T) {
		env := newTestEnv(t)
		env.execSQL(t, "CREATE TABLE products (id INT, category TEXT, name TEXT)")
		env.execSQL(t, "INSERT INTO products VALUES (1, 'fruit', 'apple')")
		env.execSQL(t, "INSERT INTO products VALUES (2, 'fruit', 'banana')")
		env.execSQL(t, "INSERT INTO products VALUES (3, 'vegetable', 'carrot')")
		env.execSQL(t, "INSERT INTO products VALUES (4, 'fruit', 'apple')")
		env.execSQL(t, "INSERT INTO products VALUES (5, 'vegetable', 'carrot')")

		// DISTINCT with WHERE: only fruits
		result := env.execSQL(t, "SELECT DISTINCT category FROM products WHERE category = 'fruit'")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 distinct category", len(result.Rows))
		}
		if result.Rows[0][0].Text != "fruit" {
			t.Errorf("category = %q, want fruit", result.Rows[0][0].Text)
		}
	})

	t.Run("distinct_no_duplicates", func(t *testing.T) {
		env := newTestEnv(t)
		env.execSQL(t, "CREATE TABLE t (a INT, b TEXT)")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'x')")
		env.execSQL(t, "INSERT INTO t VALUES (2, 'y')")
		env.execSQL(t, "INSERT INTO t VALUES (3, 'z')")

		// No duplicates — should return all rows
		result := env.execSQL(t, "SELECT DISTINCT a FROM t")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
	})

	t.Run("distinct_multiple_columns", func(t *testing.T) {
		env := newTestEnv(t)
		env.execSQL(t, "CREATE TABLE orders (id INT, product TEXT, color TEXT)")
		env.execSQL(t, "INSERT INTO orders VALUES (1, 'widget', 'red')")
		env.execSQL(t, "INSERT INTO orders VALUES (2, 'widget', 'red')")
		env.execSQL(t, "INSERT INTO orders VALUES (3, 'widget', 'blue')")
		env.execSQL(t, "INSERT INTO orders VALUES (4, 'gadget', 'red')")

		// DISTINCT on (product, color) — should return 3 unique pairs
		result := env.execSQL(t, "SELECT DISTINCT product, color FROM orders")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3 distinct (product,color) pairs", len(result.Rows))
		}
	})

	t.Run("distinct_all_columns", func(t *testing.T) {
		env := newTestEnv(t)
		env.execSQL(t, "CREATE TABLE t (a INT, b TEXT, c INT)")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'x', 10)")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'x', 10)")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'x', 20)")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'y', 10)")

		// SELECT DISTINCT * — all columns
		result := env.execSQL(t, "SELECT DISTINCT * FROM t")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3 distinct rows", len(result.Rows))
		}
	})
}

func TestGroupByJoinOrderBy(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()
	
	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'carol')")
	
	env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
	env.execSQL(t, "INSERT INTO orders VALUES (3, 50)")
	
	// Test GROUP BY + ORDER BY name DESC
	// Should be: carol (c > a), alice (a < c) - DESC by name
	result := env.execSQL(t, "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY users.name DESC")
	
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	if result.Rows[0][0].Text != "carol" {
		t.Errorf("row[0] = %q, want carol", result.Rows[0][0].Text)
	}
	if result.Rows[0][1].Int != 1 {
		t.Errorf("carol count = %d, want 1", result.Rows[0][1].Int)
	}
	if result.Rows[1][0].Text != "alice" {
		t.Errorf("row[1] = %q, want alice", result.Rows[1][0].Text)
	}
	if result.Rows[1][1].Int != 2 {
		t.Errorf("alice count = %d, want 2", result.Rows[1][1].Int)
	}
}

func TestExec_Coalesce(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, discount INT)")

	t.Run("coalesce_with_null", func(t *testing.T) {
		env.execSQL(t, "INSERT INTO products VALUES (1, 'Apple', NULL, 10)")
		env.execSQL(t, "INSERT INTO products VALUES (2, 'Banana', 30, 5)")

		// COALESCE(col, 'default') returns 'default' for NULL values
		result := env.execSQL(t, "SELECT name, COALESCE(price, 0) FROM products ORDER BY id")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		// Apple has NULL price, should get 0
		if result.Rows[0][1].Int != 0 {
			t.Errorf("Apple price = %d, want 0", result.Rows[0][1].Int)
		}
		// Banana has price 30, should get 30
		if result.Rows[1][1].Int != 30 {
			t.Errorf("Banana price = %d, want 30", result.Rows[1][1].Int)
		}
	})

	t.Run("coalesce_multiple_nulls", func(t *testing.T) {
		// SELECT COALESCE(NULL, NULL, 'third') returns 'third'
		result := env.execSQL(t, "SELECT COALESCE(NULL, NULL, 'third')")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Text != "third" {
			t.Errorf("result = %q, want 'third'", result.Rows[0][0].Text)
		}
	})

	t.Run("coalesce_with_column", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM products")
		env.execSQL(t, "INSERT INTO products VALUES (1, 'Apple', NULL, 10)")
		env.execSQL(t, "INSERT INTO products VALUES (2, 'Banana', 30, 5)")

		// SELECT COALESCE(price, discount) uses discount when price is NULL
		result := env.execSQL(t, "SELECT COALESCE(price, discount) FROM products ORDER BY id")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
		// Apple has NULL price, should get discount 10
		if result.Rows[0][0].Int != 10 {
			t.Errorf("Apple coalesce = %d, want 10", result.Rows[0][0].Int)
		}
		// Banana has price 30, should get 30 (not discount 5)
		if result.Rows[1][0].Int != 30 {
			t.Errorf("Banana coalesce = %d, want 30", result.Rows[1][0].Int)
		}
	})

	t.Run("coalesce_all_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM products")
		env.execSQL(t, "INSERT INTO products VALUES (1, 'Apple', NULL, NULL)")

		// COALESCE(NULL, NULL) returns NULL
		result := env.execSQL(t, "SELECT COALESCE(price, discount) FROM products")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if !result.Rows[0][0].IsNull {
			t.Errorf("result.IsNull = false, want true")
		}
	})

	t.Run("coalesce_first_non_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM products")
		env.execSQL(t, "INSERT INTO products VALUES (1, 'First', 100, 200)")

		// First non-NULL value
		result := env.execSQL(t, "SELECT COALESCE(NULL, 100, 200, 300)")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 100 {
			t.Errorf("result = %d, want 100", result.Rows[0][0].Int)
		}
	})
}

func TestExec_CoalesceGroupByJoin(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'carol')")

	env.execSQL(t, "CREATE TABLE orders (user_id INT, amount INT)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, NULL)")
	env.execSQL(t, "INSERT INTO orders VALUES (3, 50)")
	env.execSQL(t, "INSERT INTO orders VALUES (3, NULL)")

	// Test COALESCE in GROUP BY with JOIN
	// alice has orders: 100, 200, NULL -> GROUP BY picks one value
	// carol has orders: 50, NULL -> GROUP BY picks 50
	// bob has no orders -> won't appear in JOIN result
	result := env.execSQL(t, "SELECT users.name, COALESCE(orders.amount, 0) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name")

	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2 (alice and carol)", len(result.Rows))
	}

	// Find alice and carol rows
	var aliceIdx, carolIdx int = -1, -1
	for i, row := range result.Rows {
		if row[0].Text == "alice" {
			aliceIdx = i
		} else if row[0].Text == "carol" {
			carolIdx = i
		}
	}

	if aliceIdx == -1 {
		t.Fatal("alice row not found")
	}
	if carolIdx == -1 {
		t.Fatal("carol row not found")
	}

	aliceRow := result.Rows[aliceIdx]
	carolRow := result.Rows[carolIdx]

	// Verify coalesced values are not NULL
	if aliceRow[1].IsNull {
		t.Error("alice COALESCE(amount, 0) is NULL, expected non-NULL")
	}
	if carolRow[1].IsNull {
		t.Error("carol COALESCE(amount, 0) is NULL, expected non-NULL")
	}

	// Verify carol's coalesced amount is 50 (her non-NULL order)
	if carolRow[1].Int != 50 {
		t.Errorf("carol amount = %d, want 50", carolRow[1].Int)
	}
}

// ─── INTERSECT / EXCEPT Tests ──────────────────────────────────────

func TestIntersect_Basic(t *testing.T) {
	env := newTestEnv(t)
	result := env.execSQL(t, "SELECT 1 INTERSECT SELECT 1")
	if len(result.Rows) != 1 {
		t.Errorf("SELECT 1 INTERSECT SELECT 1: expected 1 row, got %d", len(result.Rows))
	}
}

func TestIntersect_NoMatch(t *testing.T) {
	env := newTestEnv(t)
	result := env.execSQL(t, "SELECT 1 INTERSECT SELECT 2")
	if len(result.Rows) != 0 {
		t.Errorf("SELECT 1 INTERSECT SELECT 2: expected 0 rows, got %d", len(result.Rows))
	}
}

func TestIntersect_Dedup(t *testing.T) {
	env := newTestEnv(t)
	// INTERSECT with duplicates should dedup
	result := env.execSQL(t, "SELECT 1 INTERSECT SELECT 1 INTERSECT SELECT 1")
	if len(result.Rows) != 1 {
		t.Errorf("INTERSECT dedup: expected 1 row, got %d", len(result.Rows))
	}
}

func TestExcept_Basic(t *testing.T) {
	env := newTestEnv(t)
	result := env.execSQL(t, "SELECT 1 EXCEPT SELECT 2")
	if len(result.Rows) != 1 {
		t.Errorf("SELECT 1 EXCEPT SELECT 2: expected 1 row, got %d", len(result.Rows))
	}
}

func TestExcept_ReturnsEmpty(t *testing.T) {
	env := newTestEnv(t)
	result := env.execSQL(t, "SELECT 1 EXCEPT SELECT 1")
	if len(result.Rows) != 0 {
		t.Errorf("SELECT 1 EXCEPT SELECT 1: expected 0 rows, got %d", len(result.Rows))
	}
}

func TestExcept_Dedup(t *testing.T) {
	env := newTestEnv(t)
	result := env.execSQL(t, "SELECT 1 EXCEPT SELECT 999")
	if len(result.Rows) != 1 {
		t.Errorf("SELECT 1 EXCEPT SELECT 999: expected 1 row, got %d", len(result.Rows))
	}
}

func TestUnionIntersectCombination(t *testing.T) {
	env := newTestEnv(t)
	// SELECT 1 UNION SELECT 2 INTERSECT SELECT 2
	// With right-associativity: SELECT 1 UNION (SELECT 2 INTERSECT SELECT 2)
	// = {1} UNION {2} = {1, 2} with dedup = 2 rows
	result := env.execSQL(t, "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2")
	if len(result.Rows) != 2 {
		t.Errorf("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2: got %d rows (right-assoc), expected 2", len(result.Rows))
	}
}

func TestUnionExceptCombination(t *testing.T) {
	env := newTestEnv(t)
	// SELECT 1 UNION SELECT 2 EXCEPT SELECT 1
	// With right-associativity: SELECT 1 UNION (SELECT 2 EXCEPT SELECT 1)
	// = {1} UNION {2} = {1, 2} with dedup = 2 rows
	result := env.execSQL(t, "SELECT 1 UNION SELECT 2 EXCEPT SELECT 1")
	if len(result.Rows) != 2 {
		t.Errorf("SELECT 1 UNION SELECT 2 EXCEPT SELECT 1: got %d rows (right-assoc), expected 2", len(result.Rows))
	}
}

func TestIntersectExcept_NullHandling(t *testing.T) {
	env := newTestEnv(t)
	// Two NULLs should be equal in SQL set operations
	result := env.execSQL(t, "SELECT NULL INTERSECT SELECT NULL")
	if len(result.Rows) != 1 {
		t.Errorf("SELECT NULL INTERSECT SELECT NULL: expected 1 row (NULLs equal), got %d", len(result.Rows))
	}
}

func TestExec_InsertSelect(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)")
	env.execSQL(t, "CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)")

	env.execSQL(t, "INSERT INTO t1 VALUES (1, 'Alice')")
	env.execSQL(t, "INSERT INTO t1 VALUES (2, 'Bob')")
	env.execSQL(t, "INSERT INTO t1 VALUES (3, 'Charlie')")

	result := env.execSQL(t, "INSERT INTO t2 SELECT * FROM t1")
	if result.RowsAffected != 3 {
		t.Errorf("RowsAffected = %d, want 3", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM t2 ORDER BY id")
	if len(sel.Rows) != 3 {
		t.Fatalf("t2 rows = %d, want 3", len(sel.Rows))
	}
	if sel.Rows[0][0].Int != 1 || sel.Rows[0][1].Text != "Alice" {
		t.Errorf("row[0] = (%v, %v), want (1, Alice)", sel.Rows[0][0], sel.Rows[0][1])
	}
	if sel.Rows[1][0].Int != 2 || sel.Rows[1][1].Text != "Bob" {
		t.Errorf("row[1] = (%v, %v), want (2, Bob)", sel.Rows[1][0], sel.Rows[1][1])
	}
	if sel.Rows[2][0].Int != 3 || sel.Rows[2][1].Text != "Charlie" {
		t.Errorf("row[2] = (%v, %v), want (3, Charlie)", sel.Rows[2][0], sel.Rows[2][1])
	}
}

func TestExec_InsertSelectWithColumns(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE src (a INT, b TEXT, c INT)")
	env.execSQL(t, "CREATE TABLE dst (id INT, label TEXT)")

	env.execSQL(t, "INSERT INTO src VALUES (10, 'x', 100)")
	env.execSQL(t, "INSERT INTO src VALUES (20, 'y', 200)")

	result := env.execSQL(t, "INSERT INTO dst (id, label) SELECT a, b FROM src")
	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM dst ORDER BY id")
	if len(sel.Rows) != 2 {
		t.Fatalf("dst rows = %d, want 2", len(sel.Rows))
	}
	if sel.Rows[0][0].Int != 10 || sel.Rows[0][1].Text != "x" {
		t.Errorf("row[0] = (%v, %v), want (10, x)", sel.Rows[0][0], sel.Rows[0][1])
	}
	if sel.Rows[1][0].Int != 20 || sel.Rows[1][1].Text != "y" {
		t.Errorf("row[1] = (%v, %v), want (20, y)", sel.Rows[1][0], sel.Rows[1][1])
	}
}

func TestExec_InsertSelectWithIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE src (id INT, val INT)")
	env.execSQL(t, "CREATE TABLE dst (id INT PRIMARY KEY, val INT)")

	env.execSQL(t, "INSERT INTO src VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO src VALUES (2, 200)")

	result := env.execSQL(t, "INSERT INTO dst SELECT * FROM src")
	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}

	sel := env.execSQL(t, "SELECT * FROM dst ORDER BY id")
	if len(sel.Rows) != 2 {
		t.Fatalf("dst rows = %d, want 2", len(sel.Rows))
	}

	idx := env.execSQL(t, "SELECT * FROM dst WHERE id = 1")
	if len(idx.Rows) != 1 || idx.Rows[0][1].Int != 100 {
		t.Errorf("index lookup: got %v, want (1, 100)", idx.Rows[0])
	}
}

func TestExec_DerivedTable(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie')")

	t.Run("basic_derived_table", func(t *testing.T) {
		// SELECT * FROM (SELECT id, name FROM users) AS u
		result := env.execSQL(t, "SELECT * FROM (SELECT id, name FROM users) AS u")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		if len(result.Columns) != 2 {
			t.Errorf("columns count = %d, want 2", len(result.Columns))
		}
	})

	t.Run("derived_table_with_where", func(t *testing.T) {
		// The acceptance test case: SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id = 1
		result := env.execSQL(t, "SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id = 1")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 1 {
			t.Errorf("id = %d, want 1", result.Rows[0][0].Int)
		}
		if result.Rows[0][1].Text != "Alice" {
			t.Errorf("name = %q, want Alice", result.Rows[0][1].Text)
		}
	})

	t.Run("derived_table_with_alias_columns", func(t *testing.T) {
		// SELECT alias.column FROM (SELECT col AS alias FROM ...) AS t
		result := env.execSQL(t, "SELECT u.id, u.uname FROM (SELECT id, name AS uname FROM users) AS u WHERE u.id = 2")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 2 {
			t.Errorf("id = %d, want 2", result.Rows[0][0].Int)
		}
		if result.Rows[0][1].Text != "Bob" {
			t.Errorf("uname = %q, want Bob", result.Rows[0][1].Text)
		}
	})

	t.Run("derived_table_projection", func(t *testing.T) {
		// Only select specific columns from derived table
		result := env.execSQL(t, "SELECT u.name FROM (SELECT id, name FROM users) AS u")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		if len(result.Columns) != 1 {
			t.Errorf("columns = %d, want 1", len(result.Columns))
		}
	})

	t.Run("derived_table_limit", func(t *testing.T) {
		result := env.execSQL(t, "SELECT * FROM (SELECT id, name FROM users) AS u LIMIT 2")
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}
	})

	t.Run("derived_table_order_by", func(t *testing.T) {
		result := env.execSQL(t, "SELECT * FROM (SELECT id, name FROM users) AS u ORDER BY name DESC")
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}
		if result.Rows[0][1].Text != "Charlie" {
			t.Errorf("first row name = %q, want Charlie (descending)", result.Rows[0][1].Text)
		}
	})

	t.Run("derived_table_with_aggregate", func(t *testing.T) {
		result := env.execSQL(t, "SELECT COUNT(*) FROM (SELECT id, name FROM users) AS u")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 3 {
			t.Errorf("COUNT(*) = %d, want 3", result.Rows[0][0].Int)
		}
	})

	t.Run("nested_subquery_in_derived", func(t *testing.T) {
		// Derived table containing a subquery
		result := env.execSQL(t, "SELECT * FROM (SELECT id FROM users WHERE id IN (SELECT id FROM users)) AS u WHERE u.id = 1")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 1 {
			t.Errorf("id = %d, want 1", result.Rows[0][0].Int)
		}
	})
}

func TestDerivedTableColumnNamesLowercase(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT, name TEXT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob')")
	
	// Bug fix verification: column names must be lowercase
	result := env.execSQL(t, "SELECT * FROM (SELECT id, name FROM users) AS u")
	
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	
	if result.Columns[0] != "id" {
		t.Errorf("Column 0: got %q, want 'id'", result.Columns[0])
	}
	if result.Columns[1] != "name" {
		t.Errorf("Column 1: got %q, want 'name'", result.Columns[1])
	}
	
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestExec_IndexNestedLoopJoin(t *testing.T) {
	env := newTestEnv(t)

	// Setup tables
	env.execSQL(t, "CREATE TABLE customers (id INT, name TEXT)")
	env.execSQL(t, "CREATE TABLE orders (customer_id INT, amount INT)")
	env.execSQL(t, "CREATE INDEX idx_orders_customer ON orders (customer_id)")

	// Insert data
	env.execSQL(t, "INSERT INTO customers VALUES (1, 'Alice')")
	env.execSQL(t, "INSERT INTO customers VALUES (2, 'Bob')")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO orders VALUES (1, 200)")
	env.execSQL(t, "INSERT INTO orders VALUES (2, 50)")

	t.Run("basic_index_nested_loop_join", func(t *testing.T) {
		// Query: SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id
		result := env.execSQL(t, "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id")

		// Expected: Alice 100, Alice 200, Bob 50 (3 rows)
		if len(result.Rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(result.Rows))
		}

		// Verify results
		results := make(map[string][]int64)
		for _, row := range result.Rows {
			name := row[0].Text
			amount := row[1].Int
			results[name] = append(results[name], amount)
		}

		aliceAmounts := results["Alice"]
		if len(aliceAmounts) != 2 {
			t.Errorf("Alice should have 2 orders, got %d", len(aliceAmounts))
		}
		// Check Alice's amounts (order may vary)
		hasAlice100 := false
		hasAlice200 := false
		for _, amt := range aliceAmounts {
			if amt == 100 {
				hasAlice100 = true
			}
			if amt == 200 {
				hasAlice200 = true
			}
		}
		if !hasAlice100 || !hasAlice200 {
			t.Errorf("Alice's amounts = %v, want [100, 200]", aliceAmounts)
		}

		bobAmounts := results["Bob"]
		if len(bobAmounts) != 1 {
			t.Errorf("Bob should have 1 order, got %d", len(bobAmounts))
		}
		if bobAmounts[0] != 50 {
			t.Errorf("Bob's amount = %d, want 50", bobAmounts[0])
		}
	})

	t.Run("index_nested_loop_join_with_where", func(t *testing.T) {
		// Filter on outer table first - should still use index on inner
		result := env.execSQL(t, "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id WHERE customers.id = 1")

		// Only Alice (id=1) should appear with her 2 orders
		if len(result.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(result.Rows))
		}

		for _, row := range result.Rows {
			if row[0].Text != "Alice" {
				t.Errorf("expected Alice, got %s", row[0].Text)
			}
		}
	})

	t.Run("index_nested_loop_left_join", func(t *testing.T) {
		// Add a customer with no orders
		env.execSQL(t, "INSERT INTO customers VALUES (3, 'Carol')")

		// LEFT JOIN: Carol should appear with NULL for amount
		result := env.execSQL(t, "SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id WHERE customers.id = 3")

		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}

		if result.Rows[0][0].Text != "Carol" {
			t.Errorf("expected Carol, got %s", result.Rows[0][0].Text)
		}
		if !result.Rows[0][1].IsNull {
			t.Errorf("Carol's amount should be NULL, got %v", result.Rows[0][1])
		}
	})
}


func TestExec_UpdateWithExpressionIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
	env.execSQL(t, "CREATE INDEX idx_lower_email ON users (LOWER(email))")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Test@Example.COM')")

	// Verify LOWER expression works in SELECT
	r := env.execSQL(t, "SELECT id FROM users WHERE LOWER(email) = 'test@example.com'")
	if len(r.Rows) != 1 {
		t.Errorf("expected 1 row before update, got %d", len(r.Rows))
	}

	// UPDATE - should update both table and expression index
	env.execSQL(t, "UPDATE users SET email = 'New@Example.com' WHERE id = 1")

	// Query using expression index - should find the updated row
	r = env.execSQL(t, "SELECT id FROM users WHERE LOWER(email) = 'new@example.com'")
	if len(r.Rows) != 1 {
		t.Errorf("expected 1 row after update, got %d", len(r.Rows))
	}

	// Verify old value is gone
	r = env.execSQL(t, "SELECT id FROM users WHERE LOWER(email) = 'test@example.com'")
	if len(r.Rows) != 0 {
		t.Errorf("expected 0 rows for old value, got %d", len(r.Rows))
	}
}
