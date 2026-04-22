package internal

import (
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	catalog "github.com/akzj/go-fast-kv/internal/sql/catalog"
	engine "github.com/akzj/go-fast-kv/internal/sql/engine"
	parser "github.com/akzj/go-fast-kv/internal/sql/parser"
	planner "github.com/akzj/go-fast-kv/internal/sql/planner"
)

func newTestEnvCTE(t *testing.T) *testEnv {
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
	ex := New(store, cat, tbl, idx, nil, pl, p)

	return &testEnv{
		store:   store,
		cat:     cat,
		parser:  p,
		planner: pl,
		exec:    ex,
		enc:     enc,
	}
}

func execSQL(t *testing.T, env *testEnv, sql string) {
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	plan, err := env.planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan %q: %v", sql, err)
	}
	_, err = env.exec.Execute(plan)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func querySQL(t *testing.T, env *testEnv, sql string) ([][]catalogapi.Value, []string) {
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
	return result.Rows, result.Columns
}

// TestCTE_Simple tests simple CTE (WITH ... AS ...)
func TestCTE_Simple(t *testing.T) {
	env := newTestEnvCTE(t)

	execSQL(t, env, "CREATE TABLE numbers (id INT PRIMARY KEY, val INT)")
	execSQL(t, env, "INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)")

	// Simple CTE - filter from real table
	rows, cols := querySQL(t, env, "WITH temp AS (SELECT id, val FROM numbers WHERE id > 1) SELECT * FROM temp")

	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Check first value is 2 (id > 1, skipping row with id=1)
	v := rows[0][0].Int
	if v != 2 {
		t.Errorf("row 0, col 0: expected 2, got %v", v)
	}
	v2 := rows[1][0].Int
	if v2 != 3 {
		t.Errorf("row 1, col 0: expected 3, got %v", v2)
	}
}

// TestCTE_Multiple tests multiple CTEs
func TestCTE_Multiple(t *testing.T) {
	env := newTestEnvCTE(t)

	execSQL(t, env, "CREATE TABLE a (id INT PRIMARY KEY, val INT)")
	execSQL(t, env, "CREATE TABLE b (id INT PRIMARY KEY, val INT)")
	execSQL(t, env, "INSERT INTO a VALUES (1, 10), (2, 20)")
	execSQL(t, env, "INSERT INTO b VALUES (1, 100), (2, 200)")

	// Multiple CTEs with UNION ALL
	rows, _ := querySQL(t, env, "WITH cte_a AS (SELECT id, val FROM a), cte_b AS (SELECT id, val FROM b) SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b")

	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

// TestCTE_WithInsert tests CTE with INSERT
func TestCTE_WithInsert(t *testing.T) {
	env := newTestEnvCTE(t)

	execSQL(t, env, "CREATE TABLE source (id INT PRIMARY KEY, val INT)")
	execSQL(t, env, "CREATE TABLE target (id INT PRIMARY KEY, val INT)")
	execSQL(t, env, "INSERT INTO source VALUES (1, 10), (2, 20), (3, 30)")

	execSQL(t, env, "WITH cte AS (SELECT id, val FROM source WHERE id > 1) INSERT INTO target SELECT * FROM cte")

	rows, _ := querySQL(t, env, "SELECT * FROM target")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// TestCTE_Referenced tests CTE referenced in main query
func TestCTE_Referenced(t *testing.T) {
	env := newTestEnvCTE(t)

	execSQL(t, env, "CREATE TABLE numbers (id INT PRIMARY KEY, val INT)")
	execSQL(t, env, "INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)")

	// CTE used as a table in main query
	rows, _ := querySQL(t, env, "WITH temp AS (SELECT id, val FROM numbers WHERE id > 1) SELECT * FROM temp WHERE val > 15")

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// TestCTE_OrderBy tests CTE with ORDER BY clause
func TestCTE_OrderBy(t *testing.T) {
	env := newTestEnvCTE(t)

	execSQL(t, env, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	execSQL(t, env, "INSERT INTO users VALUES (3, 'Alice'), (1, 'Bob'), (2, 'Carol')")

	// CTE with ORDER BY DESC - results should be in descending order by id
	rows, _ := querySQL(t, env, `
		WITH cte AS (
			SELECT id, name FROM users ORDER BY id DESC
		)
		SELECT * FROM cte
	`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Expected order: Alice (3), Carol (2), Bob (1) - descending by id
	expected := []struct {
		id   int64
		name string
	}{
		{3, "Alice"},
		{2, "Carol"},
		{1, "Bob"},
	}

	for i, row := range rows {
		if row[0].Int != expected[i].id {
			t.Errorf("row %d: expected id=%d, got %v", i, expected[i].id, row[0].Int)
		}
		if row[1].Text != expected[i].name {
			t.Errorf("row %d: expected name=%s, got %s", i, expected[i].name, row[1].Text)
		}
	}
}
