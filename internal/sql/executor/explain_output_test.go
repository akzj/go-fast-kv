package executor_test

import (
	"fmt"
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	"github.com/akzj/go-fast-kv/internal/sql/engine"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	"github.com/akzj/go-fast-kv/internal/sql/executor"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// TestExplainOutputCapture runs EXPLAIN/EXPLAIN ANALYZE and captures actual output
// for documentation purposes. The output is printed to stdout.
func TestExplainOutputCapture(t *testing.T) {
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
	pl := planner.New(cat, p)
	exec := executor.New(store, cat, tbl, idx, nil, pl, p)

	// Create test tables
	setupTables(t, p, pl, exec, cat)

	// Test cases: (name, query, isAnalyze)
	testCases := []struct {
		name      string
		query     string
		isAnalyze bool
	}{
		// EXPLAIN tests
		{"EXPLAIN SELECT *", "EXPLAIN SELECT * FROM users", false},
		{"EXPLAIN SELECT WHERE", "EXPLAIN SELECT * FROM users WHERE id = 1", false},
		{"EXPLAIN JOIN", "EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id", false},
		{"EXPLAIN GROUP BY", "EXPLAIN SELECT department, COUNT(*) FROM employees GROUP BY department", false},
		{"EXPLAIN INSERT", "EXPLAIN INSERT INTO users VALUES (100, 'Test')", false},
		{"EXPLAIN UPDATE", "EXPLAIN UPDATE users SET name = 'Updated' WHERE id = 1", false},
		{"EXPLAIN DELETE", "EXPLAIN DELETE FROM users WHERE id = 1", false},

		// EXPLAIN ANALYZE tests
		{"EXPLAIN ANALYZE SELECT *", "EXPLAIN ANALYZE SELECT * FROM users", true},
		{"EXPLAIN ANALYZE SELECT WHERE", "EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1", true},
		{"EXPLAIN ANALYZE JOIN", "EXPLAIN ANALYZE SELECT * FROM users JOIN orders ON users.id = orders.user_id", true},
		{"EXPLAIN ANALYZE GROUP BY", "EXPLAIN ANALYZE SELECT department, COUNT(*) FROM employees GROUP BY department", true},
		{"EXPLAIN ANALYZE INSERT", "EXPLAIN ANALYZE INSERT INTO users VALUES (200, 'AnalyzeTest')", true},
		{"EXPLAIN ANALYZE UPDATE", "EXPLAIN ANALYZE UPDATE users SET name = 'Analyzed' WHERE id = 1", true},
		{"EXPLAIN ANALYZE DELETE", "EXPLAIN ANALYZE DELETE FROM users WHERE id = 1", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := p.Parse(tc.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}

			plan, err := pl.Plan(stmt)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}

			result, err := exec.Execute(plan)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}

			if len(result.Rows) < 1 {
				t.Fatal("expected at least 1 row")
			}

			planText := result.Rows[0][0].Text
			if planText == "" {
				t.Fatal("expected non-empty plan text")
			}

			// Print the output for documentation capture
			fmt.Printf("\n========== %s ==========\n", tc.name)
			fmt.Printf("Query: %s\n", tc.query)
			fmt.Printf("Output:\n%s\n", planText)
		})
	}
}

func setupTables(t *testing.T, p parserapi.Parser, pl plannerapi.Planner, exec executorapi.Executor, cat catalogapi.CatalogManager) {
	// Create users table
	_, err := exec.Execute(&plannerapi.CreateTablePlan{
		Schema: catalogapi.TableSchema{
			Name: "users",
			Columns: []catalogapi.ColumnDef{
				{Name: "id", Type: catalogapi.TypeInt},
				{Name: "name", Type: catalogapi.TypeText},
			},
			PrimaryKey: "id",
		},
	})
	if err != nil {
		t.Fatalf("create users table: %v", err)
	}

	// Create orders table
	_, err = exec.Execute(&plannerapi.CreateTablePlan{
		Schema: catalogapi.TableSchema{
			Name: "orders",
			Columns: []catalogapi.ColumnDef{
				{Name: "id", Type: catalogapi.TypeInt},
				{Name: "user_id", Type: catalogapi.TypeInt},
				{Name: "amount", Type: catalogapi.TypeInt},
			},
			PrimaryKey: "id",
		},
	})
	if err != nil {
		t.Fatalf("create orders table: %v", err)
	}

	// Create employees table for GROUP BY
	_, err = exec.Execute(&plannerapi.CreateTablePlan{
		Schema: catalogapi.TableSchema{
			Name: "employees",
			Columns: []catalogapi.ColumnDef{
				{Name: "id", Type: catalogapi.TypeInt},
				{Name: "department", Type: catalogapi.TypeText},
				{Name: "salary", Type: catalogapi.TypeInt},
			},
			PrimaryKey: "id",
		},
	})
	if err != nil {
		t.Fatalf("create employees table: %v", err)
	}

	// Insert test data
	testData := []struct {
		table  string
		rows   [][]catalogapi.Value
	}{
		{
			"users",
			[][]catalogapi.Value{
				{{Type: catalogapi.TypeInt, Int: 1}, {Type: catalogapi.TypeText, Text: "Alice"}},
				{{Type: catalogapi.TypeInt, Int: 2}, {Type: catalogapi.TypeText, Text: "Bob"}},
				{{Type: catalogapi.TypeInt, Int: 3}, {Type: catalogapi.TypeText, Text: "Carol"}},
			},
		},
		{
			"orders",
			[][]catalogapi.Value{
				{{Type: catalogapi.TypeInt, Int: 1}, {Type: catalogapi.TypeInt, Int: 1}, {Type: catalogapi.TypeInt, Int: 100}},
				{{Type: catalogapi.TypeInt, Int: 2}, {Type: catalogapi.TypeInt, Int: 1}, {Type: catalogapi.TypeInt, Int: 200}},
				{{Type: catalogapi.TypeInt, Int: 3}, {Type: catalogapi.TypeInt, Int: 3}, {Type: catalogapi.TypeInt, Int: 50}},
			},
		},
		{
			"employees",
			[][]catalogapi.Value{
				{{Type: catalogapi.TypeInt, Int: 1}, {Type: catalogapi.TypeText, Text: "Engineering"}, {Type: catalogapi.TypeInt, Int: 100000}},
				{{Type: catalogapi.TypeInt, Int: 2}, {Type: catalogapi.TypeText, Text: "Engineering"}, {Type: catalogapi.TypeInt, Int: 110000}},
				{{Type: catalogapi.TypeInt, Int: 3}, {Type: catalogapi.TypeText, Text: "Sales"}, {Type: catalogapi.TypeInt, Int: 90000}},
				{{Type: catalogapi.TypeInt, Int: 4}, {Type: catalogapi.TypeText, Text: "Sales"}, {Type: catalogapi.TypeInt, Int: 95000}},
			},
		},
	}

	for _, td := range testData {
		tbl, err := cat.GetTable(td.table)
		if err != nil {
			t.Fatalf("get table %s: %v", td.table, err)
		}
		_, err = exec.Execute(&plannerapi.InsertPlan{
			Table: tbl,
			Rows:  td.rows,
		})
		if err != nil {
			t.Fatalf("insert into %s: %v", td.table, err)
		}
	}
}
