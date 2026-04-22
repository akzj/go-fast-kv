package executor_test

import (
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

func setupExplainTest(t *testing.T) (parserapi.Parser, plannerapi.Planner, executorapi.Executor, catalogapi.CatalogManager) {
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
	return p, pl, exec, cat
}

func TestExplainBasic(t *testing.T) {
	p, pl, ex, _ := setupExplainTest(t)

	// Create test table
	_, err := ex.Execute(&plannerapi.CreateTablePlan{
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
		t.Fatalf("create table: %v", err)
	}

	tests := []struct {
		name  string
		query string
	}{
		{"EXPLAIN SELECT", "EXPLAIN SELECT * FROM users"},
		{"EXPLAIN INSERT", "EXPLAIN INSERT INTO users VALUES (1, 'Alice')"},
		{"EXPLAIN UPDATE", "EXPLAIN UPDATE users SET name = 'Bob' WHERE id = 1"},
		{"EXPLAIN DELETE", "EXPLAIN DELETE FROM users WHERE id = 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}

			plan, err := pl.Plan(stmt)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}

			result, err := ex.Execute(plan)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result.Rows))
			}
			if len(result.Columns) != 1 || result.Columns[0] != "QUERY PLAN" {
				t.Fatalf("unexpected columns: %v", result.Columns)
			}
			if result.Rows[0][0].Text == "" {
				t.Fatal("expected non-empty plan text")
			}
		})
	}
}

func TestExplainAnalyzeSelect(t *testing.T) {
	p, pl, ex, cat := setupExplainTest(t)

	// Create test table
	_, err := ex.Execute(&plannerapi.CreateTablePlan{
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
		t.Fatalf("create table: %v", err)
	}

	// Get table with TableID for insert
	tbl, err := cat.GetTable("users")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}

	// Insert test data using the table with TableID
	_, err = ex.Execute(&plannerapi.InsertPlan{
		Table: tbl,
		Rows: [][]catalogapi.Value{
			{{Type: catalogapi.TypeInt, Int: 1}, {Type: catalogapi.TypeText, Text: "Alice"}},
			{{Type: catalogapi.TypeInt, Int: 2}, {Type: catalogapi.TypeText, Text: "Bob"}},
		},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// EXPLAIN ANALYZE a SELECT
	stmt, err := p.Parse("EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := pl.Plan(stmt)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	result, err := ex.Execute(plan)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Check that result contains timing info
	planText := result.Rows[0][0].Text
	if planText == "" {
		t.Fatal("expected non-empty plan text")
	}
	hasTime := contains(planText, "actual time=")
	hasAnalyze := contains(planText, "EXPLAIN ANALYZE")
	if !hasTime || !hasAnalyze {
		t.Fatalf("expected 'actual time=' and 'EXPLAIN ANALYZE' in output, got: %s", planText)
	}
}

func TestExplainAnalyzeDML(t *testing.T) {
	p, pl, ex, _ := setupExplainTest(t)

	// Create test table
	_, err := ex.Execute(&plannerapi.CreateTablePlan{
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
		t.Fatalf("create table: %v", err)
	}

	// EXPLAIN ANALYZE an INSERT
	t.Run("INSERT", func(t *testing.T) {
		stmt, err := p.Parse("EXPLAIN ANALYZE INSERT INTO users VALUES (1, 'Alice')")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		plan, err := pl.Plan(stmt)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}

		result, err := ex.Execute(plan)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}

		planText := result.Rows[0][0].Text
		if !contains(planText, "rows affected=") {
			t.Fatalf("expected 'rows affected=' in output, got: %s", planText)
		}
		if !contains(planText, "actual time=") {
			t.Fatalf("expected 'actual time=' in output, got: %s", planText)
		}
	})

	// EXPLAIN ANALYZE a DELETE
	t.Run("DELETE", func(t *testing.T) {
		stmt, err := p.Parse("EXPLAIN ANALYZE DELETE FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		plan, err := pl.Plan(stmt)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}

		result, err := ex.Execute(plan)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}

		planText := result.Rows[0][0].Text
		if !contains(planText, "rows affected=") {
			t.Fatalf("expected 'rows affected=' in output, got: %s", planText)
		}
	})
}

// contains is a simple string contains helper
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
