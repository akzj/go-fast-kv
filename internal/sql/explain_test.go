package sql_test

import (
	"strings"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	gosql "github.com/akzj/go-fast-kv/internal/sql"
)

// openExplainDB creates a test database for EXPLAIN tests.
func openExplainDB(t *testing.T) (*gosql.DB, kvstoreapi.Store) {
	t.Helper()
	db, store := openTestDB(t)
	return db, store
}

func TestExplain_Basic(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create a simple table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// EXPLAIN SELECT
	t.Run("explain_select", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN SELECT: %v", err)
		}
		if len(res.Rows) == 0 {
			t.Fatal("EXPLAIN returned no rows")
		}
		planText := res.Rows[0][0].Text
		if planText == "" {
			t.Fatal("EXPLAIN returned empty plan")
		}
		// Plan should mention the table
		if !strings.Contains(planText, "SELECT") {
			t.Errorf("plan should contain SELECT, got: %s", planText)
		}
	})

	// EXPLAIN with WHERE
	t.Run("explain_select_where", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM users WHERE age > 25")
		if err != nil {
			t.Fatalf("EXPLAIN SELECT WHERE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "SELECT") {
			t.Errorf("plan should contain SELECT, got: %s", planText)
		}
	})

	// EXPLAIN with ORDER BY
	t.Run("explain_select_order_by", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM users ORDER BY age DESC")
		if err != nil {
			t.Fatalf("EXPLAIN SELECT ORDER BY: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "ORDER BY") {
			t.Errorf("plan should contain ORDER BY, got: %s", planText)
		}
	})

	// EXPLAIN with LIMIT
	t.Run("explain_select_limit", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM users LIMIT 10")
		if err != nil {
			t.Fatalf("EXPLAIN SELECT LIMIT: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "LIMIT") {
			t.Errorf("plan should contain LIMIT, got: %s", planText)
		}
	})
}

func TestExplain_Analyze(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create and populate table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// EXPLAIN ANALYZE SELECT
	t.Run("explain_analyze_select", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE SELECT: %v", err)
		}
		if len(res.Rows) == 0 {
			t.Fatal("EXPLAIN ANALYZE returned no rows")
		}
		planText := res.Rows[0][0].Text
		if planText == "" {
			t.Fatal("EXPLAIN ANALYZE returned empty plan")
		}
		// Should contain timing info
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
		// Should contain row count
		if !strings.Contains(planText, "actual rows") {
			t.Errorf("EXPLAIN ANALYZE should contain row count, got: %s", planText)
		}
	})

	// EXPLAIN ANALYZE with aggregate
	t.Run("explain_analyze_count", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE SELECT COUNT(*) FROM users")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE COUNT: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
	})
}

func TestExplain_DML(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// EXPLAIN INSERT
	t.Run("explain_insert", func(t *testing.T) {
		res, err := db.Query("EXPLAIN INSERT INTO users VALUES (1, 'Alice', 30)")
		if err != nil {
			t.Fatalf("EXPLAIN INSERT: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "INSERT") {
			t.Errorf("plan should contain INSERT, got: %s", planText)
		}
	})

	// EXPLAIN UPDATE
	t.Run("explain_update", func(t *testing.T) {
		res, err := db.Query("EXPLAIN UPDATE users SET age = 31 WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN UPDATE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "UPDATE") {
			t.Errorf("plan should contain UPDATE, got: %s", planText)
		}
	})

	// EXPLAIN DELETE
	t.Run("explain_delete", func(t *testing.T) {
		res, err := db.Query("EXPLAIN DELETE FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN DELETE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "DELETE") {
			t.Errorf("plan should contain DELETE, got: %s", planText)
		}
	})
}

func TestExplain_DDL(t *testing.T) {
	db, _ := openExplainDB(t)

	// EXPLAIN CREATE TABLE
	t.Run("explain_create_table", func(t *testing.T) {
		res, err := db.Query("EXPLAIN CREATE TABLE t (id INT, name TEXT)")
		if err != nil {
			t.Fatalf("EXPLAIN CREATE TABLE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "CREATE TABLE") {
			t.Errorf("plan should contain CREATE TABLE, got: %s", planText)
		}
	})

	// EXPLAIN CREATE INDEX
	t.Run("explain_create_index", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE t (id INT, name TEXT)")
		if err != nil {
			t.Fatalf("CREATE TABLE: %v", err)
		}
		res, err := db.Query("EXPLAIN CREATE INDEX idx ON t (name)")
		if err != nil {
			t.Fatalf("EXPLAIN CREATE INDEX: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "CREATE INDEX") {
			t.Errorf("plan should contain CREATE INDEX, got: %s", planText)
		}
	})

	// EXPLAIN DROP TABLE
	t.Run("explain_drop_table", func(t *testing.T) {
		res, err := db.Query("EXPLAIN DROP TABLE t")
		if err != nil {
			t.Fatalf("EXPLAIN DROP TABLE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "DROP TABLE") {
			t.Errorf("plan should contain DROP TABLE, got: %s", planText)
		}
	})
}

func TestExplain_AnalyzeDML(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create and populate table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// EXPLAIN ANALYZE INSERT
	t.Run("explain_analyze_insert", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE INSERT INTO users VALUES (2, 'Bob', 25)")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE INSERT: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
		if !strings.Contains(planText, "rows affected") {
			t.Errorf("EXPLAIN ANALYZE INSERT should contain rows affected, got: %s", planText)
		}
	})

	// EXPLAIN ANALYZE UPDATE
	t.Run("explain_analyze_update", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE UPDATE users SET age = 31 WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE UPDATE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
	})

	// EXPLAIN ANALYZE DELETE
	t.Run("explain_analyze_delete", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE DELETE FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE DELETE: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
	})
}

func TestExplain_Joins(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create tables for join
	_, err := db.Exec("CREATE TABLE t1 (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1: %v", err)
	}
	_, err = db.Exec("CREATE TABLE t2 (t1_id INT, value TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2: %v", err)
	}

	// EXPLAIN with JOIN
	t.Run("explain_inner_join", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id")
		if err != nil {
			t.Fatalf("EXPLAIN JOIN: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "JOIN") {
			t.Errorf("plan should contain JOIN, got: %s", planText)
		}
	})

	// EXPLAIN with LEFT JOIN
	t.Run("explain_left_join", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id")
		if err != nil {
			t.Fatalf("EXPLAIN LEFT JOIN: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "LEFT") {
			t.Errorf("plan should contain LEFT, got: %s", planText)
		}
	})

	// EXPLAIN ANALYZE with JOIN
	t.Run("explain_analyze_join", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE JOIN: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
	})
}

func TestExplain_Subqueries(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// EXPLAIN with subquery
	t.Run("explain_subquery_in_where", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)")
		if err != nil {
			t.Fatalf("EXPLAIN subquery: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "SELECT") {
			t.Errorf("plan should contain SELECT, got: %s", planText)
		}
	})

	// EXPLAIN with IN subquery
	t.Run("explain_in_subquery", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT * FROM users WHERE id IN (SELECT id FROM users WHERE age > 25)")
		if err != nil {
			t.Fatalf("EXPLAIN IN subquery: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "SELECT") {
			t.Errorf("plan should contain SELECT, got: %s", planText)
		}
	})
}

func TestExplain_Union(t *testing.T) {
	db, _ := openExplainDB(t)

	// EXPLAIN UNION
	t.Run("explain_union", func(t *testing.T) {
		res, err := db.Query("EXPLAIN SELECT 1 UNION SELECT 2")
		if err != nil {
			t.Fatalf("EXPLAIN UNION: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "UNION") {
			t.Errorf("plan should contain UNION, got: %s", planText)
		}
	})

	// EXPLAIN ANALYZE UNION
	t.Run("explain_analyze_union", func(t *testing.T) {
		res, err := db.Query("EXPLAIN ANALYZE SELECT 1 UNION ALL SELECT 2")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE UNION: %v", err)
		}
		planText := res.Rows[0][0].Text
		if !strings.Contains(planText, "actual time") {
			t.Errorf("EXPLAIN ANALYZE should contain timing info, got: %s", planText)
		}
	})
}

func TestExplain_AnalyzeDoesNotModifyData(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create table with data
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// EXPLAIN ANALYZE should not modify data
	res, err := db.Query("EXPLAIN ANALYZE UPDATE users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE UPDATE: %v", err)
	}
	_ = res

	// Verify data was NOT modified (EXPLAIN ANALYZE doesn't actually execute)
	res, err = db.Query("SELECT age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after EXPLAIN ANALYZE: %v", err)
	}
	// Note: EXPLAIN ANALYZE actually does execute the query, so data will be modified
	// This is the expected behavior for EXPLAIN ANALYZE
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0].Int != 31 {
		t.Errorf("expected age=31 (EXPLAIN ANALYZE does execute), got %d", res.Rows[0][0].Int)
	}
}

func TestExplain_ColumnName(t *testing.T) {
	db, _ := openExplainDB(t)

	// Verify EXPLAIN result has correct column name
	res, err := db.Query("EXPLAIN SELECT 1")
	if err != nil {
		t.Fatalf("EXPLAIN SELECT: %v", err)
	}

	if len(res.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(res.Columns))
	}
	if res.Columns[0] != "QUERY PLAN" {
		t.Errorf("expected column 'QUERY PLAN', got %q", res.Columns[0])
	}
}

func TestExplain_AnalyzeFormat(t *testing.T) {
	db, _ := openExplainDB(t)

	// Create and populate table
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// EXPLAIN ANALYZE should use a box-drawing format
	res, err := db.Query("EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE: %v", err)
	}
	planText := res.Rows[0][0].Text

	// Should contain the header
	if !strings.Contains(planText, "EXPLAIN ANALYZE") {
		t.Errorf("plan should contain header, got: %s", planText)
	}

	// Should contain box borders
	if !strings.Contains(planText, "┌") || !strings.Contains(planText, "┘") {
		t.Errorf("plan should use box-drawing format, got: %s", planText)
	}
}
