package internal

import (
	"testing"
)

func TestWindowFunctions_Basic(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (id INT, name TEXT, salary INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 'a', 100)")
	env.execSQL(t, "INSERT INTO t VALUES (2, 'b', 200)")
	env.execSQL(t, "INSERT INTO t VALUES (3, 'c', 150)")

	t.Run("row_number", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) FROM t")
		if len(result.Rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result.Rows))
		}
		// Check that row numbers are 1, 2, 3 in order
		for i, row := range result.Rows {
			expected := int64(i + 1)
			got := row[1].Int // column 1 is the window function result
			if got != expected {
				t.Errorf("row %d: expected %d, got %d", i, expected, got)
			}
		}
	})

	t.Run("sum_over", func(t *testing.T) {
		result := env.execSQL(t, "SELECT salary, SUM(salary) OVER (ORDER BY id) FROM t ORDER BY id")
		if len(result.Rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result.Rows))
		}
		// cumulative: 100, 300, 450
		expected := []int64{100, 300, 450}
		for i, row := range result.Rows {
			got := row[1].Int // column 1 is SUM window
			if got != expected[i] {
				t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
			}
		}
	})
}

func TestWindowFunctions_PartitionBy(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (id INT, dept TEXT, salary INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 'A', 100)")
	env.execSQL(t, "INSERT INTO t VALUES (2, 'A', 200)")
	env.execSQL(t, "INSERT INTO t VALUES (3, 'B', 150)")
	env.execSQL(t, "INSERT INTO t VALUES (4, 'B', 250)")

	t.Run("partition_sum", func(t *testing.T) {
		result := env.execSQL(t, "SELECT id, dept, SUM(salary) OVER (PARTITION BY dept ORDER BY id) FROM t ORDER BY id")
		if len(result.Rows) != 4 {
			t.Errorf("expected 4 rows, got %d", len(result.Rows))
		}
		// A: 100, 300; B: 150, 400
		expected := map[int64]int64{1: 100, 2: 300, 3: 150, 4: 400}
		for _, row := range result.Rows {
			id := row[0].Int
			sum := row[2].Int // column 2 is SUM window
			exp := expected[id]
			if sum != exp {
				t.Errorf("id %d: expected %d, got %d", id, exp, sum)
			}
		}
	})
}

func TestWindowFunctions_Rank(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (id INT, name TEXT, score INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 'a', 100)")
	env.execSQL(t, "INSERT INTO t VALUES (2, 'b', 100)")
	env.execSQL(t, "INSERT INTO t VALUES (3, 'c', 200)")

	t.Run("rank", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name, RANK() OVER (ORDER BY score DESC) FROM t ORDER BY score DESC")
		if len(result.Rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result.Rows))
		}
		// score 200 -> rank 1, score 100 -> rank 2 (with gap)
		if result.Rows[0][1].Int != 1 {
			t.Errorf("first row: expected rank 1, got %d", result.Rows[0][1].Int)
		}
		if result.Rows[1][1].Int != 2 {
			t.Errorf("second row: expected rank 2, got %d", result.Rows[1][1].Int)
		}
	})

	t.Run("dense_rank", func(t *testing.T) {
		result := env.execSQL(t, "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) FROM t ORDER BY score DESC")
		if len(result.Rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result.Rows))
		}
		// score 200 -> rank 1, score 100 -> rank 2 (no gap)
		if result.Rows[0][1].Int != 1 {
			t.Errorf("first row: expected rank 1, got %d", result.Rows[0][1].Int)
		}
		if result.Rows[1][1].Int != 2 {
			t.Errorf("second row: expected rank 2, got %d", result.Rows[1][1].Int)
		}
	})
}