package internal

import (
	"testing"
)

// TestExec_Cast tests CAST type conversion function.
func TestExec_Cast(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, num INT, txt TEXT, flt FLOAT)")

	t.Run("cast_text_to_int", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, '123', 3.14)")

		result := env.execSQL(t, "SELECT CAST(txt AS INT) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 123 {
			t.Errorf("CAST('123' AS INT) = %d, want 123", result.Rows[0][0].Int)
		}
	})

	t.Run("cast_int_to_text", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, 'hello', 3.14)")

		result := env.execSQL(t, "SELECT CAST(num AS TEXT) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Text != "42" {
			t.Errorf("CAST(42 AS TEXT) = %q, want '42'", result.Rows[0][0].Text)
		}
	})

	t.Run("cast_text_to_float", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, '3.14', 3.14)")

		result := env.execSQL(t, "SELECT CAST(txt AS FLOAT) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Float != 3.14 {
			t.Errorf("CAST('3.14' AS FLOAT) = %f, want 3.14", result.Rows[0][0].Float)
		}
	})

	t.Run("cast_float_to_text", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, 'hello', 3.14)")

		result := env.execSQL(t, "SELECT CAST(flt AS TEXT) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Text != "3.14" {
			t.Errorf("CAST(3.14 AS TEXT) = %q, want '3.14'", result.Rows[0][0].Text)
		}
	})

	t.Run("cast_int_to_float", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, 'hello', 3.14)")

		result := env.execSQL(t, "SELECT CAST(num AS FLOAT) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Float != 42.0 {
			t.Errorf("CAST(42 AS FLOAT) = %f, want 42.0", result.Rows[0][0].Float)
		}
	})

	t.Run("cast_null_to_int", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, NULL, 'hello', 3.14)")

		result := env.execSQL(t, "SELECT CAST(num AS TEXT) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if !result.Rows[0][0].IsNull {
			t.Errorf("CAST(NULL AS TEXT) should be NULL")
		}
	})

	t.Run("cast_in_expression", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 10, '5', 3.14)")

		// CAST in arithmetic expression
		result := env.execSQL(t, "SELECT num + CAST(txt AS INT) FROM t")
		if result.Rows[0][0].Int != 15 {
			t.Errorf("10 + CAST('5' AS INT) = %d, want 15", result.Rows[0][0].Int)
		}
	})

	t.Run("cast_in_where", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 10, '5', 3.14)")
		env.execSQL(t, "INSERT INTO t VALUES (2, 20, '15', 3.14)")

		result := env.execSQL(t, "SELECT id FROM t WHERE CAST(txt AS INT) > 10")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Int != 2 {
			t.Errorf("WHERE CAST(txt AS INT) > 10, got id = %d, want 2", result.Rows[0][0].Int)
		}
	})

	t.Run("cast_text_to_blob", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, 'hello', 3.14)")

		result := env.execSQL(t, "SELECT CAST(txt AS BLOB) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if string(result.Rows[0][0].Blob) != "hello" {
			t.Errorf("CAST('hello' AS BLOB) = %q, want 'hello'", string(result.Rows[0][0].Blob))
		}
	})

	t.Run("cast_integer_keyword", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 42, '100', 3.14)")

		result := env.execSQL(t, "SELECT CAST(txt AS INTEGER) FROM t")
		if result.Rows[0][0].Int != 100 {
			t.Errorf("CAST('100' AS INTEGER) = %d, want 100", result.Rows[0][0].Int)
		}
	})
}