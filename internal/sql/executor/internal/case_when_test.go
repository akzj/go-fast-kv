package internal

import (
	"testing"
)

// TestCaseWhen_SearchedCase tests searched CASE with multiple WHEN clauses and ELSE.
func TestCaseWhen_SearchedCase(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (col INT)")
	env.execSQL(t, "INSERT INTO t VALUES (3), (7), (12), (15)")

	r := env.execSQL(t, "SELECT CASE WHEN col > 10 THEN 'big' WHEN col > 5 THEN 'medium' ELSE 'small' END FROM t")

	// 3 → small, 7 → medium, 12 → big, 15 → big
	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(r.Rows))
	}
	expected := []string{"small", "medium", "big", "big"}
	for i, exp := range expected {
		if r.Rows[i][0].Text != exp {
			t.Errorf("row[%d]: got %q, want %q", i, r.Rows[i][0].Text, exp)
		}
	}
}

// TestCaseWhen_NullCondition tests that NULL condition falls through to ELSE.
func TestCaseWhen_NullCondition(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (cond INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1)")
	env.execSQL(t, "INSERT INTO t VALUES (NULL)")

	r := env.execSQL(t, "SELECT CASE WHEN cond THEN 1 ELSE 2 END FROM t")

	// cond=1 → truthy → matches → 1; cond=NULL → falls through → 2
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Int != 1 {
		t.Errorf("row[0]: got %d, want 1", r.Rows[0][0].Int)
	}
	// NULL condition falls through to ELSE → 2
	if r.Rows[1][0].Int != 2 {
		t.Errorf("row[1]: got %d, want 2", r.Rows[1][0].Int)
	}
}

// TestCaseWhen_NoElse tests CASE without ELSE returns NULL when no WHEN matches.
func TestCaseWhen_NoElse(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (val INT)")
	env.execSQL(t, "INSERT INTO t VALUES (0)")

	r := env.execSQL(t, "SELECT CASE WHEN val > 0 THEN 'positive' END FROM t")

	// val=0 → first WHEN is false, no ELSE → NULL
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if !r.Rows[0][0].IsNull {
		t.Errorf("expected NULL, got %v", r.Rows[0][0])
	}
}

// TestCaseWhen_SimpleLiterals tests CASE with equality condition.
func TestCaseWhen_SimpleLiterals(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (id INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1)")

	r := env.execSQL(t, "SELECT CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM t")

	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "one" {
		t.Errorf("got %q, want 'one'", r.Rows[0][0].Text)
	}
}
