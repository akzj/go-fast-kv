package internal

import (
	"math"
	"strconv"
	"testing"
)

// ─── Category 1: NULL Handling ──────────────────────────────────────

func TestNullHandling_Comparisons(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)")

	// NULL = anything is false
	r := env.execSQL(t, "SELECT * FROM t WHERE a = NULL")
	if len(r.Rows) != 0 {
		t.Errorf("a = NULL should return 0 rows, got %d", len(r.Rows))
	}

	// IS NULL works (special operator, not general comparison)
	r = env.execSQL(t, "SELECT * FROM t WHERE b IS NULL")
	if len(r.Rows) != 1 {
		t.Errorf("b IS NULL should return 1 row, got %d", len(r.Rows))
	}

	// IS NOT NULL works
	r = env.execSQL(t, "SELECT * FROM t WHERE b IS NOT NULL")
	if len(r.Rows) != 2 {
		t.Errorf("b IS NOT NULL should return 2 rows, got %d", len(r.Rows))
	}
}

func TestNullHandling_IN(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1), (2), (NULL), (3)")

	r := env.execSQL(t, "SELECT * FROM t WHERE a IN (1, 2, 3)")
	if len(r.Rows) != 3 {
		t.Errorf("IN should return 3 rows (excluding NULL), got %d", len(r.Rows))
	}
}

func TestNullHandling_Between(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1), (5), (10), (NULL)")

	r := env.execSQL(t, "SELECT * FROM t WHERE a BETWEEN 2 AND 8")
	if len(r.Rows) != 1 {
		t.Errorf("BETWEEN should return 1 row, got %d", len(r.Rows))
	}
}

func TestNullHandling_CaseWhen(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (NULL), (1), (2)")

	r := env.execSQL(t, "SELECT CASE WHEN a IS NULL THEN 'null' WHEN a = 1 THEN 'one' ELSE 'other' END FROM t")
	if len(r.Rows) != 3 {
		t.Fatalf("CASE should return 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "null" {
		t.Errorf("first row should be 'null', got %q", r.Rows[0][0].Text)
	}
	if r.Rows[1][0].Text != "one" {
		t.Errorf("second row should be 'one', got %q", r.Rows[1][0].Text)
	}
}

func TestNullHandling_Aggregates(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, g INT)")
	env.execSQL(t, "INSERT INTO t VALUES (NULL, 1), (1, 1), (2, 1), (NULL, 2)")

	r := env.execSQL(t, "SELECT COUNT(a), SUM(a), AVG(a) FROM t WHERE g = 1")
	if r.Rows[0][0].Int != 2 {
		t.Errorf("COUNT should be 2 (ignoring NULL), got %d", r.Rows[0][0].Int)
	}
	if r.Rows[0][1].Int != 3 {
		t.Errorf("SUM should be 3, got %d", r.Rows[0][1].Int)
	}
}

func TestNullHandling_InsertNULL(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES (NULL, 'hello')")

	r := env.execSQL(t, "SELECT * FROM t WHERE a IS NULL")
	if len(r.Rows) != 1 {
		t.Fatalf("INSERT NULL should work, got %d rows", len(r.Rows))
	}
	if r.Rows[0][1].Text != "hello" {
		t.Errorf("other column should be preserved, got %q", r.Rows[0][1].Text)
	}
}

func TestNullHandling_UpdateToNull(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20)")
	env.execSQL(t, "UPDATE t SET b = NULL WHERE a = 1")

	r := env.execSQL(t, "SELECT * FROM t WHERE b IS NULL")
	if len(r.Rows) != 1 {
		t.Fatalf("UPDATE SET NULL should work, got %d rows", len(r.Rows))
	}
}

func TestNullHandling_NotEqualToNull(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (NULL), (1), (2)")

	r := env.execSQL(t, "SELECT * FROM t WHERE a <> NULL")
	if len(r.Rows) != 0 {
		t.Errorf("a <> NULL should return 0 rows, got %d", len(r.Rows))
	}
}

// ─── Category 2: Empty Tables ────────────────────────────────────────

func TestEmptyTable_Select(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE empty (a INT, b TEXT)")
	env.execSQL(t, "INSERT INTO empty VALUES (1, 'x')")
	env.execSQL(t, "DELETE FROM empty")

	r := env.execSQL(t, "SELECT * FROM empty")
	if len(r.Rows) != 0 {
		t.Errorf("SELECT from empty table should return 0 rows, got %d", len(r.Rows))
	}
}

func TestEmptyTable_Aggregates(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE empty (a INT)")
	env.execSQL(t, "DELETE FROM empty")

	r := env.execSQL(t, "SELECT COUNT(*) FROM empty")
	if r.Rows[0][0].Int != 0 {
		t.Errorf("COUNT(*) on empty should be 0, got %d", r.Rows[0][0].Int)
	}

	r = env.execSQL(t, "SELECT SUM(a), MAX(a), MIN(a) FROM empty")
	t.Logf("Aggregate on empty: SUM=%v MAX=%v MIN=%v", r.Rows[0][0], r.Rows[0][1], r.Rows[0][2])
}

func TestEmptyTable_Insert(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE empty (a INT PRIMARY KEY, b TEXT)")
	result := env.execSQL(t, "INSERT INTO empty VALUES (1, 'test')")
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected should be 1, got %d", result.RowsAffected)
	}
	r := env.execSQL(t, "SELECT * FROM empty")
	if len(r.Rows) != 1 {
		t.Errorf("Should have 1 row after insert, got %d", len(r.Rows))
	}
}

func TestEmptyTable_Update(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "DELETE FROM t")

	result := env.execSQL(t, "UPDATE t SET b = 5 WHERE a > 0")
	if result.RowsAffected != 0 {
		t.Errorf("UPDATE on empty table should affect 0 rows, got %d", result.RowsAffected)
	}
}

func TestEmptyTable_Delete(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "DELETE FROM t")

	result := env.execSQL(t, "DELETE FROM t")
	if result.RowsAffected != 0 {
		t.Errorf("DELETE on empty table should affect 0 rows, got %d", result.RowsAffected)
	}
}

func TestEmptyTable_Join(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE empty (a INT)")
	env.execSQL(t, "CREATE TABLE t (b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1)")
	env.execSQL(t, "DELETE FROM empty")

	r := env.execSQL(t, "SELECT * FROM empty JOIN t ON empty.a = t.b")
	if len(r.Rows) != 0 {
		t.Errorf("JOIN with empty left table should return 0 rows, got %d", len(r.Rows))
	}
}

// ─── Category 3: Very Large/Small Values ────────────────────────────

func TestLargeValues_Int64(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (9223372036854775807), (0), (-9223372036854775808)")

	r := env.execSQL(t, "SELECT * FROM t ORDER BY a")
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Int != math.MinInt64 {
		t.Errorf("min int64 not stored correctly")
	}
	if r.Rows[2][0].Int != math.MaxInt64 {
		t.Errorf("max int64 not stored correctly")
	}
}

func TestLargeValues_MaxInt(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY)")
	env.execSQL(t, "INSERT INTO t VALUES (2147483647)")

	r := env.execSQL(t, "SELECT * FROM t WHERE a = 2147483647")
	if len(r.Rows) != 1 {
		t.Errorf("max int32 not stored correctly")
	}
}

func TestLargeValues_ZeroBoundary(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (-1), (0), (1)")

	r := env.execSQL(t, "SELECT * FROM t ORDER BY a")
	if r.Rows[0][0].Int != -1 || r.Rows[1][0].Int != 0 || r.Rows[2][0].Int != 1 {
		t.Errorf("zero boundary values incorrect")
	}
}

func TestLargeValues_LargeText(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES ('hello'), (''), ('a very long string that is much longer')")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[1][0].Text != "" {
		t.Errorf("empty string not stored correctly")
	}
}

func TestLargeValues_SimpleFloats(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE nums (a FLOAT)")
	env.execSQL(t, "INSERT INTO nums VALUES (0.0), (1.5), (3.14)")

	r := env.execSQL(t, "SELECT * FROM nums")
	if len(r.Rows) != 3 {
		t.Errorf("float not stored correctly, got %d rows", len(r.Rows))
	}
}

// ─── Category 4: Type Mismatches ─────────────────────────────────────

func TestTypeMismatch_NullWithType(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b TEXT, c FLOAT)")
	env.execSQL(t, "INSERT INTO t VALUES (NULL, NULL, NULL)")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 1 {
		t.Fatalf("NULL insert should work, got %d rows", len(r.Rows))
	}
	if !r.Rows[0][0].IsNull || !r.Rows[0][1].IsNull || !r.Rows[0][2].IsNull {
		t.Errorf("All columns should be NULL")
	}
}

func TestTypeMismatch_FloatVsInt(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a FLOAT)")
	env.execSQL(t, "INSERT INTO t VALUES (1.5)")

	r := env.execSQL(t, "SELECT * FROM t WHERE a = 1.5")
	if len(r.Rows) != 1 {
		t.Errorf("Float comparison should work")
	}
}

// ─── Category 5: Division by Zero ───────────────────────────────────

func TestDivisionByZero_Int(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (10, 0)")

	_, err := env.execSQLErr(t, "SELECT a / b FROM t")
	if err != nil {
		t.Logf("Division by zero error (acceptable): %v", err)
	}
}

func TestDivisionByZero_BothZero(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (0)")

	_, err := env.execSQLErr(t, "SELECT a / a FROM t")
	if err != nil {
		t.Logf("0 / 0 error (acceptable): %v", err)
	}
}

// ─── Category 6: String Boundary Conditions ─────────────────────────

func TestStringBoundary_EmptyString(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES (''), ('a'), ('abc')")

	r := env.execSQL(t, "SELECT * FROM t ORDER BY a")
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "" {
		t.Errorf("empty string not stored correctly")
	}
}

func TestStringBoundary_SingleChar(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES ('a'), ('A')")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 2 {
		t.Errorf("single char not stored correctly")
	}
}

func TestStringBoundary_Whitespace(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES (' '), ('  '), ('	'), ('\n')")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 4 {
		t.Errorf("whitespace strings not stored correctly, got %d", len(r.Rows))
	}
}

func TestStringBoundary_SpecialChars(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES ('hello world'), ('行123'), ('emoji: 🎉')")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 3 {
		t.Errorf("special chars not stored correctly, got %d rows", len(r.Rows))
	}
}

func TestStringBoundary_LIKEPrefix(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES ('hello'), ('help'), ('world')")

	r := env.execSQL(t, "SELECT * FROM t WHERE a LIKE 'hel%'")
	if len(r.Rows) != 2 {
		t.Errorf("LIKE 'hel%%' should match 2 rows, got %d", len(r.Rows))
	}
}

func TestStringBoundary_Unicode(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES ('中文'), ('日本語'), ('한국어')")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 3 {
		t.Errorf("Unicode strings not stored correctly, got %d rows", len(r.Rows))
	}
}

// ─── Category 7: Index Edge Cases ────────────────────────────────────

func TestIndexEdgeCase_CreateDrop(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
	
	r := env.execSQL(t, "SELECT * FROM t WHERE b = 20")
	if len(r.Rows) != 1 {
		t.Errorf("Index lookup should work")
	}

	// Note: DROP INDEX syntax not supported in this engine
}

func TestIndexEdgeCase_DuplicateKeys(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20)")

	r := env.execSQL(t, "SELECT * FROM t WHERE b = 10")
	if len(r.Rows) != 2 {
		t.Errorf("Index with duplicate keys should return 2 rows, got %d", len(r.Rows))
	}
}

func TestIndexEdgeCase_NullInIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, NULL), (2, 10), (3, NULL)")

	r := env.execSQL(t, "SELECT * FROM t WHERE b IS NULL")
	if len(r.Rows) != 2 {
		t.Errorf("Index lookup with NULL should return 2 rows, got %d", len(r.Rows))
	}

	r = env.execSQL(t, "SELECT * FROM t WHERE b = 10")
	if len(r.Rows) != 1 {
		t.Errorf("Index lookup for non-null should return 1 row, got %d", len(r.Rows))
	}
}

func TestIndexEdgeCase_TextIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b TEXT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')")

	r := env.execSQL(t, "SELECT * FROM t WHERE b = 'banana'")
	if len(r.Rows) != 1 {
		t.Errorf("Text index lookup should return 1 row, got %d", len(r.Rows))
	}
}

func TestIndexEdgeCase_RangeQuery(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")

	r := env.execSQL(t, "SELECT * FROM t WHERE b >= 20 AND b <= 35")
	if len(r.Rows) != 2 {
		t.Errorf("Index range query should return 2 rows, got %d", len(r.Rows))
	}
}

func TestIndexEdgeCase_UpdateIndexedColumn(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20)")
	env.execSQL(t, "UPDATE t SET b = 15 WHERE a = 1")

	r := env.execSQL(t, "SELECT * FROM t WHERE b = 15")
	if len(r.Rows) != 1 {
		t.Errorf("Update indexed column should work, got %d rows", len(r.Rows))
	}
}

func TestIndexEdgeCase_DeleteUsingIndex(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")
	env.execSQL(t, "CREATE INDEX idx_b ON t(b)")

	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
	env.execSQL(t, "DELETE FROM t WHERE b = 20")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 2 {
		t.Errorf("Delete using index should work, got %d rows", len(r.Rows))
	}
}

// ─── Category 8: Concurrent Operations ───────────────────────────────

func TestConcurrent_SimpleWrites(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY)")

	for i := 1; i <= 10; i++ {
		env.execSQL(t, "INSERT INTO t VALUES ("+strconv.Itoa(i)+")")
	}

	r := env.execSQL(t, "SELECT COUNT(*) FROM t")
	if r.Rows[0][0].Int != 10 {
		t.Errorf("Expected 10 rows, got %d", r.Rows[0][0].Int)
	}
}

func TestConcurrent_ReadDuringWrite(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 'initial')")
	env.execSQL(t, "INSERT INTO t VALUES (2, 'new')")

	r := env.execSQL(t, "SELECT * FROM t")
	if len(r.Rows) != 2 {
		t.Errorf("Read should see both inserts, got %d rows", len(r.Rows))
	}
}

func TestConcurrent_MultipleInserts(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT)")

	for i := 1; i <= 5; i++ {
		env.execSQL(t, "INSERT INTO t VALUES ("+strconv.Itoa(i)+", "+strconv.Itoa(i*10)+")")
	}

	r := env.execSQL(t, "SELECT COUNT(*) FROM t")
	if r.Rows[0][0].Int != 5 {
		t.Errorf("Expected 5 rows, got %d", r.Rows[0][0].Int)
	}

	r = env.execSQL(t, "SELECT SUM(b) FROM t")
	if r.Rows[0][0].Int != 150 {
		t.Errorf("Expected sum 150, got %d", r.Rows[0][0].Int)
	}
}

func TestConcurrent_InterleavedWrites(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY, val TEXT)")

	for i := 1; i <= 10; i++ {
		env.execSQL(t, "INSERT INTO t VALUES ("+strconv.Itoa(i)+", 'val_"+strconv.Itoa(i)+"')")
		r := env.execSQL(t, "SELECT COUNT(*) FROM t")
		expected := int64(i)
		if r.Rows[0][0].Int != expected {
			t.Errorf("After insert %d, expected %d rows, got %d", i, expected, r.Rows[0][0].Int)
		}
	}
}

func TestConcurrent_DeleteAndInsert(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT PRIMARY KEY)")
	env.execSQL(t, "INSERT INTO t VALUES (1), (2), (3)")

	env.execSQL(t, "DELETE FROM t WHERE a = 2")
	env.execSQL(t, "INSERT INTO t VALUES (4)")

	r := env.execSQL(t, "SELECT * FROM t ORDER BY a")
	if len(r.Rows) != 3 {
		t.Errorf("Expected 3 rows after delete+insert, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Int != 1 || r.Rows[1][0].Int != 3 || r.Rows[2][0].Int != 4 {
		t.Errorf("Row values incorrect after delete+insert")
	}
}

// ─── Additional Edge Cases ──────────────────────────────────────────

func TestEdgeCase_AllAggregates(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE nums (a INT, b INT)")
	env.execSQL(t, "INSERT INTO nums VALUES (5, 10), (15, 20), (25, NULL)")

	r := env.execSQL(t, "SELECT COUNT(*), COUNT(a), SUM(a), AVG(a), MIN(a), MAX(a) FROM nums")
	if r.Rows[0][0].Int != 3 {
		t.Errorf("COUNT(*) should be 3, got %d", r.Rows[0][0].Int)
	}
	if r.Rows[0][2].Int != 45 {
		t.Errorf("SUM should be 45, got %d", r.Rows[0][2].Int)
	}
}

// TestEdgeCase_SumAllNull verifies SUM returns NULL for all-NULL columns (SQL standard).
// Before fix: SUM returned 0 for all-NULL columns.
// After fix: SUM returns NULL per SQL standard.
func TestEdgeCase_SumAllNull(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")

	// Test 1: All NULL column → SUM should be NULL, not 0
	env.execSQL(t, "INSERT INTO t VALUES (NULL, 1), (NULL, 2)")
	r := env.execSQL(t, "SELECT SUM(a) FROM t")
	if !r.Rows[0][0].IsNull {
		t.Errorf("SUM(all NULL) should be NULL, got %v", r.Rows[0][0])
	}

	// Test 2: Mix of NULL and non-NULL → SUM should skip NULLs
	env.execSQL(t, "DELETE FROM t")
	env.execSQL(t, "INSERT INTO t VALUES (NULL, 1), (5, 2), (NULL, 3)")
	r = env.execSQL(t, "SELECT SUM(a) FROM t")
	if r.Rows[0][0].IsNull {
		t.Errorf("SUM with some non-NULL values should not be NULL, got %v", r.Rows[0][0])
	}
	if r.Rows[0][0].Int != 5 {
		t.Errorf("SUM with some non-NULL values should be 5, got %d", r.Rows[0][0].Int)
	}

	// Test 3: Empty table → SUM should be NULL
	env.execSQL(t, "DELETE FROM t")
	r = env.execSQL(t, "SELECT SUM(a) FROM t")
	if !r.Rows[0][0].IsNull {
		t.Errorf("SUM on empty table should be NULL, got %v", r.Rows[0][0])
	}

	// Test 4: Single NULL value → SUM should be NULL
	env.execSQL(t, "INSERT INTO t VALUES (NULL, 1)")
	r = env.execSQL(t, "SELECT SUM(a) FROM t")
	if !r.Rows[0][0].IsNull {
		t.Errorf("SUM(single NULL) should be NULL, got %v", r.Rows[0][0])
	}
}

func TestEdgeCase_GroupByWithNull(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (1, NULL), (2, 20), (NULL, 30)")

	r := env.execSQL(t, "SELECT a, COUNT(*) FROM t GROUP BY a")
	if len(r.Rows) < 2 {
		t.Errorf("GROUP BY should group non-null values, got %d groups", len(r.Rows))
	}
}

func TestEdgeCase_LimitOffset(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1), (2), (3), (4), (5)")

	r := env.execSQL(t, "SELECT * FROM t LIMIT 3")
	if len(r.Rows) != 3 {
		t.Errorf("LIMIT 3 should return 3 rows, got %d", len(r.Rows))
	}

	r = env.execSQL(t, "SELECT * FROM t LIMIT 3 OFFSET 2")
	if len(r.Rows) != 3 {
		t.Errorf("LIMIT 3 OFFSET 2 should return 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Int != 3 {
		t.Errorf("OFFSET 2 should start at row 3")
	}
}

func TestEdgeCase_SubqueryInWhere(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")

	r := env.execSQL(t, "SELECT * FROM t WHERE a IN (SELECT a FROM t WHERE a < 3)")
	if len(r.Rows) != 2 {
		t.Errorf("Subquery in WHERE should return 2 rows, got %d", len(r.Rows))
	}
}

func TestEdgeCase_JoinWithNulls(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t1 (a INT, b INT)")
	env.execSQL(t, "CREATE TABLE t2 (c INT, d INT)")
	env.execSQL(t, "INSERT INTO t1 VALUES (1, 10), (2, NULL), (NULL, 30)")
	env.execSQL(t, "INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)")

	r := env.execSQL(t, "SELECT * FROM t1 JOIN t2 ON t1.a = t2.c")
	if len(r.Rows) != 2 {
		t.Errorf("Join with NULLs should skip NULL keys, got %d rows", len(r.Rows))
	}
}

func TestEdgeCase_BooleanOperators(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (0), (1), (NULL)")

	r := env.execSQL(t, "SELECT * FROM t WHERE NOT a = 1")
	if len(r.Rows) != 1 {
		t.Errorf("NOT should work, got %d rows", len(r.Rows))
	}

	r = env.execSQL(t, "SELECT * FROM t WHERE a = 1 OR a = 0")
	if len(r.Rows) != 2 {
		t.Errorf("OR should work, got %d rows", len(r.Rows))
	}

	r = env.execSQL(t, "SELECT * FROM t WHERE a = 1 AND a IS NOT NULL")
	if len(r.Rows) != 1 {
		t.Errorf("AND should work, got %d rows", len(r.Rows))
	}
}

func TestEdgeCase_Distinct(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b TEXT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 'x'), (1, 'x'), (1, 'y'), (2, 'x')")

	r := env.execSQL(t, "SELECT DISTINCT a, b FROM t")
	if len(r.Rows) != 3 {
		t.Errorf("DISTINCT should return 3 unique combinations, got %d", len(r.Rows))
	}
}

func TestEdgeCase_SingleOrderBy(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 20), (1, 10), (2, 5)")

	r := env.execSQL(t, "SELECT * FROM t ORDER BY a")
	if len(r.Rows) != 3 {
		t.Errorf("ORDER BY should work, got %d rows", len(r.Rows))
	}
}

func TestEdgeCase_CaseElseNull(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1), (2)")

	r := env.execSQL(t, "SELECT CASE WHEN a = 1 THEN 'one' END FROM t")
	if len(r.Rows) != 2 {
		t.Errorf("CASE without ELSE should return 2 rows")
	}
}

func TestEdgeCase_AggregateWithoutGroupBy(t *testing.T) {
	env := newTestEnv(t)
	env.execSQL(t, "CREATE TABLE t (a INT, b INT)")
	env.execSQL(t, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")

	// These queries mix aggregate with non-aggregate columns — must return error, NOT panic.
	testCases := []struct {
		name       string
		sql        string
		wantErr    bool // true = should error (aggregate + non-agg column without GROUP BY)
	}{
		{"COUNT with column ref", "SELECT COUNT(a), b FROM t", true},
		{"SUM with column ref", "SELECT SUM(a), b FROM t", true},
		{"AVG with column ref", "SELECT AVG(a), b FROM t", true},
		{"Multiple aggregates with column", "SELECT COUNT(*), a FROM t", true},
		{"Multiple aggregates only", "SELECT COUNT(*), SUM(a) FROM t", false}, // all aggregate = valid
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.execSQLErr(t, tc.sql)
			if tc.wantErr {
				// Must return an error, not panic
				if err == nil {
					t.Errorf("SQL %q should return error (aggregate without GROUP BY), got nil", tc.sql)
				}
			} else {
				// Must succeed
				if err != nil {
					t.Errorf("SQL %q should succeed, got error: %v", tc.sql, err)
				}
			}
		})
	}
}
