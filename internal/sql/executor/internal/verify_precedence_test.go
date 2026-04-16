package internal

import (
	"testing"
)

func TestVerifyPrecedence(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()
	
	// Test 1: SELECT 1 < 2 + 3 should return TRUE (not 4)
	// Before fix: parseCompareExpr would parse 1 < 2 first, then + 3 = BinAdd(BinLT(1,2), 3) = 4
	// After fix: parseTermExpr handles +, so 2 + 3 = 5, then 1 < 5 = TRUE
	result := env.execSQL(t, "SELECT 1 < 2 + 3")
	if len(result.Rows) != 1 {
		t.Fatalf("SELECT 1 < 2 + 3: expected 1 row, got %d", len(result.Rows))
	}
	// Result should be TRUE (1) since 1 < 5
	t.Logf("SELECT 1 < 2 + 3 = %v", result.Rows[0][0])
	if result.Rows[0][0].Int != 1 {
		t.Errorf("SELECT 1 < 2 + 3: got %d, expected TRUE (1)", result.Rows[0][0].Int)
	}

	// Test 2: SELECT 2 * 3 should return 6
	result = env.execSQL(t, "SELECT 2 * 3")
	if len(result.Rows) != 1 {
		t.Fatalf("SELECT 2 * 3: expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int != 6 {
		t.Errorf("SELECT 2 * 3: got %v, expected 6", result.Rows[0][0].Int)
	}
	t.Logf("PASS: SELECT 2 * 3 = %d", result.Rows[0][0].Int)

	// Test 3: SELECT 10 / 2 should return 5
	result = env.execSQL(t, "SELECT 10 / 2")
	if len(result.Rows) != 1 {
		t.Fatalf("SELECT 10 / 2: expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int != 5 {
		t.Errorf("SELECT 10 / 2: got %v, expected 5", result.Rows[0][0].Int)
	}
	t.Logf("PASS: SELECT 10 / 2 = %d", result.Rows[0][0].Int)

	// Test 4: SELECT 1 + 2 * 3 should return 7 (not 9)
	// With correct precedence: * binds tighter than +, so 2 * 3 = 6, then 1 + 6 = 7
	result = env.execSQL(t, "SELECT 1 + 2 * 3")
	if len(result.Rows) != 1 {
		t.Fatalf("SELECT 1 + 2 * 3: expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int != 7 {
		t.Errorf("SELECT 1 + 2 * 3: got %v, expected 7 (1 + (2*3))", result.Rows[0][0].Int)
	}
	t.Logf("PASS: SELECT 1 + 2 * 3 = %d (correct precedence: 1 + (2*3) = 7)", result.Rows[0][0].Int)
}
