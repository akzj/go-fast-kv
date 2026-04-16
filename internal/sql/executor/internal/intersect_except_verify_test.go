package internal

import (
	"testing"
)

func TestIntersectExceptVerify(t *testing.T) {
	env := newTestEnv(t)

	// Basic INTERSECT tests
	t.Run("intersect_same_values", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1 INTERSECT SELECT 1")
		if len(result.Rows) != 1 {
			t.Errorf("SELECT 1 INTERSECT SELECT 1: got %d rows, want 1", len(result.Rows))
		}
	})
	
	t.Run("intersect_different_values", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1 INTERSECT SELECT 2")
		if len(result.Rows) != 0 {
			t.Errorf("SELECT 1 INTERSECT SELECT 2: got %d rows, want 0", len(result.Rows))
		}
	})
	
	// Basic EXCEPT tests
	t.Run("except_same_values", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1 EXCEPT SELECT 1")
		if len(result.Rows) != 0 {
			t.Errorf("SELECT 1 EXCEPT SELECT 1: got %d rows, want 0", len(result.Rows))
		}
	})
	
	t.Run("except_different_values", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1 EXCEPT SELECT 2")
		if len(result.Rows) != 1 {
			t.Errorf("SELECT 1 EXCEPT SELECT 2: got %d rows, want 1", len(result.Rows))
		}
	})
	
	// Combined with UNION - Right-associative parsing:
	// SELECT 1 UNION SELECT 2 INTERSECT SELECT 2 = SELECT 1 UNION (SELECT 2 INTERSECT SELECT 2)
	// = {1} ∪ ({2} ∩ {2}) = {1} ∪ {2} = {1, 2} = 2 rows
	t.Run("union_intersect", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2")
		if len(result.Rows) != 2 {
			t.Errorf("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2: got %d rows, want 2", len(result.Rows))
		}
	})
	
	// SELECT 1 UNION SELECT 2 EXCEPT SELECT 1 = SELECT 1 UNION (SELECT 2 EXCEPT SELECT 1)
	// = {1} ∪ ({2} \ {1}) = {1} ∪ {2} = {1, 2} = 2 rows
	t.Run("union_except", func(t *testing.T) {
		result := env.execSQL(t, "SELECT 1 UNION SELECT 2 EXCEPT SELECT 1")
		if len(result.Rows) != 2 {
			t.Errorf("SELECT 1 UNION SELECT 2 EXCEPT SELECT 1: got %d rows, want 2", len(result.Rows))
		}
	})
}
