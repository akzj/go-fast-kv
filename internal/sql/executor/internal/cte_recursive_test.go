package internal

import (
	"testing"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// TestRecursiveCTE_Counter tests the basic recursive CTE pattern: counting 1-10.
// This is the canonical recursive CTE test case.
func TestRecursiveCTE_Counter(t *testing.T) {
	env := newTestEnvCTE(t)

	// No table needed for this simple recursive CTE
	// It uses constant values only
	rows, cols := querySQL(t, env, `
		WITH RECURSIVE cnt AS (
			SELECT 1 as n
			UNION ALL
			SELECT n+1 FROM cnt WHERE n < 10
		)
		SELECT * FROM cnt
	`)

	// Expected: 10 rows (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if len(rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows))
	}

	// Verify column name (SQL identifiers are case-insensitive, stored as uppercase)
	if len(cols) != 1 || cols[0] != "N" {
		t.Errorf("expected columns [N], got %v", cols)
	}

	// Verify values
	for i, row := range rows {
		expected := int64(i + 1)
		if row[0].Type != catalogapi.TypeInt || row[0].Int != expected {
			t.Errorf("row %d: expected %d, got %v", i, expected, row[0])
		}
	}
}
