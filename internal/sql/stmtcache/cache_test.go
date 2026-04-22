package stmtcache

import (
	"testing"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

func TestStatementCache_Basic(t *testing.T) {
	cache := NewStatementCache(10)

	// Create a mock execute function
	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		return nil, nil
	}

	// Put a prepared statement
	p := NewPreparedStmt("SELECT * FROM users", nil, execFn)
	cache.Put("SELECT * FROM users", p)

	// Get it back
	got := cache.Get("SELECT * FROM users")
	if got == nil {
		t.Fatal("expected to find cached statement")
	}
	if got.SQL != "SELECT * FROM users" {
		t.Errorf("expected SQL 'SELECT * FROM users', got %q", got.SQL)
	}

	// Size should be 1
	if cache.Size() != 1 {
		t.Errorf("expected size 1, got %d", cache.Size())
	}
}

func TestStatementCache_LRU(t *testing.T) {
	cache := NewStatementCache(3)

	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		return nil, nil
	}

	// Add 3 statements
	p1 := NewPreparedStmt("SELECT 1", nil, execFn)
	p2 := NewPreparedStmt("SELECT 2", nil, execFn)
	p3 := NewPreparedStmt("SELECT 3", nil, execFn)
	cache.Put("SELECT 1", p1)
	cache.Put("SELECT 2", p2)
	cache.Put("SELECT 3", p3)

	// Size should be 3
	if cache.Size() != 3 {
		t.Errorf("expected size 3, got %d", cache.Size())
	}

	// Add a 4th statement - should evict "SELECT 1"
	p4 := NewPreparedStmt("SELECT 4", nil, execFn)
	cache.Put("SELECT 4", p4)

	// Size should still be 3
	if cache.Size() != 3 {
		t.Errorf("expected size 3, got %d", cache.Size())
	}

	// "SELECT 1" should be evicted
	if cache.Get("SELECT 1") != nil {
		t.Error("expected SELECT 1 to be evicted")
	}

	// Others should still be there
	if cache.Get("SELECT 2") == nil {
		t.Error("SELECT 2 should still be cached")
	}
	if cache.Get("SELECT 3") == nil {
		t.Error("SELECT 3 should still be cached")
	}
	if cache.Get("SELECT 4") == nil {
		t.Error("SELECT 4 should be cached")
	}
}

func TestStatementCache_Whitespace(t *testing.T) {
	cache := NewStatementCache(10)

	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		return nil, nil
	}

	p := NewPreparedStmt("SELECT 1", nil, execFn)
	cache.Put("  SELECT 1  ", p)

	// Should find it with different whitespace
	got := cache.Get("  SELECT 1  ")
	if got == nil {
		t.Error("expected to find statement with same whitespace")
	}

	got = cache.Get("SELECT 1")
	if got == nil {
		t.Error("expected to find statement with trimmed whitespace")
	}
}

func TestStatementCache_Clear(t *testing.T) {
	cache := NewStatementCache(10)

	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		return nil, nil
	}

	p := NewPreparedStmt("SELECT 1", nil, execFn)
	cache.Put("SELECT 1", p)

	// Clear
	cache.Clear()

	// Should be empty
	if cache.Size() != 0 {
		t.Errorf("expected size 0 after clear, got %d", cache.Size())
	}
	if cache.Get("SELECT 1") != nil {
		t.Error("expected SELECT 1 to be removed after clear")
	}
}

func TestStatementCache_Remove(t *testing.T) {
	cache := NewStatementCache(10)

	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		return nil, nil
	}

	p := NewPreparedStmt("SELECT 1", nil, execFn)
	cache.Put("SELECT 1", p)

	// Remove
	cache.Remove("SELECT 1")

	// Should be gone
	if cache.Size() != 0 {
		t.Errorf("expected size 0 after remove, got %d", cache.Size())
	}
	if cache.Get("SELECT 1") != nil {
		t.Error("expected SELECT 1 to be removed")
	}
}

func TestPreparedStmt_UpdatePlan(t *testing.T) {
	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		return nil, nil
	}

	p := NewPreparedStmt("SELECT 1", nil, execFn)

	// Plan should be nil initially
	p.mu.RLock()
	if p.plan != nil {
		t.Error("expected plan to be nil initially")
	}
	p.mu.RUnlock()

	// Set plan
	p.setPlan(nil) // nil plan for testing

	// Plan should still be nil
	p.mu.RLock()
	if p.plan != nil {
		t.Error("expected plan to be nil after setPlan(nil)")
	}
	p.mu.RUnlock()
}

func TestNormalizeSQL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SELECT 1", "SELECT 1"},
		{"  SELECT 1", "SELECT 1"},
		{"SELECT 1  ", "SELECT 1"},
		{"  SELECT 1  ", "SELECT 1"},
		{"\tSELECT 1\t", "SELECT 1"},
		{"\nSELECT 1\n", "SELECT 1"},
	}

	for _, tc := range tests {
		got := normalizeSQL(tc.input)
		if got != tc.expected {
			t.Errorf("normalizeSQL(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}
