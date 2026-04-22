package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// Tests for window functions: ROW_NUMBER, RANK, DENSE_RANK, SUM, AVG, etc. OVER (...)

func TestParse_WindowFunc_RowNumber(t *testing.T) {
	p := New()

	stmt, err := p.Parse("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM orders")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	sel := stmt.(*api.SelectStmt)
	wf, ok := sel.Columns[0].Expr.(*api.WindowFuncExpr)
	if !ok {
		t.Fatalf("expected *api.WindowFuncExpr, got %T", sel.Columns[0].Expr)
	}
	if wf.Func != "ROW_NUMBER" {
		t.Errorf("expected ROW_NUMBER, got %s", wf.Func)
	}
	if wf.Window == nil {
		t.Fatal("expected window spec")
	}
}

func TestParse_WindowFunc_Rank(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		fn   string
	}{
		{"rank", "SELECT RANK() OVER (ORDER BY score DESC) FROM students", "RANK"},
		{"dense_rank", "SELECT DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) FROM employees", "DENSE_RANK"},
		{"row_number", "SELECT ROW_NUMBER() OVER (ORDER BY created_at) FROM logs", "ROW_NUMBER"},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			sel := stmt.(*api.SelectStmt)
			wf := sel.Columns[0].Expr.(*api.WindowFuncExpr)
			if wf.Func != tt.fn {
				t.Errorf("expected %s, got %s", tt.fn, wf.Func)
			}
		})
	}
}

func TestParse_WindowFunc_AggregateWithOver(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		fn   string
	}{
		{"sum_over", "SELECT SUM(amount) OVER (PARTITION BY customer_id ORDER BY date) FROM orders", "SUM"},
		{"avg_over", "SELECT AVG(price) OVER (ORDER BY category) FROM products", "AVG"},
		{"count_over", "SELECT COUNT(*) OVER (PARTITION BY region) FROM sales", "COUNT"},
		{"max_over", "SELECT MAX(salary) OVER (ORDER BY department) FROM employees", "MAX"},
		{"min_over", "SELECT MIN(value) OVER (PARTITION BY category) FROM metrics", "MIN"},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			sel := stmt.(*api.SelectStmt)
			wf := sel.Columns[0].Expr.(*api.WindowFuncExpr)
			if wf.Func != tt.fn {
				t.Errorf("expected %s, got %s", tt.fn, wf.Func)
			}
			if wf.Window == nil {
				t.Fatal("expected window spec")
			}
		})
	}
}

func TestParse_WindowFunc_AggregateWithoutOver(t *testing.T) {
	p := New()

	// Aggregate functions without OVER should still work
	tests := []struct {
		name string
		sql  string
		fn   string
	}{
		{"sum_no_over", "SELECT SUM(amount) FROM orders", "SUM"},
		{"avg_no_over", "SELECT AVG(price) FROM products", "AVG"},
		{"count_no_over", "SELECT COUNT(*) FROM logs", "COUNT"},
		{"max_no_over", "SELECT MAX(salary) FROM employees", "MAX"},
		{"min_no_over", "SELECT MIN(value) FROM metrics", "MIN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			sel := stmt.(*api.SelectStmt)
			ae, ok := sel.Columns[0].Expr.(*api.AggregateCallExpr)
			if !ok {
				t.Fatalf("expected *api.AggregateCallExpr, got %T", sel.Columns[0].Expr)
			}
			if ae.Func != tt.fn {
				t.Errorf("expected %s, got %s", tt.fn, ae.Func)
			}
		})
	}
}

func TestParse_WindowFunc_LeadLag(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		fn   string
	}{
		{"lead", "SELECT LEAD(salary) OVER (ORDER BY hire_date) FROM employees", "LEAD"},
		{"lag", "SELECT LAG(revenue) OVER (PARTITION BY quarter ORDER BY month) FROM financials", "LAG"},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			sel := stmt.(*api.SelectStmt)
			wf := sel.Columns[0].Expr.(*api.WindowFuncExpr)
			if wf.Func != tt.fn {
				t.Errorf("expected %s, got %s", tt.fn, wf.Func)
			}
		})
	}
}

func TestParse_WindowFunc_FirstLastValue(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		fn   string
	}{
		{"first_value", "SELECT FIRST_VALUE(price) OVER (ORDER BY created_at) FROM products", "FIRST_VALUE"},
		// ROWS BETWEEN not supported - simplified test
		{"last_value", "SELECT LAST_VALUE(discount) OVER (PARTITION BY category ORDER BY date) FROM orders", "LAST_VALUE"},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			sel := stmt.(*api.SelectStmt)
			wf := sel.Columns[0].Expr.(*api.WindowFuncExpr)
			if wf.Func != tt.fn {
				t.Errorf("expected %s, got %s", tt.fn, wf.Func)
			}
		})
	}
}

func TestParse_WindowFunc_Errors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"window_func_missing_over", "SELECT ROW_NUMBER() ORDER BY id FROM orders", true},
		{"window_func_missing_paren", "SELECT SUM(amount) OVER ORDER BY id FROM orders", true},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
