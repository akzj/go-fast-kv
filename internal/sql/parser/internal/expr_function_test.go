package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// Tests for function expressions: CAST, COALESCE, NULLIF, SUBSTRING
// These are parsed in parsePrimary around line 2577

func TestParse_Cast(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.CastExpr) string
	}{
		{
			name: "cast_to_int",
			sql:  "SELECT CAST(x AS INT) FROM t",
			checkFn: func(ce *api.CastExpr) string {
				if ce.TypeName != "INT" {
					return "expected type INT"
				}
				return ""
			},
		},
		{
			name: "cast_to_text",
			sql:  "SELECT CAST(name AS TEXT) FROM users",
			checkFn: func(ce *api.CastExpr) string {
				if ce.TypeName != "TEXT" {
					return "expected type TEXT"
				}
				return ""
			},
		},
		{
			name: "cast_to_float",
			sql:  "SELECT CAST(price AS FLOAT) FROM products",
			checkFn: func(ce *api.CastExpr) string {
				if ce.TypeName != "FLOAT" {
					return "expected type FLOAT"
				}
				return ""
			},
		},
		{
			name: "cast_to_integer",
			sql:  "SELECT CAST(x AS INTEGER) FROM t",
			checkFn: func(ce *api.CastExpr) string {
				if ce.TypeName != "INTEGER" {
					return "expected type INTEGER"
				}
				return ""
			},
		},
		{
			name: "cast_to_blob",
			sql:  "SELECT CAST(data AS BLOB) FROM files",
			checkFn: func(ce *api.CastExpr) string {
				if ce.TypeName != "BLOB" {
					return "expected type BLOB"
				}
				return ""
			},
		},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				sel := stmt.(*api.SelectStmt)
				ce, ok := sel.Columns[0].Expr.(*api.CastExpr)
				if !ok {
					t.Fatalf("expected *api.CastExpr")
				}
				if msg := tt.checkFn(ce); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Coalesce(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.CoalesceExpr) string
	}{
		{
			name: "coalesce_two_args",
			sql:  "SELECT COALESCE(a, b) FROM t",
			checkFn: func(ce *api.CoalesceExpr) string {
				if len(ce.Args) != 2 {
					return "expected 2 arguments"
				}
				return ""
			},
		},
		{
			name: "coalesce_three_args",
			sql:  "SELECT COALESCE(a, b, c) FROM t",
			checkFn: func(ce *api.CoalesceExpr) string {
				if len(ce.Args) != 3 {
					return "expected 3 arguments"
				}
				return ""
			},
		},
		{
			name: "coalesce_with_null",
			sql:  "SELECT COALESCE(name, 'unknown') FROM users",
			checkFn: func(ce *api.CoalesceExpr) string {
				if len(ce.Args) != 2 {
					return "expected 2 arguments"
				}
				return ""
			},
		},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				sel := stmt.(*api.SelectStmt)
				ce, ok := sel.Columns[0].Expr.(*api.CoalesceExpr)
				if !ok {
					t.Fatalf("expected *api.CoalesceExpr")
				}
				if msg := tt.checkFn(ce); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_NullIf(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.NullIfExpr) string
	}{
		{
			name: "nullif_basic",
			sql:  "SELECT NULLIF(a, b) FROM t",
			checkFn: func(ne *api.NullIfExpr) string {
				if ne.Left == nil || ne.Right == nil {
					return "expected both left and right arguments"
				}
				return ""
			},
		},
		{
			name: "nullif_with_literals",
			sql:  "SELECT NULLIF(x, 0) FROM t",
			checkFn: func(ne *api.NullIfExpr) string {
				return ""
			},
		},
		// NULLIF in expression - skip complex expression
		{name: "nullif_in_expression_skip", sql: "SELECT NULLIF(price, 0) FROM products", wantErr: false, checkFn: func(ne *api.NullIfExpr) string {
			return ""
		}},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				sel := stmt.(*api.SelectStmt)
				ne, ok := sel.Columns[0].Expr.(*api.NullIfExpr)
				if !ok {
					t.Fatalf("expected *api.NullIfExpr")
				}
				if msg := tt.checkFn(ne); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Substring(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.StringFuncExpr) string
	}{
		{
			name: "substring_from_for",
			sql:  "SELECT SUBSTRING(name FROM 1 FOR 5) FROM users",
			checkFn: func(sf *api.StringFuncExpr) string {
				if sf.Func != "SUBSTRING" {
					return "expected SUBSTRING"
				}
				if sf.Start == nil {
					return "expected start position"
				}
				if sf.Len == nil {
					return "expected length"
				}
				return ""
			},
		},
		{
			name: "substring_from_only",
			sql:  "SELECT SUBSTRING(name FROM 5) FROM users",
			checkFn: func(sf *api.StringFuncExpr) string {
				if sf.Func != "SUBSTRING" {
					return "expected SUBSTRING"
				}
				if sf.Start == nil {
					return "expected start position"
				}
				return ""
			},
		},
		{
			name: "substring_comma_syntax",
			sql:  "SELECT SUBSTRING(name, 1, 5) FROM users",
			checkFn: func(sf *api.StringFuncExpr) string {
				if sf.Func != "SUBSTRING" {
					return "expected SUBSTRING"
				}
				return ""
			},
		},
		{
			name: "substring_comma_start_only",
			sql:  "SELECT SUBSTRING(name, 5) FROM users",
			checkFn: func(sf *api.StringFuncExpr) string {
				if sf.Func != "SUBSTRING" {
					return "expected SUBSTRING"
				}
				return ""
			},
		},
	}

	p := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				sel := stmt.(*api.SelectStmt)
				sf, ok := sel.Columns[0].Expr.(*api.StringFuncExpr)
				if !ok {
					t.Fatalf("expected *api.StringFuncExpr")
				}
				if msg := tt.checkFn(sf); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Function_Errors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"cast_missing_paren", "SELECT CAST(x AS INT FROM t", true},
		{"cast_missing_as", "SELECT CAST(x INT) FROM t", true},
		{"cast_invalid_type", "SELECT CAST(x AS INVALID) FROM t", true},
		{"coalesce_no_args", "SELECT COALESCE() FROM t", true},
		{"nullif_missing_comma", "SELECT NULLIF(a b) FROM t", true},
		{"nullif_single_arg", "SELECT NULLIF(a) FROM t", true},
		{"substring_missing_paren", "SELECT SUBSTRING(x FROM 1 FOR 5 FROM t", true},
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
