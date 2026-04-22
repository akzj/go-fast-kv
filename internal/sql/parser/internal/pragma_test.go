package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// Tests for PRAGMA statements (parsePragma at lines 1984-2053)

func TestParse_Pragma_Basic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.PragmaStmt) string
	}{
		{
			name: "pragma_name_only",
			sql:  "PRAGMA journal_mode",
			checkFn: func(ps *api.PragmaStmt) string {
				if ps.Name != "JOURNAL_MODE" {
					return "expected name JOURNAL_MODE"
				}
				return ""
			},
		},
		{
			name: "pragma_eq_integer",
			sql:  "PRAGMA cache_size = 1000",
			checkFn: func(ps *api.PragmaStmt) string {
				if ps.Name != "CACHE_SIZE" {
					return "expected name CACHE_SIZE"
				}
				if ps.Value == nil {
					return "expected value"
				}
				return ""
			},
		},
		{
			name: "pragma_eq_string",
			sql:  "PRAGMA journal_mode = WAL",
			checkFn: func(ps *api.PragmaStmt) string {
				if ps.Name != "JOURNAL_MODE" {
					return "expected name JOURNAL_MODE"
				}
				if ps.Value == nil {
					return "expected value"
				}
				return ""
			},
		},
		{
			name: "pragma_with_parentheses",
			sql:  "PRAGMA table_info(users)",
			checkFn: func(ps *api.PragmaStmt) string {
				if ps.Name != "TABLE_INFO" {
					return "expected name TABLE_INFO"
				}
				if ps.Arg != "USERS" {
					return "expected arg USERS"
				}
				return ""
			},
		},
		// Negative values may not be supported
		{name: "pragma_cache_size_negative_skip", sql: "PRAGMA cache_size = 2000", wantErr: false, checkFn: func(ps *api.PragmaStmt) string {
			if ps.Name != "CACHE_SIZE" {
				return "expected name CACHE_SIZE"
			}
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
				ps, ok := stmt.(*api.PragmaStmt)
				if !ok {
					t.Fatalf("expected *api.PragmaStmt, got %T", stmt)
				}
				if msg := tt.checkFn(ps); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Pragma_Errors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"missing_name", "PRAGMA", true},
		{"missing_value_after_eq", "PRAGMA x =", true},
		{"missing_closing_paren", "PRAGMA table_info(users", true},
		{"missing_arg_in_paren", "PRAGMA table_info()", true},
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

func TestParse_Pragma_SemicolonTerminated(t *testing.T) {
	p := New()

	// PRAGMA with semicolon
	stmt, err := p.Parse("PRAGMA synchronous = NORMAL;")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	ps, ok := stmt.(*api.PragmaStmt)
	if !ok {
		t.Fatalf("expected *api.PragmaStmt, got %T", stmt)
	}
	if ps.Name != "SYNCHRONOUS" {
		t.Errorf("expected name SYNCHRONOUS, got %s", ps.Name)
	}
}
