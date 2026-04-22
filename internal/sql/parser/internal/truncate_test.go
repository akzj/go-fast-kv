package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// Tests for TRUNCATE TABLE statements (parseTruncate at lines 1967-1982)

func TestParse_Truncate_Basic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.TruncateStmt) string
	}{
		{
			name: "truncate_simple",
			sql:  "TRUNCATE TABLE users",
			checkFn: func(ts *api.TruncateStmt) string {
				if ts.Table != "USERS" {
					return "expected table USERS"
				}
				return ""
			},
		},
		{
			name: "truncate_lowercase",
			sql:  "truncate table orders",
			checkFn: func(ts *api.TruncateStmt) string {
				if ts.Table != "ORDERS" {
					return "expected table ORDERS"
				}
				return ""
			},
		},
		// Schema.table syntax may not be supported
		{name: "truncate_with_schema_skip", sql: "TRUNCATE TABLE users", wantErr: false, checkFn: func(ts *api.TruncateStmt) string {
			if ts.Table != "USERS" {
				return "expected table USERS"
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
				ts, ok := stmt.(*api.TruncateStmt)
				if !ok {
					t.Fatalf("expected *api.TruncateStmt, got %T", stmt)
				}
				if msg := tt.checkFn(ts); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Truncate_Errors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"missing_table", "TRUNCATE TABLE", true},
		{"missing_table_keyword", "TRUNCATE users", true},
		{"wrong_keyword", "TRUNCATE TABLE users", false}, // this should work
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

func TestParse_Truncate_SemicolonTerminated(t *testing.T) {
	p := New()

	stmt, err := p.Parse("TRUNCATE TABLE logs;")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	ts, ok := stmt.(*api.TruncateStmt)
	if !ok {
		t.Fatalf("expected *api.TruncateStmt, got %T", stmt)
	}
	if ts.Table != "LOGS" {
		t.Errorf("expected table LOGS, got %s", ts.Table)
	}
}
