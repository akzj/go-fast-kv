package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

func TestParseBegin(t *testing.T) {
	tests := []struct {
		sql    string
		stmt   api.Statement
	}{
		{"BEGIN", &api.BeginStmt{}},
		{"BEGIN;", &api.BeginStmt{}},
		{"begin", &api.BeginStmt{}},
		{"begin;", &api.BeginStmt{}},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			p := New()
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.sql, err)
			}
			if _, ok := stmt.(*api.BeginStmt); !ok {
				t.Fatalf("Parse(%q) returned %T, want *api.BeginStmt", tt.sql, stmt)
			}
		})
	}
}

func TestParseCommit(t *testing.T) {
	tests := []struct {
		sql  string
		stmt api.Statement
	}{
		{"COMMIT", &api.CommitStmt{}},
		{"COMMIT;", &api.CommitStmt{}},
		{"commit", &api.CommitStmt{}},
		{"commit;", &api.CommitStmt{}},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			p := New()
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.sql, err)
			}
			if _, ok := stmt.(*api.CommitStmt); !ok {
				t.Fatalf("Parse(%q) returned %T, want *api.CommitStmt", tt.sql, stmt)
			}
		})
	}
}

func TestParseRollback(t *testing.T) {
	tests := []struct {
		sql  string
		stmt api.Statement
	}{
		{"ROLLBACK", &api.RollbackStmt{}},
		{"ROLLBACK;", &api.RollbackStmt{}},
		{"rollback", &api.RollbackStmt{}},
		{"rollback;", &api.RollbackStmt{}},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			p := New()
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.sql, err)
			}
			if _, ok := stmt.(*api.RollbackStmt); !ok {
				t.Fatalf("Parse(%q) returned %T, want *api.RollbackStmt", tt.sql, stmt)
			}
		})
	}
}

func TestTransactionStatements(t *testing.T) {
	p := New()

	// Test BEGIN
	stmt, err := p.Parse("BEGIN")
	if err != nil {
		t.Fatalf("Parse(BEGIN) error: %v", err)
	}
	if _, ok := stmt.(*api.BeginStmt); !ok {
		t.Errorf("Parse(BEGIN) = %T, want *api.BeginStmt", stmt)
	}

	// Test COMMIT
	stmt, err = p.Parse("COMMIT")
	if err != nil {
		t.Fatalf("Parse(COMMIT) error: %v", err)
	}
	if _, ok := stmt.(*api.CommitStmt); !ok {
		t.Errorf("Parse(COMMIT) = %T, want *api.CommitStmt", stmt)
	}

	// Test ROLLBACK
	stmt, err = p.Parse("ROLLBACK")
	if err != nil {
		t.Fatalf("Parse(ROLLBACK) error: %v", err)
	}
	if _, ok := stmt.(*api.RollbackStmt); !ok {
		t.Errorf("Parse(ROLLBACK) = %T, want *api.RollbackStmt", stmt)
	}
}
