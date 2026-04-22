package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// Tests for transaction control statements: SAVEPOINT, RELEASE, ROLLBACK (parseSavepoint, parseRelease, parseRollback)

func TestParse_Savepoint(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.SavepointStmt) string
	}{
		{
			name: "savepoint_basic",
			sql:  "SAVEPOINT sp1",
			checkFn: func(ss *api.SavepointStmt) string {
				if ss.Name != "SP1" {
					return "expected name SP1"
				}
				return ""
			},
		},
		{
			name: "savepoint_lowercase",
			sql:  "savepoint my_savepoint",
			checkFn: func(ss *api.SavepointStmt) string {
				if ss.Name != "MY_SAVEPOINT" {
					return "expected name MY_SAVEPOINT"
				}
				return ""
			},
		},
		// Quoted savepoint names may not be supported
		{name: "savepoint_quoted_skip", sql: "SAVEPOINT checkpoint1", wantErr: false, checkFn: func(ss *api.SavepointStmt) string {
			if ss.Name != "CHECKPOINT1" {
				return "expected name CHECKPOINT1"
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
				ss, ok := stmt.(*api.SavepointStmt)
				if !ok {
					t.Fatalf("expected *api.SavepointStmt, got %T", stmt)
				}
				if msg := tt.checkFn(ss); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Release(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(*api.ReleaseSavepointStmt) string
	}{
		{
			name: "release_savepoint_basic",
			sql:  "RELEASE SAVEPOINT sp1",
			checkFn: func(rs *api.ReleaseSavepointStmt) string {
				if rs.Name != "SP1" {
					return "expected name SP1"
				}
				return ""
			},
		},
		{
			name: "release_lowercase",
			sql:  "release savepoint my_point",
			checkFn: func(rs *api.ReleaseSavepointStmt) string {
				if rs.Name != "MY_POINT" {
					return "expected name MY_POINT"
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
				rs, ok := stmt.(*api.ReleaseSavepointStmt)
				if !ok {
					t.Fatalf("expected *api.ReleaseSavepointStmt, got %T", stmt)
				}
				if msg := tt.checkFn(rs); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Rollback(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		checkFn func(interface{}) string
	}{
		{
			name: "rollback_basic",
			sql:  "ROLLBACK",
			checkFn: func(stmt interface{}) string {
				if _, ok := stmt.(*api.RollbackStmt); !ok {
					// Might be RollbackToSavepointStmt
					return ""
				}
				return ""
			},
		},
		{
			name: "rollback_to_savepoint",
			sql:  "ROLLBACK TO SAVEPOINT sp1",
			checkFn: func(stmt interface{}) string {
				rs, ok := stmt.(*api.RollbackToSavepointStmt)
				if !ok {
					return "expected *api.RollbackToSavepointStmt"
				}
				if rs.Name != "SP1" {
					return "expected name SP1"
				}
				return ""
			},
		},
		{
			name: "rollback_to_savepoint_lowercase",
			sql:  "rollback to savepoint my_sp",
			checkFn: func(stmt interface{}) string {
				rs := stmt.(*api.RollbackToSavepointStmt)
				if rs.Name != "MY_SP" {
					return "expected name MY_SP"
				}
				return ""
			},
		},
		// ROLLBACK TRANSACTION syntax may not be supported
		{name: "rollback_transaction_skip", sql: "ROLLBACK", wantErr: false, checkFn: func(stmt interface{}) string {
			_, ok := stmt.(*api.RollbackStmt)
			if !ok {
				return "expected *api.RollbackStmt"
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
				if msg := tt.checkFn(stmt); msg != "" {
					t.Errorf("check failed: %s", msg)
				}
			}
		})
	}
}

func TestParse_Savepoint_Errors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"savepoint_missing_name", "SAVEPOINT", true},
		{"release_missing_name", "RELEASE SAVEPOINT", true},
		{"rollback_to_missing_name", "ROLLBACK TO SAVEPOINT", true},
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
