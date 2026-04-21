package internal

import (
	"testing"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

func TestParseParamRef(t *testing.T) {
	p := New()
	
	// Test $1 in SELECT
	stmt, err := p.Parse("SELECT * FROM t WHERE id = $1")
	if err != nil {
		t.Fatalf("Parse SELECT $1 failed: %v", err)
	}
	t.Logf("SELECT parsed: %T", stmt)
	
	// Test $1 in UPDATE SET
	stmt2, err := p.Parse("UPDATE t SET age = $1 WHERE id = $2")
	if err != nil {
		t.Fatalf("Parse UPDATE failed: %v", err)
	}
	if update, ok := stmt2.(*parserapi.UpdateStmt); ok {
		for i, a := range update.Assignments {
			if pr, ok := a.Value.(*parserapi.ParamRef); ok {
				t.Logf("  Assignment %d: col=%s, ParamRef.Index=%d", i, a.Column, pr.Index)
			}
		}
	}
	
	// Test ? placeholder (sequential)
	stmt3, err := p.Parse("INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Parse INSERT with ? failed: %v", err)
	}
	t.Logf("INSERT parsed: %T", stmt3)
	if insert, ok := stmt3.(*parserapi.InsertStmt); ok {
		for i, row := range insert.Values {
			for j, expr := range row {
				if pr, ok := expr.(*parserapi.ParamRef); ok {
					t.Logf("  Row %d, Col %d: ParamRef.Index=%d", i, j, pr.Index)
				}
			}
		}
	}
	
	// Test multiple ? in UPDATE
	stmt4, err := p.Parse("UPDATE t SET age = ? WHERE id = ?")
	if err != nil {
		t.Fatalf("Parse UPDATE with ? failed: %v", err)
	}
	if update, ok := stmt4.(*parserapi.UpdateStmt); ok {
		for i, a := range update.Assignments {
			if pr, ok := a.Value.(*parserapi.ParamRef); ok {
				t.Logf("  Assignment %d: ParamRef.Index=%d", i, pr.Index)
			}
		}
	}
}
