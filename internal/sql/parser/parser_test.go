package parser

import (
	"testing"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "SELECT * FROM users",
			expected: []Token{
				{Type: TokenKeyword, Value: "SELECT"},
				{Type: TokenOperator, Value: "*"},
				{Type: TokenKeyword, Value: "FROM"},
				{Type: TokenIdent, Value: "USERS"},
				{Type: TokenEOF},
			},
		},
		{
			input: "SELECT id, name FROM users WHERE age > 18",
			expected: []Token{
				{Type: TokenKeyword, Value: "SELECT"},
				{Type: TokenIdent, Value: "ID"},
				{Type: TokenPunct, Value: ","},
				{Type: TokenIdent, Value: "NAME"},
				{Type: TokenKeyword, Value: "FROM"},
				{Type: TokenIdent, Value: "USERS"},
				{Type: TokenKeyword, Value: "WHERE"},
				{Type: TokenIdent, Value: "AGE"},
				{Type: TokenOperator, Value: ">"},
				{Type: TokenInt, Value: "18"},
				{Type: TokenEOF},
			},
		},
		{
			input: "INSERT INTO users VALUES (1, 'Alice', 30)",
			expected: []Token{
				{Type: TokenKeyword, Value: "INSERT"},
				{Type: TokenKeyword, Value: "INTO"},
				{Type: TokenIdent, Value: "USERS"},
				{Type: TokenKeyword, Value: "VALUES"},
				{Type: TokenPunct, Value: "("},
				{Type: TokenInt, Value: "1"},
				{Type: TokenPunct, Value: ","},
				{Type: TokenString, Value: "Alice"},
				{Type: TokenPunct, Value: ","},
				{Type: TokenInt, Value: "30"},
				{Type: TokenPunct, Value: ")"},
				{Type: TokenEOF},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := NewLexer(tt.input)
			for i, exp := range tt.expected {
				got := lex.Next()
				if got.Type != exp.Type || got.Value != exp.Value {
					t.Errorf("token %d: got %s, want %s", i, got, exp)
				}
			}
		})
	}
}

func TestParseSelect(t *testing.T) {
	p := New()

	tests := []struct {
		sql      string
		expected *SelectStmt
	}{
		{
			sql: "SELECT * FROM users",
			expected: &SelectStmt{
				Columns: []string{"*"},
				Table:   "USERS",
			},
		},
		{
			sql: "SELECT id, name FROM users",
			expected: &SelectStmt{
				Columns: []string{"ID", "NAME"},
				Table:   "USERS",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			s, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", stmt)
			}
			if s.Table != tt.expected.Table {
				t.Errorf("table: got %s, want %s", s.Table, tt.expected.Table)
			}
			if len(s.Columns) != len(tt.expected.Columns) {
				t.Errorf("columns: got %v, want %v", s.Columns, tt.expected.Columns)
			}
		})
	}
}

func TestParseSelectWhere(t *testing.T) {
	p := New()
	stmt, err := p.Parse("SELECT * FROM users WHERE age > 18")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*SelectStmt)
	if s.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if s.Where.Column != "AGE" {
		t.Errorf("column: got %s, want AGE", s.Where.Column)
	}
	if s.Where.Op != ">" {
		t.Errorf("op: got %s, want >", s.Where.Op)
	}
	if s.Where.Value.AsInt() != 18 {
		t.Errorf("value: got %v, want 18", s.Where.Value.AsInt())
	}
}

func TestParseSelectOrderByLimit(t *testing.T) {
	p := New()
	stmt, err := p.Parse("SELECT * FROM users ORDER BY name DESC LIMIT 10")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*SelectStmt)
	if len(s.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(s.OrderBy))
	}
	if s.OrderBy[0].Column != "NAME" {
		t.Errorf("order column: got %s, want NAME", s.OrderBy[0].Column)
	}
	if s.OrderBy[0].Ascending {
		t.Error("expected DESC, got ASC")
	}
	if s.Limit != 10 {
		t.Errorf("limit: got %d, want 10", s.Limit)
	}
}

func TestParseInsert(t *testing.T) {
	p := New()
	stmt, err := p.Parse("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*InsertStmt)
	if s.Table != "USERS" {
		t.Errorf("table: got %s, want USERS", s.Table)
	}
	if len(s.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(s.Values))
	}
	if s.Values[0].AsInt() != 1 {
		t.Errorf("value 0: got %d, want 1", s.Values[0].AsInt())
	}
	if s.Values[1].AsText() != "Alice" {
		t.Errorf("value 1: got %s, want Alice", s.Values[1].AsText())
	}
	if s.Values[2].AsInt() != 30 {
		t.Errorf("value 2: got %d, want 30", s.Values[2].AsInt())
	}
}

func TestParseCreateTable(t *testing.T) {
	p := New()
	stmt, err := p.Parse("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*CreateTableStmt)
	if s.Name != "USERS" {
		t.Errorf("table name: got %s, want USERS", s.Name)
	}
	if len(s.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(s.Columns))
	}
	if s.Columns[0].Name != "ID" || s.Columns[0].Type != "INT" {
		t.Errorf("column 0: got %s %s, want ID INT", s.Columns[0].Name, s.Columns[0].Type)
	}
	if s.Columns[1].Name != "NAME" || s.Columns[1].Type != "TEXT" {
		t.Errorf("column 1: got %s %s, want NAME TEXT", s.Columns[1].Name, s.Columns[1].Type)
	}
}

func TestParseCreateIndex(t *testing.T) {
	p := New()
	stmt, err := p.Parse("CREATE INDEX idx_users_age ON users (age)")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*CreateIndexStmt)
	if s.IndexName != "IDX_USERS_AGE" {
		t.Errorf("index name: got %s, want IDX_USERS_AGE", s.IndexName)
	}
	if s.TableName != "USERS" {
		t.Errorf("table name: got %s, want USERS", s.TableName)
	}
	if s.Column != "AGE" {
		t.Errorf("column: got %s, want AGE", s.Column)
	}
}

func TestParseUpdate(t *testing.T) {
	p := New()
	stmt, err := p.Parse("UPDATE users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*UpdateStmt)
	if s.Table != "USERS" {
		t.Errorf("table: got %s, want USERS", s.Table)
	}
	if s.Column != "AGE" {
		t.Errorf("column: got %s, want AGE", s.Column)
	}
	if s.Where == nil || s.Where.Column != "ID" {
		t.Error("expected WHERE id = 1")
	}
}

func TestParseDelete(t *testing.T) {
	p := New()
	stmt, err := p.Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*DeleteStmt)
	if s.Table != "USERS" {
		t.Errorf("table: got %s, want USERS", s.Table)
	}
	if s.Where == nil || s.Where.Column != "ID" {
		t.Error("expected WHERE id = 1")
	}
}

func TestParseDropIndex(t *testing.T) {
	p := New()
	stmt, err := p.Parse("DROP INDEX idx_users_age ON users")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	s := stmt.(*DropIndexStmt)
	if s.IndexName != "IDX_USERS_AGE" {
		t.Errorf("index name: got %s, want IDX_USERS_AGE", s.IndexName)
	}
	if s.TableName != "USERS" {
		t.Errorf("table name: got %s, want USERS", s.TableName)
	}
}
