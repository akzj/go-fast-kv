package internal

import (
	"testing"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

func newParser() api.Parser {
	return New()
}

// ─── Lexer Tests ──────────────────────────────────────────────────

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		expected api.TokenType
	}{
		{"SELECT", api.TokSelect},
		{"from", api.TokFrom},
		{"Where", api.TokWhere},
		{"INSERT", api.TokInsert},
		{"INTO", api.TokInto},
		{"VALUES", api.TokValues},
		{"DELETE", api.TokDelete},
		{"UPDATE", api.TokUpdate},
		{"SET", api.TokSet},
		{"CREATE", api.TokCreate},
		{"DROP", api.TokDrop},
		{"TABLE", api.TokTable},
		{"INDEX", api.TokIndex},
		{"ON", api.TokOn},
		{"AND", api.TokAnd},
		{"OR", api.TokOr},
		{"NOT", api.TokNot},
		{"NULL", api.TokNull},
		{"IS", api.TokIs},
		{"ORDER", api.TokOrder},
		{"BY", api.TokBy},
		{"ASC", api.TokAsc},
		{"DESC", api.TokDesc},
		{"LIMIT", api.TokLimit},
		{"INT", api.TokIntKw},
		{"INTEGER", api.TokInteger2},
		{"TEXT", api.TokTextKw},
		{"FLOAT", api.TokFloatKw},
		{"BLOB", api.TokBlobKw},
		{"PRIMARY", api.TokPrimary},
		{"KEY", api.TokKey},
		{"UNIQUE", api.TokUnique},
		{"IF", api.TokIf},
		{"EXISTS", api.TokExists},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := newLexer(tt.input)
			tok := lex.nextToken()
			if tok.Type != tt.expected {
				t.Errorf("keyword %q: expected token type %d, got %d (literal=%q)", tt.input, tt.expected, tok.Type, tok.Literal)
			}
		})
	}
}

func TestLexer_Operators(t *testing.T) {
	tests := []struct {
		input    string
		expected api.TokenType
		literal  string
	}{
		{"=", api.TokEQ, "="},
		{"!=", api.TokNE, "!="},
		{"<>", api.TokNE, "<>"},
		{"<", api.TokLT, "<"},
		{"<=", api.TokLE, "<="},
		{">", api.TokGT, ">"},
		{">=", api.TokGE, ">="},
		{"+", api.TokPlus, "+"},
		{"-", api.TokMinus, "-"},
		{"*", api.TokStar, "*"},
		{",", api.TokComma, ","},
		{"(", api.TokLParen, "("},
		{")", api.TokRParen, ")"},
		{";", api.TokSemicolon, ";"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := newLexer(tt.input)
			tok := lex.nextToken()
			if tok.Type != tt.expected {
				t.Errorf("operator %q: expected type %d, got %d", tt.input, tt.expected, tok.Type)
			}
			if tok.Literal != tt.literal {
				t.Errorf("operator %q: expected literal %q, got %q", tt.input, tt.literal, tok.Literal)
			}
		})
	}
}

func TestLexer_Literals(t *testing.T) {
	t.Run("integer", func(t *testing.T) {
		lex := newLexer("42")
		tok := lex.nextToken()
		if tok.Type != api.TokInteger || tok.Literal != "42" {
			t.Errorf("expected TokInteger '42', got type=%d literal=%q", tok.Type, tok.Literal)
		}
	})

	t.Run("float", func(t *testing.T) {
		lex := newLexer("3.14")
		tok := lex.nextToken()
		if tok.Type != api.TokFloat || tok.Literal != "3.14" {
			t.Errorf("expected TokFloat '3.14', got type=%d literal=%q", tok.Type, tok.Literal)
		}
	})

	t.Run("string", func(t *testing.T) {
		lex := newLexer("'hello world'")
		tok := lex.nextToken()
		if tok.Type != api.TokString || tok.Literal != "hello world" {
			t.Errorf("expected TokString 'hello world', got type=%d literal=%q", tok.Type, tok.Literal)
		}
	})

	t.Run("identifier", func(t *testing.T) {
		lex := newLexer("my_table")
		tok := lex.nextToken()
		if tok.Type != api.TokIdent || tok.Literal != "MY_TABLE" {
			t.Errorf("expected TokIdent 'MY_TABLE', got type=%d literal=%q", tok.Type, tok.Literal)
		}
	})
}

func TestLexer_StringEscape(t *testing.T) {
	lex := newLexer("'it''s a test'")
	tok := lex.nextToken()
	if tok.Type != api.TokString {
		t.Fatalf("expected TokString, got %d", tok.Type)
	}
	if tok.Literal != "it's a test" {
		t.Errorf("expected \"it's a test\", got %q", tok.Literal)
	}
}

func TestLexer_LineComment(t *testing.T) {
	lex := newLexer("SELECT -- this is a comment\nname")
	tok1 := lex.nextToken()
	if tok1.Type != api.TokSelect {
		t.Errorf("expected SELECT, got %d", tok1.Type)
	}
	tok2 := lex.nextToken()
	if tok2.Type != api.TokIdent || tok2.Literal != "NAME" {
		t.Errorf("expected IDENT 'NAME', got type=%d literal=%q", tok2.Type, tok2.Literal)
	}
}

// ─── Parser Tests: DDL ────────────────────────────────────────────

func TestParse_CreateTable(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE TABLE users (id INT, name TEXT, age INT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		if ct.Table != "USERS" {
			t.Errorf("table name: expected USERS, got %s", ct.Table)
		}
		if len(ct.Columns) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
		}
		if ct.Columns[0].Name != "ID" || ct.Columns[0].TypeName != "INT" {
			t.Errorf("col 0: expected ID INT, got %s %s", ct.Columns[0].Name, ct.Columns[0].TypeName)
		}
		if ct.Columns[1].Name != "NAME" || ct.Columns[1].TypeName != "TEXT" {
			t.Errorf("col 1: expected NAME TEXT, got %s %s", ct.Columns[1].Name, ct.Columns[1].TypeName)
		}
	})

	t.Run("with_inline_pk", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct := stmt.(*api.CreateTableStmt)
		if ct.PrimaryKey != "ID" {
			t.Errorf("expected PK 'ID', got %q", ct.PrimaryKey)
		}
	})

	t.Run("with_table_pk", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct := stmt.(*api.CreateTableStmt)
		if ct.PrimaryKey != "ID" {
			t.Errorf("expected PK 'ID', got %q", ct.PrimaryKey)
		}
	})

	t.Run("if_not_exists", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE TABLE IF NOT EXISTS users (id INT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct := stmt.(*api.CreateTableStmt)
		if !ct.IfNotExists {
			t.Error("expected IfNotExists=true")
		}
	})

	t.Run("integer_type", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE TABLE t (id INTEGER)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct := stmt.(*api.CreateTableStmt)
		if ct.Columns[0].TypeName != "INT" {
			t.Errorf("expected INT, got %s", ct.Columns[0].TypeName)
		}
	})
}

func TestParse_DropTable(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("DROP TABLE users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		dt, ok := stmt.(*api.DropTableStmt)
		if !ok {
			t.Fatalf("expected DropTableStmt, got %T", stmt)
		}
		if dt.Table != "USERS" {
			t.Errorf("expected USERS, got %s", dt.Table)
		}
		if dt.IfExists {
			t.Error("expected IfExists=false")
		}
	})

	t.Run("if_exists", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("DROP TABLE IF EXISTS users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		dt := stmt.(*api.DropTableStmt)
		if !dt.IfExists {
			t.Error("expected IfExists=true")
		}
	})
}

func TestParse_CreateIndex(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE INDEX idx_age ON users (age)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ci, ok := stmt.(*api.CreateIndexStmt)
		if !ok {
			t.Fatalf("expected CreateIndexStmt, got %T", stmt)
		}
		if ci.Index != "IDX_AGE" {
			t.Errorf("index name: expected IDX_AGE, got %s", ci.Index)
		}
		if ci.Table != "USERS" {
			t.Errorf("table name: expected USERS, got %s", ci.Table)
		}
		if ci.Column != "AGE" {
			t.Errorf("column: expected AGE, got %s", ci.Column)
		}
		if ci.Unique {
			t.Error("expected Unique=false")
		}
	})

	t.Run("unique", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE UNIQUE INDEX idx_email ON users (email)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ci := stmt.(*api.CreateIndexStmt)
		if !ci.Unique {
			t.Error("expected Unique=true")
		}
	})

	t.Run("if_not_exists", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("CREATE INDEX IF NOT EXISTS idx_age ON users (age)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ci := stmt.(*api.CreateIndexStmt)
		if !ci.IfNotExists {
			t.Error("expected IfNotExists=true")
		}
	})
}

func TestParse_DropIndex(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("DROP INDEX idx_age ON users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		di, ok := stmt.(*api.DropIndexStmt)
		if !ok {
			t.Fatalf("expected DropIndexStmt, got %T", stmt)
		}
		if di.Index != "IDX_AGE" || di.Table != "USERS" {
			t.Errorf("expected IDX_AGE ON USERS, got %s ON %s", di.Index, di.Table)
		}
	})

	t.Run("if_exists", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("DROP INDEX IF EXISTS idx_age ON users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		di := stmt.(*api.DropIndexStmt)
		if !di.IfExists {
			t.Error("expected IfExists=true")
		}
	})
}

func TestParse_AlterTable(t *testing.T) {
	t.Run("add_column_basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col INT")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at, ok := stmt.(*api.AlterTableStmt)
		if !ok {
			t.Fatalf("expected AlterTableStmt, got %T", stmt)
		}
		if at.Table != "T" {
			t.Errorf("table: expected T, got %s", at.Table)
		}
		if at.Operation != api.AlterAddColumn {
			t.Errorf("operation: expected AlterAddColumn, got %v", at.Operation)
		}
		if at.Column != "COL" {
			t.Errorf("column: expected COL, got %s", at.Column)
		}
		if at.TypeName != "INT" {
			t.Errorf("type: expected INT, got %s", at.TypeName)
		}
	})

	t.Run("add_column_text", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE users ADD COLUMN name TEXT")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if at.TypeName != "TEXT" {
			t.Errorf("type: expected TEXT, got %s", at.TypeName)
		}
	})

	t.Run("add_column_not_null", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col TEXT NOT NULL")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if !at.NotNull {
			t.Error("expected NotNull=true")
		}
	})

	t.Run("add_column_unique", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col TEXT UNIQUE")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if !at.Unique {
			t.Error("expected Unique=true")
		}
	})

	t.Run("add_column_not_null_unique", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col TEXT NOT NULL UNIQUE")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if !at.NotNull || !at.Unique {
			t.Errorf("expected NotNull=true and Unique=true, got NotNull=%v Unique=%v", at.NotNull, at.Unique)
		}
	})

	t.Run("add_column_integer", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col INTEGER")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if at.TypeName != "INTEGER" {
			t.Errorf("type: expected INTEGER, got %s", at.TypeName)
		}
	})

	t.Run("add_column_float", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col FLOAT")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if at.TypeName != "FLOAT" {
			t.Errorf("type: expected FLOAT, got %s", at.TypeName)
		}
	})

	t.Run("add_column_blob", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE t ADD COLUMN col BLOB")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if at.TypeName != "BLOB" {
			t.Errorf("type: expected BLOB, got %s", at.TypeName)
		}
	})

	t.Run("drop_column", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE users DROP COLUMN age")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at, ok := stmt.(*api.AlterTableStmt)
		if !ok {
			t.Fatalf("expected AlterTableStmt, got %T", stmt)
		}
		if at.Operation != api.AlterDropColumn {
			t.Errorf("operation: expected AlterDropColumn, got %v", at.Operation)
		}
		if at.Column != "AGE" {
			t.Errorf("column: expected AGE, got %s", at.Column)
		}
	})

	t.Run("rename_column", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE users RENAME COLUMN old_name TO new_name")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at, ok := stmt.(*api.AlterTableStmt)
		if !ok {
			t.Fatalf("expected AlterTableStmt, got %T", stmt)
		}
		if at.Operation != api.AlterRenameColumn {
			t.Errorf("operation: expected AlterRenameColumn, got %v", at.Operation)
		}
		if at.Column != "OLD_NAME" {
			t.Errorf("column: expected OLD_NAME, got %s", at.Column)
		}
		if at.ColumnNew != "NEW_NAME" {
			t.Errorf("columnNew: expected NEW_NAME, got %s", at.ColumnNew)
		}
	})

	t.Run("rename_to", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("ALTER TABLE old_name RENAME TO new_name")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at, ok := stmt.(*api.AlterTableStmt)
		if !ok {
			t.Fatalf("expected AlterTableStmt, got %T", stmt)
		}
		if at.Operation != api.AlterRenameTable {
			t.Errorf("operation: expected AlterRenameTable, got %v", at.Operation)
		}
		if at.Table != "OLD_NAME" {
			t.Errorf("table: expected OLD_NAME, got %s", at.Table)
		}
		if at.TableNew != "NEW_NAME" {
			t.Errorf("tableNew: expected NEW_NAME, got %s", at.TableNew)
		}
	})

	t.Run("case_insensitive", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("alter table t add column col int")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		at := stmt.(*api.AlterTableStmt)
		if at.Table != "T" || at.Column != "COL" {
			t.Errorf("expected T and COL (uppercased), got %s and %s", at.Table, at.Column)
		}
	})

	t.Run("error_missing_table", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("ALTER TABLE ADD COLUMN col INT")
		if err == nil {
			t.Error("expected parse error for missing table name")
		}
	})

	t.Run("error_missing_column", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("ALTER TABLE t ADD INT")
		if err == nil {
			t.Error("expected parse error for missing COLUMN keyword")
		}
	})

	t.Run("error_missing_type", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("ALTER TABLE t ADD COLUMN col")
		if err == nil {
			t.Error("expected parse error for missing type")
		}
	})
}

// ─── Parser Tests: DML ────────────────────────────────────────────

func TestParse_Insert(t *testing.T) {
	t.Run("single_row", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO users VALUES (1, 'Alice', 30)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if ins.Table != "USERS" {
			t.Errorf("table: expected USERS, got %s", ins.Table)
		}
		if len(ins.Columns) != 0 {
			t.Errorf("expected no columns, got %d", len(ins.Columns))
		}
		if len(ins.Values) != 1 {
			t.Fatalf("expected 1 row, got %d", len(ins.Values))
		}
		if len(ins.Values[0]) != 3 {
			t.Fatalf("expected 3 values, got %d", len(ins.Values[0]))
		}
	})

	t.Run("with_columns", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO users (name, age) VALUES ('Bob', 25)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		if len(ins.Columns) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(ins.Columns))
		}
		if ins.Columns[0] != "NAME" || ins.Columns[1] != "AGE" {
			t.Errorf("columns: expected [NAME AGE], got %v", ins.Columns)
		}
	})

	t.Run("multiple_rows", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		if len(ins.Values) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(ins.Values))
		}
	})

	t.Run("null_value", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO users VALUES (1, NULL)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		lit, ok := ins.Values[0][1].(*api.Literal)
		if !ok {
			t.Fatalf("expected Literal, got %T", ins.Values[0][1])
		}
		if !lit.Value.IsNull {
			t.Error("expected NULL value")
		}
	})
}

func TestParse_Select(t *testing.T) {
	t.Run("star", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if len(sel.Columns) != 1 {
			t.Fatalf("expected 1 column, got %d", len(sel.Columns))
		}
		if _, ok := sel.Columns[0].Expr.(*api.StarExpr); !ok {
			t.Errorf("expected StarExpr, got %T", sel.Columns[0].Expr)
		}
		if sel.Table != "USERS" {
			t.Errorf("table: expected USERS, got %s", sel.Table)
		}
	})

	t.Run("columns", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT name, age FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
		}
		col0, ok := sel.Columns[0].Expr.(*api.ColumnRef)
		if !ok {
			t.Fatalf("col 0: expected ColumnRef, got %T", sel.Columns[0].Expr)
		}
		if col0.Column != "NAME" {
			t.Errorf("col 0: expected NAME, got %s", col0.Column)
		}
	})

	t.Run("where", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users WHERE age > 18")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Where == nil {
			t.Fatal("expected WHERE clause")
		}
		bin, ok := sel.Where.(*api.BinaryExpr)
		if !ok {
			t.Fatalf("expected BinaryExpr, got %T", sel.Where)
		}
		if bin.Op != api.BinGT {
			t.Errorf("expected GT, got %d", bin.Op)
		}
	})

	t.Run("order_by", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users ORDER BY age DESC")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.OrderBy) == 0 {
			t.Fatal("expected ORDER BY clause")
		}
		if sel.OrderBy[0].Column != "AGE" {
			t.Errorf("expected AGE, got %s", sel.OrderBy[0].Column)
		}
		if !sel.OrderBy[0].Desc {
			t.Error("expected DESC=true")
		}
	})

	t.Run("limit", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users LIMIT 10")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Limit == nil {
			t.Fatal("expected LIMIT clause")
		}
		lit, ok := sel.Limit.(*api.Literal)
		if !ok {
			t.Fatalf("expected Literal, got %T", sel.Limit)
		}
		if lit.Value.Int != 10 {
			t.Errorf("expected 10, got %d", lit.Value.Int)
		}
	})

	t.Run("full", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT name, age FROM users WHERE age > 18 ORDER BY name ASC LIMIT 100")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) != 2 {
			t.Errorf("expected 2 columns, got %d", len(sel.Columns))
		}
		if sel.Where == nil {
			t.Error("expected WHERE")
		}
		if len(sel.OrderBy) == 0 || sel.OrderBy[0].Column != "NAME" || sel.OrderBy[0].Desc {
			t.Error("expected ORDER BY NAME ASC")
		}
		if sel.Limit == nil {
			t.Error("expected LIMIT")
		}
	})
}

func TestParse_Delete(t *testing.T) {
	t.Run("with_where", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("DELETE FROM users WHERE id = 5")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		del, ok := stmt.(*api.DeleteStmt)
		if !ok {
			t.Fatalf("expected DeleteStmt, got %T", stmt)
		}
		if del.Table != "USERS" {
			t.Errorf("table: expected USERS, got %s", del.Table)
		}
		if del.Where == nil {
			t.Fatal("expected WHERE clause")
		}
	})

	t.Run("without_where", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("DELETE FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		del := stmt.(*api.DeleteStmt)
		if del.Where != nil {
			t.Error("expected no WHERE clause")
		}
	})
}

func TestParse_Update(t *testing.T) {
	t.Run("single_set", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("UPDATE users SET age = 31 WHERE id = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		upd, ok := stmt.(*api.UpdateStmt)
		if !ok {
			t.Fatalf("expected UpdateStmt, got %T", stmt)
		}
		if upd.Table != "USERS" {
			t.Errorf("table: expected USERS, got %s", upd.Table)
		}
		if len(upd.Assignments) != 1 {
			t.Fatalf("expected 1 assignment, got %d", len(upd.Assignments))
		}
		if upd.Assignments[0].Column != "AGE" {
			t.Errorf("expected AGE, got %s", upd.Assignments[0].Column)
		}
		lit, ok := upd.Assignments[0].Value.(*api.Literal)
		if !ok {
			t.Fatalf("expected Literal, got %T", upd.Assignments[0].Value)
		}
		if lit.Value.Int != 31 {
			t.Errorf("expected 31, got %d", lit.Value.Int)
		}
	})

	t.Run("multiple_set", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("UPDATE users SET name = 'Bob', age = 25 WHERE id = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		upd := stmt.(*api.UpdateStmt)
		if len(upd.Assignments) != 2 {
			t.Fatalf("expected 2 assignments, got %d", len(upd.Assignments))
		}
	})

	t.Run("without_where", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("UPDATE users SET age = 0")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		upd := stmt.(*api.UpdateStmt)
		if upd.Where != nil {
			t.Error("expected no WHERE clause")
		}
	})
}

// ─── Expression Tests ─────────────────────────────────────────────

func TestParse_WhereExpressions(t *testing.T) {
	t.Run("and", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE a = 1 AND b = 2")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		bin, ok := sel.Where.(*api.BinaryExpr)
		if !ok {
			t.Fatalf("expected BinaryExpr, got %T", sel.Where)
		}
		if bin.Op != api.BinAnd {
			t.Errorf("expected AND, got %d", bin.Op)
		}
	})

	t.Run("or", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE a = 1 OR b = 2")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		bin := sel.Where.(*api.BinaryExpr)
		if bin.Op != api.BinOr {
			t.Errorf("expected OR, got %d", bin.Op)
		}
	})

	t.Run("precedence_and_or", func(t *testing.T) {
		// a = 1 OR b = 2 AND c = 3 should parse as a = 1 OR (b = 2 AND c = 3)
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		or, ok := sel.Where.(*api.BinaryExpr)
		if !ok || or.Op != api.BinOr {
			t.Fatalf("expected OR at top, got %T op=%v", sel.Where, or.Op)
		}
		// Right side should be AND
		and, ok := or.Right.(*api.BinaryExpr)
		if !ok || and.Op != api.BinAnd {
			t.Errorf("expected AND on right side of OR")
		}
	})

	t.Run("not", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE NOT a = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		un, ok := sel.Where.(*api.UnaryExpr)
		if !ok {
			t.Fatalf("expected UnaryExpr, got %T", sel.Where)
		}
		if un.Op != api.UnaryNot {
			t.Errorf("expected NOT, got %d", un.Op)
		}
	})

	t.Run("not_not", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE NOT NOT a = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		// Should be NOT(NOT(a = 1))
		outer, ok := sel.Where.(*api.UnaryExpr)
		if !ok || outer.Op != api.UnaryNot {
			t.Fatalf("expected outer NOT, got %T", sel.Where)
		}
		inner, ok := outer.Operand.(*api.UnaryExpr)
		if !ok || inner.Op != api.UnaryNot {
			t.Fatalf("expected inner NOT, got %T", outer.Operand)
		}
		// Inner operand should be comparison a = 1
		cmp, ok := inner.Operand.(*api.BinaryExpr)
		if !ok || cmp.Op != api.BinEQ {
			t.Fatalf("expected comparison inside NOT NOT, got %T", inner.Operand)
		}
	})

	t.Run("not_not_not", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE NOT NOT NOT a = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		// Should be NOT(NOT(NOT(a = 1)))
		n1, ok := sel.Where.(*api.UnaryExpr)
		if !ok || n1.Op != api.UnaryNot {
			t.Fatalf("expected NOT at level 1, got %T", sel.Where)
		}
		n2, ok := n1.Operand.(*api.UnaryExpr)
		if !ok || n2.Op != api.UnaryNot {
			t.Fatalf("expected NOT at level 2, got %T", n1.Operand)
		}
		n3, ok := n2.Operand.(*api.UnaryExpr)
		if !ok || n3.Op != api.UnaryNot {
			t.Fatalf("expected NOT at level 3, got %T", n2.Operand)
		}
	})

	t.Run("is_null", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE name IS NULL")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		isn, ok := sel.Where.(*api.IsNullExpr)
		if !ok {
			t.Fatalf("expected IsNullExpr, got %T", sel.Where)
		}
		if isn.Not {
			t.Error("expected Not=false")
		}
	})

	t.Run("is_not_null", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE name IS NOT NULL")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		isn := sel.Where.(*api.IsNullExpr)
		if !isn.Not {
			t.Error("expected Not=true")
		}
	})

	t.Run("nested_parens", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		and, ok := sel.Where.(*api.BinaryExpr)
		if !ok || and.Op != api.BinAnd {
			t.Fatalf("expected AND at top, got %T", sel.Where)
		}
		// Left side should be OR (from parenthesized expr)
		or, ok := and.Left.(*api.BinaryExpr)
		if !ok || or.Op != api.BinOr {
			t.Errorf("expected OR on left side of AND")
		}
	})

	t.Run("negative_number", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE x = -42")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		bin := sel.Where.(*api.BinaryExpr)
		lit, ok := bin.Right.(*api.Literal)
		if !ok {
			t.Fatalf("expected Literal, got %T", bin.Right)
		}
		if lit.Value.Int != -42 {
			t.Errorf("expected -42, got %d", lit.Value.Int)
		}
	})

	t.Run("all_comparison_ops", func(t *testing.T) {
		ops := []struct {
			sql string
			op  api.BinaryOp
		}{
			{"a = 1", api.BinEQ},
			{"a != 1", api.BinNE},
			{"a <> 1", api.BinNE},
			{"a < 1", api.BinLT},
			{"a <= 1", api.BinLE},
			{"a > 1", api.BinGT},
			{"a >= 1", api.BinGE},
		}
		for _, tt := range ops {
			t.Run(tt.sql, func(t *testing.T) {
				p := newParser()
				stmt, err := p.Parse("SELECT * FROM t WHERE " + tt.sql)
				if err != nil {
					t.Fatalf("parse error: %v", err)
				}
				sel := stmt.(*api.SelectStmt)
				bin := sel.Where.(*api.BinaryExpr)
				if bin.Op != tt.op {
					t.Errorf("expected op %d, got %d", tt.op, bin.Op)
				}
			})
		}
	})
}

// ─── Case Sensitivity ─────────────────────────────────────────────

func TestParse_CaseInsensitive(t *testing.T) {
	sqls := []string{
		"select * from users",
		"SELECT * FROM USERS",
		"Select * From Users",
		"sElEcT * fRoM uSeRs",
	}
	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			p := newParser()
			stmt, err := p.Parse(sql)
			if err != nil {
				t.Fatalf("parse error for %q: %v", sql, err)
			}
			sel, ok := stmt.(*api.SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", stmt)
			}
			if sel.Table != "USERS" {
				t.Errorf("expected table USERS, got %s", sel.Table)
			}
		})
	}
}

// ─── Error Cases ──────────────────────────────────────────────────

func TestParse_Errors(t *testing.T) {
	t.Run("missing_from", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT * users")
		if err == nil {
			t.Fatal("expected error for missing FROM")
		}
	})

	t.Run("unclosed_paren", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT * FROM t WHERE (a = 1")
		if err == nil {
			t.Fatal("expected error for unclosed paren")
		}
	})

	t.Run("unexpected_token", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("FOOBAR")
		if err == nil {
			t.Fatal("expected error for unknown statement")
		}
	})

	t.Run("empty_input", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("")
		if err == nil {
			t.Fatal("expected error for empty input")
		}
	})

	t.Run("trailing_garbage", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT * FROM t GARBAGE")
		if err == nil {
			t.Fatal("expected error for trailing garbage")
		}
	})

	t.Run("parse_error_type", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT FROM")
		if err == nil {
			t.Fatal("expected error")
		}
		_, ok := err.(*api.ParseError)
		if !ok {
			t.Errorf("expected *ParseError, got %T", err)
		}
	})
}

// ─── Semicolons ───────────────────────────────────────────────────

func TestParse_Semicolons(t *testing.T) {
	t.Run("with_semicolon", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT * FROM users;")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
	})

	t.Run("without_semicolon", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT * FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
	})
}

// ─── Literal Types ────────────────────────────────────────────────

func TestParse_LiteralTypes(t *testing.T) {
	t.Run("int_literal", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO t VALUES (42)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		lit := ins.Values[0][0].(*api.Literal)
		if lit.Value.Type != catalogapi.TypeInt || lit.Value.Int != 42 {
			t.Errorf("expected Int(42), got type=%d int=%d", lit.Value.Type, lit.Value.Int)
		}
	})

	t.Run("float_literal", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO t VALUES (3.14)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		lit := ins.Values[0][0].(*api.Literal)
		if lit.Value.Type != catalogapi.TypeFloat || lit.Value.Float != 3.14 {
			t.Errorf("expected Float(3.14), got type=%d float=%f", lit.Value.Type, lit.Value.Float)
		}
	})

	t.Run("string_literal", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO t VALUES ('hello')")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		lit := ins.Values[0][0].(*api.Literal)
		if lit.Value.Type != catalogapi.TypeText || lit.Value.Text != "hello" {
			t.Errorf("expected Text('hello'), got type=%d text=%q", lit.Value.Type, lit.Value.Text)
		}
	})

	t.Run("null_literal", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("INSERT INTO t VALUES (NULL)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins := stmt.(*api.InsertStmt)
		lit := ins.Values[0][0].(*api.Literal)
		if !lit.Value.IsNull {
			t.Error("expected IsNull=true")
		}
	})

}

func TestParse_InExpr(t *testing.T) {
	t.Run("in_single", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (1)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		in, ok := sel.Where.(*api.InExpr)
		if !ok {
			t.Fatalf("expected InExpr, got %T", sel.Where)
		}
		if in.Not {
			t.Error("expected Not=false")
		}
		if len(in.Values) != 1 {
			t.Fatalf("values len = %d, want 1", len(in.Values))
		}
	})

	t.Run("in_multiple", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE name IN ('a', 'b', 'c')")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		in, ok := sel.Where.(*api.InExpr)
		if !ok {
			t.Fatalf("expected InExpr, got %T", sel.Where)
		}
		if len(in.Values) != 3 {
			t.Fatalf("values len = %d, want 3", len(in.Values))
		}
	})

	t.Run("in_in_expression", func(t *testing.T) {
		// id IN (1,2) AND status = 'active'
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (1, 2) AND status = 'active'")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		and, ok := sel.Where.(*api.BinaryExpr)
		if !ok || and.Op != api.BinAnd {
			t.Fatalf("expected AND, got %T op=%v", sel.Where, and.Op)
		}
		in, ok := and.Left.(*api.InExpr)
		if !ok {
			t.Fatalf("expected InExpr on left, got %T", and.Left)
		}
		if len(in.Values) != 2 {
			t.Fatalf("values len = %d, want 2", len(in.Values))
		}
	})
}

func TestParse_BetweenExpr(t *testing.T) {
	t.Run("between", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE age BETWEEN 18 AND 65")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		be, ok := sel.Where.(*api.BetweenExpr)
		if !ok {
			t.Fatalf("expected BetweenExpr, got %T", sel.Where)
		}
		if be.Not {
			t.Error("expected Not=false")
		}
		ref, ok := be.Expr.(*api.ColumnRef)
		t.Logf("got ref.Column=%q", ref.Column)
		if !ok || ref.Column != "AGE" {
			t.Errorf("expected AGE column ref, got %q", ref.Column)
		}
	})

	t.Run("between_requires_and", func(t *testing.T) {
		p := newParser()
		_, err := p.Parse("SELECT * FROM t WHERE age BETWEEN 18")
		if err == nil {
			t.Error("expected error: BETWEEN without AND")
		}
	})

	t.Run("between_in_expression", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE price BETWEEN 10 AND 100 AND category = 'food'")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		and, ok := sel.Where.(*api.BinaryExpr)
		if !ok || and.Op != api.BinAnd {
			t.Fatalf("expected AND, got %T", sel.Where)
		}
		be, ok := and.Left.(*api.BetweenExpr)
		if !ok {
			t.Fatalf("expected BetweenExpr on left, got %T", and.Left)
		}
		_ = be // suppress unused warning
	})
}


func TestParse_Subquery(t *testing.T) {
	p := newParser()

	t.Run("scalar_subquery_in_comparison", func(t *testing.T) {
		// Use bare column — aggregate functions not yet supported in parsePrimary
		stmt, err := p.Parse("SELECT * FROM t WHERE id = (SELECT id FROM t2)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		bin, ok := sel.Where.(*api.BinaryExpr)
		if !ok || bin.Op != api.BinEQ {
			t.Fatalf("expected EQ comparison, got %T", sel.Where)
		}
		subq, ok := bin.Right.(*api.SubqueryExpr)
		if !ok {
			t.Fatalf("expected SubqueryExpr on right, got %T", bin.Right)
		}
		innerSel, ok := subq.Stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt inside subquery, got %T", subq.Stmt)
		}
		if innerSel.Table != "T2" {
			t.Errorf("expected inner table T2, got %s", innerSel.Table)
		}
	})

	t.Run("in_with_subquery", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (SELECT id FROM t2)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		in, ok := sel.Where.(*api.InExpr)
		if !ok {
			t.Fatalf("expected InExpr, got %T", sel.Where)
		}
		if in.Not {
			t.Error("expected Not=false for IN")
		}
		if len(in.Values) != 1 {
			t.Fatalf("expected 1 value (subquery), got %d", len(in.Values))
		}
		subq, ok := in.Values[0].(*api.SubqueryExpr)
		if !ok {
			t.Fatalf("expected SubqueryExpr in IN values, got %T", in.Values[0])
		}
		_ = subq
	})

	t.Run("not_in_with_subquery", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE id NOT IN (SELECT id FROM t2)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		in, ok := sel.Where.(*api.InExpr)
		if !ok {
			t.Fatalf("expected InExpr, got %T", sel.Where)
		}
		if !in.Not {
			t.Error("expected Not=true for NOT IN")
		}
	})

	t.Run("parenthesized_expression_still_works", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE (id) = 5")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		bin, ok := sel.Where.(*api.BinaryExpr)
		if !ok || bin.Op != api.BinEQ {
			t.Fatalf("expected EQ comparison, got %T", sel.Where)
		}
		if _, ok := bin.Left.(*api.SubqueryExpr); ok {
			t.Error("expected ColumnRef, not SubqueryExpr for (id)")
		}
	})
}


func TestParse_Aggregates(t *testing.T) {
	t.Run("max", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT MAX(id) FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) != 1 {
			t.Fatalf("expected 1 column, got %d", len(sel.Columns))
		}
		agg, ok := sel.Columns[0].Expr.(*api.AggregateCallExpr)
		if !ok {
			t.Fatalf("expected AggregateCallExpr, got %T", sel.Columns[0].Expr)
		}
		if agg.Func != "MAX" {
			t.Errorf("expected MAX, got %s", agg.Func)
		}
		col, ok := agg.Arg.(*api.ColumnRef)
		if !ok {
			t.Fatalf("expected ColumnRef arg, got %T", agg.Arg)
		}
		if col.Column != "ID" {
			t.Errorf("expected ID, got %s", col.Column)
		}
	})

	t.Run("min", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT min(age) FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		agg, ok := sel.Columns[0].Expr.(*api.AggregateCallExpr)
		if !ok {
			t.Fatalf("expected AggregateCallExpr, got %T", sel.Columns[0].Expr)
		}
		if agg.Func != "MIN" {
			t.Errorf("expected MIN, got %s", agg.Func)
		}
	})

	t.Run("count_star", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT COUNT(*) FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		agg, ok := sel.Columns[0].Expr.(*api.AggregateCallExpr)
		if !ok {
			t.Fatalf("expected AggregateCallExpr, got %T", sel.Columns[0].Expr)
		}
		if agg.Func != "COUNT" {
			t.Errorf("expected COUNT, got %s", agg.Func)
		}
		if agg.Arg != nil {
			t.Errorf("expected nil arg for COUNT(*), got %T", agg.Arg)
		}
	})

	t.Run("sum", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT SUM(amount) FROM orders")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		agg, ok := sel.Columns[0].Expr.(*api.AggregateCallExpr)
		if !ok {
			t.Fatalf("expected AggregateCallExpr, got %T", sel.Columns[0].Expr)
		}
		if agg.Func != "SUM" {
			t.Errorf("expected SUM, got %s", agg.Func)
		}
	})

	t.Run("avg", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT AVG(score) FROM exams")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		agg, ok := sel.Columns[0].Expr.(*api.AggregateCallExpr)
		if !ok {
			t.Fatalf("expected AggregateCallExpr, got %T", sel.Columns[0].Expr)
		}
		if agg.Func != "AVG" {
			t.Errorf("expected AVG, got %s", agg.Func)
		}
	})

	t.Run("aggregate_in_subquery", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (SELECT MAX(id) FROM t2)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		in, ok := sel.Where.(*api.InExpr)
		if !ok {
			t.Fatalf("expected InExpr, got %T", sel.Where)
		}
		subq, ok := in.Values[0].(*api.SubqueryExpr)
		if !ok {
			t.Fatalf("expected SubqueryExpr in IN values, got %T", in.Values[0])
		}
		subSel := subq.Stmt.(*api.SelectStmt)
		agg, ok := subSel.Columns[0].Expr.(*api.AggregateCallExpr)
		if !ok {
			t.Fatalf("expected AggregateCallExpr in subquery, got %T", subSel.Columns[0].Expr)
		}
		if agg.Func != "MAX" {
			t.Errorf("expected MAX in subquery, got %s", agg.Func)
		}
	})

	t.Run("unknown_function_treated_as_column", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT myfunc(id) FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		_, ok := sel.Columns[0].Expr.(*api.ColumnRef)
		if !ok {
			t.Fatalf("expected ColumnRef for unknown function, got %T", sel.Columns[0].Expr)
		}
	})
}

func TestParse_Explain(t *testing.T) {
	t.Run("explain select", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("EXPLAIN SELECT id FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		explain, ok := stmt.(*api.ExplainStmt)
		if !ok {
			t.Fatalf("expected ExplainStmt, got %T", stmt)
		}
		if explain.Analyze {
			t.Error("expected Analyze=false")
		}
		sel, ok := explain.Statement.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", explain.Statement)
		}
		if len(sel.Columns) != 1 {
			t.Fatalf("expected 1 column, got %d", len(sel.Columns))
		}
	})

	t.Run("explain analyze select", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("EXPLAIN ANALYZE SELECT id FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		explain, ok := stmt.(*api.ExplainStmt)
		if !ok {
			t.Fatalf("expected ExplainStmt, got %T", stmt)
		}
		if !explain.Analyze {
			t.Error("expected Analyze=true")
		}
		_, ok = explain.Statement.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", explain.Statement)
		}
	})
}


func TestParse_Join(t *testing.T) {
	t.Run("inner join basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.Join == nil {
			t.Fatal("expected Join to be set")
		}
		if sel.Join.Left != "T1" {
			t.Errorf("expected left=T1, got %s", sel.Join.Left)
		}
		if sel.Join.Right != "T2" {
			t.Errorf("expected right=T2, got %s", sel.Join.Right)
		}
		if sel.Join.Type != api.JoinType("INNER") {
			t.Errorf("expected type=INNER, got %s", sel.Join.Type)
		}
		if sel.Join.On == nil {
			t.Fatal("expected On to be set")
		}
		// t1.id = t2.t1_id is a BinaryExpr
		binOp, ok := sel.Join.On.(*api.BinaryExpr)
		if !ok {
			t.Fatalf("expected BinaryExpr in ON, got %T", sel.Join.On)
		}
		if binOp.Op != api.BinEQ {
			t.Errorf("expected op=BinEQ, got %v", binOp.Op)
		}
	})

	t.Run("left join", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Join.Type != api.JoinType("LEFT") {
			t.Errorf("expected type=LEFT, got %s", sel.Join.Type)
		}
		if sel.Join.Left != "USERS" || sel.Join.Right != "ORDERS" {
			t.Errorf("unexpected tables: %s %s", sel.Join.Left, sel.Join.Right)
		}
	})

	t.Run("right join", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Join.Type != api.JoinType("RIGHT") {
			t.Errorf("expected type=RIGHT, got %s", sel.Join.Type)
		}
	})

	t.Run("cross join", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t1 CROSS JOIN t2")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Join.Type != api.JoinType("CROSS") {
			t.Errorf("expected type=CROSS, got %s", sel.Join.Type)
		}
		if sel.Join.On != nil {
			t.Error("CROSS JOIN should have nil On")
		}
	})

	t.Run("join with where", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.x = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Join == nil {
			t.Fatal("expected Join to be set")
		}
		if sel.Where == nil {
			t.Fatal("expected WHERE to be set")
		}
	})

	t.Run("qualified columns in join", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.id = t2.t1_id")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
		}
		col1, ok := sel.Columns[0].Expr.(*api.ColumnRef)
		if !ok {
			t.Fatalf("expected ColumnRef, got %T", sel.Columns[0].Expr)
		}
		if col1.Table != "T1" || col1.Column != "ID" {
			t.Errorf("expected T1.ID, got %s.%s", col1.Table, col1.Column)
		}
	})

	t.Run("single table still works", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Join != nil {
			t.Error("single table should have nil Join")
		}
		if sel.Table != "USERS" {
			t.Errorf("expected Table=USERS, got %s", sel.Table)
		}
	})
}

// ─── Parser Tests: FOR UPDATE ──────────────────────────────────────

func TestParse_ForUpdate(t *testing.T) {
	t.Run("for_update_basic", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users FOR UPDATE")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.LockMode != api.UpdateExclusive {
			t.Errorf("expected LockMode=UpdateExclusive(%d), got %d", api.UpdateExclusive, sel.LockMode)
		}
		if sel.LockWait != api.LockWaitDefault {
			t.Errorf("expected LockWait=LockWaitDefault(%d), got %d", api.LockWaitDefault, sel.LockWait)
		}
	})

	t.Run("for_update_nowait", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users FOR UPDATE NOWAIT")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.LockMode != api.UpdateExclusive {
			t.Errorf("expected LockMode=UpdateExclusive(%d), got %d", api.UpdateExclusive, sel.LockMode)
		}
		if sel.LockWait != api.LockWaitNowait {
			t.Errorf("expected LockWait=LockWaitNowait(%d), got %d", api.LockWaitNowait, sel.LockWait)
		}
	})

	t.Run("for_update_skip_locked", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users FOR UPDATE SKIP LOCKED")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.LockMode != api.UpdateExclusive {
			t.Errorf("expected LockMode=UpdateExclusive(%d), got %d", api.UpdateExclusive, sel.LockMode)
		}
		if sel.LockWait != api.LockWaitSkipLocked {
			t.Errorf("expected LockWait=LockWaitSkipLocked(%d), got %d", api.LockWaitSkipLocked, sel.LockWait)
		}
	})

	t.Run("for_update_with_where", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users WHERE id = 1 FOR UPDATE")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.LockMode != api.UpdateExclusive {
			t.Errorf("expected LockMode=UpdateExclusive(%d), got %d", api.UpdateExclusive, sel.LockMode)
		}
		if sel.Where == nil {
			t.Error("expected WHERE to be parsed")
		}
	})

	t.Run("for_update_with_order_by", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users ORDER BY id FOR UPDATE")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.LockMode != api.UpdateExclusive {
			t.Errorf("expected LockMode=UpdateExclusive(%d), got %d", api.UpdateExclusive, sel.LockMode)
		}
		if sel.OrderBy == nil {
			t.Error("expected ORDER BY to be parsed")
		}
	})

	t.Run("no_for_update", func(t *testing.T) {
		p := newParser()
		stmt, err := p.Parse("SELECT * FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel, ok := stmt.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", stmt)
		}
		if sel.LockMode != api.NoUpdate {
			t.Errorf("expected LockMode=NoUpdate(%d), got %d", api.NoUpdate, sel.LockMode)
		}
		if sel.LockWait != api.LockWaitDefault {
			t.Errorf("expected LockWait=LockWaitDefault(%d), got %d", api.LockWaitDefault, sel.LockWait)
		}
	})
}

func TestColumnAliasWithoutAS(t *testing.T) {
	p := New()
	
	tests := []struct {
		sql     string
		wantErr bool
		alias   string
	}{
		{"SELECT id AS alias FROM t", false, "ALIAS"},
		{"SELECT id alias FROM t", false, "ALIAS"},
		{"SELECT id AS a, val AS b FROM t", false, "A"},
		{"SELECT id a, val b FROM t", false, "A"},
		{"SELECT id name FROM t WHERE name = 'x'", false, "NAME"},
	}
	
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				sel := stmt.(*api.SelectStmt)
				if sel.Columns[0].Alias != tt.alias {
					t.Errorf("Parse() alias = %v, want %v", sel.Columns[0].Alias, tt.alias)
				}
			}
		})
	}
}
