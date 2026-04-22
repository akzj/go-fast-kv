package internal

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// Compile-time interface check.
var _ api.Parser = (*parser)(nil)

// parser is a recursive descent SQL parser.
type parser struct {
	lex   *lexer
	cur   api.Token // current token
	peek  api.Token // lookahead token
	depth int       // recursion depth for stack overflow prevention
	// questionCount tracks ? placeholder position for sequential numbering
	questionCount int
}

// valToValue converts a parser Literal expression to a catalog Value.
func valToValue(expr api.Expr) catalogapi.Value {
	if lit, ok := expr.(*api.Literal); ok {
		return lit.Value
	}
	return catalogapi.Value{}
}

// New creates a new Parser.
func New() api.Parser {
	return &parser{}
}

// Parse parses a single SQL statement.
func (p *parser) Parse(sql string) (api.Statement, error) {
	p.lex = newLexer(sql)
	p.cur = p.lex.nextToken()
	p.peek = p.lex.nextToken()
	p.questionCount = 0 // reset ? placeholder counter

	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// Consume optional semicolon
	if p.cur.Type == api.TokSemicolon {
		p.advance()
	}
	if p.cur.Type != api.TokEOF {
		return nil, p.errorf("unexpected token after statement")
	}
	return stmt, nil
}

// ─── Statement Dispatch ───────────────────────────────────────────

func (p *parser) parseStatement() (api.Statement, error) {
	switch p.cur.Type {
	case api.TokCreate:
		return p.parseCreate()
	case api.TokDrop:
		return p.parseDrop()
	case api.TokInsert:
		return p.parseInsert()
	case api.TokSelect:
		return p.parseSelect()
	case api.TokWith:
		return p.parseWith()
	case api.TokDelete:
		return p.parseDelete()
	case api.TokUpdate:
		return p.parseUpdate()
	case api.TokExplain:
		return p.parseExplain()
	case api.TokBegin:
		return p.parseBegin()
	case api.TokCommit:
		return p.parseCommit()
	case api.TokRollback:
		return p.parseRollback()
	case api.TokSavepoint:
		return p.parseSavepoint()
	case api.TokRelease:
		return p.parseRelease()
	case api.TokAlter:
		return p.parseAlterTable()
	case api.TokTruncate:
		return p.parseTruncate()
	case api.TokPragma:
		return p.parsePragma()
	default:
		return nil, p.errorf("expected SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, BEGIN, COMMIT, ROLLBACK, PRAGMA)")
	}
}

func (p *parser) parseBegin() (api.Statement, error) {
	p.advance() // consume BEGIN
	return &api.BeginStmt{}, nil
}

func (p *parser) parseCommit() (api.Statement, error) {
	p.advance() // consume COMMIT
	return &api.CommitStmt{}, nil
}

func (p *parser) parseRollback() (api.Statement, error) {
	p.advance() // consume ROLLBACK
	// Check for ROLLBACK TO SAVEPOINT name
	if p.cur.Type == api.TokTo || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "TO") {
		if p.cur.Type == api.TokIdent {
			p.advance() // consume TO
		} else {
			p.advance() // consume TokTo
		}
		// Expect SAVEPOINT
		if p.cur.Type != api.TokSavepoint && !(p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "SAVEPOINT") {
			return nil, p.errorf("expected SAVEPOINT after ROLLBACK TO")
		}
		if p.cur.Type == api.TokIdent {
			p.advance() // consume SAVEPOINT
		} else {
			p.advance() // consume TokSavepoint
		}
		// Savepoint name
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected savepoint name")
		}
		name := p.cur.Literal
		p.advance()
		return &api.RollbackToSavepointStmt{Name: name}, nil
	}
	return &api.RollbackStmt{}, nil
}

func (p *parser) parseSavepoint() (api.Statement, error) {
	p.advance() // consume SAVEPOINT
	// Savepoint name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected savepoint name")
	}
	name := p.cur.Literal
	p.advance()
	return &api.SavepointStmt{Name: name}, nil
}

func (p *parser) parseRelease() (api.Statement, error) {
	p.advance() // consume RELEASE
	// Expect SAVEPOINT
	if p.cur.Type != api.TokSavepoint && !(p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "SAVEPOINT") {
		return nil, p.errorf("expected SAVEPOINT after RELEASE")
	}
	if p.cur.Type == api.TokIdent {
		p.advance() // consume SAVEPOINT
	} else {
		p.advance() // consume TokSavepoint
	}
	// Savepoint name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected savepoint name")
	}
	name := p.cur.Literal
	p.advance()
	return &api.ReleaseSavepointStmt{Name: name}, nil
}

// ─── ALTER TABLE ──────────────────────────────────────────────────

func (p *parser) parseAlterTable() (api.Statement, error) {
	p.advance() // consume ALTER
	if err := p.expect(api.TokTable); err != nil {
		return nil, err
	}

	// Table name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name")
	}
	stmt := &api.AlterTableStmt{
		Table: p.cur.Literal,
	}
	p.advance()

	// Operation: ADD COLUMN, DROP COLUMN, RENAME COLUMN, RENAME TO
	switch p.cur.Type {
	case api.TokAdd:
		return p.parseAlterAddColumn(stmt)
	case api.TokDrop:
		return p.parseAlterDropColumn(stmt)
	case api.TokRename:
		// Could be RENAME COLUMN or RENAME TO — check what's after RENAME
		// If COLUMN keyword follows, it's RENAME COLUMN old TO new
		// If table name follows (no COLUMN), it's RENAME TO new_name
		if p.peek.Type == api.TokColumn {
			return p.parseAlterRenameColumn(stmt)
		}
		// RENAME TO new_name
		return p.parseAlterRenameTo(stmt)
	default:
		return nil, p.errorf("expected ADD, DROP, RENAME COLUMN, or RENAME TO after ALTER TABLE table_name")
	}
}

func (p *parser) parseAlterAddColumn(stmt *api.AlterTableStmt) (api.Statement, error) {
	p.advance() // consume ADD
	if err := p.expect(api.TokColumn); err != nil {
		return nil, err
	}

	// Column name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected column name")
	}
	stmt.Column = p.cur.Literal
	p.advance()

	// Column type
	switch p.cur.Type {
	case api.TokIntKw:
		stmt.TypeName = "INT"
	case api.TokInteger2:
		stmt.TypeName = "INTEGER"
	case api.TokTextKw:
		stmt.TypeName = "TEXT"
	case api.TokFloatKw:
		stmt.TypeName = "FLOAT"
	case api.TokBlobKw:
		stmt.TypeName = "BLOB"
	default:
		return nil, p.errorf("expected column type (INT, TEXT, FLOAT, BLOB)")
	}
	p.advance()

	// Optional constraints: [NOT NULL] [UNIQUE]
	for {
		switch p.cur.Type {
		case api.TokNot:
			p.advance()
			if err := p.expect(api.TokNull); err != nil {
				return nil, err
			}
			stmt.NotNull = true
		case api.TokUnique:
			p.advance()
			stmt.Unique = true
		default:
			goto done
		}
	}
done:
	stmt.Operation = api.AlterAddColumn
	return stmt, nil
}

func (p *parser) parseAlterDropColumn(stmt *api.AlterTableStmt) (api.Statement, error) {
	p.advance() // consume DROP
	if err := p.expect(api.TokColumn); err != nil {
		return nil, err
	}

	// Column name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected column name")
	}
	stmt.Column = p.cur.Literal
	p.advance()

	stmt.Operation = api.AlterDropColumn
	return stmt, nil
}

func (p *parser) parseAlterRenameColumn(stmt *api.AlterTableStmt) (api.Statement, error) {
	p.advance() // consume RENAME
	if err := p.expect(api.TokColumn); err != nil {
		return nil, err
	}

	// Old column name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected column name")
	}
	stmt.Column = p.cur.Literal
	p.advance()

	if err := p.expect(api.TokTo); err != nil {
		return nil, err
	}

	// New column name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected new column name")
	}
	stmt.ColumnNew = p.cur.Literal
	p.advance()

	stmt.Operation = api.AlterRenameColumn
	return stmt, nil
}

// parseAlterRenameTo parses: ALTER TABLE t RENAME TO new_name
func (p *parser) parseAlterRenameTo(stmt *api.AlterTableStmt) (api.Statement, error) {
	// p.cur = TokRename, p.peek = TokTo
	p.advance() // consume TokRename

	// Expect "TO" keyword
	if err := p.expect(api.TokTo); err != nil {
		return nil, err
	}
	// cur = new table name

	// New table name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected new table name")
	}
	stmt.TableNew = p.cur.Literal
	p.advance() // consume new table name

	stmt.Operation = api.AlterRenameTable
	return stmt, nil
}

// ─── CREATE ───────────────────────────────────────────────────────

func (p *parser) parseCreate() (api.Statement, error) {
	p.advance() // consume CREATE
	if p.cur.Type == api.TokUnique {
		return p.parseCreateIndex(true)
	}
	switch p.cur.Type {
	case api.TokTable:
		return p.parseCreateTable()
	case api.TokIndex:
		return p.parseCreateIndex(false)
	default:
		return nil, p.errorf("expected TABLE or INDEX after CREATE")
	}
}

func (p *parser) parseCreateTable() (api.Statement, error) {
	p.advance() // consume TABLE
	stmt := &api.CreateTableStmt{}

	// IF NOT EXISTS
	if p.cur.Type == api.TokIf {
		p.advance()
		if err := p.expect(api.TokNot); err != nil {
			return nil, err
		}
		if err := p.expect(api.TokExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	// Table name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name")
	}
	stmt.Table = p.cur.Literal
	p.advance()

	// ( column definitions )
	if err := p.expect(api.TokLParen); err != nil {
		return nil, err
	}

	for {
		// Check for trailing PRIMARY KEY (tableName, ..., PRIMARY KEY (col))
		if p.cur.Type == api.TokPrimary {
			p.advance()
			if err := p.expect(api.TokKey); err != nil {
				return nil, err
			}
			if err := p.expect(api.TokLParen); err != nil {
				return nil, err
			}
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name in PRIMARY KEY")
			}
			stmt.PrimaryKey = p.cur.Literal
			p.advance()
			if err := p.expect(api.TokRParen); err != nil {
				return nil, err
			}
			break
		}

		// Check for table-level CHECK constraint: CHECK (expr)
		if p.cur.Type == api.TokCheck {
			p.advance()
			if p.cur.Type != api.TokLParen {
				return nil, p.errorf("expected ( after CHECK")
			}
			p.advance()
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if p.cur.Type != api.TokRParen {
				return nil, p.errorf("expected ) after CHECK expression")
			}
			p.advance()
			// Add a column def with only the check expression for table-level constraint
			col := api.ColumnDef{Name: "", CheckExpr: expr}
			stmt.Columns = append(stmt.Columns, col)
			if p.cur.Type == api.TokComma {
				p.advance()
				continue
			}
			break
		}

		// Check for table-level FOREIGN KEY constraint: FOREIGN KEY (col, ...) REFERENCES table (col, ...)
		if p.cur.Type == api.TokForeign {
			fk, err := p.parseTableLevelForeignKey()
			if err != nil {
				return nil, err
			}
			stmt.ForeignKeys = append(stmt.ForeignKeys, *fk)
			if p.cur.Type == api.TokComma {
				p.advance()
				continue
			}
			break
		}

		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		if col.PrimaryKey {
			stmt.PrimaryKey = col.Name
		}
		stmt.Columns = append(stmt.Columns, col)

		if p.cur.Type == api.TokComma {
			p.advance()
			continue
		}
		break
	}

	if err := p.expect(api.TokRParen); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *parser) parseColumnDef() (api.ColumnDef, error) {
	col := api.ColumnDef{}
	if p.cur.Type != api.TokIdent {
		return col, p.errorf("expected column name")
	}
	col.Name = p.cur.Literal
	p.advance()

	// Type name
	switch p.cur.Type {
	case api.TokIntKw:
		col.TypeName = "INT"
	case api.TokInteger2:
		col.TypeName = "INT"
	case api.TokTextKw:
		col.TypeName = "TEXT"
	case api.TokFloatKw:
		col.TypeName = "FLOAT"
	case api.TokBlobKw:
		col.TypeName = "BLOB"
	default:
		return col, p.errorf("expected type name (INT, TEXT, FLOAT, BLOB)")
	}
	p.advance()

	// Optional AUTOINCREMENT / SERIAL — can appear before or after PRIMARY KEY
	if p.cur.Type == api.TokAutoIncrement || p.cur.Type == api.TokSerial {
		p.advance()
		col.AutoInc = true
	}

	// Optional PRIMARY KEY
	if p.cur.Type == api.TokPrimary {
		p.advance()
		if err := p.expect(api.TokKey); err != nil {
			return col, err
		}
		col.PrimaryKey = true
	}

	// Optional UNIQUE
	if p.cur.Type == api.TokUnique {
		p.advance()
		col.Unique = true
	}

	// Optional NOT NULL
	if p.cur.Type == api.TokNot {
		p.advance()
		if p.cur.Type != api.TokNull {
			return col, p.errorf("expected NULL after NOT")
		}
		p.advance()
		col.NotNull = true
	}

	// Optional AUTOINCREMENT / SERIAL — marks the column for auto-generated IDs
	if p.cur.Type == api.TokAutoIncrement || p.cur.Type == api.TokSerial {
		p.advance()
		col.AutoInc = true
	}

	// Optional DEFAULT value
	if p.cur.Type == api.TokDefault {
		p.advance()
		// Parse the default value expression
		if p.cur.Type == api.TokLParen {
			// Parenthesized expression: DEFAULT (expr)
			p.advance()
			val, err := p.parseExpr()
			if err != nil {
				return col, err
			}
			if p.cur.Type != api.TokRParen {
				return col, p.errorf("expected ) after default value expression")
			}
			p.advance()
			col.DefaultValue = valToValue(val)
		} else {
			// Simple literal: DEFAULT 0, DEFAULT 'hello', DEFAULT NULL
			val, err := p.parseExpr()
			if err != nil {
				return col, err
			}
			col.DefaultValue = valToValue(val)
		}
	}

	// Optional CHECK constraint
	if p.cur.Type == api.TokCheck {
		p.advance()
		if p.cur.Type != api.TokLParen {
			return col, p.errorf("expected ( after CHECK")
		}
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return col, err
		}
		if p.cur.Type != api.TokRParen {
			return col, p.errorf("expected ) after CHECK expression")
		}
		p.advance()
		col.CheckExpr = expr
	}

	return col, nil
}

// parseTableLevelForeignKey parses: FOREIGN KEY (col, ...) REFERENCES table (col, ...) [ON DELETE action] [ON UPDATE action]
func (p *parser) parseTableLevelForeignKey() (*api.ForeignKey, error) {
	p.advance() // consume FOREIGN
	if err := p.expect(api.TokKey); err != nil {
		return nil, err
	}
	if err := p.expect(api.TokLParen); err != nil {
		return nil, err
	}

	// Parse column list
	var columns []string
	for {
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected column name in FOREIGN KEY")
		}
		columns = append(columns, p.cur.Literal)
		p.advance()
		if p.cur.Type != api.TokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(api.TokRParen); err != nil {
		return nil, err
	}

	// REFERENCES
	if err := p.expect(api.TokReferences); err != nil {
		return nil, err
	}

	// Referenced table name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected referenced table name")
	}
	refTable := p.cur.Literal
	p.advance()

	// Optional (column list)
	var refColumns []string
	if p.cur.Type == api.TokLParen {
		p.advance()
		for {
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected referenced column name")
			}
			refColumns = append(refColumns, p.cur.Literal)
			p.advance()
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(api.TokRParen); err != nil {
			return nil, err
		}
	}

	fk := &api.ForeignKey{
		Columns:           columns,
		ReferencedTable:   refTable,
		ReferencedColumns: refColumns,
		OnDelete:          "NO ACTION",
		OnUpdate:          "NO ACTION",
	}

	// Optional ON DELETE / ON UPDATE
	p.parseReferentialActions(fk)

	return fk, nil
}

// parseReferentialActions parses ON DELETE/UPDATE actions for foreign keys.
func (p *parser) parseReferentialActions(fk *api.ForeignKey) {
	for {
		if p.cur.Type == api.TokOn {
			p.advance()
			var action *string
			if p.cur.Type == api.TokDelete || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "DELETE") {
				if p.cur.Type == api.TokIdent {
					p.advance() // consume DELETE identifier
				} else {
					p.advance() // consume DELETE token
				}
				action = &fk.OnDelete
			} else if p.cur.Type == api.TokUpdate || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "UPDATE") {
				if p.cur.Type == api.TokIdent {
					p.advance() // consume UPDATE identifier
				} else {
					p.advance() // consume UPDATE token
				}
				action = &fk.OnUpdate
			} else {
				break
			}

			// Parse action: CASCADE, SET NULL, RESTRICT, NO ACTION
			var act string
			switch p.cur.Type {
			case api.TokCascade:
				act = "CASCADE"
				p.advance()
			case api.TokSetNull:
				act = "SET NULL"
				p.advance()
			case api.TokRestrict:
				act = "RESTRICT"
				p.advance()
			case api.TokNoAction:
				act = "NO ACTION"
				p.advance()
				// Consume the following "ACTION" keyword if present
				if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "ACTION" {
					p.advance()
				}
			case api.TokSet:
				// SET NULL
				if p.peek.Type == api.TokNull {
					act = "SET NULL"
					p.advance()
					p.advance()
				} else {
					break
				}
			case api.TokIdent:
				upper := strings.ToUpper(p.cur.Literal)
				if upper == "CASCADE" {
					act = "CASCADE"
					p.advance()
				} else if upper == "SET" {
					// SET NULL
					if p.peek.Type == api.TokNull {
						act = "SET NULL"
						p.advance()
						p.advance()
					} else {
						break
					}
				} else if upper == "NO" {
					// NO ACTION
					if p.peek.Type == api.TokIdent && strings.ToUpper(p.peek.Literal) == "ACTION" {
						act = "NO ACTION"
						p.advance()
						p.advance()
					} else {
						break
					}
				} else if upper == "RESTRICT" {
					act = "RESTRICT"
					p.advance()
				} else {
					break
				}
			default:
				break
			}
			if action != nil && act != "" {
				*action = act
			}
		} else {
			break
		}
	}
}

func (p *parser) parseCreateIndex(unique bool) (api.Statement, error) {
	if !unique {
		p.advance() // consume INDEX
	} else {
		p.advance() // consume UNIQUE
		if err := p.expect(api.TokIndex); err != nil {
			return nil, err
		}
	}
	stmt := &api.CreateIndexStmt{Unique: unique}

	// IF NOT EXISTS
	if p.cur.Type == api.TokIf {
		p.advance()
		if err := p.expect(api.TokNot); err != nil {
			return nil, err
		}
		if err := p.expect(api.TokExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	// Index name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected index name")
	}
	stmt.Index = p.cur.Literal
	p.advance()

	// ON table
	if err := p.expect(api.TokOn); err != nil {
		return nil, err
	}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after ON")
	}
	stmt.Table = p.cur.Literal
	p.advance()

	// (column | expression)
	if err := p.expect(api.TokLParen); err != nil {
		return nil, err
	}

	// Try to parse as expression first (handles function calls, arithmetic, etc.)
	expr, err := p.parseExpr()
	if err == nil && expr != nil {
		stmt.Expr = expr
		// Extract column for backward compat
		stmt.Column = extractFirstColumn(expr)
	} else {
		// Fallback to simple column name
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected column name or expression")
		}
		stmt.Column = p.cur.Literal
		p.advance()
	}

	if err := p.expect(api.TokRParen); err != nil {
		return nil, err
	}
	return stmt, nil
}

// ─── DROP ─────────────────────────────────────────────────────────

func (p *parser) parseDrop() (api.Statement, error) {
	p.advance() // consume DROP
	switch p.cur.Type {
	case api.TokTable:
		return p.parseDropTable()
	case api.TokIndex:
		return p.parseDropIndex()
	default:
		return nil, p.errorf("expected TABLE or INDEX after DROP")
	}
}

func (p *parser) parseDropTable() (api.Statement, error) {
	p.advance() // consume TABLE
	stmt := &api.DropTableStmt{}

	if p.cur.Type == api.TokIf {
		p.advance()
		if err := p.expect(api.TokExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name")
	}
	stmt.Table = p.cur.Literal
	p.advance()
	return stmt, nil
}

func (p *parser) parseDropIndex() (api.Statement, error) {
	p.advance() // consume INDEX
	stmt := &api.DropIndexStmt{}

	if p.cur.Type == api.TokIf {
		p.advance()
		if err := p.expect(api.TokExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected index name")
	}
	stmt.Index = p.cur.Literal
	p.advance()

	if err := p.expect(api.TokOn); err != nil {
		return nil, err
	}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after ON")
	}
	stmt.Table = p.cur.Literal
	p.advance()
	return stmt, nil
}

// ─── INSERT ───────────────────────────────────────────────────────

func (p *parser) parseInsert() (api.Statement, error) {
	p.advance() // consume INSERT
	if err := p.expect(api.TokInto); err != nil {
		return nil, err
	}

	stmt := &api.InsertStmt{}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after INTO")
	}
	stmt.Table = p.cur.Literal
	p.advance()

	// Optional column list
	if p.cur.Type == api.TokLParen {
		p.advance()
		for {
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name")
			}
			stmt.Columns = append(stmt.Columns, p.cur.Literal)
			p.advance()
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(api.TokRParen); err != nil {
			return nil, err
		}
	}

	// VALUES or SET
	var cols []string
	var row []api.Expr // single row for SET syntax
	if p.cur.Type == api.TokSet {
		// INSERT INTO t SET col = val, col = val, ...
		p.advance()
		for {
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name after SET")
			}
			cols = append(cols, p.cur.Literal)
			p.advance()
			if err := p.expect(api.TokEQ); err != nil {
				return nil, err
			}
			var expr api.Expr
			if p.cur.Type == api.TokDefault {
				p.advance()
				expr = &api.DefaultExpr{}
			} else {
				val, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				expr = val
			}
			row = append(row, expr)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
		stmt.Columns = cols
		stmt.Values = [][]api.Expr{row}
		return stmt, nil
	}
	if p.cur.Type == api.TokSelect {
		// Note: do NOT advance here — parseSelect consumes SELECT itself.
		// (parseInsert's SET branch also does NOT advance before returning.)
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.SelectStmt = sel.(*api.SelectStmt)
		return stmt, nil
	}
	if err := p.expect(api.TokValues); err != nil {
		return nil, err
	}

	// Value rows: (expr, expr), (expr, expr), ...
	for {
		if err := p.expect(api.TokLParen); err != nil {
			return nil, err
		}
		var row []api.Expr
		for {
			// Check for DEFAULT keyword
			if p.cur.Type == api.TokDefault {
				p.advance()
				row = append(row, &api.DefaultExpr{})
			} else {
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				row = append(row, expr)
			}
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(api.TokRParen); err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, row)

		if p.cur.Type != api.TokComma {
			break
		}
		p.advance()
	}
	// Optional ON CONFLICT clause
	if p.cur.Type == api.TokOn {
		p.advance() // consume ON
		if p.cur.Type == api.TokConflict || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "CONFLICT") {
			if p.cur.Type == api.TokIdent {
				p.advance() // consume CONFLICT identifier
			} else {
				p.advance() // consume CONFLICT token
			}
			// ON CONFLICT (column, ...) or ON CONFLICT DO NOTHING
			onConflict, err := p.parseOnConflictClause()
			if err != nil {
				return nil, err
			}
			stmt.OnConflict = onConflict
		}
	}
	return stmt, nil
}

// parseOnConflictClause parses: ON CONFLICT (col, ...) DO [NOTHING | UPDATE SET col=val]
func (p *parser) parseOnConflictClause() (*api.OnConflictClause, error) {
	clause := &api.OnConflictClause{}

	// Check if there's a conflict column list: ON CONFLICT (col, ...)
	if p.cur.Type == api.TokLParen {
		p.advance() // consume '('
		for {
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name in ON CONFLICT")
			}
			clause.ConflictColumns = append(clause.ConflictColumns, p.cur.Literal)
			p.advance()
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(api.TokRParen); err != nil {
			return nil, err
		}
	}

	// DO NOTHING or DO UPDATE SET ...
	if p.cur.Type != api.TokIdent || strings.ToUpper(p.cur.Literal) != "DO" {
		return nil, p.errorf("expected DO after ON CONFLICT")
	}
	p.advance() // consume DO

	// Check for DO NOTHING
	if p.cur.Type == api.TokNothing || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "NOTHING") {
		clause.Action = api.ConflictDoNothing
		if p.cur.Type == api.TokIdent {
			p.advance() // consume NOTHING identifier
		} else {
			p.advance() // consume NOTHING token
		}
		return clause, nil
	}

	// DO UPDATE SET col=val, col=val, ...
	// Accept both UPDATE keyword and UPDATE identifier
	if p.cur.Type == api.TokUpdate || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "UPDATE") {
		if p.cur.Type == api.TokIdent {
			p.advance() // consume UPDATE identifier
		} else {
			p.advance() // consume UPDATE token
		}
	}
	if err := p.expect(api.TokSet); err != nil {
		return nil, err
	}

	clause.Action = api.ConflictDoUpdate
	// Parse SET assignments
	for {
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected column name in ON CONFLICT UPDATE SET")
		}
		clause.UpdateColumns = append(clause.UpdateColumns, p.cur.Literal)
		p.advance()
		if err := p.expect(api.TokEQ); err != nil {
			return nil, err
		}
		// Parse value expression
		var expr api.Expr
		if p.cur.Type == api.TokDefault {
			p.advance()
			expr = &api.DefaultExpr{}
		} else {
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			expr = val
		}
		clause.UpdateValues = append(clause.UpdateValues, expr)
		if p.cur.Type != api.TokComma {
			break
		}
		p.advance()
	}

	return clause, nil
}

// ─── EXPLAIN ──────────────────────────────────────────────────────

func (p *parser) parseExplain() (api.Statement, error) {
	p.advance() // consume EXPLAIN
	analyze := false
	// Check for ANALYZE keyword
	if p.cur.Type == api.TokAnalyze || p.cur.Literal == "ANALYZE" {
		analyze = true
		p.advance()
	}
	inner, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	return &api.ExplainStmt{Statement: inner, Analyze: analyze}, nil
}

// ─── SELECT ───────────────────────────────────────────────────────

// parseWith handles WITH clause (CTE - Common Table Expressions).
// Syntax: WITH cte1 AS (SELECT ...), cte2 AS (SELECT ...) SELECT ...
//        WITH RECURSIVE cte AS (...) SELECT ...
func (p *parser) parseWith() (api.Statement, error) {
	p.advance() // consume WITH

	isRecursive := false
	// Check for RECURSIVE keyword
	if p.cur.Type == api.TokRecursive || strings.ToUpper(p.cur.Literal) == "RECURSIVE" {
		isRecursive = true
		p.advance() // consume RECURSIVE
	}

	// Parse one or more CTE definitions separated by comma
	var ctes []*api.CTEClause
	for {
		// Parse CTE name
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected CTE name")
		}
		cteName := p.cur.Literal
		p.advance()

		// Expect AS
		if p.cur.Type != api.TokIdent || strings.ToUpper(p.cur.Literal) != "AS" {
			return nil, p.errorf("expected AS after CTE name")
		}
		p.advance()

		// Expect '('
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after AS")
		}
		p.advance() // consume '('

		// Parse the CTE's SELECT statement
		subq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		// Expect ')'
		if p.cur.Type != api.TokRParen {
			return nil, p.errorf("expected ) after CTE definition")
		}
		p.advance() // consume ')'

		ctes = append(ctes, &api.CTEClause{
			Name:        cteName,
			SelectStmt:  subq, // can be SelectStmt or UnionStmt
			IsRecursive: isRecursive,
		})

		// Check for comma (more CTEs) or end
		if p.cur.Type == api.TokComma {
			p.advance() // consume ','
			continue
		}
		break
	}

	// Parse the main statement (usually SELECT)
	var mainStmt api.Statement
	var err error
	switch {
	case p.cur.Type == api.TokSelect:
		mainStmt, err = p.parseSelect()
	case p.cur.Type == api.TokInsert:
		mainStmt, err = p.parseInsert()
	case p.cur.Type == api.TokUpdate:
		mainStmt, err = p.parseUpdate()
	case p.cur.Type == api.TokDelete:
		mainStmt, err = p.parseDelete()
	default:
		return nil, p.errorf("expected SELECT/INSERT/UPDATE/DELETE after CTE definition")
	}
	if err != nil {
		return nil, err
	}

	return &api.WithStmt{
		CTEs:      ctes,
		Statement: mainStmt,
	}, nil
}

func (p *parser) parseSelect() (api.Statement, error) {
	p.advance() // consume SELECT
	stmt := &api.SelectStmt{}

	// DISTINCT keyword
	if p.cur.Type == api.TokDistinct || strings.ToUpper(p.cur.Literal) == "DISTINCT" {
		stmt.Distinct = true
		p.advance()
	}

	// Columns
	if p.cur.Type == api.TokStar {
		stmt.Columns = []api.SelectColumn{{Expr: &api.StarExpr{}}}
		p.advance()
	} else {
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			col := api.SelectColumn{Expr: expr}
			// Optional AS [alias] — supports both "SELECT id AS alias" and "SELECT id alias"
			if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "AS" {
				p.advance()
				if p.cur.Type != api.TokIdent {
					return nil, p.errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.advance()
			} else if p.cur.Type == api.TokIdent && isAliasTerminator(p.peek.Type) {
				// Implicit alias without AS: "SELECT id alias FROM t" or "SELECT id alias, ..."
				col.Alias = p.cur.Literal
				p.advance()
			}
			stmt.Columns = append(stmt.Columns, col)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
	}

	// FROM (optional — allows SELECT 1, SELECT 'hello')
	var leftTable string
	if p.cur.Type == api.TokFrom {
		p.advance()
		// Check for subquery: FROM (SELECT ...)
		if p.cur.Type == api.TokLParen {
			// Parse subquery
			p.advance() // consume '(' — now at SELECT
			subq, err := p.parseSubquerySelect()
			if err != nil {
				return nil, err
			}
			if p.cur.Type != api.TokRParen {
				return nil, p.errorf("expected ) after subquery")
			}
			p.advance() // consume ')'

			// Parse alias: [AS] alias
			alias := ""
			if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "AS" {
				p.advance()
			}
			if p.cur.Type == api.TokIdent {
				alias = p.cur.Literal
				p.advance()
			} else {
				return nil, p.errorf("expected alias for subquery")
			}

			stmt.DerivedTable = &api.DerivedTable{
				Subquery: &api.SubqueryExpr{Stmt: subq},
				Alias:    alias,
			}
		} else if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected table name or subquery after FROM")
		} else {
			leftTable = p.cur.Literal
			p.advance()
		}
	}

	// Check for JOIN (INNER, LEFT, RIGHT, CROSS all start with their own token)
	if p.cur.Type == api.TokJoin || p.cur.Type == api.TokLeft ||
		p.cur.Type == api.TokRight || p.cur.Type == api.TokCross {
		// Parse first join
		join, err := p.parseJoin(leftTable)
		if err != nil {
			return nil, err
		}
		// Chain additional JOINs iteratively — build left-associative structure
		// so outer ON can reference columns from all previous tables
		for {
			if p.cur.Type == api.TokJoin || p.cur.Type == api.TokLeft ||
				p.cur.Type == api.TokRight || p.cur.Type == api.TokCross {
				// Parse the next join
				nextJoin, err := p.parseJoin("")  // placeholder, will set Left below
				if err != nil {
					return nil, err
				}
				// Build nested structure: (previous) JOIN (next) with previous as LEFT
				// For t1 JOIN t2 JOIN t3:
				// After first join: join = t1 JOIN t2
				// Second: nested = t1 JOIN t2, then outer = nested JOIN t3
				if join != nil {
					// wrap previous join as left of new outer join
					nextJoin.Left = join
					join = nextJoin
				} else {
					join = nextJoin
				}
			} else {
				break
			}
		}
		stmt.Join = join
	} else {
		stmt.Table = leftTable
	}

	// Optional WHERE
	if p.cur.Type == api.TokWhere {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Optional GROUP BY
	if p.cur.Type == api.TokGroup {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		// Parse GROUP BY column [, column ...]
		for {
			colExpr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, colExpr)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
	}

	// Optional HAVING
	if p.cur.Type == api.TokHaving {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// Optional ORDER BY — parse via parseExpr so qualified names (t.col) work
	// Supports multiple columns: ORDER BY col1, col2 DESC, col3
	if p.cur.Type == api.TokOrder {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			// Extract column from expression (parseExpr handles t.col via parsePrimary)
			if ref, ok := expr.(*api.ColumnRef); ok {
				col := ref.Column
				if ref.Table != "" {
					col = ref.Table + "." + ref.Column
				}
				ob := &api.OrderByClause{Column: col}
				if p.cur.Type == api.TokDesc {
					ob.Desc = true
					p.advance()
				} else if p.cur.Type == api.TokAsc {
					p.advance()
				}
				stmt.OrderBy = append(stmt.OrderBy, ob)
			} else {
				return nil, p.errorf("ORDER BY must be a column reference, got %T", expr)
			}
			// Check for comma (more columns)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance() // consume comma
		}
	}

	// Optional LIMIT
	if p.cur.Type == api.TokLimit {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Limit = expr
	}

	// Optional OFFSET (must follow LIMIT)
	if p.cur.Type == api.TokOffset {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Offset = expr
	}

	// Optional FOR UPDATE
	if p.cur.Type == api.TokUpdate || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "FOR") {
		// Check if it's FOR UPDATE (either as TokUpdate or FOR keyword)
		if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "FOR" {
			p.advance() // consume FOR
			if p.cur.Type != api.TokUpdate {
				return nil, p.errorf("expected UPDATE after FOR")
			}
		}
		p.advance() // consume UPDATE

		// Default: FOR UPDATE with LockWaitDefault
		stmt.LockMode = api.UpdateExclusive
		stmt.LockWait = api.LockWaitDefault

		// Check for NOWAIT or SKIP LOCKED
		if p.cur.Type == api.TokIdent {
			upper := strings.ToUpper(p.cur.Literal)
			if upper == "NOWAIT" {
				p.advance()
				stmt.LockWait = api.LockWaitNowait
			} else if upper == "SKIP" {
				p.advance()
				if p.cur.Type != api.TokLocked && !(p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "LOCKED") {
					return nil, p.errorf("expected LOCKED after SKIP")
				}
				if p.cur.Type == api.TokLocked {
					p.advance()
				} else {
					p.advance() // consume LOCKED identifier
				}
				stmt.LockWait = api.LockWaitSkipLocked
			}
		} else if p.cur.Type == api.TokSkip {
			p.advance()
			if p.cur.Type != api.TokLocked && !(p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "LOCKED") {
				return nil, p.errorf("expected LOCKED after SKIP")
			}
			if p.cur.Type == api.TokLocked {
				p.advance()
			} else {
				p.advance() // consume LOCKED identifier
			}
			stmt.LockWait = api.LockWaitSkipLocked
		}
	}

	// Check for UNION [ALL]
	// Right-associative: A UNION B UNION C parses as A UNION (B UNION C)
	// by recursively parsing the right side as another SELECT
	if p.cur.Type == api.TokUnion || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "UNION") {
		p.advance() // consume UNION
		unionAll := false
		// Check for ALL keyword
		if p.cur.Type == api.TokAll || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "ALL") {
			unionAll = true
			p.advance() // consume ALL
		}
		// Parse right side as a statement (may be another UNION → right-assoc)
		rightStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &api.UnionStmt{
			Left:     stmt,
			Right:    rightStmt,
			UnionAll: unionAll,
		}, nil
	}

	// Check for INTERSECT
	// Right-associative: A INTERSECT B INTERSECT C parses as A INTERSECT (B INTERSECT C)
	if p.cur.Type == api.TokIntersect || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "INTERSECT") {
		p.advance() // consume INTERSECT
		// Parse right side as a statement (may be another INTERSECT → right-assoc)
		rightStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &api.IntersectStmt{
			Left:  stmt,
			Right: rightStmt,
		}, nil
	}

	// Check for EXCEPT
	// Right-associative: A EXCEPT B EXCEPT C parses as A EXCEPT (B EXCEPT C)
	if p.cur.Type == api.TokExcept || (p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "EXCEPT") {
		p.advance() // consume EXCEPT
		// Parse right side as a statement (may be another EXCEPT → right-assoc)
		rightStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &api.ExceptStmt{
			Left:  stmt,
			Right: rightStmt,
		}, nil
	}

	return stmt, nil
}

// ─── DELETE ───────────────────────────────────────────────────────


// parseSubquerySelect parses a SELECT statement inside a parenthesized expression
// (e.g., (SELECT col FROM t2 WHERE ...)). It stops at the closing ')' so the
// caller can consume it. Unlike parseSelect, it does NOT consume TokRParen.
func (p *parser) parseSubquerySelect() (*api.SelectStmt, error) {
	if p.cur.Type != api.TokSelect {
		return nil, p.errorf("expected SELECT in subquery")
	}
	p.advance() // consume SELECT
	stmt := &api.SelectStmt{}

	// Columns
	if p.cur.Type == api.TokStar {
		stmt.Columns = []api.SelectColumn{{Expr: &api.StarExpr{}}}
		p.advance()
	} else {
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			col := api.SelectColumn{Expr: expr}
			if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "AS" {
				p.advance()
				if p.cur.Type != api.TokIdent {
					return nil, p.errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.advance()
			}
			stmt.Columns = append(stmt.Columns, col)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
	}

	// FROM
	if err := p.expect(api.TokFrom); err != nil {
		return nil, err
	}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after FROM in subquery")
	}
	stmt.Table = p.cur.Literal
	p.advance()

	// Optional WHERE
	if p.cur.Type == api.TokWhere {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Optional GROUP BY
	if p.cur.Type == api.TokGroup {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		for {
			colExpr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, colExpr)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance()
		}
	}

	// Optional HAVING
	if p.cur.Type == api.TokHaving {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// Optional ORDER BY — supports multiple columns: ORDER BY col1, col2 DESC
	if p.cur.Type == api.TokOrder {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if ref, ok := expr.(*api.ColumnRef); ok {
				col := ref.Column
				if ref.Table != "" {
					col = ref.Table + "." + ref.Column
				}
				ob := &api.OrderByClause{Column: col}
				if p.cur.Type == api.TokDesc {
					ob.Desc = true
					p.advance()
				} else if p.cur.Type == api.TokAsc {
					p.advance()
				}
				stmt.OrderBy = append(stmt.OrderBy, ob)
			} else {
				return nil, p.errorf("ORDER BY must be a column reference, got %T", expr)
			}
			// Check for comma (more columns)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance() // consume comma
		}
	}

	// Optional LIMIT
	if p.cur.Type == api.TokLimit {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Limit = expr
	}

	// Optional OFFSET (must follow LIMIT)
	if p.cur.Type == api.TokOffset {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Offset = expr
	}

	// NOTE: Do NOT consume the trailing TokRParen. Callers handle it.
	return stmt, nil
}

func (p *parser) parseDelete() (api.Statement, error) {
	p.advance() // consume DELETE
	if err := p.expect(api.TokFrom); err != nil {
		return nil, err
	}

	stmt := &api.DeleteStmt{}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after FROM")
	}
	stmt.Table = p.cur.Literal
	p.advance()

	// Optional WHERE
	if p.cur.Type == api.TokWhere {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}
	return stmt, nil
}

// ─── TRUNCATE ─────────────────────────────────────────────────────

func (p *parser) parseTruncate() (api.Statement, error) {
	p.advance() // consume TRUNCATE
	if err := p.expect(api.TokTable); err != nil {
		return nil, err
	}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after TABLE")
	}
	stmt := &api.TruncateStmt{
		Table: p.cur.Literal,
	}
	p.advance()
	return stmt, nil
}

// ─── PRAGMA ────────────────────────────────────────────────────────

func (p *parser) parsePragma() (api.Statement, error) {
	p.advance() // consume PRAGMA

	// Pragma name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected pragma name")
	}
	name := p.cur.Literal
	p.advance()

	stmt := &api.PragmaStmt{Name: name}

	// Check for = value or (arg)
	switch p.cur.Type {
	case api.TokEQ:
		// PRAGMA name = value
		p.advance()
		// Value can be integer or identifier (like "0", "1", "NONE", etc.)
		switch p.cur.Type {
		case api.TokInteger:
			stmt.Value = &api.Literal{
				Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 0},
			}
			// Try to parse the integer value
			if val, err := strconv.ParseInt(p.cur.Literal, 10, 64); err == nil {
				stmt.Value = &api.Literal{
					Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: val},
				}
			}
			p.advance()
		case api.TokIdent:
			// Identifier like "NONE", "FULL", etc.
			stmt.Value = &api.Literal{
				Value: catalogapi.Value{Type: catalogapi.TypeText, Text: p.cur.Literal},
			}
			p.advance()
		case api.TokString:
			stmt.Value = &api.Literal{
				Value: catalogapi.Value{Type: catalogapi.TypeText, Text: p.cur.Literal},
			}
			p.advance()
		default:
			return nil, p.errorf("expected value after =")
		}
	case api.TokLParen:
		// PRAGMA name(arg)
		p.advance()
		// Argument is typically a table name (identifier)
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected argument")
		}
		stmt.Arg = p.cur.Literal
		p.advance()
		if err := p.expect(api.TokRParen); err != nil {
			return nil, err
		}
	case api.TokEOF, api.TokSemicolon:
		// No value, just PRAGMA name
	default:
		// Might be identifier without parentheses: PRAGMA name value
		if p.cur.Type == api.TokIdent {
			stmt.Arg = p.cur.Literal
			p.advance()
		}
	}

	return stmt, nil
}

// ─── UPDATE ───────────────────────────────────────────────────────

func (p *parser) parseUpdate() (api.Statement, error) {
	p.advance() // consume UPDATE
	stmt := &api.UpdateStmt{}

	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after UPDATE")
	}
	stmt.Table = p.cur.Literal
	p.advance()

	if err := p.expect(api.TokSet); err != nil {
		return nil, err
	}

	// Assignments: col = expr, col = expr, ...
	for {
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected column name in SET clause")
		}
		colName := p.cur.Literal
		p.advance()
		if err := p.expect(api.TokEQ); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Assignments = append(stmt.Assignments, api.Assignment{Column: colName, Value: val})
		if p.cur.Type != api.TokComma {
			break
		}
		p.advance()
	}

	// Optional WHERE
	if p.cur.Type == api.TokWhere {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}
	return stmt, nil
}

// ─── Expression Parsing (Precedence Climbing) ─────────────────────

// parseExpr: entry point = or_expr
func (p *parser) parseExpr() (api.Expr, error) {
	if p.depth > 1000 {
		return nil, p.errorf("expression too deeply nested (max 1000 levels)")
	}
	p.depth++
	defer func() { p.depth-- }()
	return p.parseOrExpr()
}

// or_expr = and_expr {"OR" and_expr}
func (p *parser) parseOrExpr() (api.Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == api.TokOr {
		p.advance()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &api.BinaryExpr{Left: left, Op: api.BinOr, Right: right}
	}
	return left, nil
}

// and_expr = not_expr {"AND" not_expr}
func (p *parser) parseAndExpr() (api.Expr, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == api.TokAnd {
		p.advance()
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = &api.BinaryExpr{Left: left, Op: api.BinAnd, Right: right}
	}
	return left, nil
}

// not_expr = ["NOT"] compare_expr
func (p *parser) parseNotExpr() (api.Expr, error) {
	if p.cur.Type == api.TokNot {
		p.advance()
		operand, err := p.parseNotExpr() // recursive — supports NOT NOT x
		if err != nil {
			return nil, err
		}
		return &api.UnaryExpr{Op: api.UnaryNot, Operand: operand}, nil
	}
	return p.parseCompareExpr()
}

// compare_expr = term [cmp_op term]
func (p *parser) parseCompareExpr() (api.Expr, error) {
	left, err := p.parseTermExpr()
	if err != nil {
		return nil, err
	}

	// IS [NOT] NULL
	if p.cur.Type == api.TokIs {
		p.advance()
		not := false
		if p.cur.Type == api.TokNot {
			not = true
			p.advance()
		}
		if p.cur.Type != api.TokNull {
			return nil, p.errorf("expected NULL after IS")
		}
		p.advance()
		return &api.IsNullExpr{Expr: left, Not: not}, nil
	}

	// LIKE
	if p.cur.Type == api.TokLike {
		p.advance()
		if p.cur.Type != api.TokString {
			return nil, p.errorf("expected string pattern after LIKE")
		}
		pattern := p.cur.Literal
		p.advance()
		return &api.LikeExpr{Expr: left, Pattern: pattern}, nil
	}

	// BETWEEN ... AND ...
	if p.cur.Type == api.TokBetween || (p.cur.Type == api.TokNot && p.peek.Type == api.TokBetween) {
		not := false
		if p.cur.Type == api.TokNot {
			not = true
			p.advance()
		}
		p.advance() // consume TokBetween
		low, err := p.parseCompareExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokAnd {
			return nil, p.errorf("expected AND after BETWEEN low value")
		}
		p.advance()
		high, err := p.parseCompareExpr()
		if err != nil {
			return nil, err
		}
		return &api.BetweenExpr{Expr: left, Low: low, High: high, Not: not}, nil
	}

	// [NOT] IN (...)
	if p.cur.Type == api.TokIn || (p.cur.Type == api.TokNot && p.peek.Type == api.TokIn) {
		not := false
		if p.cur.Type == api.TokNot {
			not = true
			p.advance()
		}
		p.advance() // consume TokIn
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after IN")
		}
		p.advance() // consume '(' — now at first element
		var values []api.Expr
		if p.cur.Type != api.TokRParen {
			for {
				// Subquery: ( SELECT ... ) in IN list
				if p.cur.Type == api.TokSelect {
					subq, err := p.parseSubquerySelect()
					if err != nil {
						return nil, err
					}
					// parseSubquerySelect consumed the subquery's ')' — add result and break.
					// If there's a comma, advance and continue for more elements.
					values = append(values, &api.SubqueryExpr{Stmt: subq})
					if p.cur.Type == api.TokComma {
						p.advance()
						continue
					}
					break // at ')' of IN list
				} else {
					val, err := p.parseCompareExpr()
					if err != nil {
						return nil, err
					}
					values = append(values, val)
				}
				if p.cur.Type == api.TokRParen {
					break
				}
				if p.cur.Type != api.TokComma {
					return nil, p.errorf("expected , or ) in IN list")
				}
				p.advance()
			}
		}
		if p.cur.Type != api.TokRParen {
			return nil, p.errorf("expected ) after IN list")
		}
		p.advance()
		return &api.InExpr{Expr: left, Values: values, Not: not}, nil
	}

	// Comparison operators
	var op api.BinaryOp
	switch p.cur.Type {
	case api.TokEQ:
		op = api.BinEQ
	case api.TokNE:
		op = api.BinNE
	case api.TokLT:
		op = api.BinLT
	case api.TokLE:
		op = api.BinLE
	case api.TokGT:
		op = api.BinGT
	case api.TokGE:
		op = api.BinGE
	default:
		return left, nil // no comparison operator
	}
	p.advance()
	right, err := p.parseTermExpr()
	if err != nil {
		return nil, err
	}
	return &api.BinaryExpr{Left: left, Op: op, Right: right}, nil
}

// term_expr = factor {("+" | "-") factor}
// Handles + and - with higher precedence than comparison operators.
func (p *parser) parseTermExpr() (api.Expr, error) {
	left, err := p.parseFactorExpr()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == api.TokPlus || p.cur.Type == api.TokMinus {
		arithOp := p.cur.Type
		p.advance()
		right, err := p.parseFactorExpr()
		if err != nil {
			return nil, err
		}
		var binOp api.BinaryOp
		if arithOp == api.TokPlus {
			binOp = api.BinAdd
		} else {
			binOp = api.BinSub
		}
		left = &api.BinaryExpr{Left: left, Op: binOp, Right: right}
	}
	return left, nil
}

// factor_expr = primary {("*" | "/") primary}
// Handles * and / with higher precedence than + and -.
func (p *parser) parseFactorExpr() (api.Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == api.TokStar || p.cur.Type == api.TokSlash {
		op := p.cur.Type
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if op == api.TokStar {
			left = &api.BinaryExpr{Left: left, Op: api.BinMul, Right: right}
		} else {
			left = &api.BinaryExpr{Left: left, Op: api.BinDiv, Right: right}
		}
	}
	return left, nil
}

// isAggregateFunc returns true for built-in aggregate function names (case-insensitive).
func isAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

// isStringFunc returns true for built-in string function names (case-insensitive).
func isStringFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "SUBSTRING", "CONCAT", "TRIM", "UPPER", "LOWER", "LENGTH":
		return true
	default:
		return false
	}
}

// isJsonFunc returns true for JSON function names (case-insensitive).
func isJsonFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "JSON_EXTRACT", "JSON_SET", "JSON_INSERT", "JSON_REMOVE", "JSON_TYPE":
		return true
	default:
		return false
	}
}

// parseFunctionArgs parses a comma-separated list of expressions inside parentheses.
// For COUNT(*), the '*' is represented as a nil Expr (AggregateCallExpr.Arg == nil).
// The opening '(' has already been consumed by the caller.
func (p *parser) parseFunctionArgs() ([]api.Expr, error) {
	var args []api.Expr

	// Empty args: COUNT()
	if p.cur.Type == api.TokRParen {
		return args, nil
	}

	for {
		// COUNT(*) — '*' as sole argument
		if p.cur.Type == api.TokStar && len(args) == 0 {
			p.advance()
			// Allow only COUNT(*), not COUNT(*, col) etc.
			if p.cur.Type != api.TokRParen {
				return nil, p.errorf("unexpected token after * in aggregate: %s", p.cur.Literal)
			}
			args = append(args, nil) // nil = star
			break
		}

		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		if p.cur.Type == api.TokRParen {
			break
		}
		if p.cur.Type == api.TokComma {
			p.advance()
			continue
		}
		// FROM terminates function args (e.g., SELECT myfunc(id) FROM t)
		if p.cur.Type == api.TokFrom {
			break
		}
		return nil, p.errorf("expected , or ) in function args, got %s", p.cur.Literal)
	}
	return args, nil
}

// parseCaseExpr parses CASE WHEN cond THEN val [WHEN ...] [ELSE val] END
func (p *parser) parseCaseExpr() (api.Expr, error) {
	p.advance() // consume CASE
	var whens []api.WhenClause
	for {
		if p.cur.Type != api.TokWhen {
			return nil, p.errorf("expected WHEN after CASE")
		}
		p.advance() // consume WHEN
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokThen {
			return nil, p.errorf("expected THEN after WHEN condition")
		}
		p.advance() // consume THEN
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		whens = append(whens, api.WhenClause{Cond: cond, Val: val})
		if p.cur.Type != api.TokWhen {
			break
		}
	}
	// Optional ELSE
	var elseVal api.Expr
	if p.cur.Type == api.TokElse {
		p.advance() // consume ELSE
		var err error
		elseVal, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	if p.cur.Type != api.TokEnd {
		return nil, p.errorf("expected END after CASE")
	}
	p.advance() // consume END
	return &api.CaseExpr{Whens: whens, Else: elseVal}, nil
}

// primary = literal | ident ["." ident] | "(" expr ")" | "-" primary | "*"
func (p *parser) parseJoin(left interface{}) (*api.JoinExpr, error) {
	// Check for LEFT/RIGHT/CROSS prefix
	joinType := api.JoinType("INNER")
	if p.cur.Type == api.TokLeft {
		joinType = api.JoinType("LEFT")
		p.advance()
		// Check for optional OUTER keyword
		if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "OUTER" {
			p.advance()
		}
	} else if p.cur.Type == api.TokRight {
		joinType = api.JoinType("RIGHT")
		p.advance()
		// Check for optional OUTER keyword
		if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "OUTER" {
			p.advance()
		}
	} else if p.cur.Type == api.TokCross {
		joinType = api.JoinType("CROSS")
		p.advance()
	}

	// Now expect JOIN keyword (not consumed yet for bare JOIN)
	if p.cur.Type != api.TokJoin {
		return nil, p.errorf("expected JOIN")
	}
	p.advance()

	// CROSS JOIN — no ON
	if joinType == api.JoinType("CROSS") {
		// No ON clause for CROSS JOIN
	}

	// Parse right table name
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected table name after JOIN")
	}
	rightTable := p.cur.Literal
	p.advance()

	// Parse ON condition (not for CROSS JOIN)
	var on api.Expr
	if joinType != api.JoinType("CROSS") {
		if p.cur.Type != api.TokOn {
			return nil, p.errorf("expected ON after JOIN")
		}
		p.advance()
		var err error
		on, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	// If left is empty string (placeholder from chain), try to detect from ON
	joinLeft := left
	if leftStr, ok := left.(string); ok && leftStr == "" {
		// Placeholder — let the chain set the actual left
		joinLeft = nil
	}
	return &api.JoinExpr{
		Left:  joinLeft,
		Right: rightTable,
		Type:  joinType,
		On:    on,
	}, nil
}

func (p *parser) parsePrimary() (api.Expr, error) {
	if p.depth > 1000 {
		return nil, p.errorf("expression too deeply nested (max 1000 levels)")
	}
	p.depth++
	defer func() { p.depth-- }()

	switch p.cur.Type {
	case api.TokInteger:
		val, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, p.errorf("invalid integer: %s", p.cur.Literal)
		}
		p.advance()
		return &api.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: val}}, nil

	case api.TokFloat:
		val, err := strconv.ParseFloat(p.cur.Literal, 64)
		if err != nil {
			return nil, p.errorf("invalid float: %s", p.cur.Literal)
		}
		p.advance()
		return &api.Literal{Value: catalogapi.Value{Type: catalogapi.TypeFloat, Float: val}}, nil

	case api.TokString:
		val := p.cur.Literal
		p.advance()
		return &api.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: val}}, nil

	case api.TokNull:
		p.advance()
		return &api.Literal{Value: catalogapi.Value{IsNull: true}}, nil

	case api.TokMax, api.TokMin, api.TokCount, api.TokSum, api.TokAvg:
		// Aggregate function: MAX(expr), COUNT(*), etc.
		// Also supports OVER clause for window functions: SUM(x) OVER (...)
		funcName := p.cur.Literal
		p.advance() // consume function name
		// expect '('
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after %s", funcName)
		}
		p.advance() // consume '('
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		// p.cur is now ')'
		p.advance() // consume ')'
		var arg api.Expr
		if len(args) == 1 {
			arg = args[0] // nil for COUNT(*)
		} else if len(args) > 1 {
			return nil, p.errorf("%s requires at most one argument", funcName)
		}
		// Check for OVER clause (window function)
		if p.cur.Type == api.TokOver {
			p.advance() // consume OVER
			window, err := p.parseWindowSpec()
			if err != nil {
				return nil, err
			}
			return &api.WindowFuncExpr{Func: strings.ToUpper(funcName), Args: args, Window: window}, nil
		}
		return &api.AggregateCallExpr{Func: strings.ToUpper(funcName), Arg: arg}, nil

	case api.TokRowNumber, api.TokRank, api.TokDenseRank, api.TokFirstValue, api.TokLastValue, api.TokLag, api.TokLead:
		// Window function: ROW_NUMBER() OVER (...), RANK() OVER (...), SUM(x) OVER (...), etc.
		funcName := strings.ToUpper(p.cur.Literal)
		p.advance() // consume function name
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after %s", funcName)
		}
		p.advance() // consume '('
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		// p.cur is now ')'
		p.advance() // consume ')'
		var window *api.WindowSpec
		if p.cur.Type == api.TokOver {
			p.advance() // consume OVER
			window, err = p.parseWindowSpec()
			if err != nil {
				return nil, err
			}
		}
		return &api.WindowFuncExpr{Func: funcName, Args: args, Window: window}, nil

	case api.TokCoalesce:
		// COALESCE(expr1, expr2, ...)
		p.advance() // consume COALESCE
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after COALESCE")
		}
		p.advance() // consume '('
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			return nil, p.errorf("COALESCE requires at least one argument")
		}
		// p.cur is now ')'
		p.advance() // consume ')'
		return &api.CoalesceExpr{Args: args}, nil

	case api.TokNullIf:
		// NULLIF(expr, expr)
		p.advance() // consume NULLIF
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after NULLIF")
		}
		p.advance() // consume '('
		left, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokComma {
			return nil, p.errorf("expected , between NULLIF arguments")
		}
		p.advance() // consume ','
		right, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokRParen {
			return nil, p.errorf("expected ) after NULLIF arguments")
		}
		p.advance() // consume ')'
		return &api.NullIfExpr{Left: left, Right: right}, nil

	case api.TokCast:
		// CAST(expr AS type)
		p.advance() // consume CAST
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after CAST")
		}
		p.advance() // consume '('
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokIdent || strings.ToUpper(p.cur.Literal) != "AS" {
			return nil, p.errorf("expected AS after CAST expression")
		}
		p.advance() // consume AS
		// Parse target type name
		var typeName string
		switch p.cur.Type {
		case api.TokIntKw:
			typeName = "INT"
		case api.TokInteger2:
			typeName = "INTEGER"
		case api.TokTextKw:
			typeName = "TEXT"
		case api.TokFloatKw:
			typeName = "FLOAT"
		case api.TokBlobKw:
			typeName = "BLOB"
		default:
			return nil, p.errorf("expected type name (INT, TEXT, FLOAT, BLOB) after CAST AS")
		}
		p.advance()
		if p.cur.Type != api.TokRParen {
			return nil, p.errorf("expected ) after CAST type")
		}
		p.advance() // consume ')'
		return &api.CastExpr{Expr: expr, TypeName: typeName}, nil

	case api.TokSubstring:
		// SUBSTRING(str FROM start FOR len) or SUBSTRING(str, start, len)
		p.advance() // consume SUBSTRING
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after SUBSTRING")
		}
		p.advance() // consume '('
		str, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		var start, length api.Expr
		// Check for FROM syntax: SUBSTRING(str FROM start [FOR len])
		if p.cur.Type == api.TokFrom {
			p.advance() // consume FROM
			start, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
			// Optional FOR length
			if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "FOR" {
				p.advance()
				length, err = p.parseExpr()
				if err != nil {
					return nil, err
				}
			}
		} else if p.cur.Type == api.TokComma {
			// Comma syntax: SUBSTRING(str, start, len)
			p.advance() // consume ','
			start, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
			// Optional length
			if p.cur.Type == api.TokComma {
				p.advance() // consume ','
				length, err = p.parseExpr()
				if err != nil {
					return nil, err
				}
			}
		}
		if p.cur.Type != api.TokRParen {
			return nil, p.errorf("expected ) after SUBSTRING arguments")
		}
		p.advance() // consume ')'
		return &api.StringFuncExpr{Func: "SUBSTRING", Args: []api.Expr{str}, Start: start, Len: length}, nil

	case api.TokConcat:
		// CONCAT(str1, str2, ...)
		p.advance() // consume CONCAT
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after CONCAT")
		}
		p.advance() // consume '('
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			return nil, p.errorf("CONCAT requires at least one argument")
		}
		// p.cur is now ')'
		p.advance() // consume ')'
		return &api.StringFuncExpr{Func: "CONCAT", Args: args}, nil

	case api.TokTrim, api.TokUpper, api.TokLower, api.TokLength:
		// TRIM(str), UPPER(str), LOWER(str), LENGTH(str)
		funcName := p.cur.Literal
		p.advance() // consume keyword
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after %s", funcName)
		}
		p.advance() // consume '('
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		// p.cur is now ')'
		p.advance() // consume ')'
		return &api.StringFuncExpr{Func: strings.ToUpper(funcName), Args: args}, nil

	case api.TokJsonExtract, api.TokJsonSet, api.TokJsonInsert, api.TokJsonRemove, api.TokJsonType:
		// JSON_EXTRACT(json, path), JSON_SET(json, path, value), JSON_INSERT(json, path, value),
		// JSON_REMOVE(json, path), JSON_TYPE(json, path)
		funcName := p.cur.Literal
		p.advance() // consume keyword
		if p.cur.Type != api.TokLParen {
			return nil, p.errorf("expected ( after %s", funcName)
		}
		p.advance() // consume '('
		args, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		// p.cur is now ')'
		p.advance() // consume ')'
		return &api.JsonFuncExpr{Func: strings.ToUpper(funcName), Args: args}, nil

	case api.TokIdent:
		name := p.cur.Literal
		p.advance()
		// Qualified name: ident.ident (e.g., t1.id)
		if p.cur.Type == api.TokDot {
			p.advance()
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name after .")
			}
			col := p.cur.Literal
			p.advance()
			return &api.ColumnRef{Table: name, Column: col}, nil
		}
		// Function call: ident followed by '('
		if p.cur.Type == api.TokLParen {
			args, err := p.parseFunctionArgs()
			if err != nil {
				return nil, err
			}
			// COUNT(*) — arg is nil, already set
			if isAggregateFunc(name) {
				var arg api.Expr
				if len(args) == 1 {
					arg = args[0]
				} else if len(args) > 1 {
					return nil, p.errorf("aggregate functions require at most one argument")
				}
				return &api.AggregateCallExpr{Func: strings.ToUpper(name), Arg: arg}, nil
			}
			// String functions: SUBSTRING, CONCAT, TRIM, UPPER, LOWER, LENGTH
			if isStringFunc(name) {
				upperName := strings.ToUpper(name)
				// SUBSTRING needs special handling for FROM/FOR syntax
				if upperName == "SUBSTRING" {
					return nil, p.errorf("expected SUBSTRING keyword, not identifier")
				}
				// Other string functions: all use standard (arg1, arg2, ...) syntax
				return &api.StringFuncExpr{Func: upperName, Args: args}, nil
			}
			// JSON functions: JSON_EXTRACT, JSON_SET, JSON_INSERT, JSON_REMOVE, JSON_TYPE
			if isJsonFunc(name) {
				return &api.JsonFuncExpr{Func: strings.ToUpper(name), Args: args}, nil
			}
			// Unknown function — treat as column reference for backward compatibility
			return &api.ColumnRef{Column: name}, nil
		}
		return &api.ColumnRef{Column: name}, nil

	case api.TokStar:
		p.advance()
		return &api.StarExpr{}, nil

	case api.TokLParen:
		if p.depth > 1000 {
			return nil, p.errorf("expression too deeply nested (max 1000 levels)")
		}
		// EXISTS (SELECT ...) or NOT EXISTS (SELECT ...) — check peek since cur is TokLParen.
		// TokNot must be checked too: NOT EXISTS has peek=NOT, not EXISTS.
		if p.peek.Type == api.TokExists || p.peek.Type == api.TokNot {
			p.advance() // consume '('
			not := false
			if p.cur.Type == api.TokNot {
				not = true
				p.advance() // consume NOT
				if p.cur.Type != api.TokExists {
					return nil, p.errorf("expected EXISTS after NOT")
				}
				p.advance() // consume EXISTS
			} else {
				p.advance() // consume EXISTS
			}
			if p.cur.Type != api.TokLParen {
				return nil, p.errorf("expected ( after EXISTS")
			}
			p.advance() // consume '(' — now at SELECT
			subq, err := p.parseSubquerySelect()
			if err != nil {
				return nil, err
			}
			if err := p.expect(api.TokRParen); err != nil {
				return nil, err
			}
			return &api.ExistsExpr{Subquery: &api.SubqueryExpr{Stmt: subq}, Not: not}, nil
		}
		// Subquery: ( SELECT ... ) — check peek since cur is TokLParen.
		// parseSubquerySelect stops at ')', so consume it here.
		if p.peek.Type == api.TokSelect {
			p.advance() // consume '('
			subq, err := p.parseSubquerySelect()
			if err != nil {
				return nil, err
			}
			if err := p.expect(api.TokRParen); err != nil {
				return nil, err
			}
			return &api.SubqueryExpr{Stmt: subq}, nil
		}
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(api.TokRParen); err != nil {
			return nil, err
		}
		return expr, nil

	case api.TokMinus:
		// Check depth for unary minus before descending into operand.
		// We deliberately do NOT call parsePrimary recursively here to avoid
		// depth double-counting: parsePrimary already increments depth.
		// Instead, inline the integer literal fast path.
		if p.peek.Type == api.TokInteger {
			// Fast path: -<integer literal>. Parse the token, fold the negation.
			litTok := p.peek
			val, err := strconv.ParseInt(litTok.Literal, 10, 64)
			if err != nil {
				// Overflow: e.g. -9223372036854775808 where lexer gave us 9223372036854775808
				// Check if this is MaxInt64 being negated to MinInt64.
				if litTok.Literal == "9223372036854775808" {
					p.advance() // consume '-'
					p.advance() // consume TokInteger
					return &api.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: math.MinInt64}}, nil
				}
				return nil, p.errorf("invalid integer: %s", litTok.Literal)
			}
			// Normal negation with overflow check
			if val == math.MinInt64 {
				return nil, p.errorf("integer overflow: cannot negate %d", math.MinInt64)
			}
			p.advance() // consume '-'
			p.advance() // consume TokInteger
			return &api.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: -val}}, nil
		}
		// General case: recurse for non-integer operands (column refs, parens, etc.)
		operand, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if lit, ok := operand.(*api.Literal); ok {
			switch lit.Value.Type {
			case catalogapi.TypeFloat:
				lit.Value.Float = -lit.Value.Float
				return lit, nil
			case catalogapi.TypeInt:
				if lit.Value.Int == math.MinInt64 {
					return nil, p.errorf("integer overflow: cannot negate %d", math.MinInt64)
				}
				lit.Value.Int = -lit.Value.Int
				return lit, nil
			}
		}
		return &api.UnaryExpr{Op: api.UnaryMinus, Operand: operand}, nil

	case api.TokCase:
		return p.parseCaseExpr()

	case api.TokParam:
		// $1, $2, ... positional parameter
		lit := p.cur.Literal
		p.advance()
		if len(lit) < 2 || lit[0] != '$' {
			return nil, p.errorf("invalid parameter: %s", lit)
		}
		index, err := strconv.Atoi(lit[1:])
		if err != nil {
			return nil, p.errorf("invalid parameter index: %s", lit[1:])
		}
		if index < 1 {
			return nil, p.errorf("parameter index must be >= 1: %d", index)
		}
		return &api.ParamRef{Index: index}, nil

	case api.TokQuestion:
		// ? placeholder (ODBC style) — sequential numbering based on occurrence
		p.questionCount++
		p.advance()
		return &api.ParamRef{Index: p.questionCount}, nil

	default:
		return nil, p.errorf("expected expression")
	}
}

// ─── Helpers ──────────────────────────────────────────────────────

// advance moves to the next token.
func (p *parser) advance() {
	p.cur = p.peek
	p.peek = p.lex.nextToken()
}

// expect consumes the current token if it matches the expected type.
func (p *parser) expect(typ api.TokenType) error {
	if p.cur.Type != typ {
		return p.errorf("expected %s", tokenName(typ))
	}
	p.advance()
	return nil
}

// errorf creates a ParseError at the current token position.
func (p *parser) errorf(format string, args ...interface{}) *api.ParseError {
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	return &api.ParseError{
		Message: msg,
		Pos:     p.cur.Pos,
		Token:   p.cur,
	}
}

// tokenName returns a human-readable name for a token type.
func tokenName(typ api.TokenType) string {
	switch typ {
	case api.TokLParen:
		return "'('"
	case api.TokRParen:
		return "')'"
	case api.TokComma:
		return "','"
	case api.TokSemicolon:
		return "';'"
	case api.TokEQ:
		return "'='"
	case api.TokFrom:
		return "FROM"
	case api.TokInto:
		return "INTO"
	case api.TokValues:
		return "VALUES"
	case api.TokSet:
		return "SET"
	case api.TokOn:
		return "ON"
	case api.TokBy:
		return "BY"
	case api.TokKey:
		return "KEY"
	case api.TokIndex:
		return "INDEX"
	case api.TokNull:
		return "NULL"
	case api.TokNot:
		return "NOT"
	case api.TokExists:
		return "EXISTS"
	case api.TokIdent:
		return "identifier"
	case api.TokEOF:
		return "end of input"
	default:
		return "token"
	}
}

// isAliasTerminator returns true if token type terminates a column alias without AS.
// parseWindowSpec parses: OVER ( [PARTITION BY expr [, expr]*] [ORDER BY sortspec [, sortspec]*] [ROWS|RANGE BETWEEN bound AND bound] )
func (p *parser) parseWindowSpec() (*api.WindowSpec, error) {
	// Expect '(' after OVER
	if p.cur.Type != api.TokLParen {
		return nil, p.errorf("expected ( after OVER")
	}
	p.advance() // consume '('

	spec := &api.WindowSpec{}

	// Parse PARTITION BY clause (optional)
	if p.cur.Type == api.TokPartition {
		p.advance() // consume PARTITION
		if p.cur.Type == api.TokBy {
			p.advance() // consume BY
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			spec.PartitionBy = append(spec.PartitionBy, expr)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance() // consume ','
		}
	}

	// Parse ORDER BY clause (optional)
	if p.cur.Type == api.TokOrder {
		p.advance() // consume ORDER
		if p.cur.Type == api.TokBy {
			p.advance() // consume BY
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			// Extract column from expression
			var col string
			if ref, ok := expr.(*api.ColumnRef); ok {
				col = ref.Column
				if ref.Table != "" {
					col = ref.Table + "." + ref.Column
				}
			} else {
				return nil, p.errorf("ORDER BY in window must be a column reference, got %T", expr)
			}
			ob := &api.OrderByClause{Column: col}
			if p.cur.Type == api.TokDesc {
				ob.Desc = true
				p.advance()
			} else if p.cur.Type == api.TokAsc {
				p.advance()
			}
			spec.OrderBy = append(spec.OrderBy, ob)
			if p.cur.Type != api.TokComma {
				break
			}
			p.advance() // consume ','
		}
	}

	// Parse ROWS/RANGE BETWEEN ... AND ... (optional)
	if p.cur.Type == api.TokRows || p.cur.Type == api.TokRange {
		spec.FrameMode = strings.ToUpper(p.cur.Literal)
		p.advance() // consume ROWS or RANGE

		// Expect BETWEEN
		if p.cur.Type != api.TokBetween {
			return nil, p.errorf("expected BETWEEN after %s", spec.FrameMode)
		}
		p.advance() // consume BETWEEN

		// Parse frame start
		startBound, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		spec.FrameStart = *startBound

		// Expect AND
		if p.cur.Type != api.TokAnd {
			return nil, p.errorf("expected AND between frame bounds")
		}
		p.advance() // consume AND

		// Parse frame end
		endBound, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		spec.FrameEnd = *endBound
	}

	// Expect ')'
	if p.cur.Type != api.TokRParen {
		return nil, p.errorf("expected ) after window specification")
	}
	p.advance() // consume ')'

	return spec, nil
}

// parseFrameBound parses a single frame bound: UNBOUNDED PRECEDING, CURRENT ROW, <expr> PRECEDING, <expr> FOLLOWING
func (p *parser) parseFrameBound() (*api.FrameBound, error) {
	bound := &api.FrameBound{}

	if p.cur.Type == api.TokUnbounded {
		bound.Type = "UNBOUNDED PRECEDING"
		p.advance()
		if p.cur.Type == api.TokIdent && strings.ToUpper(p.cur.Literal) == "PRECEDING" {
			p.advance()
		}
	} else if p.cur.Type == api.TokCurrent {
		bound.Type = "CURRENT ROW"
		p.advance()
	} else {
		// Could be <n> PRECEDING or <n> FOLLOWING
		// Parse expression first
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		bound.Expr = expr

		// Look ahead for PRECEDING or FOLLOWING
		if p.cur.Type == api.TokIdent {
			keyword := strings.ToUpper(p.cur.Literal)
			if keyword == "PRECEDING" {
				bound.Type = "PRECEDING"
				p.advance()
			} else if keyword == "FOLLOWING" {
				bound.Type = "FOLLOWING"
				p.advance()
			} else {
				return nil, p.errorf("expected PRECEDING or FOLLOWING after expression")
			}
		} else if p.cur.Type == api.TokFollowing {
			bound.Type = "FOLLOWING"
			p.advance()
		}
	}

	return bound, nil
}

func isAliasTerminator(t api.TokenType) bool {
	switch t {
	case api.TokFrom, api.TokWhere, api.TokGroup, api.TokHaving,
		api.TokOrder, api.TokLimit, api.TokOffset, api.TokComma,
		api.TokRParen, api.TokSemicolon, api.TokEOF, api.TokOn,
		api.TokJoin, api.TokCross, api.TokLeft, api.TokRight,
		api.TokUnion, api.TokIntersect, api.TokExcept, api.TokDistinct,
		api.TokIn, api.TokBetween, api.TokLike, api.TokIs, api.TokNot,
		api.TokAnd, api.TokOr:
		return true
	default:
		return false
	}
}

// extractFirstColumn extracts the first column reference from an expression.
// Used to set Column field for backward compatibility with simple column indexes.
func extractFirstColumn(expr api.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *api.ColumnRef:
		return e.Column
	case *api.StringFuncExpr:
		if len(e.Args) > 0 {
			return extractFirstColumn(e.Args[0])
		}
	case *api.BinaryExpr:
		if e.Left != nil {
			return extractFirstColumn(e.Left)
		}
	case *api.UnaryExpr:
		return extractFirstColumn(e.Operand)
	}
	return ""
}
