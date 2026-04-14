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
	case api.TokDelete:
		return p.parseDelete()
	case api.TokUpdate:
		return p.parseUpdate()
	case api.TokExplain:
		return p.parseExplain()
	default:
		return nil, p.errorf("expected SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP)")
	}
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

	// Optional PRIMARY KEY
	if p.cur.Type == api.TokPrimary {
		p.advance()
		if err := p.expect(api.TokKey); err != nil {
			return col, err
		}
		col.PrimaryKey = true
	}
	return col, nil
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

	// (column)
	if err := p.expect(api.TokLParen); err != nil {
		return nil, err
	}
	if p.cur.Type != api.TokIdent {
		return nil, p.errorf("expected column name")
	}
	stmt.Column = p.cur.Literal
	p.advance()
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

	// VALUES
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
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			row = append(row, expr)
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
	return stmt, nil
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

func (p *parser) parseSelect() (api.Statement, error) {
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
			// Optional AS alias
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

	// Optional GROUP BY
	if p.cur.Type == api.TokGroup {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		// Parse GROUP BY column [, column ...]
		for {
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name in GROUP BY")
			}
			colExpr := &api.ColumnRef{Column: p.cur.Literal}
			p.advance()
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

	// Optional ORDER BY
	if p.cur.Type == api.TokOrder {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected column name after ORDER BY")
		}
		ob := &api.OrderByClause{Column: p.cur.Literal}
		p.advance()
		if p.cur.Type == api.TokDesc {
			ob.Desc = true
			p.advance()
		} else if p.cur.Type == api.TokAsc {
			p.advance()
		}
		stmt.OrderBy = ob
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
			if p.cur.Type != api.TokIdent {
				return nil, p.errorf("expected column name in GROUP BY")
			}
			stmt.GroupBy = append(stmt.GroupBy, &api.ColumnRef{Column: p.cur.Literal})
			p.advance()
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

	// Optional ORDER BY
	if p.cur.Type == api.TokOrder {
		p.advance()
		if err := p.expect(api.TokBy); err != nil {
			return nil, err
		}
		if p.cur.Type != api.TokIdent {
			return nil, p.errorf("expected column name after ORDER BY")
		}
		stmt.OrderBy = &api.OrderByClause{Column: p.cur.Literal}
		p.advance()
		if p.cur.Type == api.TokDesc {
			stmt.OrderBy.Desc = true
			p.advance()
		} else if p.cur.Type == api.TokAsc {
			p.advance()
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

// compare_expr = primary [cmp_op primary | "IS" ["NOT"] "NULL"]
func (p *parser) parseCompareExpr() (api.Expr, error) {
	left, err := p.parsePrimary()
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
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return &api.BinaryExpr{Left: left, Op: op, Right: right}, nil
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

// primary = literal | ident ["." ident] | "(" expr ")" | "-" primary | "*"
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
		return &api.AggregateCallExpr{Func: strings.ToUpper(funcName), Arg: arg}, nil

	case api.TokIdent:
		name := p.cur.Literal
		p.advance()
		// Function call: ident followed by '('
		if p.cur.Type == api.TokLParen {
			args, err := p.parseFunctionArgs()
			if err != nil {
				return nil, err
			}
			// Exactly one argument, or COUNT(*) with nil
			var arg api.Expr
			if len(args) == 1 {
				arg = args[0]
			} else if len(args) > 1 {
				return nil, p.errorf("aggregate functions require at most one argument")
			}
			// COUNT(*) — arg is nil, already set
			if isAggregateFunc(name) {
				return &api.AggregateCallExpr{Func: strings.ToUpper(name), Arg: arg}, nil
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
