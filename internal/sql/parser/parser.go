package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/akzj/go-fast-kv/internal/sql/value"
)

// Parser is a recursive descent SQL parser.
type Parser struct {
	lexer  *Lexer
	token  Token
	peeked bool
}

// New creates a new Parser.
func New() *Parser {
	return &Parser{}
}

// Parse parses a SQL statement and returns an AST node.
func (p *Parser) Parse(sql string) (Node, error) {
	p.lexer = NewLexer(sql)
	p.nextToken()
	p.peeked = false

	// Skip leading semicolons
	for p.match(TokenPunct, ";") {
	}

	if p.isEOF() {
		return nil, fmt.Errorf("sql: empty statement")
	}

	var stmt Node
	var err error

	switch p.token.Value {
	case "SELECT":
		stmt, err = p.parseSelect()
	case "INSERT":
		stmt, err = p.parseInsert()
	case "UPDATE":
		stmt, err = p.parseUpdate()
	case "DELETE":
		stmt, err = p.parseDelete()
	case "CREATE":
		stmt, err = p.parseCreate()
	case "DROP":
		stmt, err = p.parseDrop()
	default:
		err = fmt.Errorf("sql: unexpected token %q", p.token.Value)
	}

	if err != nil {
		return nil, err
	}

	// Skip trailing semicolons
	for p.match(TokenPunct, ";") {
	}

	if !p.isEOF() {
		return nil, fmt.Errorf("sql: unexpected token after statement: %s", p.token)
	}

	return stmt, nil
}

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	if p.peeked {
		p.peeked = false
		return
	}
	p.token = p.lexer.Next()
}

func (p *Parser) peek() Token {
	if p.peeked {
		return p.token
	}
	p.nextToken()
	p.peeked = true
	return p.token
}

func (p *Parser) isEOF() bool {
	return p.token.Type == TokenEOF
}

func (p *Parser) match(typ TokenType, value string) bool {
	if p.token.Type == typ && p.token.Value == value {
		p.nextToken()
		return true
	}
	return false
}

func (p *Parser) expect(typ TokenType, value string) error {
	if p.token.Type != typ || p.token.Value != value {
		return fmt.Errorf("sql: expected %v %q, got %s", typ, value, p.token)
	}
	p.nextToken()
	return nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	// SELECT
	if err := p.expect(TokenKeyword, "SELECT"); err != nil {
		return nil, err
	}

	// Columns
	var columns []string
	if p.match(TokenOperator, "*") {
		columns = []string{"*"}
	} else {
		for {
			if p.token.Type != TokenIdent {
				return nil, fmt.Errorf("sql: expected column name, got %s", p.token)
			}
			columns = append(columns, p.token.Value)
			p.nextToken()
			if !p.match(TokenPunct, ",") {
				break
			}
		}
	}

	// FROM
	if err := p.expect(TokenKeyword, "FROM"); err != nil {
		return nil, err
	}
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
	}
	table := p.token.Value
	p.nextToken()

	stmt := &SelectStmt{
		Columns: columns,
		Table:   table,
	}

	// WHERE
	if p.match(TokenKeyword, "WHERE") {
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}

	// ORDER BY
	if p.match(TokenKeyword, "ORDER") {
		if err := p.expect(TokenKeyword, "BY"); err != nil {
			return nil, err
		}
		for {
			if p.token.Type != TokenIdent {
				return nil, fmt.Errorf("sql: expected column name, got %s", p.token)
			}
			col := p.token.Value
			p.nextToken()
			asc := true
			if p.match(TokenKeyword, "DESC") {
				asc = false
			} else {
				p.match(TokenKeyword, "ASC") // optional
			}
			stmt.OrderBy = append(stmt.OrderBy, OrderBy{Column: col, Ascending: asc})
			if !p.match(TokenPunct, ",") {
				break
			}
		}
	}

	// LIMIT
	if p.match(TokenKeyword, "LIMIT") {
		if p.token.Type != TokenInt {
			return nil, fmt.Errorf("sql: expected number after LIMIT, got %s", p.token)
		}
		limit, _ := strconv.Atoi(p.token.Value)
		stmt.Limit = limit
		p.nextToken()
	}

	return stmt, nil
}

func (p *Parser) parseCondition() (*Condition, error) {
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected column name, got %s", p.token)
	}
	column := p.token.Value
	p.nextToken()

	// Operator
	var op string
	switch p.token.Value {
	case "=", "<", ">", "<=", ">=", "!=":
		op = p.token.Value
	default:
		return nil, fmt.Errorf("sql: expected operator, got %s", p.token)
	}
	p.nextToken()

	// Value
	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	return &Condition{Column: column, Op: op, Value: val}, nil
}

func (p *Parser) parseValue() (value.Value, error) {
	switch p.token.Type {
	case TokenInt:
		i, _ := strconv.ParseInt(p.token.Value, 10, 64)
		p.nextToken()
		return value.NewInt(i), nil
	case TokenFloat:
		f, _ := strconv.ParseFloat(p.token.Value, 64)
		p.nextToken()
		return value.NewFloat(f), nil
	case TokenString:
		s := p.token.Value
		p.nextToken()
		return value.NewText(s), nil
	case TokenKeyword:
		if p.token.Value == "NULL" {
			p.nextToken()
			return value.Value{Type: value.TypeNull}, nil
		}
		return value.NewText(p.token.Value), nil
	default:
		return value.NewText(p.token.Value), nil
	}
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	// INSERT
	if err := p.expect(TokenKeyword, "INSERT"); err != nil {
		return nil, err
	}

	// INTO
	if err := p.expect(TokenKeyword, "INTO"); err != nil {
		return nil, err
	}

	// Table name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
	}
	table := p.token.Value
	p.nextToken()

	// VALUES
	if err := p.expect(TokenKeyword, "VALUES"); err != nil {
		return nil, err
	}

	// (
	if err := p.expect(TokenPunct, "("); err != nil {
		return nil, err
	}

	// Values
	var values []value.Value
	for {
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		if !p.match(TokenPunct, ",") {
			break
		}
	}

	// )
	if err := p.expect(TokenPunct, ")"); err != nil {
		return nil, err
	}

	return &InsertStmt{Table: table, Values: values}, nil
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	// UPDATE
	if err := p.expect(TokenKeyword, "UPDATE"); err != nil {
		return nil, err
	}

	// Table name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
	}
	table := p.token.Value
	p.nextToken()

	// SET
	if err := p.expect(TokenKeyword, "SET"); err != nil {
		return nil, err
	}

	// Column =
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected column name, got %s", p.token)
	}
	column := p.token.Value
	p.nextToken()

	if err := p.expect(TokenOperator, "="); err != nil {
		return nil, err
	}

	// Value
	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	stmt := &UpdateStmt{Table: table, Column: column, Value: val}

	// WHERE
	if p.match(TokenKeyword, "WHERE") {
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}

	return stmt, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	// DELETE
	if err := p.expect(TokenKeyword, "DELETE"); err != nil {
		return nil, err
	}

	// FROM
	if err := p.expect(TokenKeyword, "FROM"); err != nil {
		return nil, err
	}

	// Table name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
	}
	table := p.token.Value
	p.nextToken()

	stmt := &DeleteStmt{Table: table}

	// WHERE
	if p.match(TokenKeyword, "WHERE") {
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (Node, error) {
	// CREATE
	if err := p.expect(TokenKeyword, "CREATE"); err != nil {
		return nil, err
	}

	if p.match(TokenKeyword, "TABLE") {
		return p.parseCreateTable()
	}
	if p.match(TokenKeyword, "INDEX") {
		return p.parseCreateIndex()
	}

	return nil, fmt.Errorf("sql: expected TABLE or INDEX after CREATE, got %s", p.token)
}

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	// Table name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
	}
	name := p.token.Value
	p.nextToken()

	// (
	if err := p.expect(TokenPunct, "("); err != nil {
		return nil, err
	}

	// Column definitions
	var columns []ColumnDef
	for {
		if p.token.Type != TokenIdent {
			return nil, fmt.Errorf("sql: expected column name, got %s", p.token)
		}
		colName := p.token.Value
		p.nextToken()

		// Type
		if p.token.Type != TokenIdent {
			return nil, fmt.Errorf("sql: expected column type, got %s", p.token)
		}
		colType := strings.ToUpper(p.token.Value)
		p.nextToken()

		columns = append(columns, ColumnDef{Name: colName, Type: colType})

		if !p.match(TokenPunct, ",") {
			break
		}
	}

	// )
	if err := p.expect(TokenPunct, ")"); err != nil {
		return nil, err
	}

	return &CreateTableStmt{Name: name, Columns: columns}, nil
}

func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	// INDEX name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected index name, got %s", p.token)
	}
	indexName := p.token.Value
	p.nextToken()

	// ON
	if err := p.expect(TokenKeyword, "ON"); err != nil {
		return nil, err
	}

	// Table name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
	}
	tableName := p.token.Value
	p.nextToken()

	// (
	if err := p.expect(TokenPunct, "("); err != nil {
		return nil, err
	}

	// Column name
	if p.token.Type != TokenIdent {
		return nil, fmt.Errorf("sql: expected column name, got %s", p.token)
	}
	column := p.token.Value
	p.nextToken()

	// )
	if err := p.expect(TokenPunct, ")"); err != nil {
		return nil, err
	}

	return &CreateIndexStmt{IndexName: indexName, TableName: tableName, Column: column}, nil
}

func (p *Parser) parseDrop() (Node, error) {
	// DROP
	if err := p.expect(TokenKeyword, "DROP"); err != nil {
		return nil, err
	}

	if p.match(TokenKeyword, "INDEX") {
		// DROP INDEX name ON table
		if p.token.Type != TokenIdent {
			return nil, fmt.Errorf("sql: expected index name, got %s", p.token)
		}
		indexName := p.token.Value
		p.nextToken()

		if err := p.expect(TokenKeyword, "ON"); err != nil {
			return nil, err
		}

		if p.token.Type != TokenIdent {
			return nil, fmt.Errorf("sql: expected table name, got %s", p.token)
		}
		tableName := p.token.Value
		p.nextToken()

		return &DropIndexStmt{IndexName: indexName, TableName: tableName}, nil
	}

	return nil, fmt.Errorf("sql: expected INDEX after DROP, got %s", p.token)
}
