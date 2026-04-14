// Package api defines the public interfaces and types for the SQL parser module.
//
// To understand the parser module, read only this file.
package api

import (
	"fmt"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// ─── Token Types ───────────────────────────────────────────────────

// TokenType identifies the type of a lexical token.
type TokenType int

const (
	// Literals
	TokInteger TokenType = 0  // 42
	TokFloat   TokenType = 1  // 3.14
	TokString  TokenType = 2  // 'hello'
	TokIdent   TokenType = 3  // users, name, age

	// Keywords
	TokSelect   TokenType = 4
	TokFrom     TokenType = 5
	TokWhere    TokenType = 6
	TokInsert   TokenType = 7
	TokInto     TokenType = 8
	TokValues   TokenType = 9
	TokDelete   TokenType = 10
	TokUpdate   TokenType = 11
	TokSet      TokenType = 12
	TokCreate   TokenType = 13
	TokDrop     TokenType = 14
	TokTable    TokenType = 15
	TokIndex    TokenType = 16
	TokOn       TokenType = 17
	TokAnd      TokenType = 18
	TokOr       TokenType = 19
	TokNot      TokenType = 20
	TokNull     TokenType = 21
	TokIs       TokenType = 22
	TokIn       TokenType = 24
	TokBetween  TokenType = 53 // BETWEEN
	TokOrder    TokenType = 23
	TokBy       TokenType = 24
	TokAsc      TokenType = 25
	TokDesc     TokenType = 26
	TokLimit    TokenType = 27
	TokIntKw    TokenType = 28 // INT type keyword
	TokTextKw   TokenType = 29 // TEXT type keyword
	TokFloatKw  TokenType = 30 // FLOAT type keyword
	TokBlobKw   TokenType = 31 // BLOB type keyword
	TokPrimary  TokenType = 32
	TokKey      TokenType = 33
	TokUnique   TokenType = 34
	TokIf       TokenType = 35
	TokGroup   TokenType = 37 // GROUP
	TokHaving  TokenType = 38 // HAVING
	TokCount   TokenType = 39 // COUNT
	TokSum     TokenType = 40 // SUM
	TokAvg     TokenType = 41 // AVG
	TokMin     TokenType = 42 // MIN
	TokMax     TokenType = 43 // MAX
	TokLike    TokenType = 44 // LIKE
	TokExists  TokenType = 44 // EXISTS (moved from 36)
	TokInteger2 TokenType = 37 // INTEGER type keyword (alias for INT)

	// Operators
	TokEQ        TokenType = 38 // =
	TokNE        TokenType = 39 // != or <>
	TokLT        TokenType = 40 // <
	TokLE        TokenType = 41 // <=
	TokGT        TokenType = 42 // >
	TokGE        TokenType = 43 // >=
	TokPlus      TokenType = 44 // +
	TokMinus     TokenType = 45 // -
	TokStar      TokenType = 46 // *
	TokComma     TokenType = 47 // ,
	TokLParen    TokenType = 48 // (
	TokRParen    TokenType = 49 // )
	TokSemicolon TokenType = 50 // ;

	// Special
	TokEOF     TokenType = 51
	TokIllegal TokenType = 52
)

// Token represents a single lexical token.
type Token struct {
	Type    TokenType
	Literal string // raw text of the token
	Pos     int    // byte offset in source
}

// ─── AST Node Interfaces ──────────────────────────────────────────

// Statement is the interface for all top-level SQL statements.
type Statement interface {
	stmtNode()
}

// Expr is the interface for all expression nodes.
type Expr interface {
	exprNode()
}

// ─── DDL Statements ───────────────────────────────────────────────

// CreateTableStmt: CREATE TABLE [IF NOT EXISTS] name (col1 type1, ...)
type CreateTableStmt struct {
	Table       string
	Columns     []ColumnDef
	PrimaryKey  string // optional: column name
	IfNotExists bool
}

func (*CreateTableStmt) stmtNode() {}

// DropTableStmt: DROP TABLE [IF EXISTS] name
type DropTableStmt struct {
	Table    string
	IfExists bool
}

func (*DropTableStmt) stmtNode() {}

// CreateIndexStmt: CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (column)
type CreateIndexStmt struct {
	Index       string
	Table       string
	Column      string
	Unique      bool
	IfNotExists bool
}

func (*CreateIndexStmt) stmtNode() {}

// DropIndexStmt: DROP INDEX [IF EXISTS] name ON table
type DropIndexStmt struct {
	Index    string
	Table    string
	IfExists bool
}

func (*DropIndexStmt) stmtNode() {}

// ─── DML Statements ───────────────────────────────────────────────

// InsertStmt: INSERT INTO table [(col1, col2)] VALUES (v1, v2), ...
type InsertStmt struct {
	Table   string
	Columns []string   // optional column list
	Values  [][]Expr   // multiple rows
}

func (*InsertStmt) stmtNode() {}

// SelectStmt: SELECT columns FROM table [WHERE expr] [ORDER BY col] [LIMIT n]
type SelectStmt struct {
	Columns []SelectColumn
	Table   string
	Where   Expr            // nil if no WHERE
	GroupBy []Expr          // nil if no GROUP BY
	Having  Expr            // nil if no HAVING
	OrderBy *OrderByClause  // nil if no ORDER BY
	Limit   Expr            // nil if no LIMIT
}

func (*SelectStmt) stmtNode() {}

// SelectColumn represents a single column in a SELECT list.
type SelectColumn struct {
	Expr  Expr
	Alias string // optional AS alias
}

// OrderByClause represents ORDER BY column [ASC|DESC].
type OrderByClause struct {
	Column string
	Desc   bool
}

// DeleteStmt: DELETE FROM table [WHERE expr]
type DeleteStmt struct {
	Table string
	Where Expr // nil = delete all rows
}

func (*DeleteStmt) stmtNode() {}

// UpdateStmt: UPDATE table SET col1=val1, ... [WHERE expr]
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr // nil = update all rows
}

func (*UpdateStmt) stmtNode() {}

// Assignment represents col = expr in an UPDATE SET clause.
type Assignment struct {
	Column string
	Value  Expr
}

// ─── Expression Nodes ─────────────────────────────────────────────

// ColumnRef: column reference, e.g. "age" or "users.age"
type ColumnRef struct {
	Table  string // optional qualifier
	Column string
}

func (*ColumnRef) exprNode() {}

// Literal: a literal value (42, 3.14, 'hello', NULL)
type Literal struct {
	Value catalogapi.Value
}

func (*Literal) exprNode() {}

// BinaryExpr: left op right (e.g. age > 18, a AND b)
type BinaryExpr struct {
	Left  Expr
	Op    BinaryOp
	Right Expr
}

func (*BinaryExpr) exprNode() {}

// BinaryOp represents a binary operator.
type BinaryOp int

const (
	BinEQ  BinaryOp = 0 // =
	BinNE  BinaryOp = 1 // !=
	BinLT  BinaryOp = 2 // <
	BinLE  BinaryOp = 3 // <=
	BinGT  BinaryOp = 4 // >
	BinGE  BinaryOp = 5 // >=
	BinAnd BinaryOp = 6 // AND
	BinOr      BinaryOp = 7  // OR
	BinBetween BinaryOp = 8  // BETWEEN
)

// UnaryExpr: op operand (e.g. NOT x, -42)
type UnaryExpr struct {
	Op      UnaryOp
	Operand Expr
}

func (*UnaryExpr) exprNode() {}

// UnaryOp represents a unary operator.
type UnaryOp int

const (
	UnaryNot   UnaryOp = 0 // NOT
	UnaryMinus UnaryOp = 1 // -
)

// IsNullExpr: expr IS [NOT] NULL
type IsNullExpr struct {
	Expr Expr
	Not  bool // true = IS NOT NULL
}

func (*IsNullExpr) exprNode() {}

// StarExpr: SELECT *
type StarExpr struct{}

func (*StarExpr) exprNode() {}

// AggregateCallExpr: COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)
type AggregateCallExpr struct {
	Func string // "COUNT", "SUM", "AVG", "MIN", "MAX"
	Arg  Expr  // nil for COUNT(*), ColumnRef for others
}

func (*AggregateCallExpr) exprNode() {}

// LikeExpr: col LIKE 'pattern'
type LikeExpr struct {
	Expr    Expr  // the column expression
	Pattern string // pattern string
	Escape  byte  // escape char (0 = none)
}

func (*LikeExpr) exprNode() {}

// InExpr: col IN (val1, val2, ...) — Phase 1 only supports literal values.
type InExpr struct {
	Expr   Expr   // the column expression
	Values []Expr // the IN values (literals for Phase 1)
	Not    bool   // true for NOT IN
}

func (*InExpr) exprNode() {}

// BetweenExpr: col BETWEEN low AND high
type BetweenExpr struct {
	Expr Expr // the column expression
	Low  Expr // lower bound
	High Expr // upper bound
	Not  bool // true for NOT BETWEEN
}

func (*BetweenExpr) exprNode() {}

// ─── Parser's own ColumnDef ───────────────────────────────────────

// ColumnDef represents a column definition in CREATE TABLE (parser's own type).
type ColumnDef struct {
	Name       string
	TypeName   string // "INT", "INTEGER", "TEXT", "FLOAT", "BLOB"
	PrimaryKey bool
}

// ─── Parser Interface ─────────────────────────────────────────────

// Parser parses SQL text into AST statements.
type Parser interface {
	// Parse parses a single SQL statement.
	Parse(sql string) (Statement, error)
}

// ─── Errors ───────────────────────────────────────────────────────

// ParseError provides detailed parse error information.
type ParseError struct {
	Message string
	Pos     int   // byte offset in source
	Token   Token // the problematic token
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s (got %q)", e.Pos, e.Message, e.Token.Literal)
}
