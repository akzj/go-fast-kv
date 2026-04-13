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
	TokInteger TokenType = iota // 42
	TokFloat                    // 3.14
	TokString                   // 'hello'
	TokIdent                    // users, name, age

	// Keywords
	TokSelect
	TokFrom
	TokWhere
	TokInsert
	TokInto
	TokValues
	TokDelete
	TokUpdate
	TokSet
	TokCreate
	TokDrop
	TokTable
	TokIndex
	TokOn
	TokAnd
	TokOr
	TokNot
	TokNull
	TokIs
	TokOrder
	TokBy
	TokAsc
	TokDesc
	TokLimit
	TokIntKw    // INT type keyword
	TokTextKw   // TEXT type keyword
	TokFloatKw  // FLOAT type keyword
	TokBlobKw   // BLOB type keyword
	TokPrimary
	TokKey
	TokUnique
	TokIf
	TokExists
	TokInteger2 // INTEGER type keyword (alias for INT)

	// Operators
	TokEQ        // =
	TokNE        // != or <>
	TokLT        // <
	TokLE        // <=
	TokGT        // >
	TokGE        // >=
	TokPlus      // +
	TokMinus     // -
	TokStar      // *
	TokComma     // ,
	TokLParen    // (
	TokRParen    // )
	TokSemicolon // ;

	// Special
	TokEOF
	TokIllegal
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
	BinEQ  BinaryOp = iota // =
	BinNE                   // !=
	BinLT                   // <
	BinLE                   // <=
	BinGT                   // >
	BinGE                   // >=
	BinAnd                  // AND
	BinOr                   // OR
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
	UnaryNot   UnaryOp = iota // NOT
	UnaryMinus                 // -
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
