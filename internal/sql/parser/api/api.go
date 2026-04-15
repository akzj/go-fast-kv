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
	TokInteger  TokenType = 0  // 42
	TokFloat    TokenType = 1  // 3.14
	TokString   TokenType = 2  // 'hello'
	TokIdent    TokenType = 3  // users, name, age

	// Keywords
	TokSelect    TokenType = 4
	TokFrom      TokenType = 5
	TokWhere     TokenType = 6
	TokInsert    TokenType = 7
	TokInto      TokenType = 8
	TokValues    TokenType = 9
	TokDelete    TokenType = 10
	TokUpdate    TokenType = 11
	TokSet       TokenType = 12
	TokCreate    TokenType = 13
	TokDrop      TokenType = 14
	TokTable     TokenType = 15
	TokIndex     TokenType = 16
	TokOn        TokenType = 17
	TokAnd       TokenType = 18
	TokOr        TokenType = 19
	TokNot       TokenType = 20
	TokNull      TokenType = 21
	TokIs        TokenType = 22
	TokOrder     TokenType = 23
	TokBy        TokenType = 24
	TokIn        TokenType = 25
	TokBetween   TokenType = 26 // BETWEEN
	TokAsc       TokenType = 27
	TokDesc      TokenType = 28
	TokLimit     TokenType = 29
	TokIntKw     TokenType = 30 // INT type keyword
	TokTextKw    TokenType = 31 // TEXT type keyword
	TokFloatKw   TokenType = 32 // FLOAT type keyword
	TokBlobKw    TokenType = 33 // BLOB type keyword
	TokPrimary   TokenType = 34
	TokKey       TokenType = 35
	TokUnique    TokenType = 36
	TokIf        TokenType = 37
	TokDistinct  TokenType = 38 // DISTINCT
	TokGroup     TokenType = 39 // GROUP
	TokInteger2  TokenType = 40 // INTEGER type keyword (alias for INT)
	TokHaving    TokenType = 41 // HAVING
	TokExists    TokenType = 42 // EXISTS
	TokCount     TokenType = 43 // COUNT
	TokSum       TokenType = 44 // SUM
	TokAvg       TokenType = 45 // AVG
	TokMin       TokenType = 46 // MIN
	TokMax       TokenType = 47 // MAX
	TokLike      TokenType = 48 // LIKE

	// Operators
	TokEQ        TokenType = 49 // =
	TokNE        TokenType = 50 // != or <>
	TokLT        TokenType = 51 // <
	TokLE        TokenType = 52 // <=
	TokGT        TokenType = 53 // >
	TokGE        TokenType = 54 // >=
	TokPlus      TokenType = 55 // +
	TokMinus     TokenType = 56 // -
	TokStar      TokenType = 57 // *
	TokComma     TokenType = 58 // ,
	TokLParen    TokenType = 59 // (
	TokRParen    TokenType = 60 // )
	TokSemicolon TokenType = 61 // ;
	TokDot       TokenType = 62 // .

	// Special
	TokEOF      TokenType = 63
	TokIllegal  TokenType = 64
	TokExplain  TokenType = 65 // EXPLAIN
	TokAnalyze  TokenType = 66 // ANALYZE
	TokJoin     TokenType = 67 // JOIN
	TokLeft     TokenType = 68 // LEFT
	TokRight    TokenType = 69 // RIGHT
	TokCross    TokenType = 70 // CROSS
	TokCoalesce TokenType = 71 // COALESCE
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
// JoinType represents the type of JOIN.
type JoinType string

// JoinExpr represents a JOIN ... ON ... clause in FROM.
type JoinExpr struct {
	Left  interface{} // left table name (string) or nested JoinExpr for chained joins
	Right string   // right table name
	Type  JoinType // "INNER", "LEFT", "RIGHT", "CROSS"
	On    Expr     // join condition (nil for CROSS)
}

type SelectStmt struct {
	Columns []SelectColumn
	Table   string      // single table name (used when Join is nil)
	Join    *JoinExpr   // non-nil means this is a JOIN query
	Where   Expr        // nil if no WHERE
	GroupBy []Expr      // nil if no GROUP BY
	Having  Expr        // nil if no HAVING
	OrderBy *OrderByClause // nil if no ORDER BY
	Limit   Expr        // nil if no LIMIT
	Distinct bool       // true for SELECT DISTINCT
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

// CoalesceExpr: COALESCE(expr1, expr2, ...) returns first non-NULL value.
type CoalesceExpr struct {
	Args []Expr // at least one argument
}

func (*CoalesceExpr) exprNode() {}

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

// SubqueryPlan holds the planner's pre-computed subquery plan.
// Runtime type is *plannerapi.SelectPlan (type-assert at use site).
// This is an interface{} with a type alias for documentation clarity.
type SubqueryPlan = interface{}

// SubqueryExpr represents a subquery in an expression context, e.g. (SELECT ...).
type SubqueryExpr struct {
	Stmt Statement   // the subquery (always SelectStmt at parse time)
	Plan SubqueryPlan // the planned subquery; set by the planner during Plan()
}

func (*SubqueryExpr) exprNode() {}

// ExplainStmt wraps a statement for EXPLAIN output.
type ExplainStmt struct {
	Statement Statement // the inner statement to explain
	Analyze   bool     // true for EXPLAIN ANALYZE
}

func (*ExplainStmt) stmtNode() {}

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
