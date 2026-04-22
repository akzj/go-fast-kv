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
	TokSlash     TokenType = 58 // /
	TokComma     TokenType = 59 // ,
	TokLParen    TokenType = 60 // (
	TokRParen    TokenType = 61 // )
	TokSemicolon TokenType = 62 // ;
	TokDot       TokenType = 63 // .

	// Special
	TokEOF      TokenType = 64
	TokIllegal  TokenType = 64
	TokExplain  TokenType = 65 // EXPLAIN
	TokAnalyze  TokenType = 66 // ANALYZE
	TokJoin     TokenType = 67 // JOIN
	TokLeft     TokenType = 68 // LEFT
	TokRight    TokenType = 69 // RIGHT
	TokCross    TokenType = 70 // CROSS
	TokCoalesce TokenType = 71 // COALESCE
	TokOffset   TokenType = 72 // OFFSET
	TokCase     TokenType = 73 // CASE
	TokWhen     TokenType = 74 // WHEN
	TokThen     TokenType = 75 // THEN
	TokElse     TokenType = 76 // ELSE
	TokEnd      TokenType = 77 // END
	TokUnion    TokenType = 78 // UNION
	TokAll      TokenType = 79 // ALL
	TokIntersect TokenType = 80 // INTERSECT
	TokExcept   TokenType = 81 // EXCEPT
	TokSkip     TokenType = 82 // SKIP
	TokLocked   TokenType = 83 // LOCKED
	TokBegin    TokenType = 84 // BEGIN
	TokCommit   TokenType = 85 // COMMIT
	TokRollback TokenType = 86 // ROLLBACK
	TokAlter    TokenType = 87 // ALTER
	TokAdd      TokenType = 88 // ADD
	TokColumn   TokenType = 89 // COLUMN
	TokRename   TokenType = 90 // RENAME
	TokTo       TokenType = 91 // TO
	TokType     TokenType = 92 // TYPE
	TokDefault  TokenType = 93 // DEFAULT
	TokNullIf   TokenType = 94 // NULLIF
	TokSubstring TokenType = 95 // SUBSTRING
	TokConcat   TokenType = 96 // CONCAT
	TokTrim     TokenType = 97 // TRIM
	TokUpper    TokenType = 98 // UPPER
	TokLower    TokenType = 99 // LOWER
	TokLength   TokenType = 100 // LENGTH
	TokCast           TokenType = 101 // CAST
	TokQuestion       TokenType = 102 // ? placeholder (ODBC style)
	TokAutoIncrement  TokenType = 103 // AUTOINCREMENT
	TokSerial         TokenType = 104 // SERIAL (PostgreSQL alias)
	TokCheck          TokenType = 105 // CHECK constraint
	TokSavepoint      TokenType = 106 // SAVEPOINT
	TokRelease        TokenType = 107 // RELEASE
	TokForeign        TokenType = 108 // FOREIGN
	TokReferences     TokenType = 109 // REFERENCES
	TokCascade        TokenType = 110 // CASCADE
	TokSetNull        TokenType = 111 // SET NULL
	TokNoAction       TokenType = 112 // NO ACTION
	TokRestrict       TokenType = 113 // RESTRICT
	TokParam          TokenType = 114 // $1, $2, ... positional parameter
	TokTruncate       TokenType = 115 // TRUNCATE
	TokConflict       TokenType = 116 // CONFLICT (for ON CONFLICT)
	TokNothing        TokenType = 117 // NOTHING (for DO NOTHING)
	TokWith           TokenType = 118 // WITH (for CTE)
	TokRecursive      TokenType = 119 // RECURSIVE (for CTE)
	TokOver           TokenType = 130 // OVER (window function)
	TokRowNumber      TokenType = 131 // ROW_NUMBER
	TokRank           TokenType = 132 // RANK
	TokDenseRank      TokenType = 133 // DENSE_RANK
	TokPartition      TokenType = 134 // PARTITION
	TokFirstValue     TokenType = 135 // FIRST_VALUE
	TokLastValue      TokenType = 136 // LAST_VALUE
	TokLag            TokenType = 137 // LAG
	TokLead           TokenType = 138 // LEAD
	TokRows           TokenType = 139 // ROWS (window frame)
	TokRange          TokenType = 140 // RANGE (window frame)
	TokUnbounded      TokenType = 141 // UNBOUNDED (window frame)
	TokCurrent        TokenType = 142 // CURRENT (window frame)
	TokFollowing      TokenType = 143 // FOLLOWING (window frame)
	TokJsonExtract    TokenType = 144 // JSON_EXTRACT
	TokJsonSet        TokenType = 145 // JSON_SET
	TokJsonInsert     TokenType = 146 // JSON_INSERT
	TokJsonRemove     TokenType = 147 // JSON_REMOVE
	TokJsonType       TokenType = 148 // JSON_TYPE
	TokPragma        TokenType = 149 // PRAGMA
	TokTrigger       TokenType = 150 // TRIGGER
	TokBefore        TokenType = 151 // BEFORE
	TokAfter         TokenType = 152 // AFTER
	TokForEachRow    TokenType = 153 // FOR EACH ROW
	TokBeginTrigger  TokenType = 154 // BEGIN (trigger body)
	TokEndTrigger    TokenType = 155 // END (trigger body)
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
	ForeignKeys []ForeignKey
}

// ForeignKey represents a parsed FOREIGN KEY constraint (table-level or column-level).
type ForeignKey struct {
	Columns           []string // column names in this table
	ReferencedTable   string   // referenced table name
	ReferencedColumns []string // column names in referenced table (optional)
	OnDelete          string   // ON DELETE action: "CASCADE", "SET NULL", "RESTRICT", "NO ACTION"
	OnUpdate          string   // ON UPDATE action: "CASCADE", "SET NULL", "RESTRICT", "NO ACTION"
}

func (*CreateTableStmt) stmtNode() {}

// DropTableStmt: DROP TABLE [IF EXISTS] name
type DropTableStmt struct {
	Table    string
	IfExists bool
}
// CreateIndexStmt: CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (column | expr)
// When Column is non-empty, it's a simple column index.
// When Expr is non-nil, it's an expression index.
type CreateIndexStmt struct {
	Index       string
	Table       string
	Column      string  // simple column name (for backward compat)
	Expr        Expr    // expression for expression index (e.g., LOWER(email), col1+col2)
	Unique      bool
	IfNotExists bool
}
func (*DropTableStmt) stmtNode() {}



func (*CreateIndexStmt) stmtNode() {}

// DropIndexStmt: DROP INDEX [IF EXISTS] name ON table
type DropIndexStmt struct {
	Index    string
	Table    string
	IfExists bool
}

func (*DropIndexStmt) stmtNode() {}

// AlterTableStmt: ALTER TABLE t ADD COLUMN col TYPE [NOT NULL] [UNIQUE]
//                 ALTER TABLE t DROP COLUMN col
//                 ALTER TABLE t RENAME COLUMN old TO new
//                 ALTER TABLE t RENAME TO new_name
type AlterTableStmt struct {
	Table       string
	Operation   AlterOp
	Column      string       // column name (for ADD, DROP, RENAME COLUMN)
	ColumnNew   string       // new column name (for RENAME COLUMN)
	TableNew    string       // new table name (for RENAME TO)
	TypeName    string       // column type (for ADD)
	NotNull     bool         // NOT NULL constraint (for ADD)
	Unique      bool         // UNIQUE constraint (for ADD)
}

// AlterOp represents the type of ALTER TABLE operation.
type AlterOp int

const (
	AlterAddColumn    AlterOp = 0 // ADD COLUMN
	AlterDropColumn   AlterOp = 1 // DROP COLUMN
	AlterRenameColumn AlterOp = 2 // RENAME COLUMN
	AlterRenameTable  AlterOp = 3 // RENAME TO
)

func (*AlterTableStmt) stmtNode() {}

// ─── DML Statements ───────────────────────────────────────────────

// InsertStmt: INSERT INTO table [(col1, col2)] VALUES (v1, v2), ...
//   INSERT ... ON CONFLICT (col) DO UPDATE SET col=val
//   INSERT ... ON CONFLICT DO NOTHING
type InsertStmt struct {
	Table           string
	Columns         []string       // optional column list
	Values          [][]Expr      // multiple rows
	SelectStmt      *SelectStmt    // SELECT subquery for INSERT ... SELECT
	OnConflict      *OnConflictClause // ON CONFLICT clause for UPSERT
}

// OnConflictClause represents the ON CONFLICT (UPSERT) clause.
type OnConflictClause struct {
	ConflictColumns []string          // columns for conflict detection (e.g., id for PRIMARY KEY)
	Action          ConflictAction     // DO NOTHING or DO UPDATE
	// For DO UPDATE:
	UpdateColumns  []string           // column names to update
	UpdateValues   []Expr             // new values for UPDATE SET
}

type ConflictAction int

const (
	ConflictDoNothing ConflictAction = 0 // DO NOTHING
	ConflictDoUpdate  ConflictAction = 1 // DO UPDATE SET col=val
)

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

// DerivedTable represents a subquery in the FROM clause: (SELECT ...) AS alias
type DerivedTable struct {
	Subquery *SubqueryExpr
	Alias    string // required: the alias after AS
}

type SelectStmt struct {
	Columns []SelectColumn
	Table   string      // single table name (used when DerivedTable is nil and Join is nil)
	DerivedTable *DerivedTable // subquery AS alias (used when Table is empty)
	Join    *JoinExpr   // non-nil means this is a JOIN query
	Where   Expr        // nil if no WHERE
	GroupBy []Expr      // nil if no GROUP BY
	Having  Expr        // nil if no HAVING
	OrderBy []*OrderByClause // nil if no ORDER BY
	Limit   Expr        // nil if no LIMIT
	Offset  Expr        // nil if no OFFSET
	Distinct bool       // true for SELECT DISTINCT
	LockMode LockMode   // lock mode for FOR UPDATE
	LockWait LockWait   // lock wait behavior
}

func (*SelectStmt) stmtNode() {}

// CTEClause represents a Common Table Expression (WITH ... AS ...).
type CTEClause struct {
	Name       string    // CTE name, e.g., "temp" in "WITH temp AS (...)"
	SelectStmt Statement // the subquery defining this CTE (can be SelectStmt or UnionStmt)
	IsRecursive bool     // true for "WITH RECURSIVE"
}

func (*CTEClause) exprNode() {} // CTE is used as a statement wrapper

// WithStmt represents a WITH clause wrapping a main statement.
// The WITH clause contains one or more CTE definitions.
type WithStmt struct {
	CTEs      []*CTEClause // CTE definitions
	Statement Statement    // the main statement (usually SelectStmt)
}

func (*WithStmt) stmtNode() {}

// LockMode represents the lock mode for SELECT ... FOR UPDATE.
type LockMode int

const (
	NoUpdate        LockMode = 0 // no FOR UPDATE
	UpdateShared    LockMode = 1 // FOR UPDATE SHARE (future)
	UpdateExclusive LockMode = 2 // FOR UPDATE
)

// LockWait represents the lock wait behavior for SELECT ... FOR UPDATE.
type LockWait int

const (
	LockWaitDefault    LockWait = 0 // wait for lock (default)
	LockWaitNowait     LockWait = 1 // NOWAIT - fail immediately
	LockWaitSkipLocked LockWait = 2 // SKIP LOCKED - skip locked rows
)

// UnionStmt: SELECT ... UNION [ALL] SELECT ...
type UnionStmt struct {
	Left     Statement
	Right    Statement
	UnionAll bool
}

func (*UnionStmt) stmtNode() {}

// IntersectStmt: SELECT ... INTERSECT SELECT ...
type IntersectStmt struct {
	Left  Statement
	Right Statement
}

func (*IntersectStmt) stmtNode() {}

// ExceptStmt: SELECT ... EXCEPT SELECT ...
type ExceptStmt struct {
	Left  Statement
	Right Statement
}

func (*ExceptStmt) stmtNode() {}

// BeginStmt: BEGIN transaction
type BeginStmt struct{}

func (*BeginStmt) stmtNode() {}

// CommitStmt: COMMIT transaction
type CommitStmt struct{}

func (*CommitStmt) stmtNode() {}

// RollbackStmt: ROLLBACK transaction
type RollbackStmt struct{}

func (*RollbackStmt) stmtNode() {}

// SavepointStmt: SAVEPOINT name
type SavepointStmt struct {
	Name string
}

func (*SavepointStmt) stmtNode() {}

// RollbackToSavepointStmt: ROLLBACK TO SAVEPOINT name
type RollbackToSavepointStmt struct {
	Name string
}

func (*RollbackToSavepointStmt) stmtNode() {}

// ReleaseSavepointStmt: RELEASE SAVEPOINT name
type ReleaseSavepointStmt struct {
	Name string
}

func (*ReleaseSavepointStmt) stmtNode() {}

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

// TruncateStmt: TRUNCATE TABLE t
type TruncateStmt struct {
	Table string
}

func (*TruncateStmt) stmtNode() {}

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
	BinEQ      BinaryOp = 0 // =
	BinNE      BinaryOp = 1 // !=
	BinLT      BinaryOp = 2 // <
	BinLE      BinaryOp = 3 // <=
	BinGT      BinaryOp = 4 // >
	BinGE      BinaryOp = 5 // >=
	BinAnd     BinaryOp = 6 // AND
	BinOr      BinaryOp = 7 // OR
	BinBetween BinaryOp = 8 // BETWEEN
	BinAdd     BinaryOp = 9 // +
	BinSub     BinaryOp = 10 // -
	BinMul     BinaryOp = 11 // *
	BinDiv     BinaryOp = 12 // /
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

// NullIfExpr: NULLIF(a, b) returns a if a != b, NULL if a == b.
type NullIfExpr struct {
	Left  Expr
	Right Expr
}

func (*NullIfExpr) exprNode() {}

// StringFuncExpr represents string functions: SUBSTRING, CONCAT, UPPER, LOWER, LENGTH, TRIM
type StringFuncExpr struct {
	Func  string   // "SUBSTRING", "CONCAT", "UPPER", "LOWER", "LENGTH", "TRIM"
	Args  []Expr   // arguments for the function
	Start Expr     // start position for SUBSTRING (1-indexed)
	Len   Expr     // length for SUBSTRING (optional)
}

func (*StringFuncExpr) exprNode() {}

// JsonFuncExpr represents JSON functions: JSON_EXTRACT, JSON_SET, JSON_INSERT, JSON_REMOVE, JSON_TYPE
type JsonFuncExpr struct {
	Func  string   // "JSON_EXTRACT", "JSON_SET", "JSON_INSERT", "JSON_REMOVE", "JSON_TYPE"
	Args  []Expr   // arguments: json, path [, value...]
}

func (*JsonFuncExpr) exprNode() {}

// CastExpr: CAST(expr AS type) performs type conversion.
type CastExpr struct {
	Expr     Expr  // the expression to cast
	TypeName string // target type: "INT", "TEXT", "FLOAT", "BLOB"
}

func (*CastExpr) exprNode() {}

// StarExpr: SELECT *
type StarExpr struct{}

func (*StarExpr) exprNode() {}

// AggregateCallExpr: COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)
type AggregateCallExpr struct {
	Func string // "COUNT", "SUM", "AVG", "MIN", "MAX"
	Arg  Expr  // nil for COUNT(*), ColumnRef for others
}

func (*AggregateCallExpr) exprNode() {}

// WindowSpec: OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)
type WindowSpec struct {
	PartitionBy []Expr      // PARTITION BY expressions (nil = all rows)
	OrderBy     []*OrderByClause // ORDER BY within window (nil = no ordering)
	FrameStart  FrameBound // ROWS/RANGE start (UNBOUNDED PRECEDING, CURRENT ROW, etc.)
	FrameEnd    FrameBound // ROWS/RANGE end
	FrameMode   string     // "ROWS" or "RANGE" (empty = default)
}

// FrameBound: UNBOUNDED PRECEDING, CURRENT ROW, expr PRECEDING, expr FOLLOWING
type FrameBound struct {
	Type string // "UNBOUNDED PRECEDING", "CURRENT ROW", "PRECEDING", "FOLLOWING"
	Expr Expr   // numeric expression for PRECEDING/FOLLOWING (nil for keywords)
}

// WindowFuncExpr: window function call with OVER clause
// Examples: ROW_NUMBER() OVER (...), SUM(x) OVER (PARTITION BY y ORDER BY z)
type WindowFuncExpr struct {
	Func   string       // "ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG", "COUNT", etc.
	Args   []Expr       // Arguments (empty for ROW_NUMBER, RANK; 1 for SUM, LAG, etc.)
	Window *WindowSpec  // nil means no OVER clause (shouldn't happen after parsing)
}

func (*WindowFuncExpr) exprNode() {}

// DefaultExpr: DEFAULT keyword in INSERT VALUES (resolves to column's default value)
type DefaultExpr struct{}

func (*DefaultExpr) exprNode() {}

// ParamRef: positional parameter reference ($1, $2, ...)
// Used in prepared statements to bind values at execution time.
type ParamRef struct {
	Index int // 1-based index of the parameter
}

func (*ParamRef) exprNode() {}

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

// CaseExpr represents a CASE expression: CASE WHEN cond THEN val [WHEN ...] [ELSE val] END
type CaseExpr struct {
	Whens []WhenClause // each WHEN cond THEN val
	Else  Expr         // nil if no ELSE (result is NULL)
}

type WhenClause struct {
	Cond Expr // condition expression
	Val  Expr // result when condition is true
}

func (*CaseExpr) exprNode() {}

// SubqueryExpr represents a subquery in an expression context, e.g. (SELECT ...).
type SubqueryExpr struct {
	Stmt Statement   // the subquery (always SelectStmt at parse time)
	Plan SubqueryPlan // the planned subquery; set by the planner during Plan()
}

func (*SubqueryExpr) exprNode() {}

// ExistsExpr represents EXISTS (SELECT ...) or NOT EXISTS (SELECT ...).
type ExistsExpr struct {
	Subquery *SubqueryExpr
	Not      bool // true for NOT EXISTS
}

func (*ExistsExpr) exprNode() {}

type SubqueryPlan = interface{}

// ExplainStmt wraps a statement for EXPLAIN output.
type ExplainStmt struct {
	Statement Statement // the inner statement to explain
	Analyze   bool     // true for EXPLAIN ANALYZE
}

func (*ExplainStmt) stmtNode() {}

// PragmaStmt represents a PRAGMA command.
// Syntax: PRAGMA name [= value]
type PragmaStmt struct {
	Name  string       // pragma name: "database_list", "table_info", "index_list", etc.
	Arg   string       // optional argument: table name, value, etc.
	Value Expr         // optional value to SET (for PRAGMA name = value)
}

func (*PragmaStmt) stmtNode() {}

// TriggerStmt: CREATE TRIGGER name [BEFORE|AFTER] [INSERT|UPDATE|DELETE] ON table [WHEN condition] BEGIN ... END
type TriggerStmt struct {
	Name      string       // trigger name
	Timing    string       // "BEFORE" or "AFTER"
	Event     string       // "INSERT", "UPDATE", "DELETE"
	Table     string       // target table name
	WhenCond  Expr         // nil if no WHEN clause
	Body      []Statement  // trigger body statements
}

func (*TriggerStmt) stmtNode() {}

// DropTriggerStmt: DROP TRIGGER [IF EXISTS] name
type DropTriggerStmt struct {
	Name     string
	IfExists bool
}

func (*DropTriggerStmt) stmtNode() {}

// ─── Parser's own ColumnDef ───────────────────────────────────────

// ColumnDef represents a column definition in CREATE TABLE (parser's own type).
type ColumnDef struct {
	Name         string
	TypeName     string // "INT", "INTEGER", "TEXT", "FLOAT", "BLOB"
	PrimaryKey   bool
	NotNull      bool
	Unique       bool
	DefaultValue catalogapi.Value // DEFAULT value; zero Value means not specified
	AutoInc      bool             // AUTOINCREMENT flag
	CheckExpr    Expr              // CHECK constraint expression; nil if not specified
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
