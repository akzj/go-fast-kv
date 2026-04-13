package parser

import "github.com/akzj/go-fast-kv/internal/sql/value"

// Node represents a SQL AST node.
type Node interface {
	nodeKind() NodeKind
}

// NodeKind is the kind of AST node.
type NodeKind int

const (
	NodeSelect NodeKind = iota
	NodeInsert
	NodeUpdate
	NodeDelete
	NodeCreateTable
	NodeCreateIndex
	NodeDropIndex
)

// OrderBy represents ORDER BY clause.
type OrderBy struct {
	Column    string
	Ascending bool
}

// Condition represents a WHERE condition.
type Condition struct {
	Column string
	Op     string // "=", "<", ">", "<=", ">=", "!="
	Value  value.Value
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Columns []string // "*" means all columns
	Table   string
	Where   *Condition
	OrderBy []OrderBy
	Limit   int
}

func (s *SelectStmt) nodeKind() NodeKind { return NodeSelect }

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Table  string
	Values []value.Value
}

func (s *InsertStmt) nodeKind() NodeKind { return NodeInsert }

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Table  string
	Column string
	Value  value.Value
	Where  *Condition
}

func (s *UpdateStmt) nodeKind() NodeKind { return NodeUpdate }

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Table string
	Where *Condition
}

func (s *DeleteStmt) nodeKind() NodeKind { return NodeDelete }

// ColumnDef represents a column definition in CREATE TABLE.
type ColumnDef struct {
	Name string
	Type string
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Name    string
	Columns []ColumnDef
}

func (s *CreateTableStmt) nodeKind() NodeKind { return NodeCreateTable }

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	IndexName string
	TableName string
	Column    string
}

func (s *CreateIndexStmt) nodeKind() NodeKind { return NodeCreateIndex }

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	IndexName string
	TableName string
}

func (s *DropIndexStmt) nodeKind() NodeKind { return NodeDropIndex }
