// Package api defines the public interfaces and types for the SQL planner module.
//
// To understand the planner module, read only this file.
//
// The planner converts parsed AST statements into execution plans.
// It resolves table/column references against the catalog, selects
// scan strategies (table scan vs index scan), and validates types.
package api

import (
	"errors"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrTableNotFound is returned when the referenced table does not exist.
	ErrTableNotFound = errors.New("planner: table not found")

	// ErrColumnNotFound is returned when a referenced column does not exist.
	ErrColumnNotFound = errors.New("planner: column not found")

	// ErrTypeMismatch is returned when a value's type doesn't match the column type.
	ErrTypeMismatch = errors.New("planner: type mismatch")

	// ErrColumnCountMismatch is returned when INSERT value count doesn't match column count.
	ErrColumnCountMismatch = errors.New("planner: column count mismatch")

	// ErrUnsupportedExpr is returned for expressions not supported in Phase 1.
	// For example, SET col = other_col in UPDATE (only literals allowed).
	ErrUnsupportedExpr = errors.New("planner: unsupported expression (Phase 1: literals only)")

	// ErrEmptyTable is returned when CREATE TABLE has no columns.
	ErrEmptyTable = errors.New("planner: table must have at least one column")

	// ErrInvalidPlan is returned when a valid plan cannot be created.
	ErrInvalidPlan = errors.New("planner: cannot create valid plan")
)

// ─── Plan Interface ─────────────────────────────────────────────────

// Plan represents an execution plan for a SQL statement.
type Plan interface {
	planNode()
}

// ScanPlan describes how to find rows in a table.
type ScanPlan interface {
	scanNode()
}

// ─── DDL Plans ──────────────────────────────────────────────────────

// CreateTablePlan creates a new table.
type CreateTablePlan struct {
	Schema      catalogapi.TableSchema
	IfNotExists bool
}

func (*CreateTablePlan) planNode() {}

// DropTablePlan drops a table and its data.
type DropTablePlan struct {
	TableName string
	TableID   uint32 // 0 if table not found (IF EXISTS case)
	IfExists  bool
}

func (*DropTablePlan) planNode() {}

// CreateIndexPlan creates an index on a table column.
type CreateIndexPlan struct {
	Schema      catalogapi.IndexSchema
	IfNotExists bool
}

func (*CreateIndexPlan) planNode() {}

// DropIndexPlan drops an index.
type DropIndexPlan struct {
	IndexName string
	TableName string
	IfExists  bool
}

func (*DropIndexPlan) planNode() {}

// ─── DML Plans ──────────────────────────────────────────────────────

// InsertPlan inserts rows into a table.
type InsertPlan struct {
	Table *catalogapi.TableSchema
	Rows  [][]catalogapi.Value // resolved values, aligned with table columns
}

func (*InsertPlan) planNode() {}

// SelectPlan selects rows from a table.
type SelectPlan struct {
	Table   *catalogapi.TableSchema
	Scan    ScanPlan
	Columns []int          // column indices to project; empty = all (SELECT *)
	Filter  parserapi.Expr // residual filter not handled by index; nil = no filter
	OrderBy *OrderByPlan   // nil if no ORDER BY
	Limit   int            // -1 if no LIMIT
}

func (*SelectPlan) planNode() {}

// DeletePlan deletes rows from a table.
type DeletePlan struct {
	Table *catalogapi.TableSchema
	Scan  ScanPlan // nil WHERE → scan is TableScanPlan with nil Filter (delete all)
}

func (*DeletePlan) planNode() {}

// UpdatePlan updates rows in a table.
type UpdatePlan struct {
	Table       *catalogapi.TableSchema
	Assignments map[int]catalogapi.Value // columnIndex → new literal value
	Scan        ScanPlan
}

func (*UpdatePlan) planNode() {}

// ─── Scan Plans ─────────────────────────────────────────────────────

// TableScanPlan performs a full table scan.
type TableScanPlan struct {
	TableID uint32
	Filter  parserapi.Expr // nil = no filter (return all rows)
}

func (*TableScanPlan) scanNode() {}

// IndexScanPlan uses an index to narrow the scan.
type IndexScanPlan struct {
	TableID        uint32
	IndexID        uint32
	Index          *catalogapi.IndexSchema
	Op             encodingapi.CompareOp
	Value          catalogapi.Value
	ResidualFilter parserapi.Expr // remaining filter conditions; nil = none
}

func (*IndexScanPlan) scanNode() {}

// OrderByPlan describes an ORDER BY clause.
type OrderByPlan struct {
	ColumnIndex int
	Desc        bool
}

// ─── Planner Interface ──────────────────────────────────────────────

// Planner converts parsed AST statements into execution plans.
type Planner interface {
	// Plan converts a parsed statement into an execution plan.
	// Returns an error if the statement references non-existent tables/columns,
	// has type mismatches, or uses unsupported expressions.
	Plan(stmt parserapi.Statement) (Plan, error)
}
