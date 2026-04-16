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
	"fmt"
	"strings"

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
	planNode() // also implement Plan for executor compatibility
}

// JoinPlanNode is the common interface for join plan types (JoinPlan, HashJoinPlan).
type JoinPlanNode interface {
	planNode()
	GetOn() parserapi.Expr
	GetType() string
	String() string
	GetLeft() Plan
	GetRight() ScanPlan
	GetLeftSchema() []*catalogapi.ColumnDef
	GetRightSchema() []*catalogapi.ColumnDef
	GetLeftTableName() string
	GetRightTableName() string
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

// InsertSelectPlan inserts rows from a SELECT query into a table.
type InsertSelectPlan struct {
	Table      *catalogapi.TableSchema
	SelectPlan *SelectPlan
	Columns    []string // target column list; empty = use table schema
}

func (*InsertSelectPlan) planNode() {}

// SelectPlan selects rows from a table.
type SelectPlan struct {
	Table         *catalogapi.TableSchema
	Scan          ScanPlan
	Columns       []int                      // column indices for projection; empty = all (SELECT *)
	SelectColumns []parserapi.SelectColumn   // full SELECT list for GROUP BY executor
	Filter        parserapi.Expr            // residual filter not handled by index; nil = no filter
	GroupByExprs  []parserapi.Expr           // nil if no GROUP BY
	Having        parserapi.Expr             // nil if no HAVING
	OrderBy       *OrderByPlan               // nil if no ORDER BY
	Limit         int                        // -1 if no LIMIT
	Offset        int                        // -1 if no OFFSET
	Distinct      bool                       // true for SELECT DISTINCT

	Join            JoinPlanNode           // nil for non-join; non-nil for JOIN
	LeftColumnCount int                    // number of columns in left table (for JOIN projection)
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

// UnionPlan: SELECT ... UNION [ALL] SELECT ...
type UnionPlan struct {
	Left     Plan
	Right    Plan
	UnionAll bool
}

func (*UnionPlan) planNode() {}

// IntersectPlan: SELECT ... INTERSECT SELECT ...
type IntersectPlan struct {
	Left  Plan
	Right Plan
}

func (*IntersectPlan) planNode() {}

// ExceptPlan: SELECT ... EXCEPT SELECT ...
type ExceptPlan struct {
	Left  Plan
	Right Plan
}

func (*ExceptPlan) planNode() {}

// ─── Scan Plans ─────────────────────────────────────────────────────

// TableScanPlan performs a full table scan.
type TableScanPlan struct {
	TableID uint32
	Filter  parserapi.Expr // nil = no filter (return all rows)
}

func (*TableScanPlan) scanNode()  {}
func (*TableScanPlan) planNode() {}

// IndexScanPlan uses an index to narrow the scan.
type IndexScanPlan struct {
	TableID        uint32
	IndexID        uint32
	Index          *catalogapi.IndexSchema
	Op             encodingapi.CompareOp
	Value          catalogapi.Value
	ResidualFilter parserapi.Expr // remaining filter conditions; nil = none
}

func (*IndexScanPlan) scanNode()  {}
func (*IndexScanPlan) planNode() {}

// IndexRangePlan uses an index range scan for LIKE 'prefix%' optimization.
// Encodes LIKE 'abc%' as start='abc' (inclusive), end='abd' (exclusive).
type IndexRangePlan struct {
	TableID        uint32
	IndexID       uint32
	Index         *catalogapi.IndexSchema
	StartPrefix   string          // lower bound (inclusive)
	EndPrefix     string          // upper bound (exclusive)
	ResidualFilter parserapi.Expr // remaining non-indexed conditions
}

func (*IndexRangePlan) scanNode()  {}
func (*IndexRangePlan) planNode() {}

// OrderByPlan describes an ORDER BY clause.
type OrderByPlan struct {
	ColumnIndex int
	Desc        bool
}
// JoinPlan represents a two-table join.
type JoinPlan struct {
	Left         Plan                       // left plan (ScanPlan or nested JoinPlan for multi-join)
	Right        ScanPlan                   // Scan plan for right table
	LeftSchema   []*catalogapi.ColumnDef   // columns from left side (for ON eval)
	RightSchema  []*catalogapi.ColumnDef   // columns from right table
	LeftTable    *catalogapi.TableSchema   // Left table schema (columns)
	RightTable   *catalogapi.TableSchema    // Right table schema (columns)
	On           parserapi.Expr            // join condition (e.g. BinaryExpr t1.id = t2.t1_id)
	Type         string                    // "INNER", "LEFT", "RIGHT", "CROSS"
}

func (*JoinPlan) planNode() {}

// GetOn returns the join condition.
func (p *JoinPlan) GetOn() parserapi.Expr { return p.On }

// GetType returns the join type.
func (p *JoinPlan) GetType() string { return p.Type }

// GetLeft returns the left plan.
func (p *JoinPlan) GetLeft() Plan { return p.Left }

// GetRight returns the right scan plan.
func (p *JoinPlan) GetRight() ScanPlan { return p.Right }

// GetLeftSchema returns the left schema.
func (p *JoinPlan) GetLeftSchema() []*catalogapi.ColumnDef { return p.LeftSchema }

// GetRightSchema returns the right schema.
func (p *JoinPlan) GetRightSchema() []*catalogapi.ColumnDef { return p.RightSchema }

// GetLeftTableName returns the left table name.
func (p *JoinPlan) GetLeftTableName() string {
	if p.LeftTable != nil {
		return p.LeftTable.Name
	}
	return ""
}

// GetRightTableName returns the right table name.
func (p *JoinPlan) GetRightTableName() string {
	if p.RightTable != nil {
		return p.RightTable.Name
	}
	return ""
}

// String returns a human-readable description of the join.
func (p *JoinPlan) String() string {
	var b strings.Builder
	b.WriteString(p.Type + " JOIN")
	if p.LeftTable != nil && p.RightTable != nil {
		b.WriteString(" " + p.LeftTable.Name + " × " + p.RightTable.Name)
	}
	b.WriteString("\n")
	if p.On != nil {
		b.WriteString("├─ ON: " + formatExpr(p.On) + "\n")
	}
	if p.Left != nil {
		switch left := p.Left.(type) {
		case ScanPlan:
			b.WriteString("└─ LEFT: " + scanString(left))
		case *JoinPlan:
			b.WriteString("└─ LEFT:\n")
			for _, line := range strings.Split(left.String(), "\n") {
				b.WriteString("  " + line + "\n")
			}
		default:
			b.WriteString("└─ LEFT: " + fmt.Sprintf("%T", p.Left))
		}
	}
	if p.Right != nil {
		b.WriteString("\n└─ RIGHT: " + scanString(p.Right))
	}
	return b.String()
}

// HashJoinPlan represents an equi-join optimized with hash table.
// Uses O(n+m) hash join instead of O(n*m) nested loop for equi-joins.
type HashJoinPlan struct {
	Left        Plan                     // left plan (ScanPlan or nested JoinPlan for multi-join)
	Right       ScanPlan                 // Scan plan for right table
	LeftSchema  []*catalogapi.ColumnDef // columns from left side (for ON eval)
	RightSchema []*catalogapi.ColumnDef // columns from right table
	LeftTable   string                  // table name for left (for key resolution)
	RightTable  string                  // table name for right (for key resolution)
	LeftKeyIdx  int                     // column index in left schema for hash key
	RightKeyIdx int                     // column index in right schema for hash key
	On          parserapi.Expr          // join condition (may include non-equi parts)
	Type        string                  // "INNER", "LEFT", "RIGHT"
}

func (*HashJoinPlan) planNode() {}

// GetOn returns the join condition.
func (p *HashJoinPlan) GetOn() parserapi.Expr { return p.On }

// GetType returns the join type.
func (p *HashJoinPlan) GetType() string { return p.Type }

// GetLeft returns the left plan.
func (p *HashJoinPlan) GetLeft() Plan { return p.Left }

// GetRight returns the right scan plan.
func (p *HashJoinPlan) GetRight() ScanPlan { return p.Right }

// GetLeftSchema returns the left schema.
func (p *HashJoinPlan) GetLeftSchema() []*catalogapi.ColumnDef { return p.LeftSchema }

// GetRightSchema returns the right schema.
func (p *HashJoinPlan) GetRightSchema() []*catalogapi.ColumnDef { return p.RightSchema }

// GetLeftTableName returns the left table name.
func (p *HashJoinPlan) GetLeftTableName() string { return p.LeftTable }

// GetRightTableName returns the right table name.
func (p *HashJoinPlan) GetRightTableName() string { return p.RightTable }

// String returns a human-readable description of the hash join.
func (p *HashJoinPlan) String() string {
	var b strings.Builder
	b.WriteString(p.Type + " HASH JOIN (optimized)")
	if p.LeftTable != "" && p.RightTable != "" {
		b.WriteString(" " + p.LeftTable + " × " + p.RightTable)
	}
	b.WriteString("\n")
	if p.On != nil {
		b.WriteString("├─ ON: " + formatExpr(p.On) + "\n")
	}
	b.WriteString("└─ hash keys: " + p.LeftTable + "[col" + fmt.Sprintf("%d", p.LeftKeyIdx) + "] = " + p.RightTable + "[col" + fmt.Sprintf("%d", p.RightKeyIdx) + "]")
	return b.String()
}



// ─── Planner Interface ──────────────────────────────────────────────

// Planner converts parsed AST statements into execution plans.
type Planner interface {
	// Plan converts a parsed statement into an execution plan.
	// Returns an error if the statement references non-existent tables/columns,
	// has type mismatches, or uses unsupported expressions.
	Plan(stmt parserapi.Statement) (Plan, error)
}

// ─── EXPLAIN formatting ─────────────────────────────────────────────

// formatExpr returns a human-readable string for an expression.
func formatExpr(expr parserapi.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *parserapi.Literal:
		return fmt.Sprintf("%v", e.Value)
	case *parserapi.ColumnRef:
		return e.Column
	case *parserapi.SubqueryExpr:
		if e.Plan != nil {
			return "(subquery)"
		}
		return "(subquery [unplanned])"
	case *parserapi.BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", formatExpr(e.Left), fmt.Sprintf("%v", e.Op), formatExpr(e.Right))
	case *parserapi.UnaryExpr:
		return fmt.Sprintf("%s %s", fmt.Sprintf("%v", e.Op), formatExpr(e.Operand))
	case *parserapi.InExpr:
		return fmt.Sprintf("%s IN (...)", formatExpr(e.Expr))
	case *parserapi.LikeExpr:
		return fmt.Sprintf("%s LIKE %s", formatExpr(e.Expr), e.Pattern)
	case *parserapi.BetweenExpr:
		return fmt.Sprintf("%s BETWEEN %s AND %s", formatExpr(e.Expr), formatExpr(e.Low), formatExpr(e.High))
	case *parserapi.IsNullExpr:
		return fmt.Sprintf("%s IS NULL", formatExpr(e.Expr))
	case *parserapi.AggregateCallExpr:
		return fmt.Sprintf("%s(%s)", e.Func, formatExpr(e.Arg))
	default:
		return fmt.Sprintf("%T", expr)
	}
}

// walkExprForExplain walks an expression and appends subquery details to b.
func walkExprForExplain(expr parserapi.Expr, b *strings.Builder) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *parserapi.SubqueryExpr:
		if e.Plan != nil {
			if plan, ok := e.Plan.(*SelectPlan); ok {
				for _, line := range strings.Split(plan.String(), "\n") {
					b.WriteString("\n    └─ ")
					b.WriteString(line)
				}
			}
		}
	case *parserapi.BinaryExpr:
		walkExprForExplain(e.Left, b)
		walkExprForExplain(e.Right, b)
	case *parserapi.UnaryExpr:
		walkExprForExplain(e.Operand, b)
	case *parserapi.InExpr:
		walkExprForExplain(e.Expr, b)
		for _, v := range e.Values {
			walkExprForExplain(v, b)
		}
	case *parserapi.LikeExpr:
		walkExprForExplain(e.Expr, b)
	case *parserapi.BetweenExpr:
		walkExprForExplain(e.Expr, b)
		walkExprForExplain(e.Low, b)
		walkExprForExplain(e.High, b)
	case *parserapi.IsNullExpr:
		walkExprForExplain(e.Expr, b)
	case *parserapi.AggregateCallExpr:
		walkExprForExplain(e.Arg, b)
	}
}

// scanString returns a string for a ScanPlan by type-asserting to concrete type.
func scanString(s ScanPlan) string {
	switch s := s.(type) {
	case *TableScanPlan:
		return s.String()
	case *IndexScanPlan:
		return s.String()
	case *IndexRangePlan:
		return s.String()
	default:
		return fmt.Sprintf("%T", s)
	}
}

// String returns a human-readable plan description for EXPLAIN.
func (p *SelectPlan) String() string {
	var b strings.Builder
	b.WriteString("SELECT")
	if len(p.Columns) == 0 {
		b.WriteString(" *")
	} else {
		b.WriteString(fmt.Sprintf(" %d columns", len(p.Columns)))
	}
	if p.Table != nil && p.Join == nil {
		b.WriteString(" FROM " + p.Table.Name)
	}
	if p.Join != nil {
		b.WriteString(" FROM " + p.Join.String())
	} else if p.Scan != nil {
		b.WriteString("\n└─ " + scanString(p.Scan))
	}
	if p.Filter != nil {
		b.WriteString("\n└─ FILTER: " + formatExpr(p.Filter))
		walkExprForExplain(p.Filter, &b)
	}
	if p.GroupByExprs != nil {
		b.WriteString("\n└─ GROUP BY")
	}
	if p.Having != nil {
		b.WriteString("\n└─ HAVING: " + formatExpr(p.Having))
	}
	if p.OrderBy != nil {
		b.WriteString(fmt.Sprintf("\n└─ ORDER BY column=%d desc=%v", p.OrderBy.ColumnIndex, p.OrderBy.Desc))
	}
	if p.Limit > 0 {
		b.WriteString(fmt.Sprintf("\n└─ LIMIT %d", p.Limit))
	}
	if p.Offset > 0 {
		b.WriteString(fmt.Sprintf("\n└─ OFFSET %d", p.Offset))
	}
	return b.String()
}

// String returns a human-readable scan description.
func (p *TableScanPlan) String() string {
	return fmt.Sprintf("TABLE SCAN table=%d", p.TableID)
}

// String returns a human-readable index scan description.
func (p *IndexScanPlan) String() string {
	return fmt.Sprintf("INDEX SCAN table=%d index=%d op=%v value=%v", p.TableID, p.IndexID, p.Op, p.Value)
}

// String returns a human-readable index range description.
func (p *IndexRangePlan) String() string {
	return fmt.Sprintf("INDEX RANGE table=%d index=%d prefix=[%s..%s]", p.TableID, p.IndexID, p.StartPrefix, p.EndPrefix)
}
