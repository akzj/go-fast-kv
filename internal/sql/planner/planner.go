package planner

import (
	"fmt"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/value"
)

// PlanNode represents a node in the execution plan.
type PlanNode interface {
	// Kind returns the kind of this plan node.
	Kind() PlanKind
}

// PlanKind is the kind of plan node.
type PlanKind int

const (
	PlanCreateTable PlanKind = iota
	PlanCreateIndex
	PlanDropIndex
	PlanInsert
	PlanUpdate
	PlanDelete
	PlanTableScan
	PlanIndexScan
)

// CreateTablePlan for CREATE TABLE.
type CreateTablePlan struct {
	Schema catalog.TableSchema
}

func (p *CreateTablePlan) Kind() PlanKind { return PlanCreateTable }

// CreateIndexPlan for CREATE INDEX.
type CreateIndexPlan struct {
	Schema catalog.IndexSchema
}

func (p *CreateIndexPlan) Kind() PlanKind { return PlanCreateIndex }

// DropIndexPlan for DROP INDEX.
type DropIndexPlan struct {
	IndexName string
	TableName string
}

func (p *DropIndexPlan) Kind() PlanKind { return PlanDropIndex }

// InsertPlan for INSERT.
type InsertPlan struct {
	Table   string
	Columns []string
	Values  []value.Value
}

func (p *InsertPlan) Kind() PlanKind { return PlanInsert }

// UpdatePlan for UPDATE.
type UpdatePlan struct {
	Table  string
	Column string
	Value  value.Value
	Where  *parser.Condition
}

func (p *UpdatePlan) Kind() PlanKind { return PlanUpdate }

// DeletePlan for DELETE.
type DeletePlan struct {
	Table string
	Where *parser.Condition
}

func (p *DeletePlan) Kind() PlanKind { return PlanDelete }

// TableScanPlan for full table scan.
type TableScanPlan struct {
	Table string
}

func (p *TableScanPlan) Kind() PlanKind { return PlanTableScan }

// IndexScanPlan for index scan.
type IndexScanPlan struct {
	Table   string
	Index   string
	Column  string
	Op      string
	Value   value.Value
	RowKeys [][]byte // pre-fetched row keys from index
}

func (p *IndexScanPlan) Kind() PlanKind { return PlanIndexScan }

// Planner creates execution plans from AST.
type Planner struct {
	kv      kvstoreapi.Store
	catalog *catalog.Catalog
}

// New creates a new Planner.
func New(kv kvstoreapi.Store) *Planner {
	return &Planner{
		kv:      kv,
		catalog: catalog.New(kv),
	}
}

// Plan creates an execution plan from an AST node.
func (p *Planner) Plan(node parser.Node) (PlanNode, error) {
	switch n := node.(type) {
	case *parser.SelectStmt:
		return p.planSelect(n)
	case *parser.InsertStmt:
		return p.planInsert(n)
	case *parser.UpdateStmt:
		return p.planUpdate(n)
	case *parser.DeleteStmt:
		return p.planDelete(n)
	case *parser.CreateTableStmt:
		return p.planCreateTable(n)
	case *parser.CreateIndexStmt:
		return p.planCreateIndex(n)
	case *parser.DropIndexStmt:
		return p.planDropIndex(n)
	default:
		return nil, fmt.Errorf("planner: unsupported node type %T", node)
	}
}

func (p *Planner) planSelect(stmt *parser.SelectStmt) (PlanNode, error) {
	// TODO: Implement index selection
	// For now, always use table scan
	return &TableScanPlan{Table: stmt.Table}, nil
}

func (p *Planner) planInsert(stmt *parser.InsertStmt) (PlanNode, error) {
	// Get table schema to determine columns
	table, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	return &InsertPlan{
		Table:   stmt.Table,
		Columns: columnNames(table.Columns),
		Values:  stmt.Values,
	}, nil
}

func (p *Planner) planUpdate(stmt *parser.UpdateStmt) (PlanNode, error) {
	return &UpdatePlan{
		Table:  stmt.Table,
		Column: stmt.Column,
		Value:  stmt.Value,
		Where:  stmt.Where,
	}, nil
}

func (p *Planner) planDelete(stmt *parser.DeleteStmt) (PlanNode, error) {
	return &DeletePlan{
		Table: stmt.Table,
		Where: stmt.Where,
	}, nil
}

func (p *Planner) planCreateTable(stmt *parser.CreateTableStmt) (PlanNode, error) {
	schema := catalog.TableSchema{
		Name:    stmt.Name,
		Columns: make([]catalog.ColumnDef, len(stmt.Columns)),
	}
	for i, col := range stmt.Columns {
		schema.Columns[i] = catalog.ColumnDef{
			Name: col.Name,
			Type: parseType(col.Type),
		}
	}
	return &CreateTablePlan{Schema: schema}, nil
}

// parseType converts SQL type string to value.Type.
func parseType(typeStr string) value.Type {
	switch typeStr {
	case "INT", "INTEGER":
		return value.TypeInt
	case "FLOAT", "REAL", "DOUBLE":
		return value.TypeFloat
	case "TEXT", "VARCHAR", "CHAR":
		return value.TypeText
	case "BLOB":
		return value.TypeBlob
	default:
		return value.TypeText
	}
}

func (p *Planner) planCreateIndex(stmt *parser.CreateIndexStmt) (PlanNode, error) {
	return &CreateIndexPlan{
		Schema: catalog.IndexSchema{
			Name:   stmt.IndexName,
			Table:  stmt.TableName,
			Column: stmt.Column,
		},
	}, nil
}

func (p *Planner) planDropIndex(stmt *parser.DropIndexStmt) (PlanNode, error) {
	return &DropIndexPlan{
		IndexName: stmt.IndexName,
		TableName: stmt.TableName,
	}, nil
}

func columnNames(cols []catalog.ColumnDef) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
