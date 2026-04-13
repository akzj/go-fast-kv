// Package api defines interfaces for the SQL engine components.
// This follows the Interface-First pattern from CONVENTIONS.md.
//
// Interfaces defined here are for the composition layer (engine.go).
// Internal interfaces (like Iterator) remain in their respective packages.
package api

import (
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
)

// QueryPlanner converts parsed SQL AST to execution plans.
type QueryPlanner interface {
	// Plan converts a parsed SQL AST to an execution plan.
	Plan(node parser.Node) (planner.PlanNode, error)
}

// CatalogManager manages table and index metadata.
type CatalogManager interface {
	// CreateTable creates a new table.
	CreateTable(schema catalog.TableSchema) error

	// GetTable returns a table schema by name (case-insensitive).
	GetTable(name string) (*catalog.TableSchema, error)

	// DropTable deletes a table and its data.
	DropTable(name string) error

	// CreateIndex creates an index on a table.
	CreateIndex(schema catalog.IndexSchema) error

	// GetIndex returns an index by table and index name.
	GetIndex(tableName, indexName string) (*catalog.IndexSchema, error)

	// GetIndexByColumn finds an index on a specific column.
	GetIndexByColumn(tableName, columnName string) (*catalog.IndexSchema, error)

	// DropIndex deletes an index.
	DropIndex(tableName, indexName string) error

	// ListTables returns all table names.
	ListTables() ([]string, error)

	// ListIndexesByTable returns all indexes on a table.
	ListIndexesByTable(tableName string) ([]catalog.IndexSchema, error)
}
