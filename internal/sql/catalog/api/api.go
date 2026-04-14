// Package api defines the public interfaces for the catalog module.
//
// To understand the catalog module, read only this file.
package api

import (
	"errors"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── Types ─────────────────────────────────────────────────────────

// Type is the SQL data type for a column.
type Type int

const (
	TypeNull  Type = 0
	TypeInt   Type = 1
	TypeFloat Type = 2
	TypeText  Type = 3
	TypeBlob  Type = 4
)

// Value represents a typed SQL value.
type Value struct {
	Type   Type
	Int    int64
	Float  float64
	Text   string
	Blob   []byte
	IsNull bool
}

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrTableNotFound is returned when a table does not exist.
	ErrTableNotFound = errors.New("catalog: table not found")

	// ErrTableExists is returned when trying to create a table that already exists.
	ErrTableExists = errors.New("catalog: table already exists")

	// ErrIndexNotFound is returned when an index does not exist.
	ErrIndexNotFound = errors.New("catalog: index not found")

	// ErrIndexExists is returned when trying to create an index that already exists.
	ErrIndexExists = errors.New("catalog: index already exists")

	// ErrColumnNotFound is returned when a column does not exist in a table.
	ErrColumnNotFound = errors.New("catalog: column not found")
)

// ─── Schema Types ─────────────────────────────────────────────────

// TableSchema describes a table's structure.
type TableSchema struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string // column name, optional
	TableID    uint32 // persistent ID for key encoding (assigned at CREATE TABLE)
}

// ColumnDef describes a single column in a table.
type ColumnDef struct {
	Name string
	Type Type
}

// IndexSchema describes an index on a table.
type IndexSchema struct {
	Name    string
	Table   string
	Column  string // indexed column
	Unique  bool
	IndexID uint32 // persistent ID for key encoding (assigned at CREATE INDEX)
}

// ─── Interfaces ────────────────────────────────────────────────────

// CatalogManager manages table and index metadata.
type CatalogManager interface {
	// CreateTable creates a new table.
	// Returns ErrTableExists if the table already exists.
	CreateTable(schema TableSchema) error

	// GetTable returns a table schema by name (case-insensitive).
	// Returns ErrTableNotFound if the table does not exist.
	GetTable(name string) (*TableSchema, error)

	// DropTable removes a table and all its indexes.
	// Returns ErrTableNotFound if the table does not exist.
	DropTable(name string) error

	// CreateIndex creates an index on a table column.
	// Returns ErrIndexExists if the index already exists.
	// Returns ErrTableNotFound if the table does not exist.
	CreateIndex(schema IndexSchema) error

	// CreateIndexBatch writes an index catalog entry into a WriteBatch.
	// Both index data and catalog entry are committed atomically — no orphan.
	CreateIndexBatch(schema IndexSchema, batch kvstoreapi.WriteBatch) error

	// GetIndex returns an index by table and index name.
	// Returns ErrIndexNotFound if the index does not exist.
	GetIndex(tableName, indexName string) (*IndexSchema, error)

	// GetIndexByColumn finds an index on a specific column.
	// Returns nil if no index exists on that column.
	GetIndexByColumn(tableName, columnName string) (*IndexSchema, error)

	// DropIndex removes an index.
	// Returns ErrIndexNotFound if the index does not exist.
	DropIndex(tableName, indexName string) error

	// ListTables returns all table names.
	ListTables() ([]string, error)

	// ListIndexes returns all index schemas for a given table.
	// Returns an empty slice (not error) if the table has no indexes.
	ListIndexes(tableName string) ([]*IndexSchema, error)
}
