package api

import "github.com/akzj/go-fast-kv/internal/sql/value"

// TableSchema describes a table's structure.
type TableSchema struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string
}

// ColumnDef describes a column.
type ColumnDef struct {
	Name string
	Type value.Type
}

// IndexSchema describes an index.
type IndexSchema struct {
	Name   string
	Table  string
	Column string
	Unique bool
}

// CatalogManager manages table and index metadata.
type CatalogManager interface {
	GetTable(name string) (*TableSchema, error)
	CreateTable(schema TableSchema) error
	DropTable(name string) error
	GetIndex(table, name string) (*IndexSchema, error)
	GetIndexByColumn(table, column string) (*IndexSchema, error)
	CreateIndex(schema IndexSchema) error
	DropIndex(table, name string) error
	ListTables() ([]string, error)
	ListIndexesByTable(table string) ([]IndexSchema, error)
}

// Errors
var (
	ErrTableNotFound  = newError("catalog: table not found")
	ErrTableExists    = newError("catalog: table already exists")
	ErrColumnNotFound = newError("catalog: column not found")
	ErrIndexNotFound  = newError("catalog: index not found")
	ErrIndexExists    = newError("catalog: index already exists")
)

type publicError struct{ msg string }

func (e *publicError) Error() string { return e.msg }
func newError(msg string) error     { return &publicError{msg} }