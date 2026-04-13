package catalog

import "github.com/akzj/go-fast-kv/internal/sql/value"

type TableSchema struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string
}

type ColumnDef struct {
	Name string
	Type value.Type
}

type IndexSchema struct {
	Name   string
	Table  string
	Column string
	Unique bool
}

type CatalogManager interface {
	GetTable(name string) (*TableSchema, error)
	CreateTable(schema TableSchema) error
	GetIndex(table, name string) (*IndexSchema, error)
	GetIndexByColumn(table, column string) (*IndexSchema, error)
	CreateIndex(schema IndexSchema) error
	DropIndex(table, name string) error
}

var (
	ErrTableNotFound = newError("catalog: table not found")
	ErrTableExists   = newError("catalog: table already exists")
	ErrColumnNotFound = newError("catalog: column not found")
	ErrIndexNotFound = newError("catalog: index not found")
	ErrIndexExists   = newError("catalog: index already exists")
)

type publicError struct{ msg string }

func (e *publicError) Error() string { return e.msg }
func newError(msg string) error      { return &publicError{msg} }
