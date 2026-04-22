// Package api defines the public interfaces for the catalog module.
//
// To understand the catalog module, read only this file.
package api

import (
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	sqlerrors "github.com/akzj/go-fast-kv/internal/sql/errors"
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

// ErrTableNotFound is returned when a table does not exist.
var ErrTableNotFound = sqlerrors.ErrTableNotFound("")

// ErrTableExists is returned when trying to create a table that already exists.
var ErrTableExists = sqlerrors.ErrTableExists("")

// ErrIndexNotFound is returned when an index does not exist.
var ErrIndexNotFound = sqlerrors.ErrIndexNotFound("", "")

// ErrIndexExists is returned when trying to create an index that already exists.
var ErrIndexExists = sqlerrors.ErrIndexExists("")

// ErrColumnNotFound is returned when a column does not exist in a table.
var ErrColumnNotFound = sqlerrors.ErrColumnNotFound("", "")

// ErrTriggerNotFound is returned when a trigger does not exist.
var ErrTriggerNotFound = sqlerrors.ErrTriggerNotFound("")

// ErrTriggerExists is returned when trying to create a trigger that already exists.
var ErrTriggerExists = sqlerrors.ErrTriggerExists("")

// ErrViewNotFound is returned when a view does not exist.
var ErrViewNotFound = sqlerrors.ErrViewNotFound("")

// ErrViewExists is returned when trying to create a view that already exists.
var ErrViewExists = sqlerrors.ErrViewExists("")

// ─── Schema Types ─────────────────────────────────────────────────

// TableSchema describes a table's structure.
type TableSchema struct {
	Name             string            `json:"N,omitempty"`
	Columns          []ColumnDef       `json:"C,omitempty"`
	PrimaryKey      string            `json:"P,omitempty"`
	TableID         uint32           `json:"I,omitempty"`
	CheckConstraints []CheckConstraint `json:"CC,omitempty"`
	ForeignKeys      []ForeignKeySchema `json:"FK,omitempty"`
}

// ColumnDef describes a single column in a table.
type ColumnDef struct {
	Table        string           `json:"T,omitempty"`
	Name         string           `json:"N,omitempty"`
	Type         Type             `json:"Y,omitempty"`
	NotNull      bool             `json:"NN,omitempty"`
	DefaultValue *Value           `json:"D,omitempty"`
	AutoInc      bool             `json:"AI,omitempty"`
	Check        *CheckConstraint `json:"C,omitempty"`
}

// CheckConstraint represents a CHECK constraint (column-level or table-level).
// RawSQL stores the original expression text for the executor to evaluate.
type CheckConstraint struct {
	Name   string `json:"N,omitempty"`
	RawSQL string `json:"R,omitempty"`
}

// ForeignKeySchema describes a FOREIGN KEY constraint.
type ForeignKeySchema struct {
	Name            string   `json:"N,omitempty"`
	TableName       string   `json:"T,omitempty"`
	Columns        []string `json:"C,omitempty"`
	ReferencedTable string   `json:"RT,omitempty"`
	ReferencedColumns []string `json:"RC,omitempty"`
	OnDelete       string   `json:"OD,omitempty"`
	OnUpdate       string   `json:"OU,omitempty"`
}


// IndexSchema describes an index on a table (column or expression).
type IndexSchema struct {
	Name    string `json:"N,omitempty"`
	Table  string `json:"T,omitempty"`
	Column string `json:"C,omitempty"` // column name (for simple index)
	Unique bool   `json:"U,omitempty"`
	IndexID uint32 `json:"I,omitempty"`
	// ExprSQL stores the serialized expression for expression indexes.
	// e.g., "LOWER(email)" for CREATE INDEX idx ON t(LOWER(email)).
	// Empty for simple column indexes.
	ExprSQL string `json:"E,omitempty"`
}

// TriggerSchema describes a trigger on a table.
type TriggerSchema struct {
	Name     string `json:"N,omitempty"`
	Table    string `json:"T,omitempty"`
	Timing   string `json:"M,omitempty"` // "BEFORE", "AFTER", "INSTEAD OF"
	Event    string `json:"E,omitempty"` // "INSERT", "UPDATE", "DELETE"
	WhenCond string `json:"W,omitempty"` // WHEN condition expression (or "")
	Body     string `json:"B,omitempty"` // trigger body SQL
}

// ViewSchema describes a view's definition.
type ViewSchema struct {
	Name      string `json:"N,omitempty"`
	QuerySQL  string `json:"Q,omitempty"` // original SELECT SQL
	CreatedAt int64  `json:"T,omitempty"` // creation timestamp
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

	// GetReferencingFKs returns all foreign key schemas that reference a given table.
	// Used by FK action execution (ON DELETE/UPDATE) to find referencing tables.
	GetReferencingFKs(tableName string) ([]ForeignKeySchema, error)

	// ListIndexes returns all index schemas for a given table.
	// Returns an empty slice (not error) if the table has no indexes.
	ListIndexes(tableName string) ([]*IndexSchema, error)

	// AlterTable modifies a table's schema.
	// Returns ErrTableNotFound if the table does not exist.
	AlterTable(schema TableSchema) error

	// RenameTable renames a table.
	// Returns ErrTableNotFound if the table does not exist.
	// Returns ErrTableExists if the new name already exists.
	RenameTable(oldName, newName string) error

	// CreateTrigger creates a trigger.
	// Returns an error if the trigger already exists.
	CreateTrigger(schema TriggerSchema) error

	// GetTrigger returns a trigger by name.
	// Returns ErrTriggerNotFound if the trigger does not exist.
	GetTrigger(triggerName string) (*TriggerSchema, error)

	// DropTrigger removes a trigger.
	// Returns ErrTriggerNotFound if the trigger does not exist.
	DropTrigger(triggerName string) error

	// ListTriggers returns all triggers for a given table.
	// Returns an empty slice (not error) if the table has no triggers.
	ListTriggers(tableName string) ([]TriggerSchema, error)

	// CreateView creates a view.
	// Returns ErrViewExists if the view already exists.
	CreateView(schema ViewSchema) error

	// GetView returns a view by name (case-insensitive).
	// Returns ErrViewNotFound if the view does not exist.
	GetView(name string) (*ViewSchema, error)

	// DropView removes a view.
	// Returns ErrViewNotFound if the view does not exist.
	DropView(name string) error

	// ListViews returns all view names.
	ListViews() ([]string, error)
}
