// Package catalog provides table and index metadata management.
package catalog

import (
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"

	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog/internal"
)

// Re-export types for convenience
type TableSchema = api.TableSchema
type ColumnDef = api.ColumnDef
type IndexSchema = api.IndexSchema
type Type = api.Type
type Value = api.Value

// Re-export errors
var (
	ErrTableNotFound = api.ErrTableNotFound
	ErrTableExists   = api.ErrTableExists
	ErrIndexNotFound = api.ErrIndexNotFound
	ErrIndexExists   = api.ErrIndexExists
	ErrColumnNotFound = api.ErrColumnNotFound
)

// New creates a new CatalogManager backed by kv.
func New(kv kvstoreapi.Store) api.CatalogManager {
	return internal.New(kv)
}
