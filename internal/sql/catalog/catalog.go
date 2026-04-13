package catalog

import (
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog/impl"
)

// Re-export types from api
type TableSchema = api.TableSchema
type ColumnDef = api.ColumnDef
type IndexSchema = api.IndexSchema

// Re-export errors from api
var (
	ErrTableNotFound  = api.ErrTableNotFound
	ErrTableExists    = api.ErrTableExists
	ErrColumnNotFound = api.ErrColumnNotFound
	ErrIndexNotFound  = api.ErrIndexNotFound
	ErrIndexExists    = api.ErrIndexExists
)

// Re-export interface from api
type CatalogManager = api.CatalogManager

// New creates a new CatalogManager.
func New(kv kvstoreapi.Store) api.CatalogManager {
	return impl.New(kv)
}