// Package executor runs SQL execution plans against the storage engine.
package executor

import (
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	"github.com/akzj/go-fast-kv/internal/sql/executor/api"
	"github.com/akzj/go-fast-kv/internal/sql/executor/internal"
)

// Re-export types for convenience.
type Result = api.Result

// Re-export errors.
var (
	ErrExecFailed = api.ErrExecFailed
)

// New creates a new Executor.
func New(store kvstoreapi.Store, catalog catalogapi.CatalogManager,
	tableEngine engineapi.TableEngine, indexEngine engineapi.IndexEngine) api.Executor {
	return internal.New(store, catalog, tableEngine, indexEngine)
}
