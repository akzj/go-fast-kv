// Package planner converts parsed SQL AST into execution plans.
//
// The planner resolves table/column references against the catalog,
// selects scan strategies (table scan vs index scan), validates types,
// and produces Plan objects consumed by the executor.
package planner

import (
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	"github.com/akzj/go-fast-kv/internal/sql/planner/internal"
)

// New creates a new Planner backed by the given catalog.
func New(catalog catalogapi.CatalogManager) plannerapi.Planner {
	return internal.New(catalog)
}
