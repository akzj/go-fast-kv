// Package sql provides a SQL interface on top of go-fast-kv.
package sql

import (
	"fmt"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/executor"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
)

// Engine is the SQL engine on top of go-fast-kv.
type Engine struct {
	kv        kvstoreapi.Store
	catalog   *catalog.Catalog
	parser    *parser.Parser
	planner   *planner.Planner
	executor  *executor.Executor
}

// Config holds engine configuration.
type Config struct {
	KVStore kvstoreapi.Store
}

// Open creates a new SQL engine.
func Open(cfg Config) (*Engine, error) {
	if cfg.KVStore == nil {
		return nil, fmt.Errorf("sql: KVStore is required")
	}

	e := &Engine{
		kv:       cfg.KVStore,
		catalog:  catalog.New(cfg.KVStore),
		parser:   parser.New(),
		planner:  planner.New(cfg.KVStore),
		executor: executor.New(cfg.KVStore),
	}
	return e, nil
}

// Exec executes a SQL statement that doesn't return rows (INSERT, UPDATE, DELETE, etc.).
func (e *Engine) Exec(sql string) (int64, error) {
	stmt, err := e.parser.Parse(sql)
	if err != nil {
		return 0, fmt.Errorf("sql: parse error: %w", err)
	}

	plan, err := e.planner.Plan(stmt)
	if err != nil {
		return 0, fmt.Errorf("sql: plan error: %w", err)
	}

	return e.executor.Exec(plan)
}

// Query executes a SQL query that returns rows (SELECT).
func (e *Engine) Query(sql string) (executor.Iterator, error) {
	stmt, err := e.parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("sql: parse error: %w", err)
	}

	plan, err := e.planner.Plan(stmt)
	if err != nil {
		return nil, fmt.Errorf("sql: plan error: %w", err)
	}

	return e.executor.Query(plan)
}

// Catalog returns the catalog for metadata management.
func (e *Engine) Catalog() *catalog.Catalog {
	return e.catalog
}

// Close closes the engine.
func (e *Engine) Close() error {
	// Nothing to close for now
	return nil
}
