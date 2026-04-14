// Package sql provides a SQL database layer on top of go-fast-kv.
//
// It wires together all SQL components (catalog, encoding, engine, parser,
// planner, executor) into a single DB type with a simple Exec/Query API.
//
// Usage:
//
//	store := kvstore.Open(cfg)
//	db := sql.Open(store)
//
//	db.Exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
//	db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
//
//	result, _ := db.Query("SELECT name, age FROM users WHERE age > 25")
//	for _, row := range result.Rows {
//	    fmt.Println(row)
//	}
//
//	db.Close()
//	store.Close()
package sql

import (
	"fmt"
	"sync"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"

	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	"github.com/akzj/go-fast-kv/internal/sql/engine"
	"github.com/akzj/go-fast-kv/internal/sql/executor"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
)

// ─── Re-export types for user convenience ───────────────────────────

// Result holds the output of executing a SQL statement.
type Result = executorapi.Result

// Value represents a typed SQL value (INTEGER, FLOAT, TEXT, BLOB, or NULL).
type Value = catalogapi.Value

// ─── DB ─────────────────────────────────────────────────────────────

// DB represents a SQL database backed by a go-fast-kv store.
//
// All SQL operations are serialized via a single-writer mutex.
// The underlying KV store is NOT closed when DB.Close() is called.
type DB struct {
	closed bool
	mu     sync.Mutex
	store  kvstoreapi.Store
	catalog  catalogapi.CatalogManager
	parser   parserapi.Parser
	planner  plannerapi.Planner
	executor executorapi.Executor
}

// Open creates a new SQL database using the given KV store.
//
// Open wires all internal SQL components together:
// catalog, encoding, engine, parser, planner, and executor.
//
// The caller is responsible for closing the KV store separately.
func Open(store kvstoreapi.Store) *DB {
	// Layer 0: encoding (standalone)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()

	// Layer 1: catalog (metadata management)
	cat := catalog.New(store)

	// Layer 2: engine (table/index CRUD)
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)

	// Layer 3: parser (standalone)
	p := parser.New()

	// Layer 4: planner (AST → execution plan)
	pl := planner.New(cat)

	// Layer 5: executor (plan → result)
	ex := executor.New(store, cat, tbl, idx, pl)

	return &DB{
		store:    store,
		catalog:  cat,
		parser:   p,
		planner:  pl,
		executor: ex,
	}
}

// Exec executes a SQL statement that does not return rows.
//
// Use for: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX,
// INSERT, UPDATE, DELETE.
//
// Returns a Result with RowsAffected for DML statements.
func (db *DB) Exec(sql string) (*Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.exec(sql)
}

// Query executes a SQL query that returns rows.
//
// Use for: SELECT.
//
// Returns a Result with Columns and Rows populated.
// In Phase 1, Query and Exec use the same code path.
// In the future, Query may return a streaming iterator.
func (db *DB) Query(sql string) (*Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.exec(sql)
}

// Close releases SQL layer resources.
// Close does NOT close the underlying KV store — the caller
// is responsible for calling store.Close() separately.
func (db *DB) Close() error {
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()
	return nil
}

// exec is the shared implementation for Exec and Query.
// Caller must hold db.mu.
func (db *DB) exec(sql string) (*Result, error) {
	if db.closed {
		return nil, fmt.Errorf("sql: database is closed")
	}
	// Parse SQL → AST
	stmt, err := db.parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	// Plan AST → execution plan
	plan, err := db.planner.Plan(stmt)
	if err != nil {
		return nil, err
	}

	// Execute plan → result
	return db.executor.Execute(plan)
}
