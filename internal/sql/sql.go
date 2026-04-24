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
//	// Using prepared statements for repeated queries:
//	stmt, _ := db.Prepare("SELECT * FROM users WHERE age > $1")
//	result1, _ := stmt.Query(sql.Value{Type: catalogapi.TypeInt, Int: 25})
//	result2, _ := stmt.Query(sql.Value{Type: catalogapi.TypeInt, Int: 30})
//
//	db.Close()
//	store.Close()
package sql

import (
	"fmt"
	"sync"

	"github.com/akzj/go-fast-kv/internal/goid"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"

	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	"github.com/akzj/go-fast-kv/internal/sql/engine"
	"github.com/akzj/go-fast-kv/internal/sql/executor"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
	"github.com/akzj/go-fast-kv/internal/sql/stmtcache"
)

// ─── Re-export types for user convenience ───────────────────────────

// Result holds the output of executing a SQL statement.
type Result = executorapi.Result

// Value represents a typed SQL value (INTEGER, FLOAT, TEXT, BLOB, or NULL).
type Value = catalogapi.Value

// goroutineID returns the current goroutine's numeric ID.
// Delegates to the fast assembly-based goid package (<1ns vs ~700ns).
func goroutineID() int64 {
	return goid.Get()
}

// ─── DB ─────────────────────────────────────────────────────────────

// DB represents a SQL database backed by a go-fast-kv store.
//
// All SQL operations are fully concurrent, no global locking.
// The underlying KV store is NOT closed when DB.Close() is called.
type DB struct {
	closed bool
	store  kvstoreapi.Store
	catalog  catalogapi.CatalogManager
	parser   parserapi.Parser
	planner  plannerapi.Planner
	executor executorapi.Executor
	// Transaction state: txnMgr creates transactions, txnCtxMap tracks active transactions per goroutine.
	// Uses goroutine ID as key, so each goroutine has its own independent transaction context.
	txnMgr txnapi.TxnContextFactory
	txnCtxMap sync.Map // map[int64]txnapi.TxnContext
	// Prepared statement cache for improved performance on repeated queries.
	stmtCache *stmtcache.StatementCache
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
	fts := engine.NewFTSEngine(store)

	// Layer 3: parser (standalone)
	p := parser.New()

	// Layer 4: planner (AST → execution plan)
	pl := planner.New(cat, p)

	// Layer 5: executor (plan → result)
	ex := executor.New(store, cat, tbl, idx, fts, pl, p)

	return &DB{
		store:    store,
		catalog:  cat,
		parser:   p,
		planner:  pl,
		executor: ex,
		txnMgr:   store.TxnManager(),
		stmtCache: stmtcache.NewStatementCache(stmtcache.DefaultMaxSize),
	}
}

// Exec executes a SQL statement that does not return rows.
//
// Use for: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX,
// INSERT, UPDATE, DELETE.
//
// Returns a Result with RowsAffected for DML statements.
func (db *DB) Exec(sql string) (*Result, error) {
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
	return db.exec(sql)
}

// ExecParams executes a SQL statement with positional parameters ($1, $2, ...).
// Params are provided in order: params[0] = $1, params[1] = $2, etc.
//
// Use for: INSERT, UPDATE, DELETE with parameter placeholders.
//
// Example:
//
//	result, err := db.ExecParams("INSERT INTO users VALUES ($1, $2)", []sql.Value{
//	    {Type: catalogapi.TypeInt, Int: 1},
//	    {Type: catalogapi.TypeText, Text: "Alice"},
//	})
func (db *DB) ExecParams(sql string, params []catalogapi.Value) (*Result, error) {
	return db.execParams(sql, params)
}

// QueryParams executes a SQL query with positional parameters ($1, $2, ...).
// Params are provided in order: params[0] = $1, params[1] = $2, etc.
//
// Use for: SELECT with parameter placeholders.
//
// Example:
//
//	result, err := db.QueryParams("SELECT * FROM users WHERE age > $1", []sql.Value{
//	    {Type: catalogapi.TypeInt, Int: 25},
//	})
func (db *DB) QueryParams(sql string, params []catalogapi.Value) (*Result, error) {
	return db.execParams(sql, params)
}

// Prepare prepares a SQL statement for repeated execution.
// The prepared statement is parsed and planned once, then can be executed
// multiple times with different parameter values.
//
// Use for: queries that are executed multiple times with different parameters.
//
// Example:
//
//	stmt, err := db.Prepare("SELECT * FROM users WHERE age > $1")
//	if err != nil {
//	    return nil, err
//	}
//	result1, err := stmt.Query(sql.Value{Type: catalogapi.TypeInt, Int: 25})
//	result2, err := stmt.Query(sql.Value{Type: catalogapi.TypeInt, Int: 30})
//
// Note: Prepared statements are cached internally. Calling Prepare with the
// same SQL string returns the cached statement (or creates a new one if not cached).
func (db *DB) Prepare(sql string) (*stmtcache.PreparedStmt, error) {
	if db.closed {
		return nil, fmt.Errorf("sql: database is closed")
	}

	// Check cache first
	cached := db.stmtCache.Get(sql)
	if cached != nil {
		return cached, nil
	}

	// Parse SQL → AST
	stmt, err := db.parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	// Create execute function that captures db's dependencies
	execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		// If plan is nil, we need to plan it
		if plan == nil {
			var err error
			plan, err = db.planner.Plan(stmt)
			if err != nil {
				return nil, err
			}
		}

		// Get transaction context if available
		goroutineID := goroutineID()
		var txnCtx txnapi.TxnContext
		if val, ok := db.txnCtxMap.Load(goroutineID); ok {
			txnCtx = val.(txnapi.TxnContext)
		}

		// Execute with params
		if txnCtx != nil {
			return db.executor.ExecuteWithTxnAndParams(plan, txnCtx, params)
		}
		return db.executor.ExecuteWithParams(plan, params)
	}

	// Create prepared statement
	p := stmtcache.NewPreparedStmt(sql, stmt, execFn)

	// Store in cache
	db.stmtCache.Put(sql, p)

	return p, nil
}

// Close releases SQL layer resources.
// Close does NOT close the underlying KV store — the caller
// is responsible for calling store.Close() separately.
func (db *DB) Close() error {
	db.closed = true
	return nil
}

// exec is the shared implementation for Exec and Query.
func (db *DB) exec(sql string) (*Result, error) {
	if db.closed {
		return nil, fmt.Errorf("sql: database is closed")
	}

	// Fast path: check statement cache first.
	if cached := db.stmtCache.Get(sql); cached != nil {
		gid := goroutineID()
		var txnCtx txnapi.TxnContext
		if val, ok := db.txnCtxMap.Load(gid); ok {
			txnCtx = val.(txnapi.TxnContext)
		}
		return cached.ExecCached(txnCtx)
	}

	gid := goroutineID()
	// Get per-goroutine transaction context (if any)
	var txnCtx txnapi.TxnContext
	if val, ok := db.txnCtxMap.Load(gid); ok {
		txnCtx = val.(txnapi.TxnContext)
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

	// Cache parsed statement for future fast-path hits.
	// For transaction-control statements, plan is nil → skip cache.
	if plan != nil {
		execFn := func(stmt parserapi.Statement, plan plannerapi.Plan, _ []catalogapi.Value) (*executorapi.Result, error) {
			var err error
			if plan == nil {
				plan, err = db.planner.Plan(stmt)
				if err != nil {
					return nil, err
				}
			}
			var localTxnCtx txnapi.TxnContext
			if val, ok := db.txnCtxMap.Load(goroutineID()); ok {
				localTxnCtx = val.(txnapi.TxnContext)
			}
			if localTxnCtx != nil {
				return db.executor.ExecuteWithTxn(plan, localTxnCtx)
			}
			return db.executor.Execute(plan)
		}
		db.stmtCache.Put(sql, stmtcache.NewPreparedStmt(sql, stmt, execFn))
	}

	// Handle EXPLAIN: delegate to executor for proper plan formatting + timing
	if _, ok := stmt.(*parserapi.ExplainStmt); ok {
		var result *executorapi.Result
		var err error
		if txnCtx != nil {
			result, err = db.executor.ExecuteWithTxn(plan, txnCtx)
		} else {
			result, err = db.executor.Execute(plan)
		}
		if err != nil {
			return nil, err
		}
		// Convert executorapi.Result to our Result type
		return &Result{
			Columns:     result.Columns,
			Rows:        result.Rows,
			RowsAffected: result.RowsAffected,
		}, nil
	}

	// Handle transaction-control statements.
	// Planner returns nil plan for BeginStmt/CommitStmt/RollbackStmt.
	// nil plan + non-nil stmt means "transaction control" (not a parse error).
	if plan == nil && stmt != nil {
		switch stmt.(type) {
		case *parserapi.BeginStmt:
			if txnCtx != nil {
				return nil, fmt.Errorf("sql: transaction already active")
			}
			newTxnCtx := db.txnMgr.BeginTxnContext()
			if newTxnCtx == nil {
				return nil, fmt.Errorf("sql: failed to begin transaction")
			}
			db.txnCtxMap.Store(gid, newTxnCtx)
			db.store.SetActiveTxnContext(newTxnCtx)
			return &executorapi.Result{}, nil

		case *parserapi.CommitStmt:
			if txnCtx == nil {
				return nil, fmt.Errorf("sql: no active transaction to commit")
			}
			xid := txnCtx.XID()
			err := db.store.CommitWithXID(xid)
			db.store.ClearActiveTxnContext()
			db.txnCtxMap.Delete(gid)
			return &executorapi.Result{}, err

		case *parserapi.RollbackStmt:
			if txnCtx == nil {
				// Rollback with no transaction: no-op (per MySQL/Postgres compatibility)
				return &executorapi.Result{}, nil
			}
			xid := txnCtx.XID()
			// Roll back pending writes: mark each key as deleted (txnMax==xid → invisible).
			for _, key := range txnCtx.GetPendingWrites() {
				db.store.DeleteWithXID(key, xid)
			}
			err := db.store.AbortWithXID(xid)
			db.store.ClearActiveTxnContext()
			db.txnCtxMap.Delete(gid)
			return &executorapi.Result{}, err

		case *parserapi.SavepointStmt:
			if txnCtx == nil {
				return nil, fmt.Errorf("sql: no active transaction for SAVEPOINT")
			}
			err := txnCtx.CreateSavepoint(stmt.(*parserapi.SavepointStmt).Name)
			return &executorapi.Result{}, err

		case *parserapi.RollbackToSavepointStmt:
			if txnCtx == nil {
				return nil, fmt.Errorf("sql: no active transaction for ROLLBACK TO SAVEPOINT")
			}
			name := stmt.(*parserapi.RollbackToSavepointStmt).Name
			err := txnCtx.RollbackToSavepoint(name, db.store)
			return &executorapi.Result{}, err

		case *parserapi.ReleaseSavepointStmt:
			if txnCtx == nil {
				return nil, fmt.Errorf("sql: no active transaction for RELEASE SAVEPOINT")
			}
			name := stmt.(*parserapi.ReleaseSavepointStmt).Name
			err := txnCtx.ReleaseSavepoint(name)
			return &executorapi.Result{}, err
		}
	}

	// Normal plan execution
	if txnCtx != nil {
		// Inside a transaction: use ExecuteWithTxn for row locking
		return db.executor.ExecuteWithTxn(plan, txnCtx)
	}
	return db.executor.Execute(plan)
}

// execParams executes a SQL statement with positional parameters ($1, $2, ...).
func (db *DB) execParams(sql string, params []catalogapi.Value) (*Result, error) {
	if db.closed {
		return nil, fmt.Errorf("sql: database is closed")
	}
	goroutineID := goroutineID()
	// Get per-goroutine transaction context (if any)
	var txnCtx txnapi.TxnContext
	if val, ok := db.txnCtxMap.Load(goroutineID); ok {
		txnCtx = val.(txnapi.TxnContext)
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

	// Handle EXPLAIN: delegate to exec
	if plan == nil && stmt != nil {
		if _, ok := stmt.(*parserapi.ExplainStmt); ok {
			return nil, fmt.Errorf("sql: EXPLAIN not supported with parameters")
		}
	}

	// Handle transaction-control statements (no params allowed)
	if plan == nil && stmt != nil {
		return nil, fmt.Errorf("sql: transaction control statements not allowed with parameters")
	}

	// Execute with params
	if txnCtx != nil {
		return db.executor.ExecuteWithTxnAndParams(plan, txnCtx, params)
	}
	return db.executor.ExecuteWithParams(plan, params)
}

// SetTxnContext sets the active transaction context for SQL execution.
// Used by the gosql driver to pass gosql.Tx's TxnContext to the SQL layer.
func (db *DB) SetTxnContext(txnCtx txnapi.TxnContext) {
	goroutineID := goroutineID()
	db.txnCtxMap.Store(goroutineID, txnCtx)
	// Also register the txnCtx in the store's goroutine-local map so that
	// store.Get/Scan use the txnCtx's snapshot for own-write visibility.
	db.store.SetActiveTxnContext(txnCtx)
}

// EndTxn marks the current transaction as ended (committed or rolled back).
// Called by the gosql driver after Commit/Rollback.
func (db *DB) EndTxn() {
	goroutineID := goroutineID()
	db.store.ClearActiveTxnContext()
	db.txnCtxMap.Delete(goroutineID)
}
