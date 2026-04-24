// Package stmtcache provides prepared statement caching for the SQL engine.
//
// Prepared statements cache the parsed AST and execution plan, allowing
// repeated execution of the same SQL structure with different parameters
// to skip the Parse and Plan phases.
//
// Usage:
//
//	stmt, err := db.Prepare("SELECT * FROM users WHERE id = $1")
//	if err != nil {
//	    return err
//	}
//	result, err := stmt.Query(sql.Value{Type: catalogapi.TypeInt, Int: 1})
package stmtcache

import (
	"sync"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

// DefaultMaxSize is the default maximum number of prepared statements to cache.
const DefaultMaxSize = 128

// PreparedStatement represents a SQL statement that has been parsed and planned.
// It can be executed multiple times with different parameter values.
type PreparedStatement struct {
	SQL     string              // original SQL
	stmt    parserapi.Statement // parsed AST
	plan    plannerapi.Plan     // cached execution plan (may be nil until first execute)
	mu      sync.RWMutex
}

// ExecuteFn is the function type for executing a prepared statement.
type ExecuteFn func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error)

// PreparedStmt is a wrapper that provides Query/Exec methods for a prepared statement.
type PreparedStmt struct {
	SQL    string
	stmt   parserapi.Statement
	plan   plannerapi.Plan
	mu     sync.RWMutex
	execFn ExecuteFn
}

// NewPreparedStmt creates a new prepared statement.
func NewPreparedStmt(sql string, stmt parserapi.Statement, execFn ExecuteFn) *PreparedStmt {
	return &PreparedStmt{
		SQL:    sql,
		stmt:   stmt,
		execFn: execFn,
	}
}

// Query executes the prepared statement with the given parameters and returns rows.
func (p *PreparedStmt) Query(params ...catalogapi.Value) (*executorapi.Result, error) {
	return p.execute(params)
}

// Exec executes the prepared statement with the given parameters (for non-SELECT statements).
func (p *PreparedStmt) Exec(params ...catalogapi.Value) (*executorapi.Result, error) {
	return p.execute(params)
}

// execute runs the prepared statement with the given parameters.
func (p *PreparedStmt) execute(params []catalogapi.Value) (*executorapi.Result, error) {
	p.mu.RLock()
	plan := p.plan
	stmt := p.stmt
	p.mu.RUnlock()

	return p.execFn(stmt, plan, params)
}

// setPlan sets the execution plan. Thread-safe.
func (p *PreparedStmt) setPlan(plan plannerapi.Plan) {
	p.mu.Lock()
	p.plan = plan
	p.mu.Unlock()
}

// ExecCached executes the prepared statement with no parameters (fast path from DB.exec).
func (p *PreparedStmt) ExecCached(txnCtx txnapi.TxnContext) (*executorapi.Result, error) {
	p.mu.RLock()
	plan := p.plan
	stmt := p.stmt
	p.mu.RUnlock()
	return p.execFn(stmt, plan, nil)
}

// StatementCache caches parsed and planned SQL statements.
type StatementCache struct {
	maxSize int
	cache   map[string]*PreparedStmt
	order   []string // insertion order for LRU eviction
	mu      sync.Mutex
}

// NewStatementCache creates a new statement cache with the given maximum size.
func NewStatementCache(maxSize int) *StatementCache {
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	return &StatementCache{
		maxSize: maxSize,
		cache:   make(map[string]*PreparedStmt),
		order:   make([]string, 0, maxSize),
	}
}

// Get retrieves a cached prepared statement by SQL string.
// Returns nil if not found.
func (c *StatementCache) Get(sql string) *PreparedStmt {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeSQL(sql)
	if p, ok := c.cache[key]; ok {
		// Move to end (most recently used)
		c.moveToEnd(key)
		return p
	}
	return nil
}

// Put stores a prepared statement in the cache.
// If the cache is full, the least recently used statement is evicted.
func (c *StatementCache) Put(sql string, p *PreparedStmt) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeSQL(sql)

	// Check if already cached
	if existing, ok := c.cache[key]; ok {
		// Update existing entry with new statement
		existing.mu.Lock()
		existing.plan = p.plan
		existing.mu.Unlock()
		// Don't replace the PreparedStmt object - just update its plan
		c.moveToEnd(key)
		return
	}

	// Evict if necessary
	for len(c.order) >= c.maxSize {
		if len(c.order) > 0 {
			oldest := c.order[0]
			delete(c.cache, oldest)
			c.order = c.order[1:]
		}
	}

	// Add new entry
	c.cache[key] = p
	c.order = append(c.order, key)
}

// moveToEnd moves a key to the end of the LRU list.
func (c *StatementCache) moveToEnd(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, key)
			return
		}
	}
}

// Remove removes a prepared statement from the cache.
func (c *StatementCache) Remove(sql string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := normalizeSQL(sql)
	delete(c.cache, key)
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// Clear removes all prepared statements from the cache.
func (c *StatementCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*PreparedStmt)
	c.order = make([]string, 0, c.maxSize)
}

// Size returns the current number of cached statements.
func (c *StatementCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// normalizeSQL normalizes a SQL string for use as a cache key.
// It trims leading/trailing whitespace.
func normalizeSQL(sql string) string {
	return trimSQL(sql)
}

// trimSQL removes leading and trailing whitespace from SQL.
func trimSQL(sql string) string {
	start := 0
	end := len(sql)
	for start < end && (sql[start] == ' ' || sql[start] == '\t' || sql[start] == '\n' || sql[start] == '\r') {
		start++
	}
	for end > start && (sql[end-1] == ' ' || sql[end-1] == '\t' || sql[end-1] == '\n' || sql[end-1] == '\r') {
		end--
	}
	return sql[start:end]
}

// PlannerAccessor is the interface for components that can plan statements.
type PlannerAccessor interface {
	Plan(stmt parserapi.Statement) (plannerapi.Plan, error)
}

// TxnContextProvider provides transaction context for a goroutine.
type TxnContextProvider interface {
	GetTxnContext(goroutineID int64) txnapi.TxnContext
}

// ExecutorWithParams executes plans with parameters.
type ExecutorWithParams interface {
	ExecuteWithParams(plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error)
	ExecuteWithTxnAndParams(plan plannerapi.Plan, txnCtx txnapi.TxnContext, params []catalogapi.Value) (*executorapi.Result, error)
}

// CreateExecFn creates an execute function that binds the executor and transaction context.
func CreateExecFn(
	planner plannerapi.Planner,
	executor executorapi.Executor,
	txnCtxProvider func() txnapi.TxnContext,
) ExecuteFn {
	return func(stmt parserapi.Statement, plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
		// If plan is nil, we need to plan it
		if plan == nil {
			var err error
			plan, err = planner.Plan(stmt)
			if err != nil {
				return nil, err
			}
		}

		// Get transaction context if available
		txnCtx := txnCtxProvider()

		// Execute with params
		if txnCtx != nil {
			return executor.ExecuteWithTxnAndParams(plan, txnCtx, params)
		}
		return executor.ExecuteWithParams(plan, params)
	}
}
