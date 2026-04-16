// Package api defines the public interfaces for the executor module.
//
// To understand the executor module, read only this file.
//
// The executor takes execution plans from the planner and runs them
// against the KV store via the engine layer. It handles ID assignment,
// expression evaluation, and result construction.
package api

import (
	"errors"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrExecFailed is returned when plan execution fails.
	ErrExecFailed = errors.New("executor: execution failed")
)

// ─── Result ─────────────────────────────────────────────────────────

// Result holds the output of executing a SQL plan.
type Result struct {
	// Columns holds column names for SELECT results.
	Columns []string

	// Rows holds result rows for SELECT results.
	// Each row is aligned with Columns.
	Rows [][]catalogapi.Value

	// RowsAffected holds the number of rows affected by INSERT/UPDATE/DELETE.
	RowsAffected int64
}

// ─── Executor Interface ─────────────────────────────────────────────

// Executor executes SQL plans against the storage engine.
type Executor interface {
	// Execute runs a plan and returns the result.
	// Plans are produced by the planner module.
	Execute(plan plannerapi.Plan) (*Result, error)

	// ExecuteWithTxn runs a plan with transaction context for row locking.
	// If txnCtx is nil, behaves identically to Execute.
	// Use for executing statements inside BEGIN...COMMIT blocks.
	ExecuteWithTxn(plan plannerapi.Plan, txnCtx txnapi.TxnContext) (*Result, error)
}
