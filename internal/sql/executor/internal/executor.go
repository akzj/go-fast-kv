// Package internal implements the SQL executor.
package internal

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
	"unicode"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	rowlockapi "github.com/akzj/go-fast-kv/internal/rowlock/api"
	sqlerrors "github.com/akzj/go-fast-kv/internal/sql/errors"
)

// Compile-time interface check.
var _ executorapi.Executor = (*executor)(nil)

// Meta keys for ID counters.
var (
	metaNextTableID = []byte("_sql:meta:next_table_id")
	metaNextIndexID = []byte("_sql:meta:next_index_id")
)

type executor struct {
	store       kvstoreapi.Store
	catalog     catalogapi.CatalogManager
	tableEngine engineapi.TableEngine
	keyEncoder  encodingapi.KeyEncoder
	indexEngine engineapi.IndexEngine
	ftsEngine   engineapi.FTSEngine
	planner     plannerapi.Planner
	// parser re-parses CHECK constraint expressions at execution time.
	// CHECK expressions are stored as RawSQL strings in the catalog.
	parser parserapi.Parser
	// For correlated subquery support - outer query context set before filter evaluation
	outerCols []catalogapi.ColumnDef
	outerVals []catalogapi.Value
	// TxnContext provides row-level locking for SELECT FOR UPDATE
	txnCtx txnapi.TxnContext
	// params holds positional parameter values ($1, $2, ...) for prepared statements
	params []catalogapi.Value
	// cteResults stores materialized CTE results by name for WITH clause execution
	cteResults map[string]*executorapi.Result
	windowResults map[*parserapi.WindowFuncExpr]*WindowFunctionResult
	// For trigger support - NEW and OLD row contexts for row-level triggers
	// NEW holds the new row values (for INSERT/UPDATE); OLD holds old values (for UPDATE/DELETE)
	triggerNewCols []catalogapi.ColumnDef
	triggerNewVals []catalogapi.Value
	triggerOldCols []catalogapi.ColumnDef
	triggerOldVals []catalogapi.Value
	// triggerDepth prevents infinite recursion from nested triggers
	triggerDepth int
	// funcRegistry stores user-defined functions registered via CREATE FUNCTION
	funcRegistry *FunctionRegistry
}

// New creates a new Executor.
func New(store kvstoreapi.Store, catalog catalogapi.CatalogManager,
	tableEngine engineapi.TableEngine, indexEngine engineapi.IndexEngine,
	ftsEngine engineapi.FTSEngine,
	planner plannerapi.Planner, parser parserapi.Parser) *executor {
	return &executor{
		store:       store,
		catalog:     catalog,
		tableEngine: tableEngine,
		indexEngine: indexEngine,
		ftsEngine:   ftsEngine,
		keyEncoder:  encoding.NewKeyEncoder(),
		planner:     planner,
		parser:      parser,
		cteResults:   make(map[string]*executorapi.Result),
		windowResults: make(map[*parserapi.WindowFuncExpr]*WindowFunctionResult),
		funcRegistry: NewFunctionRegistry(),
	}
}

// execSubquery executes a scalar subquery plan with outer row context.
// outerCols/outerVals on e must be set before calling this method.
// Saves and restores outerCols/outerVals so the inner execSelect doesn't
// clobber the outer query's context.
func (e *executor) execSubquery(plan parserapi.SubqueryPlan) (catalogapi.Value, error) {
	typedPlan, ok := plan.(plannerapi.Plan)
	if !ok {
		return catalogapi.Value{}, fmt.Errorf("subquery plan has wrong type: %T", plan)
	}
	// Save outer context — inner execSelect will overwrite e.outerCols/e.outerVals.
	savedOuterCols := e.outerCols
	savedOuterVals := e.outerVals
	result, err := e.Execute(typedPlan)
	// Restore outer context after inner execution.
	e.outerCols = savedOuterCols
	e.outerVals = savedOuterVals
	if err != nil {
		return catalogapi.Value{}, err
	}
	if len(result.Rows) == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	if len(result.Rows) > 1 {
		return catalogapi.Value{}, fmt.Errorf("scalar subquery returned more than 1 row")
	}
	return result.Rows[0][0], nil
}

// Execute dispatches a plan to the appropriate handler.
func (e *executor) Execute(plan plannerapi.Plan) (*executorapi.Result, error) {
	return e.ExecuteWithTxn(plan, nil)
}

// ExecuteWithTxn dispatches a plan to the appropriate handler with transaction context.
// txnCtx provides row-level locking for SELECT FOR UPDATE.
// If txnCtx is nil, behaves like Execute (no row locking).
// All errors are returned gracefully — no panic/recover in the happy path.
func (e *executor) ExecuteWithTxn(plan plannerapi.Plan, txnCtx txnapi.TxnContext) (*executorapi.Result, error) {
	e.txnCtx = txnCtx
	defer func() {
		e.txnCtx = nil
	}()

	// Register the transaction's snapshot in readSnaps so all Get/Scan calls
	// within this transaction use the same snapshot. This provides true snapshot
	// isolation: two SELECTs within the same BEGIN...COMMIT see the same state.
	if txnCtx != nil {
		snap := txnCtx.Snapshot()
		if snap != nil {
			e.store.RegisterSnapshot(txnCtx.XID(), snap)
			defer e.store.UnregisterSnapshot(txnCtx.XID())
		}
	}

	result, err := e.executePlan(plan)
	if err != nil && txnCtx != nil {
		txnCtx.Rollback()
	}
	return result, err
}

// executePlan dispatches a plan to the appropriate handler. Extracted from
// ExecuteWithTxn/ExecuteWithTxnAndParams to avoid code duplication.
func (e *executor) executePlan(plan plannerapi.Plan) (*executorapi.Result, error) {
	switch p := plan.(type) {
	case *plannerapi.CreateTablePlan:
		return e.execCreateTable(p)
	case *plannerapi.CreateTableAsSelectPlan:
		return e.execCreateTableAsSelect(p)
	case *plannerapi.DropTablePlan:
		return e.execDropTable(p)
	case *plannerapi.CreateIndexPlan:
		return e.execCreateIndex(p)
	case *plannerapi.DropIndexPlan:
		return e.execDropIndex(p)
	case *plannerapi.InsertPlan:
		return e.execInsert(p)
	case *plannerapi.UpsertPlan:
		return e.execUpsert(p)
	case *plannerapi.InsertSelectPlan:
		return e.execInsertSelect(p)
	case *plannerapi.SelectPlan:
		if p.Join != nil {
			return e.execJoinSelect(p)
		}
		return e.execSelect(p)
	case *plannerapi.JoinPlan:
		return e.execJoin(p)
	case *plannerapi.DeletePlan:
		return e.execDelete(p)
	case *plannerapi.TruncatePlan:
		return e.execTruncate(p)
	case *plannerapi.UpdatePlan:
		return e.execUpdate(p)
	case *plannerapi.UnionPlan:
		return e.execUnion(p)
	case *plannerapi.IntersectPlan:
		return e.execIntersect(p)
	case *plannerapi.ExceptPlan:
		return e.execExcept(p)
	case *plannerapi.ExplainPlan:
		return e.execExplain(p)
	case *plannerapi.WithPlan:
		return e.execWith(p)
	case *plannerapi.AlterTablePlan:
		return e.execAlterTable(p)
	case *plannerapi.PragmaPlan:
		return e.execPragma(p)
	case *plannerapi.CreateTriggerPlan:
		return e.execCreateTrigger(p)
	case *plannerapi.DropTriggerPlan:
		return e.execDropTrigger(p)
	case *plannerapi.CreateViewPlan:
		return e.execCreateView(p)
	case *plannerapi.DropViewPlan:
		return e.execDropView(p)
	case *plannerapi.CreateFTSPlan:
		return e.execCreateFTS(p)
	case *plannerapi.FTSSearchPlan:
		return e.execFTSSearch(p)
	case *plannerapi.CreateFunctionPlan:
		return e.execCreateFunction(p)
	default:
		return nil, fmt.Errorf("%w: unsupported plan type %T", executorapi.ErrExecFailed, plan)
	}
}

// ExecuteWithParams executes a plan with positional parameters ($1, $2, ...).
// Params are provided in order (1-indexed mapping: params[0] = $1).
func (e *executor) ExecuteWithParams(plan plannerapi.Plan, params []catalogapi.Value) (*executorapi.Result, error) {
	return e.ExecuteWithTxnAndParams(plan, nil, params)
}

// ExecuteWithTxnAndParams executes a plan with transaction context and positional parameters.
// All errors are returned gracefully — no panic/recover in the happy path.
func (e *executor) ExecuteWithTxnAndParams(plan plannerapi.Plan, txnCtx txnapi.TxnContext, params []catalogapi.Value) (*executorapi.Result, error) {
	e.params = params
	e.txnCtx = txnCtx
	defer func() {
		e.txnCtx = nil
		e.params = nil
	}()

	// Register the transaction's snapshot in readSnaps so all Get/Scan calls
	// within this transaction use the same snapshot.
	if txnCtx != nil {
		snap := txnCtx.Snapshot()
		if snap != nil {
			e.store.RegisterSnapshot(txnCtx.XID(), snap)
			defer e.store.UnregisterSnapshot(txnCtx.XID())
		}
	}

	result, err := e.executePlan(plan)
	if err != nil && txnCtx != nil {
		txnCtx.Rollback()
	}
	return result, err
}

// ─── Row Locking Helpers ──────────────────────────────────────────

// rowLockKey constructs a lock key for a row.
// Format: "tableID:rowID" where both are decimal representations.
func (e *executor) rowLockKey(tableID uint32, rowID uint64) string {
	return fmt.Sprintf("%d:%d", tableID, rowID)
}

// lockModeToRowLockMode converts parser LockMode to rowlock LockMode.
func lockModeToRowLockMode(mode parserapi.LockMode) rowlockapi.LockMode {
	switch mode {
	case parserapi.UpdateExclusive:
		return rowlockapi.LockExclusive
	case parserapi.UpdateShared:
		return rowlockapi.LockShared
	default:
		return rowlockapi.LockExclusive
	}
}

// acquireRowLock attempts to acquire a lock on a row based on plan.LockMode and plan.LockWait.
// Returns:
//   - true: lock acquired (row should be included)
//   - false: lock not acquired due to SKIP LOCKED (row should be skipped)
//
// If NOWAIT is set and lock cannot be acquired immediately, returns (false, error).
func (e *executor) acquireRowLock(tableID uint32, rowID uint64, lockMode parserapi.LockMode, lockWait parserapi.LockWait) (bool, error) {
	if e.txnCtx == nil {
		return true, nil // No transaction context, no locking
	}

	lockMgr := e.txnCtx.LockManager()
	if lockMgr == nil {
		return true, nil
	}

	rowKey := e.rowLockKey(tableID, rowID)
	rowLockMode := lockModeToRowLockMode(lockMode)

	switch lockWait {
	case parserapi.LockWaitSkipLocked:
		// SKIP LOCKED: check if locked, skip if yes, acquire if not
		if lockMgr.IsLocked(rowKey) {
			return false, nil // Skip this row
		}
		// Not locked, try to acquire
		ctx := rowlockapi.LockContext{
			TxnID:     e.txnCtx.XID(),
			TimeoutMs: 0, // No timeout, just acquire
		}
		if lockMgr.Acquire(rowKey, ctx, rowLockMode) {
			return true, nil
		}
		// Lost race to another txn, skip
		return false, nil

	case parserapi.LockWaitNowait:
		// NOWAIT: return immediately if lock cannot be acquired
		if lockMgr.TryAcquire(rowKey, e.txnCtx.XID(), rowLockMode) {
			return true, nil
		}
		return false, fmt.Errorf("%w: could not obtain lock on row", executorapi.ErrExecFailed)

	default:
		// Default: wait for lock with default timeout
		ctx := rowlockapi.LockContext{
			TxnID:     e.txnCtx.XID(),
			TimeoutMs: 5000, // 5 second default timeout
		}
		if lockMgr.Acquire(rowKey, ctx, rowLockMode) {
			return true, nil
		}
		return false, fmt.Errorf("%w: lock acquisition timed out", executorapi.ErrExecFailed)
	}
}

// ─── ID Counter Management ──────────────────────────────────────────

// nextID reads and increments a counter stored at the given key.
// Returns the current value (before increment). Starts at 1 if not found.
func (e *executor) nextID(key []byte) (uint32, error) {
	var id uint32 = 1
	data, err := e.store.Get(key)
	if err == nil {
		if len(data) == 4 {
			id = binary.BigEndian.Uint32(data)
		}
	} else if err != kvstoreapi.ErrKeyNotFound {
		return 0, fmt.Errorf("%w: reading counter: %v", executorapi.ErrExecFailed, err)
	}

	// Persist incremented counter
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, id+1)
	if err := e.store.Put(key, buf); err != nil {
		return 0, fmt.Errorf("%w: persisting counter: %v", executorapi.ErrExecFailed, err)
	}

	return id, nil
}

// ─── DDL Execution ──────────────────────────────────────────────────

func (e *executor) execCreateTable(plan *plannerapi.CreateTablePlan) (*executorapi.Result, error) {
	// I-C1: check existence BEFORE allocating ID to avoid wasting IDs.
	_, err := e.catalog.GetTable(plan.Schema.Name)
	if err == nil {
		// Table exists.
		if plan.IfNotExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, catalogapi.ErrTableExists)
	}
	if err != catalogapi.ErrTableNotFound {
		return nil, fmt.Errorf("%w: checking table existence: %v", executorapi.ErrExecFailed, err)
	}

	// Now safe to allocate ID.
	tableID, err := e.nextID(metaNextTableID)
	if err != nil {
		return nil, err
	}

	schema := plan.Schema
	schema.TableID = tableID

	err = e.catalog.CreateTable(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Create indexes for UNIQUE columns.
	for i := range plan.UniqueIndexes {
		idxSchema := plan.UniqueIndexes[i]
		// Allocate index ID
		idxID, err := e.nextID(metaNextIndexID)
		if err != nil {
			return nil, err
		}
		idxSchema.IndexID = idxID
		// Create index catalog entry
		if err := e.catalog.CreateIndex(idxSchema); err != nil {
			return nil, fmt.Errorf("%w: creating unique index %q: %v", executorapi.ErrExecFailed, idxSchema.Name, err)
		}
	}

	return &executorapi.Result{RowsAffected: 0}, nil
}

// execCreateTableAsSelect executes: CREATE TABLE t AS SELECT ...
// 1. Create the table with schema inferred from SELECT columns.
// 2. Execute SELECT and insert rows.
func (e *executor) execCreateTableAsSelect(plan *plannerapi.CreateTableAsSelectPlan) (*executorapi.Result, error) {
	// I-C1: check existence BEFORE allocating ID.
	_, err := e.catalog.GetTable(plan.Schema.Name)
	if err == nil {
		if plan.IfNotExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, catalogapi.ErrTableExists)
	}
	if err != catalogapi.ErrTableNotFound {
		return nil, fmt.Errorf("%w: checking table existence: %v", executorapi.ErrExecFailed, err)
	}

	// Allocate table ID.
	tableID, err := e.nextID(metaNextTableID)
	if err != nil {
		return nil, err
	}

	schema := plan.Schema
	schema.TableID = tableID

	// Create the table.
	if err := e.catalog.CreateTable(schema); err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Execute the SELECT subquery.
	selPlan, ok := plan.SelectPlan.(*plannerapi.SelectPlan)
	if !ok {
		return nil, fmt.Errorf("%w: CTAS select plan must be SelectPlan, got %T", executorapi.ErrExecFailed, plan.SelectPlan)
	}
	subResult, err := e.execSelect(selPlan)
	if err != nil {
		// Drop the table on failure to keep catalog clean.
		_ = e.catalog.DropTable(plan.Schema.Name)
		return nil, fmt.Errorf("%w: execute select: %v", executorapi.ErrExecFailed, err)
	}

	// If SELECT returned no rows, we're done.
	if len(subResult.Rows) == 0 {
		return &executorapi.Result{RowsAffected: 0}, nil
	}

	// Get indexes for the new table (likely none, but handle UNIQUE columns).
	indexes, err := e.catalog.ListIndexes(plan.Schema.Name)
	if err != nil {
		_ = e.catalog.DropTable(plan.Schema.Name)
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	batch := e.store.NewWriteBatch()
	rowsAffected := int64(0)

	for i, row := range subResult.Rows {
		if len(row) != len(schema.Columns) {
			_ = e.catalog.DropTable(plan.Schema.Name)
			batch.Discard()
			return nil, fmt.Errorf("CTAS row %d: column count mismatch: got %d, expected %d", i+1, len(row), len(schema.Columns))
		}

		rowID, err := e.tableEngine.InsertInto(&schema, batch, row)
		if err != nil {
			_ = e.catalog.DropTable(plan.Schema.Name)
			batch.Discard()
			return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
		}

		// Insert index entries.
		for _, idx := range indexes {
			rowForIdx := &engineapi.Row{RowID: rowID, Values: row}
			val, err := getIndexValue(idx, rowForIdx, schema.Columns, e)
			if err != nil {
				_ = e.catalog.DropTable(plan.Schema.Name)
				batch.Discard()
				return nil, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(schema.TableID, idx.IndexID, val, rowID)
			if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
				_ = e.catalog.DropTable(plan.Schema.Name)
				batch.Discard()
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}
		rowsAffected++
	}

	if err := e.tableEngine.PersistCounter(batch, schema.TableID); err != nil {
		_ = e.catalog.DropTable(plan.Schema.Name)
		batch.Discard()
		return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
	}

	if err := batch.Commit(); err != nil {
		_ = e.catalog.DropTable(plan.Schema.Name)
		batch.Discard()
		return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: rowsAffected}, nil
}

func (e *executor) execDropTable(plan *plannerapi.DropTablePlan) (*executorapi.Result, error) {
	// If table not found at plan time (TableID == 0) and IfExists, succeed silently
	if plan.TableID == 0 && plan.IfExists {
		return &executorapi.Result{RowsAffected: 0}, nil
	}

	// Get table schema (need TableID for data cleanup)
	tbl, err := e.catalog.GetTable(plan.TableName)
	if err != nil {
		if err == catalogapi.ErrTableNotFound && plan.IfExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Get all indexes to clean up their data
	indexes, err := e.catalog.ListIndexes(plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Drop table data
	if err := e.tableEngine.DropTableData(tbl.TableID); err != nil {
		return nil, fmt.Errorf("%w: dropping table data: %v", executorapi.ErrExecFailed, err)
	}

	// Drop index data
	for _, idx := range indexes {
		if err := e.indexEngine.DropIndexData(tbl.TableID, idx.IndexID); err != nil {
			return nil, fmt.Errorf("%w: dropping index data: %v", executorapi.ErrExecFailed, err)
		}
	}

	// Drop catalog entry (also drops index metadata)
	if err := e.catalog.DropTable(plan.TableName); err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: 0}, nil
}

func (e *executor) execAlterTable(plan *plannerapi.AlterTablePlan) (*executorapi.Result, error) {
	// Get existing table schema
	schema, err := e.catalog.GetTable(plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	switch plan.Operation {
	case parserapi.AlterAddColumn:
		// Check column doesn't already exist
		for _, col := range schema.Columns {
			if col.Name == plan.ColumnName {
				return nil, fmt.Errorf("%w: column %s already exists", executorapi.ErrExecFailed, plan.ColumnName)
			}
		}

		// Convert type string to catalogapi.Type
		colType := catalogapi.TypeNull
		switch plan.TypeName {
		case "INT":
			colType = catalogapi.TypeInt
		case "FLOAT":
			colType = catalogapi.TypeFloat
		case "TEXT":
			colType = catalogapi.TypeText
		case "BLOB":
			colType = catalogapi.TypeBlob
		}

		// Add new column
		schema.Columns = append(schema.Columns, catalogapi.ColumnDef{
			Table:    plan.TableName,
			Name:     plan.ColumnName,
			Type:     colType,
			NotNull:  plan.NotNull,
		})

	case parserapi.AlterDropColumn:
		// Find and remove column
		found := false
		newColumns := make([]catalogapi.ColumnDef, 0, len(schema.Columns))
		for _, col := range schema.Columns {
			if col.Name == plan.ColumnName {
				found = true
				continue
			}
			newColumns = append(newColumns, col)
		}
		if !found {
			return nil, fmt.Errorf("%w: column %s not found", executorapi.ErrExecFailed, plan.ColumnName)
		}
		schema.Columns = newColumns

	case parserapi.AlterRenameColumn:
		// Find and rename column
		found := false
		for i := range schema.Columns {
			if schema.Columns[i].Name == plan.ColumnName {
				schema.Columns[i].Name = plan.ColumnNew
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: column %s not found", executorapi.ErrExecFailed, plan.ColumnName)
		}
		// Update primary key reference if needed
		if schema.PrimaryKey == plan.ColumnName {
			schema.PrimaryKey = plan.ColumnNew
		}

	case parserapi.AlterRenameTable:
		// Rename the table via catalog
		if err := e.catalog.RenameTable(plan.TableName, plan.TableNew); err != nil {
			return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
		}
		return &executorapi.Result{RowsAffected: 0}, nil

	default:
		return nil, fmt.Errorf("%w: unsupported alter operation %v", executorapi.ErrExecFailed, plan.Operation)
	}

	// Save updated schema
	if err := e.catalog.AlterTable(*schema); err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: 0}, nil
}

// execPragma handles PRAGMA commands.
func (e *executor) execPragma(plan *plannerapi.PragmaPlan) (*executorapi.Result, error) {
	// Pragma names are case-insensitive, normalize to lowercase
	name := strings.ToLower(plan.Name)
	switch name {
	case "database_list":
		return e.pragmaDatabaseList()
	case "table_info":
		if plan.Arg == "" {
			return nil, fmt.Errorf("%w: table_info requires table name argument", executorapi.ErrExecFailed)
		}
		return e.pragmaTableInfo(plan.Arg)
	case "table_list", "tables":
		return e.pragmaTableList()
	case "index_list":
		if plan.Arg == "" {
			return nil, fmt.Errorf("%w: index_list requires table name argument", executorapi.ErrExecFailed)
		}
		return e.pragmaIndexList(plan.Arg)
	default:
		return nil, fmt.Errorf("%w: unknown pragma: %s", executorapi.ErrExecFailed, name)
	}
}

// pragmaDatabaseList returns a list of all databases (just "main" for single-db).
func (e *executor) pragmaDatabaseList() (*executorapi.Result, error) {
	return &executorapi.Result{
		Columns: []string{"seq", "name", "file"},
		Rows: [][]catalogapi.Value{
			{catalogapi.Value{Type: catalogapi.TypeInt, Int: 0}, catalogapi.Value{Type: catalogapi.TypeText, Text: "main"}, catalogapi.Value{Type: catalogapi.TypeText, Text: ""}},
		},
	}, nil
}

// execCreateTrigger creates a trigger in the catalog.
func (e *executor) execCreateTrigger(plan *plannerapi.CreateTriggerPlan) (*executorapi.Result, error) {
	err := e.catalog.CreateTrigger(plan.Schema)
	if err != nil {
		return nil, fmt.Errorf("%w: create trigger: %v", executorapi.ErrExecFailed, err)
	}
	return &executorapi.Result{RowsAffected: 1}, nil
}

// execDropTrigger drops a trigger from the catalog.
func (e *executor) execDropTrigger(plan *plannerapi.DropTriggerPlan) (*executorapi.Result, error) {
	// Check if trigger exists (unless IF EXISTS was specified)
	if !plan.IfExists {
		_, err := e.catalog.GetTrigger(plan.Name)
		if err != nil {
			if err == catalogapi.ErrTriggerNotFound {
				return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
			}
			return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
		}
	}

	err := e.catalog.DropTrigger(plan.Name)
	if err != nil {
		if err == catalogapi.ErrTriggerNotFound && plan.IfExists {
			// IF EXISTS was specified and trigger doesn't exist — OK
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: drop trigger: %v", executorapi.ErrExecFailed, err)
	}
	return &executorapi.Result{RowsAffected: 1}, nil
}

// execCreateView creates a view from a SELECT statement.
func (e *executor) execCreateView(plan *plannerapi.CreateViewPlan) (*executorapi.Result, error) {
	// Check if view already exists (if not IF NOT EXISTS)
	_, err := e.catalog.GetView(plan.Name)
	if err == nil {
		return nil, fmt.Errorf("%w: view %q already exists", executorapi.ErrExecFailed, plan.Name)
	}
	if err != catalogapi.ErrViewNotFound {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Create the view schema
	schema := catalogapi.ViewSchema{
		Name:     plan.Name,
		QuerySQL: plan.QuerySQL,
	}

	err = e.catalog.CreateView(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: create view: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: 1}, nil
}

// execDropView drops a view.
func (e *executor) execDropView(plan *plannerapi.DropViewPlan) (*executorapi.Result, error) {
	// Check if view exists (unless IF EXISTS was specified)
	if !plan.IfExists {
		_, err := e.catalog.GetView(plan.Name)
		if err != nil {
			if err == catalogapi.ErrViewNotFound {
				return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
			}
			return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
		}
	}

	err := e.catalog.DropView(plan.Name)
	if err != nil {
		if err == catalogapi.ErrViewNotFound && plan.IfExists {
			// IF EXISTS was specified and view doesn't exist — OK
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: drop view: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: 1}, nil
}

// execCreateFunction registers a user-defined function in the FunctionRegistry.
func (e *executor) execCreateFunction(p *plannerapi.CreateFunctionPlan) (*executorapi.Result, error) {
	// Register function in the registry
	args := make([]string, len(p.Args))
	for i, arg := range p.Args {
		args[i] = arg.Name
	}

	e.funcRegistry.Register(&FunctionDef{
		Name:    p.Name,
		Args:    args,
		RetType: p.Returns,
		Body:    p.Body,
	})

	return &executorapi.Result{RowsAffected: 1}, nil
}

// execDropFunction drops a user-defined function from the FunctionRegistry.
func (e *executor) execDropFunction(p *plannerapi.DropFunctionPlan) (*executorapi.Result, error) {
	// For MVP, we don't implement DROP FUNCTION since functions are in-memory
	// The planner returns the plan but executor just acknowledges it
	// In a full implementation, we'd remove from funcRegistry
	_ = p // unused in MVP
	return &executorapi.Result{RowsAffected: 1}, nil
}

// execCreateFTS creates a FTS virtual table.
// FTS tables are stored in the catalog as special tables with FTS metadata.
func (e *executor) execCreateFTS(plan *plannerapi.CreateFTSPlan) (*executorapi.Result, error) {
	// Allocate table ID for the FTS table
	tableID, err := e.nextID(metaNextTableID)
	if err != nil {
		return nil, err
	}

	// Create catalog entry for the FTS table
	// We use a special table type to distinguish FTS tables
	schema := catalogapi.TableSchema{
		Name:     plan.Schema.Name,
		TableID:  tableID,
		Columns: ftsColumnsToSchema(plan.Schema.Columns),
	}

	// Store FTS metadata in a special column attribute
	// For now, we mark it as a virtual table via the catalog
	// The FTS-specific config (tokenizer, columns) is stored alongside

	// Use IF NOT EXISTS semantics
	existing, err := e.catalog.GetTable(plan.Schema.Name)
	if err == nil && existing != nil {
		if plan.IfNotExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, catalogapi.ErrTableExists)
	}
	if err != nil && err != catalogapi.ErrTableNotFound {
		return nil, fmt.Errorf("%w: checking table existence: %v", executorapi.ErrExecFailed, err)
	}

	// Store FTS table metadata
	err = e.catalog.CreateTable(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: creating FTS table: %v", executorapi.ErrExecFailed, err)
	}

	// Store FTS-specific metadata (columns, tokenizer) as a special index entry
	ftsMeta := plannerapi.FTSIndexSchema{
		Name:       plan.Schema.Name,
		TableID:    tableID,
		Columns:    plan.Schema.Columns,
		Tokenizer:  plan.Schema.Tokenizer,
		FTSVersion: plan.Schema.FTSVersion,
	}
	err = e.catalog.CreateIndex(catalogapi.IndexSchema{
		Name:    "_fts_meta_" + plan.Schema.Name,
		Table:   plan.Schema.Name,
		Column:  "", // empty column = FTS metadata marker
		IndexID: tableID, // reuse tableID
	})
	if err != nil {
		// Ignore duplicate index error - metadata might already exist
		if err != catalogapi.ErrIndexExists {
			return nil, fmt.Errorf("%w: storing FTS metadata: %v", executorapi.ErrExecFailed, err)
		}
	}

	// Store FTS metadata reference (using IndexSchema to store arbitrary FTS config)
	// We serialize the FTS schema into the catalog's internal format
	_ = ftsMeta // will be used when implementing FTS DML

	return &executorapi.Result{RowsAffected: 0}, nil
}

// ftsColumnsToSchema converts FTS column names to ColumnDef list.
func ftsColumnsToSchema(columns []string) []catalogapi.ColumnDef {
	cols := make([]catalogapi.ColumnDef, len(columns)+1)
	// Add rowid column
	cols[0] = catalogapi.ColumnDef{
		Name: "rowid",
		Type: catalogapi.TypeInt,
	}
	// Add FTS content columns
	for i, col := range columns {
		cols[i+1] = catalogapi.ColumnDef{
			Name: col,
			Type: catalogapi.TypeText,
		}
	}
	return cols
}

// isFTSColumn returns true if the column is an FTS content column (not rowid).
func isFTSColumn(col catalogapi.ColumnDef) bool {
	return col.Name != "rowid" && col.Type == catalogapi.TypeText
}

// getFTSColumnValues extracts TEXT column values from a row for FTS indexing.
func getFTSColumnValues(columns []catalogapi.ColumnDef, values []catalogapi.Value) []string {
	var texts []string
	for i, col := range columns {
		if isFTSColumn(col) && i < len(values) {
			if !values[i].IsNull {
				texts = append(texts, values[i].Text)
			}
		}
	}
	return texts
}

// indexFTSDocument adds a document to the FTS inverted index using a WriteBatch.
func (e *executor) indexFTSDocument(tableName string, rowID uint64, texts []string, tokenizer string, batch kvstoreapi.WriteBatch) error {
	if e.ftsEngine == nil {
		return nil
	}
	// Get tokens from FTS engine's tokenize function
	// We need to tokenize and store directly in KV
	tokens := e.tokenizeTexts(texts, tokenizer)
	for _, token := range tokens {
		key := e.ftsEngineSearchKey(tableName, token, rowID)
		if err := batch.Put(key, []byte{}); err != nil {
			return err
		}
	}
	return nil
}

// indexFTSWithTxn indexes a document in a transaction context.
func (e *executor) indexFTSWithTxn(tableName string, rowID uint64, texts []string, tokenizer string, xid uint64) error {
	if e.ftsEngine == nil {
		return nil
	}
	tokens := e.tokenizeTexts(texts, tokenizer)
	for _, token := range tokens {
		key := e.ftsEngineSearchKey(tableName, token, rowID)
		if err := e.store.PutWithXID(key, []byte{}, xid); err != nil {
			return err
		}
		e.txnCtx.AddPendingWrite(key, nil)
	}
	return nil
}

// tokenizeTexts tokenizes multiple text values.
func (e *executor) tokenizeTexts(texts []string, tokenizer string) []string {
	var tokens []string
	tokenMap := make(map[string]struct{})

	for _, text := range texts {
		if text == "" {
			continue
		}
		words := simpleTokenizeFTS(text)
		for _, word := range words {
			if word == "" {
				continue
			}
			// Apply stemmer if requested
			if tokenizer == "porter" {
				word = porterStemFTS(word)
			}
			if word != "" {
				tokenMap[word] = struct{}{}
			}
		}
	}

	for token := range tokenMap {
		tokens = append(tokens, token)
	}
	return tokens
}

// simpleTokenizeFTS splits text on whitespace and strips punctuation.
func simpleTokenizeFTS(text string) []string {
	var words []string
	var word strings.Builder

	for _, r := range text {
		if unicode.IsSpace(r) {
			if word.Len() > 0 {
				words = append(words, word.String())
				word.Reset()
			}
			continue
		}
		// Keep alphanumeric characters
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			word.WriteRune(unicode.ToLower(r))
		}
		// Skip punctuation
	}

	if word.Len() > 0 {
		words = append(words, word.String())
	}
	return words
}

// porterStemFTS is a simple porter stemmer.
func porterStemFTS(word string) string {
	if len(word) <= 3 {
		return word
	}

	suffixes := []struct {
		suffix string
		stem   string
	}{
		{"ational", "ate"}, {"tional", "tion"}, {"enci", "ence"},
		{"anci", "ance"}, {"izer", "ize"}, {"ation", "ate"},
		{"alism", "al"}, {"iveness", "ive"}, {"fulness", "ful"},
		{"ousness", "ous"}, {"aliti", "al"}, {"iviti", "ive"},
		{"biliti", "ble"}, {"alli", "al"}, {"entli", "ent"},
		{"eli", "e"}, {"ousli", "ous"}, {"ement", ""},
		{"ment", ""}, {"ent", ""}, {"ness", ""},
		{"ful", ""}, {"less", ""}, {"able", ""},
		{"ible", ""}, {"al", ""}, {"ive", ""},
		{"ous", ""}, {"ant", ""}, {"ence", ""},
		{"ance", ""}, {"er", ""}, {"ic", ""},
		{"ing", ""}, {"ion", ""}, {"ed", ""},
		{"es", ""}, {"ly", ""},
	}

	for _, s := range suffixes {
		if len(word) > len(s.suffix)+2 && strings.HasSuffix(word, s.suffix) {
			stem := word[:len(word)-len(s.suffix)] + s.stem
			if len(stem) >= 2 {
				return stem
			}
		}
	}
	return word
}

// ftsEngineSearchKey creates the FTS index key for KV storage.
func (e *executor) ftsEngineSearchKey(tableName, token string, rowID uint64) []byte {
	// Key format: _sql:fti:{tableName}:{token}:{rowID}
	prefix := []byte("_sql:fti:")
	nameBytes := []byte(strings.ToUpper(tableName))
	tokenBytes := []byte(strings.ToLower(token))
	keyLen := len(prefix) + len(nameBytes) + 1 + len(tokenBytes) + 1 + 8

	buf := make([]byte, keyLen)
	offset := 0
	copy(buf[offset:offset+len(prefix)], prefix)
	offset += len(prefix)
	copy(buf[offset:offset+len(nameBytes)], nameBytes)
	offset += len(nameBytes)
	buf[offset] = ':'
	offset++
	copy(buf[offset:offset+len(tokenBytes)], tokenBytes)
	offset += len(tokenBytes)
	buf[offset] = ':'
	offset++
	binary.BigEndian.PutUint64(buf[offset:offset+8], rowID)
	return buf
}

// execFTSSearch executes a FTS MATCH search.
func (e *executor) execFTSSearch(plan *plannerapi.FTSSearchPlan) (*executorapi.Result, error) {
	if e.ftsEngine == nil {
		return nil, fmt.Errorf("%w: FTS engine not available", executorapi.ErrExecFailed)
	}

	// Perform FTS search
	docIDs, err := e.ftsEngine.Search(plan.Table, plan.Query)
	if err != nil {
		return nil, fmt.Errorf("%w: FTS search: %v", executorapi.ErrExecFailed, err)
	}

	// Get FTS table schema
	schema, err := e.catalog.GetTable(plan.Table)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Fetch rows by rowID and apply residual filter
	var rows [][]catalogapi.Value
	for _, docID := range docIDs {
		row, err := e.tableEngine.Get(schema, docID)
		if err != nil {
			if err == engineapi.ErrRowNotFound {
				continue // row was deleted
			}
			return nil, fmt.Errorf("%w: fetching FTS row: %v", executorapi.ErrExecFailed, err)
		}

		// Apply residual filter if present
		if plan.ResidualFilter != nil {
			engineRow := &engineapi.Row{RowID: docID, Values: row.Values}
			match, err := matchFilter(plan.ResidualFilter, engineRow, schema.Columns, nil, e)
			if err != nil {
				return nil, fmt.Errorf("%w: evaluating FTS filter: %v", executorapi.ErrExecFailed, err)
			}
			if !match {
				continue
			}
		}

		rows = append(rows, row.Values)
	}

	// Build column names list
	colNames := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		colNames[i] = col.Name
	}

	return &executorapi.Result{
		Columns: colNames,
		Rows:    rows,
	}, nil
}

// pragmaTableInfo returns column information for a table.
func (e *executor) pragmaTableInfo(tableName string) (*executorapi.Result, error) {
	schema, err := e.catalog.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// SQLite-style table_info columns: cid, name, type, notnull, dflt_value, pk
	rows := make([][]catalogapi.Value, 0, len(schema.Columns))
	for i, col := range schema.Columns {
		var dfltValue catalogapi.Value
		if col.DefaultValue != nil {
			dfltValue = *col.DefaultValue
		} else {
			dfltValue = catalogapi.Value{IsNull: true}
		}

		pk := 0
		if schema.PrimaryKey == col.Name {
			pk = 1
			// For AUTOINCREMENT primary key, pk=2
			if col.AutoInc {
				pk = 2
			}
		}

		var typeName string
		switch col.Type {
		case catalogapi.TypeInt:
			typeName = "INT"
		case catalogapi.TypeFloat:
			typeName = "FLOAT"
		case catalogapi.TypeText:
			typeName = "TEXT"
		case catalogapi.TypeBlob:
			typeName = "BLOB"
		default:
			typeName = "NULL"
		}

		rows = append(rows, []catalogapi.Value{
			catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(i)},          // cid
			catalogapi.Value{Type: catalogapi.TypeText, Text: col.Name},         // name
			catalogapi.Value{Type: catalogapi.TypeText, Text: typeName},          // type
			catalogapi.Value{Type: catalogapi.TypeInt, Int: boolToInt(col.NotNull)}, // notnull
			dfltValue, // dflt_value
			catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(pk)}, // pk
		})
	}

	return &executorapi.Result{
		Columns: []string{"cid", "name", "type", "notnull", "dflt_value", "pk"},
		Rows:    rows,
	}, nil
}

// pragmaTableList returns a list of all tables.
func (e *executor) pragmaTableList() (*executorapi.Result, error) {
	tables, err := e.catalog.ListTables()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	rows := make([][]catalogapi.Value, 0, len(tables))
	for _, name := range tables {
		rows = append(rows, []catalogapi.Value{
			catalogapi.Value{Type: catalogapi.TypeInt, Int: 0},                      // seq
			catalogapi.Value{Type: catalogapi.TypeText, Text: name},                   // name
			catalogapi.Value{Type: catalogapi.TypeText, Text: "table"},                // type
			catalogapi.Value{Type: catalogapi.TypeText, Text: name},                    // tbl_name
			catalogapi.Value{Type: catalogapi.TypeInt, Int: 0},                       // ncol
			catalogapi.Value{Type: catalogapi.TypeText, Text: "CREATE TABLE " + name}, // sql
		})
	}

	return &executorapi.Result{
		Columns: []string{"seq", "name", "type", "tbl_name", "ncol", "sql"},
		Rows:    rows,
	}, nil
}

// pragmaIndexList returns a list of all indexes for a table.
func (e *executor) pragmaIndexList(tableName string) (*executorapi.Result, error) {
	indexes, err := e.catalog.ListIndexes(tableName)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	rows := make([][]catalogapi.Value, 0, len(indexes))
	seq := 0
	for _, idx := range indexes {
		rows = append(rows, []catalogapi.Value{
			catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(seq)},          // seq
			catalogapi.Value{Type: catalogapi.TypeText, Text: idx.Name},           // name
			catalogapi.Value{Type: catalogapi.TypeText, Text: boolToText(idx.Unique)}, // unique
			catalogapi.Value{Type: catalogapi.TypeText, Text: "c"},                 // origin
			catalogapi.Value{Type: catalogapi.TypeText, Text: "c"},                 // partial
		})
		seq++
	}

	return &executorapi.Result{
		Columns: []string{"seq", "name", "unique", "origin", "partial"},
		Rows:    rows,
	}, nil
}

// boolToInt converts bool to int (0 or 1).
func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// boolToText converts bool to SQLite text representation.
func boolToText(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

func (e *executor) execCreateIndex(plan *plannerapi.CreateIndexPlan) (*executorapi.Result, error) {
	// Check existence BEFORE allocating ID to avoid wasting IDs.
	_, err := e.catalog.GetIndex(plan.Schema.Table, plan.Schema.Name)
	if err == nil {
		// Index exists.
		if plan.IfNotExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, catalogapi.ErrIndexExists)
	}
	if err != catalogapi.ErrIndexNotFound {
		return nil, fmt.Errorf("%w: checking index existence: %v", executorapi.ErrExecFailed, err)
	}

	// Validate table exists
	tbl, err := e.catalog.GetTable(plan.Schema.Table)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Validate column exists for simple indexes
	if plan.Expr == nil {
		colIdx := findColumnIndex(tbl, plan.Schema.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("%w: column %q not found", catalogapi.ErrColumnNotFound, plan.Schema.Column)
		}
	}

	// Now safe to allocate ID.
	indexID, err := e.nextID(metaNextIndexID)
	if err != nil {
		return nil, err
	}

	schema := plan.Schema
	schema.IndexID = indexID

	// Backfill: scan existing rows and build index entries
	existingRows, err := e.tableScan(tbl, nil, nil, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: backfill scan: %v", executorapi.ErrExecFailed, err)
	}

	batch := e.store.NewWriteBatch()
	for _, row := range existingRows {
		var val catalogapi.Value
		if plan.Expr != nil {
			// Evaluate expression for expression indexes
			val, err = evalExpr(plan.Expr, row, tbl.Columns, nil, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: evaluating expression: %v", executorapi.ErrExecFailed, err)
			}
		} else {
			// Get column value for simple column indexes
			colIdx := findColumnIndex(tbl, schema.Column)
			val = row.Values[colIdx]
		}
		idxKey := e.indexEngine.EncodeIndexKey(tbl.TableID, schema.IndexID, val, row.RowID)
		if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: backfill insert index: %v", executorapi.ErrExecFailed, err)
		}
	}

	// Add catalog entry to the SAME batch as index entries for atomicity
	if err := e.catalog.CreateIndexBatch(schema, batch); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: create catalog entry: %v", executorapi.ErrExecFailed, err)
	}

	if err := batch.Commit(); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: backfill commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: 0}, nil
}

func (e *executor) execDropIndex(plan *plannerapi.DropIndexPlan) (*executorapi.Result, error) {
	// Get index schema to find IDs for data cleanup
	idx, err := e.catalog.GetIndex(plan.TableName, plan.IndexName)
	if err != nil {
		if err == catalogapi.ErrIndexNotFound && plan.IfExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Get table for tableID
	tbl, err := e.catalog.GetTable(plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Drop index data
	if err := e.indexEngine.DropIndexData(tbl.TableID, idx.IndexID); err != nil {
		return nil, fmt.Errorf("%w: dropping index data: %v", executorapi.ErrExecFailed, err)
	}

	// Drop catalog entry
	if err := e.catalog.DropIndex(plan.TableName, plan.IndexName); err != nil {
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: 0}, nil
}

// ─── DML Execution ──────────────────────────────────────────────────

func (e *executor) execInsert(plan *plannerapi.InsertPlan) (*executorapi.Result, error) {
	// Handle parameterized INSERT (when Exprs is set)
	if len(plan.Exprs) > 0 {
		return e.execInsertParameterized(plan, nil)
	}

	// Get indexes for this table (to maintain index entries)
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// When in a transaction, use WriteBatch + PutWithXID for batching.
	// All writes share the transaction's XID and are committed atomically via
	// CommitWithXID. This enables own-write visibility (txnMin==s.XID) and
	// rollback (txnMax==txnXID).
	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		batch := e.store.NewWriteBatch()
		rowIDs := make([]uint64, 0, len(plan.Rows))

		for _, row := range plan.Rows {
			// Fire BEFORE INSERT triggers
			if err := e.fireTriggers(plan.Table.Name, "BEFORE", "INSERT", plan.Table.Columns, row, nil, nil); err != nil {
				batch.Discard()
				return nil, err
			}

			// Check NOT NULL constraint before inserting.
			if err := checkNotNullConstraint(plan.Table.Columns, row); err != nil {
				batch.Discard()
				return nil, err
			}

			// Check UNIQUE constraint before inserting.
			if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, row, 0); err != nil {
				batch.Discard()
				return nil, err
			}

			// Check CHECK constraints before inserting.
			if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, row, plan.Table.CheckConstraints); err != nil {
				batch.Discard()
				return nil, err
			}

			// Check FOREIGN KEY constraints before inserting.
			if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, row, plan.Table.ForeignKeys); err != nil {
				batch.Discard()
				return nil, err
			}

			// Allocate rowID via table engine (in-memory only).
			rowID, err := e.tableEngine.AllocRowID(plan.Table.TableID)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: alloc rowid: %v", executorapi.ErrExecFailed, err)
			}
			rowIDs = append(rowIDs, rowID)

			// Stage row data write via batch.
			rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, rowID)
			rowVal := e.tableEngine.EncodeRow(row)
			if err := batch.PutWithXID(rowKey, rowVal, xid); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
			}
			e.txnCtx.AddPendingWrite(rowKey, nil) // nil preValue: INSERT rollback = delete

			// Stage index entries via batch.
			for _, idx := range indexes {
				rowForIdx := &engineapi.Row{RowID: rowID, Values: row}
				val, err := getIndexValue(idx, rowForIdx, plan.Table.Columns, e)
				if err != nil {
					batch.Discard()
					return nil, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
				}
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
				if err := batch.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
					batch.Discard()
					return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey, nil) // nil preValue: rollback = delete
			}

			// FTS: Index FTS content columns.
			if e.ftsEngine != nil {
				texts := getFTSColumnValues(plan.Table.Columns, row)
				if len(texts) > 0 {
					tokens := e.tokenizeTexts(texts, "")
					for _, token := range tokens {
						ftsKey := e.ftsEngineSearchKey(plan.Table.Name, token, rowID)
						if err := batch.PutWithXID(ftsKey, []byte{}, xid); err != nil {
							batch.Discard()
							return nil, fmt.Errorf("%w: FTS index: %v", executorapi.ErrExecFailed, err)
						}
						e.txnCtx.AddPendingWrite(ftsKey, nil)
					}
				}
			}

			// Fire AFTER INSERT triggers
			if err := e.fireTriggers(plan.Table.Name, "AFTER", "INSERT", plan.Table.Columns, row, nil, nil); err != nil {
				batch.Discard()
				return nil, err
			}
		}

		// Persist counter via batch.
		tableID := plan.Table.TableID
		metaKey := encodeMetaKeyLocal(tableID)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, e.tableEngine.GetCounter(tableID))
		if err := batch.PutWithXID(metaKey, buf, xid); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
		}
		e.txnCtx.AddPendingWrite(metaKey, nil) // nil preValue: rollback = delete

		// Commit all staged operations atomically under the SQL transaction's XID.
		// All rows share one WAL fsync instead of per-row fsync.
		if err := batch.CommitWithXID(xid); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
		}
		return &executorapi.Result{RowsAffected: int64(len(rowIDs))}, nil
	}


	// Non-transactional path: use WriteBatch for auto-commit per statement.
	batch := e.store.NewWriteBatch()

	for _, row := range plan.Rows {
		// Fire BEFORE INSERT triggers
		if err := e.fireTriggers(plan.Table.Name, "BEFORE", "INSERT", plan.Table.Columns, row, nil, nil); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check NOT NULL constraint before inserting.
		if err := checkNotNullConstraint(plan.Table.Columns, row); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check UNIQUE constraint before inserting.
		if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, row, 0); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check CHECK constraints before inserting.
		if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, row, plan.Table.CheckConstraints); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check FOREIGN KEY constraints before inserting.
		if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, row, plan.Table.ForeignKeys); err != nil {
			batch.Discard()
			return nil, err
		}

		// Insert row into the shared batch.
		rowID, err := e.tableEngine.InsertInto(plan.Table, batch, row)
		if err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
		}

		// Insert index entries into the same batch.
		for _, idx := range indexes {
			// Get index value (column or expression)
			rowForIdx := &engineapi.Row{RowID: rowID, Values: row}
			val, err := getIndexValue(idx, rowForIdx, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
			}
			// TODO(CR-B): IndexEngine.InsertBatch and EncodeIndexKey are available, but
			// they encode only (tableID, indexID, value, rowID). The index engine's
			// internal state (prefix tree) is NOT updated within the batch. Full atomicity
			// requires engine support for batch-based state updates. Rows can exist
			// without index entries on crash; index can be rebuilt via backfill.
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
			if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}

		// FTS: Index FTS content columns
		if e.ftsEngine != nil {
			texts := getFTSColumnValues(plan.Table.Columns, row)
			if len(texts) > 0 {
				// Get tokenizer from FTS metadata (stored as special index)
				tokenizer := ""
				if err := e.indexFTSDocument(plan.Table.Name, rowID, texts, tokenizer, batch); err != nil {
					batch.Discard()
					return nil, fmt.Errorf("%w: FTS index: %v", executorapi.ErrExecFailed, err)
				}
			}
		}

		// Fire AFTER INSERT triggers (after row and indexes are committed)
		if err := e.fireTriggers(plan.Table.Name, "AFTER", "INSERT", plan.Table.Columns, row, nil, nil); err != nil {
			batch.Discard()
			return nil, err
		}
	}

	// CR-A: Add counter to the batch BEFORE commit. InsertInto updates the counter
	// in-memory but does NOT persist it. By including persistCounter in the same
	// batch, rows, indexes, AND counter are committed atomically.
	if err := e.tableEngine.PersistCounter(batch, plan.Table.TableID); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
	}

	// Commit rows + indexes + counter atomically.
	if err := batch.Commit(); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: int64(len(plan.Rows))}, nil
}

// execUpsert handles INSERT ... ON CONFLICT DO UPDATE / DO NOTHING
func (e *executor) execUpsert(plan *plannerapi.UpsertPlan) (*executorapi.Result, error) {
	// Get indexes for conflict detection and index maintenance
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Find the index on conflict columns (UNIQUE index, typically PRIMARY KEY)
	var conflictIndex *catalogapi.IndexSchema
	for _, idx := range indexes {
		if !idx.Unique {
			continue
		}
		// Check if this index covers all conflict columns
		if len(plan.ConflictColumns) == 1 {
			colIdx := plan.ConflictColumns[0]
			if findColumnIndexByName(plan.Table.Columns, idx.Column) == colIdx {
				conflictIndex = idx
				break
			}
		}
	}

	var count int64

	if len(plan.Exprs) > 0 {
		// Parameterized: resolve values at execution time
		return e.execUpsertParameterized(plan, indexes, conflictIndex)
	}

	// Non-parameterized: values already resolved
	for _, row := range plan.Rows {
		affected, err := e.upsertRow(plan, row, indexes, conflictIndex)
		if err != nil {
			return nil, err
		}
		count += affected
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

// upsertRow performs upsert for a single row.
func (e *executor) upsertRow(plan *plannerapi.UpsertPlan, row []catalogapi.Value,
	indexes []*catalogapi.IndexSchema, conflictIndex *catalogapi.IndexSchema) (int64, error) {
	// Extract conflict key values
	conflictKeyVals := make([]catalogapi.Value, len(plan.ConflictColumns))
	for i, colIdx := range plan.ConflictColumns {
		conflictKeyVals[i] = row[colIdx]
	}

	// Check if row with conflict key already exists
	existingRow, err := e.findByConflictKey(plan.Table, plan.ConflictColumns, conflictKeyVals, conflictIndex)
	if err != nil {
		return 0, err
	}

	if existingRow != nil {
		// Conflict found - apply ON CONFLICT action
		if plan.Action == plannerapi.UpsertDoNothing {
			return 0, nil // DO NOTHING: skip
		}
		// DO UPDATE
		return e.upsertUpdateRow(plan, existingRow, indexes)
	}

	// No conflict - insert new row
	return e.upsertInsertRow(plan, row, indexes)
}

// findByConflictKey finds a row by conflict key using index or scan.
func (e *executor) findByConflictKey(tbl *catalogapi.TableSchema, conflictCols []int,
	vals []catalogapi.Value, idx *catalogapi.IndexSchema) (*engineapi.Row, error) {
	if idx != nil && len(conflictCols) == 1 {
		// Use index lookup (most common case: PRIMARY KEY)
		iter, err := e.indexEngine.Scan(tbl.TableID, idx.IndexID, encodingapi.OpEQ, vals[0])
		if err != nil {
			return nil, fmt.Errorf("%w: index scan: %v", executorapi.ErrExecFailed, err)
		}
		defer iter.Close()
		if iter.Next() {
			rowID := iter.RowID()
			row, err := e.tableEngine.Get(tbl, rowID)
			if err != nil {
				return nil, fmt.Errorf("%w: get row: %v", executorapi.ErrExecFailed, err)
			}
			return row, nil
		}
		return nil, nil
	}

	// Fall back to full scan (for multi-column or non-indexed conflicts)
	iter, err := e.tableEngine.Scan(tbl)
	if err != nil {
		return nil, fmt.Errorf("%w: scan: %v", executorapi.ErrExecFailed, err)
	}
	defer iter.Close()

	for iter.Next() {
		row := iter.Row()
		if row == nil {
			continue
		}
		// Check if all conflict columns match
		match := true
		for i, colIdx := range conflictCols {
			if compareValues(row.Values[colIdx], vals[i]) != 0 {
				match = false
				break
			}
		}
		if match {
			return row, nil
		}
	}
	return nil, iter.Err()
}

// upsertInsertRow inserts a new row (no conflict).
func (e *executor) upsertInsertRow(plan *plannerapi.UpsertPlan, row []catalogapi.Value,
	indexes []*catalogapi.IndexSchema) (int64, error) {
	// Check NOT NULL constraint
	if err := checkNotNullConstraint(plan.Table.Columns, row); err != nil {
		return 0, err
	}
	// Check UNIQUE constraint (exclude conflict key from duplicate detection)
	if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, row, 0); err != nil {
		// If unique violation on conflict columns, fall through to upsert
		// (This shouldn't happen since we already checked for conflicts)
	}
	// Check CHECK constraints
	if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, row, plan.Table.CheckConstraints); err != nil {
		return 0, err
	}
	// Check FK constraints
	if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, row, plan.Table.ForeignKeys); err != nil {
		return 0, err
	}

	if e.txnCtx != nil {
		return e.upsertInsertRowTxn(plan, row, indexes)
	}
	return e.upsertInsertRowBatch(plan, row, indexes)
}

func (e *executor) upsertInsertRowBatch(plan *plannerapi.UpsertPlan, row []catalogapi.Value,
	indexes []*catalogapi.IndexSchema) (int64, error) {
	batch := e.store.NewWriteBatch()
	rowID, err := e.tableEngine.InsertInto(plan.Table, batch, row)
	if err != nil {
		batch.Discard()
		return 0, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
	}
	// Insert index entries
	for _, idx := range indexes {
		rowForIdx := &engineapi.Row{RowID: rowID, Values: row}
		val, err := getIndexValue(idx, rowForIdx, plan.Table.Columns, e)
		if err != nil {
			batch.Discard()
			return 0, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
		}
		idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
		if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
			batch.Discard()
			return 0, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
		}
	}
	if err := e.tableEngine.PersistCounter(batch, plan.Table.TableID); err != nil {
		batch.Discard()
		return 0, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
	}
	if err := batch.Commit(); err != nil {
		batch.Discard()
		return 0, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}
	return 1, nil
}

func (e *executor) upsertInsertRowTxn(plan *plannerapi.UpsertPlan, row []catalogapi.Value,
	indexes []*catalogapi.IndexSchema) (int64, error) {
	xid := e.txnCtx.XID()
	rowID, err := e.tableEngine.AllocRowID(plan.Table.TableID)
	if err != nil {
		return 0, fmt.Errorf("%w: alloc rowid: %v", executorapi.ErrExecFailed, err)
	}
	rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, rowID)
	rowVal := e.tableEngine.EncodeRow(row)
	if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
		return 0, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
	}
	e.txnCtx.AddPendingWrite(rowKey, nil)
	// Write index entries
	for _, idx := range indexes {
		rowForIdx := &engineapi.Row{RowID: rowID, Values: row}
		val, err := getIndexValue(idx, rowForIdx, plan.Table.Columns, e)
		if err != nil {
			return 0, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
		}
		idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
		if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
			return 0, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
		}
		e.txnCtx.AddPendingWrite(idxKey, nil)
	}
	// Persist counter — AllocRowID already advanced it
	metaKey := encodeMetaKeyLocal(plan.Table.TableID)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, e.tableEngine.GetCounter(plan.Table.TableID))
	if err := e.store.PutWithXID(metaKey, buf, xid); err != nil {
		return 0, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
	}
	e.txnCtx.AddPendingWrite(metaKey, nil)
	return 1, nil
}

// upsertUpdateRow updates an existing row (DO UPDATE).
func (e *executor) upsertUpdateRow(plan *plannerapi.UpsertPlan, existingRow *engineapi.Row,
	indexes []*catalogapi.IndexSchema) (int64, error) {
	// Build new values by applying UPDATE assignments
	newValues := make([]catalogapi.Value, len(existingRow.Values))
	copy(newValues, existingRow.Values)

	// Apply UPDATE assignments
	for colIdx, val := range plan.UpdateAssignments {
		newValues[colIdx] = val
	}

	// Check NOT NULL
	if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
		return 0, err
	}
	// Check UNIQUE (exclude current row)
	if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues, existingRow.RowID); err != nil {
		return 0, err
	}
	// Check CHECK constraints
	if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.CheckConstraints); err != nil {
		return 0, err
	}
	// Check FK constraints
	if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.ForeignKeys); err != nil {
		return 0, err
	}

	if e.txnCtx != nil {
		return e.upsertUpdateRowTxn(plan, existingRow, newValues, indexes)
	}
	return e.upsertUpdateRowBatch(plan, existingRow, newValues, indexes)
}

func (e *executor) upsertUpdateRowBatch(plan *plannerapi.UpsertPlan, existingRow *engineapi.Row,
	newValues []catalogapi.Value, indexes []*catalogapi.IndexSchema) (int64, error) {
	// Build set of changed columns
	changedCols := make(map[int]bool)
	for colIdx := range plan.UpdateAssignments {
		changedCols[colIdx] = true
	}

	batch := e.store.NewWriteBatch()
	// Delete old index entries for changed columns
	for _, idx := range indexes {
		colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
		if colIdx < 0 || !changedCols[colIdx] {
			continue
		}
		oldVal, err := getIndexValue(idx, existingRow, plan.Table.Columns, e)
		if err != nil {
			batch.Discard()
			return 0, fmt.Errorf("%w: get old index value: %v", executorapi.ErrExecFailed, err)
		}
		idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, existingRow.RowID)
		if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
			batch.Discard()
			return 0, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
		}
	}

	// Update row
	rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, existingRow.RowID)
	rowVal := e.tableEngine.EncodeRow(newValues)
	if err := batch.Put(rowKey, rowVal); err != nil {
		batch.Discard()
		return 0, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
	}

	// Insert new index entries
	newRow := &engineapi.Row{RowID: existingRow.RowID, Values: newValues}
	for _, idx := range indexes {
		colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
		if colIdx < 0 || !changedCols[colIdx] {
			continue
		}
		newVal, err := getIndexValue(idx, newRow, plan.Table.Columns, e)
		if err != nil {
			batch.Discard()
			return 0, fmt.Errorf("%w: get new index value: %v", executorapi.ErrExecFailed, err)
		}
		idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, existingRow.RowID)
		if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
			batch.Discard()
			return 0, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
		}
	}

	if err := batch.Commit(); err != nil {
		batch.Discard()
		return 0, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}
	return 1, nil
}

func (e *executor) upsertUpdateRowTxn(plan *plannerapi.UpsertPlan, existingRow *engineapi.Row,
	newValues []catalogapi.Value, indexes []*catalogapi.IndexSchema) (int64, error) {
	xid := e.txnCtx.XID()

	// Build set of changed columns
	changedCols := make(map[int]bool)
	for colIdx := range plan.UpdateAssignments {
		changedCols[colIdx] = true
	}

	// Delete old index entries
	for _, idx := range indexes {
		colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
		if colIdx < 0 || !changedCols[colIdx] {
			continue
		}
		oldVal, err := getIndexValue(idx, existingRow, plan.Table.Columns, e)
		if err != nil {
			return 0, fmt.Errorf("%w: get old index value: %v", executorapi.ErrExecFailed, err)
		}
		idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, existingRow.RowID)
		if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
			return 0, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
		}
		e.txnCtx.AddPendingWrite(idxKey, nil)
	}

	// Update row
	oldRowVal := e.tableEngine.EncodeRow(existingRow.Values)
	rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, existingRow.RowID)
	rowVal := e.tableEngine.EncodeRow(newValues)
	if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
		return 0, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
	}
	e.txnCtx.AddPendingWrite(rowKey, oldRowVal)

	// Insert new index entries
	newRow := &engineapi.Row{RowID: existingRow.RowID, Values: newValues}
	for _, idx := range indexes {
		colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
		if colIdx < 0 || !changedCols[colIdx] {
			continue
		}
		newVal, err := getIndexValue(idx, newRow, plan.Table.Columns, e)
		if err != nil {
			return 0, fmt.Errorf("%w: get new index value: %v", executorapi.ErrExecFailed, err)
		}
		idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, existingRow.RowID)
		if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
			return 0, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
		}
		e.txnCtx.AddPendingWrite(idxKey, nil)
	}
	return 1, nil
}

// execUpsertParameterized handles parameterized UPSERT.
func (e *executor) execUpsertParameterized(plan *plannerapi.UpsertPlan, indexes []*catalogapi.IndexSchema,
	conflictIndex *catalogapi.IndexSchema) (*executorapi.Result, error) {
	// Note: ParamUpdateAssignments are resolved per-row in upsertRow
	// This is handled by modifying upsertRow to check ParamUpdateAssignments
	// For now, parameterized upsert is not supported - fall back to execUpsert path
	// The plan already has Exprs set for parameterized INSERT values
	var count int64
	for _, exprRow := range plan.Exprs {
		resolved, err := e.resolveInsertRowParameterized(plan.Table, nil, exprRow)
		if err != nil {
			return nil, fmt.Errorf("row: %w", err)
		}
		affected, err := e.upsertRow(plan, resolved, indexes, conflictIndex)
		if err != nil {
			return nil, err
		}
		count += affected
	}
	return &executorapi.Result{RowsAffected: count}, nil
}

// execInsertParameterized handles INSERT with ParamRef expressions (resolved at execution time).
func (e *executor) execInsertParameterized(plan *plannerapi.InsertPlan, columns []string) (*executorapi.Result, error) {
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	if e.txnCtx != nil {
		// Transactional path
		xid := e.txnCtx.XID()
		for i, exprRow := range plan.Exprs {
			resolved, err := e.resolveInsertRowParameterized(plan.Table, columns, exprRow)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", i+1, err)
			}

			// Check constraints
			if err := checkNotNullConstraint(plan.Table.Columns, resolved); err != nil {
				return nil, err
			}
			if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, resolved, 0); err != nil {
				return nil, err
			}
			if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, resolved, plan.Table.CheckConstraints); err != nil {
				return nil, err
			}
			if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, resolved, plan.Table.ForeignKeys); err != nil {
				return nil, err
			}

			rowID, err := e.tableEngine.AllocRowID(plan.Table.TableID)
			if err != nil {
				return nil, fmt.Errorf("%w: alloc rowid: %v", executorapi.ErrExecFailed, err)
			}

			rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, rowID)
			rowVal := e.tableEngine.EncodeRow(resolved)
			if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
				return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
			}
			e.txnCtx.AddPendingWrite(rowKey, nil)

			// Write index entries
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 {
					continue
				}
				val := resolved[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
				if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
					return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey, nil)
			}
		}

		// Persist counter — AllocRowID already advanced it
		metaKey := encodeMetaKeyLocal(plan.Table.TableID)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, e.tableEngine.GetCounter(plan.Table.TableID))
		if err := e.store.PutWithXID(metaKey, buf, xid); err != nil {
			return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
		}
		e.txnCtx.AddPendingWrite(metaKey, nil)

		return &executorapi.Result{RowsAffected: int64(len(plan.Exprs))}, nil
	}

	// Non-transactional path
	batch := e.store.NewWriteBatch()
	for i, exprRow := range plan.Exprs {
		resolved, err := e.resolveInsertRowParameterized(plan.Table, columns, exprRow)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}

		// Check constraints
		if err := checkNotNullConstraint(plan.Table.Columns, resolved); err != nil {
			return nil, err
		}
		if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, resolved, 0); err != nil {
			return nil, err
		}
		if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, resolved, plan.Table.CheckConstraints); err != nil {
			return nil, err
		}
		if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, resolved, plan.Table.ForeignKeys); err != nil {
			return nil, err
		}

		rowID, err := e.tableEngine.AllocRowID(plan.Table.TableID)
		if err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: alloc rowid: %v", executorapi.ErrExecFailed, err)
		}

		rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, rowID)
		rowVal := e.tableEngine.EncodeRow(resolved)
		if err := batch.Put(rowKey, rowVal); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
		}

		// Write index entries
		for _, idx := range indexes {
			// Get index value (column or expression)
			rowForIdx := &engineapi.Row{RowID: rowID, Values: resolved}
			val, err := getIndexValue(idx, rowForIdx, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
			if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}
	}

	if err := e.tableEngine.PersistCounter(batch, plan.Table.TableID); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
	}

	if err := batch.Commit(); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: int64(len(plan.Exprs))}, nil
}

// resolveInsertRowParameterized resolves INSERT VALUES expressions with ParamRefs.
func (e *executor) resolveInsertRowParameterized(tbl *catalogapi.TableSchema, columns []string, exprs []parserapi.Expr) ([]catalogapi.Value, error) {
	numCols := len(tbl.Columns)

	if len(columns) > 0 {
		if len(columns) != len(exprs) {
			return nil, plannerapi.ErrColumnCountMismatch
		}
		values := make([]catalogapi.Value, numCols)
		for i := range values {
			values[i] = catalogapi.Value{IsNull: true}
		}
		for i, colName := range columns {
			idx := findColumnIndex(tbl, colName)
			if idx < 0 {
				return nil, fmt.Errorf("%w: %s", plannerapi.ErrColumnNotFound, colName)
			}
			val, err := evalExpr(exprs[i], nil, tbl.Columns, nil, e)
			if err != nil {
				return nil, err
			}
			if _, ok := exprs[i].(*parserapi.DefaultExpr); ok {
				if tbl.Columns[idx].DefaultValue != nil {
					val = *tbl.Columns[idx].DefaultValue
				}
			}
			if !val.IsNull {
				if err := checkType(val, tbl.Columns[idx].Type); err != nil {
					return nil, fmt.Errorf("column %s: %w", colName, err)
				}
			}
			values[idx] = val
		}
		return values, nil
	}

	if len(exprs) != numCols {
		return nil, plannerapi.ErrColumnCountMismatch
	}
	values := make([]catalogapi.Value, numCols)
	for i, expr := range exprs {
		val, err := evalExpr(expr, nil, tbl.Columns, nil, e)
		if err != nil {
			return nil, err
		}
		if _, ok := expr.(*parserapi.DefaultExpr); ok {
			if tbl.Columns[i].DefaultValue != nil {
				val = *tbl.Columns[i].DefaultValue
			}
		}
		if !val.IsNull {
			if err := checkType(val, tbl.Columns[i].Type); err != nil {
				return nil, fmt.Errorf("column %d: %w", i+1, err)
			}
		}
		values[i] = val
	}
	return values, nil
}

func (e *executor) execInsertSelect(plan *plannerapi.InsertSelectPlan) (*executorapi.Result, error) {
	subResult, err := e.execSelect(plan.SelectPlan)
	if err != nil {
		return nil, fmt.Errorf("%w: execute select: %v", executorapi.ErrExecFailed, err)
	}

	// Use destination table's column count as the expected count.
	// When explicit columns are given (INSERT INTO t (a, b) SELECT ...), plan.Columns
	// has the list. Otherwise, the table schema determines the count.
	expectedCols := len(plan.Columns)
	if expectedCols == 0 {
		expectedCols = len(plan.Table.Columns)
	}

	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	batch := e.store.NewWriteBatch()
	rowsAffected := int64(0)

	for i, row := range subResult.Rows {
		if len(row) != expectedCols {
			return nil, fmt.Errorf("row %d: column count mismatch: got %d, expected %d", i+1, len(row), expectedCols)
		}

		rowID, err := e.tableEngine.InsertInto(plan.Table, batch, row)
		if err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
		}

		// Insert index entries into the same batch (same pattern as execInsert).
		for _, idx := range indexes {
			// Get index value (column or expression)
			rowForIdx := &engineapi.Row{RowID: rowID, Values: row}
			val, err := getIndexValue(idx, rowForIdx, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
			if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}
		rowsAffected++
	}

	if err := e.tableEngine.PersistCounter(batch, plan.Table.TableID); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
	}

	if err := batch.Commit(); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: rowsAffected}, nil
}

// isSubqueryInListContext checks if sq appears inside an InExpr's Values list.
func isSubqueryInListContext(sq *parserapi.SubqueryExpr, root parserapi.Expr) bool {
	found := false
	walkExpr(root, func(expr parserapi.Expr) {
		if expr == sq {
			found = true
		}
	})
	if !found {
		return false
	}
	// Walk again, checking if we find sq inside an InExpr.Values
	inList := false
	walkExpr(root, func(expr parserapi.Expr) {
		if inList {
			return
		}
		if inExpr, ok := expr.(*parserapi.InExpr); ok {
			for _, v := range inExpr.Values {
				if v == sq {
					inList = true
					return
				}
			}
		}
	})
	return inList
}

// ─── JOIN EXECUTION ────────────────────────────────────────────────

// execJoinSelect handles SELECT ... FROM t1 JOIN t2 ON ...
func (e *executor) execJoinSelect(plan *plannerapi.SelectPlan) (*executorapi.Result, error) {
	jplan := plan.Join

	// Dispatch based on join plan type
	switch jp := jplan.(type) {
	case *plannerapi.IndexNestedLoopJoinPlan:
		return e.execIndexNestedLoopJoinSelect(plan, jp)
	case *plannerapi.HashJoinPlan:
		return e.execHashJoinSelect(plan, jp)
	case *plannerapi.JoinPlan:
		// Regular nested loop join - use execRegularJoin
		return e.execRegularJoin(jp, plan)
	default:
		return nil, fmt.Errorf("execJoinSelect: unsupported join plan type %T", jplan)
	}
}

// execRegularJoin handles regular nested loop join (used for non-optimized joins).
func (e *executor) execRegularJoin(jp *plannerapi.JoinPlan, plan *plannerapi.SelectPlan) (*executorapi.Result, error) {

	// Collect left rows — may be ScanPlan, nested *JoinPlan, or nested *HashJoinPlan
	var leftRows []*engineapi.Row
	switch left := jp.Left.(type) {
	case *plannerapi.JoinPlan:
		result, err := e.execJoin(left)
		if err != nil {
			return nil, err
		}
		for _, v := range result.Rows {
			leftRows = append(leftRows, &engineapi.Row{Values: v})
		}
	case *plannerapi.HashJoinPlan:
		// Nested hash join - create a SelectPlan wrapper and execute
		innerPlan := &plannerapi.SelectPlan{
			Join:            left,
			Columns:         nil,
			SelectColumns:   nil,
			Filter:          nil,
			GroupByExprs:    nil,
			Having:          nil,
			OrderBy:         nil,
			Limit:           -1,
			Offset:          -1,
			LeftColumnCount: len(left.LeftSchema),
		}
		result, err := e.execHashJoinSelect(innerPlan, left)
		if err != nil {
			return nil, err
		}
		for _, v := range result.Rows {
			leftRows = append(leftRows, &engineapi.Row{Values: v})
		}
	case plannerapi.ScanPlan:
		var err error
		leftRows, err = e.scanRows(jp.LeftTable, left, nil, 0, 0)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("execJoinSelect: unexpected left type %T", jp.Left)
	}

	rightRows, err := e.scanRows(jp.RightTable, jp.Right, nil, 0, 0)
	if err != nil {
		return nil, err
	}

	colNames := make([]string, 0, len(jp.LeftSchema)+len(jp.RightSchema))
	combinedCols := make([]catalogapi.ColumnDef, 0, len(jp.LeftSchema)+len(jp.RightSchema))
	for _, c := range jp.LeftSchema {
		colNames = append(colNames, c.Name)
		col := *c
		if col.Table == "" { col.Table = jp.LeftTable.Name }
		combinedCols = append(combinedCols, col)
	}
	for _, c := range jp.RightSchema {
		colNames = append(colNames, c.Name)
		col := *c
		col.Table = jp.RightTable.Name
		combinedCols = append(combinedCols, col)
	}

	subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
	if jp.On != nil {
		e.walkExprForJoinSubqueries(jp.On, subqueryResults)
	}

	var mergedRows [][]catalogapi.Value
	leftLen := len(jp.LeftSchema)
	rightLen := len(jp.RightSchema)

	switch jp.Type {
	case "LEFT":
		mergedRows = e.execLeftJoin(leftRows, rightRows, jp, subqueryResults, leftLen, rightLen, combinedCols)
	case "RIGHT":
		mergedRows = e.execRightJoin(leftRows, rightRows, jp, subqueryResults, leftLen, rightLen, combinedCols)
	default:
		mergedRows = e.execInnerJoin(leftRows, rightRows, jp, subqueryResults, leftLen, rightLen, combinedCols)
	}

	// Apply WHERE filter on merged rows
	if plan.Filter != nil {
		// Precompute subqueries in WHERE (non-correlated only at this stage)
		if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
			return nil, err
		}
		// Set outerCols for correlated subquery resolution in WHERE.
		// filterRows sets outerVals per-row; evalColumnRef uses both to resolve
		// outer table references in correlated subqueries.
		e.outerCols = combinedCols
		mergedRows = e.filterJoinRows(mergedRows, plan.Filter, combinedCols, subqueryResults)
	}

	// GROUP BY on merged join rows
	if plan.GroupByExprs != nil {
		// Convert [][]catalogapi.Value to []*engineapi.Row for groupByRowsForJoin
		engineRows := make([]*engineapi.Row, len(mergedRows))
		for i, row := range mergedRows {
			engineRows[i] = &engineapi.Row{Values: row}
		}

		grouped, err := e.groupByRowsForJoin(engineRows, plan, combinedCols)
		if err != nil {
			return nil, err
		}

		// HAVING: filter grouped rows
		if plan.Having != nil {
			// Set outerCols for correlated subquery resolution in HAVING.
			// This is the combined schema of the join result.
			e.outerCols = combinedCols
			grouped = e.filterRows(grouped, plan.Having, combinedCols, subqueryResults)
		}

		// ORDER BY on grouped rows
		if plan.OrderBy != nil {
			sortRawRows(grouped, plan.OrderBy)
		}

		// OFFSET on grouped rows (skip first N)
		if plan.Offset >= 0 && plan.Offset < len(grouped) {
			grouped = grouped[plan.Offset:]
		} else if plan.Offset >= len(grouped) {
			grouped = nil
		}

		// LIMIT on grouped rows (take from remaining)
		if plan.Limit >= 0 && plan.Limit < len(grouped) {
			grouped = grouped[:plan.Limit]
		}

		// Extract values from grouped rows (projection done by groupByRowsForJoin)
		rows := make([][]catalogapi.Value, len(grouped))
		for i, row := range grouped {
			rows[i] = row.Values
		}

		// Build column names from SelectColumns
		projCols := make([]string, len(plan.SelectColumns))
		for i, sc := range plan.SelectColumns {
			if ref, ok := sc.Expr.(*parserapi.ColumnRef); ok {
				if ref.Table != "" {
					projCols[i] = ref.Table + "." + ref.Column
				} else {
					projCols[i] = ref.Column
				}
			} else {
				projCols[i] = sc.Alias
			}
		}

		return &executorapi.Result{Columns: projCols, Rows: rows}, nil
	}

	if plan.OrderBy != nil {
		sortJoinRows(mergedRows, plan.OrderBy, combinedCols)
	}

	// OFFSET on merged rows (skip first N)
	if plan.Offset >= 0 && plan.Offset < len(mergedRows) {
		mergedRows = mergedRows[plan.Offset:]
	} else if plan.Offset >= len(mergedRows) {
		mergedRows = nil
	}

	// LIMIT on merged rows (take from remaining)
	if plan.Limit >= 0 && plan.Limit < len(mergedRows) {
		mergedRows = mergedRows[:plan.Limit]
	}

	projected, projCols := projectJoinRows(mergedRows, colNames, plan)

	return &executorapi.Result{
		Columns: projCols,
		Rows:    projected,
	}, nil
}

// execHashJoinSelect handles SELECT with HashJoinPlan (optimized equi-join).
func (e *executor) execHashJoinSelect(plan *plannerapi.SelectPlan, hplan *plannerapi.HashJoinPlan) (*executorapi.Result, error) {
	// Execute left side
	var leftRows []*engineapi.Row
	switch left := hplan.Left.(type) {
	case plannerapi.ScanPlan:
		// Get table schema for scanning
		leftTbl, err := e.catalog.GetTable(hplan.LeftTable)
		if err != nil {
			return nil, fmt.Errorf("execHashJoin: get left table: %w", err)
		}
		var scan plannerapi.ScanPlan
		switch sp := left.(type) {
		case *plannerapi.TableScanPlan:
			scan = sp
		default:
			return nil, fmt.Errorf("execHashJoin: unsupported left scan type %T", left)
		}
		rows, err := e.scanRows(leftTbl, scan, nil, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("execHashJoin: scan left: %w", err)
		}
		leftRows = rows
	case *plannerapi.HashJoinPlan:
		// Nested hash join - should not reach here for simple nested case
		// For nested hash joins, we need to execute the inner join first
		// and then continue with the outer join
		// This case is handled in execJoinSelect
		return nil, fmt.Errorf("nested hash join should be handled in execJoinSelect")
	default:
		return nil, fmt.Errorf("execHashJoin: unsupported left type %T", hplan.Left)
	}

	// Execute right side
	rightTbl, err := e.catalog.GetTable(hplan.RightTable)
	if err != nil {
		return nil, fmt.Errorf("execHashJoin: get right table: %w", err)
	}
	rightRows, err := e.scanRows(rightTbl, hplan.Right, nil, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("execHashJoin: scan right: %w", err)
	}

	leftSchema := hplan.LeftSchema
	rightSchema := hplan.RightSchema
	leftLen := len(leftSchema)
	rightLen := len(rightSchema)

	// Build combined column definitions for ON evaluation
	combinedCols := make([]catalogapi.ColumnDef, 0, leftLen+rightLen)
	for _, c := range leftSchema {
		col := *c
		if col.Table == "" {
			col.Table = hplan.LeftTable
		}
		combinedCols = append(combinedCols, col)
	}
	for _, c := range rightSchema {
		col := *c
		col.Table = hplan.RightTable
		combinedCols = append(combinedCols, col)
	}

	// Execute hash join
	subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
	if hplan.On != nil {
		e.walkExprForJoinSubqueries(hplan.On, subqueryResults)
	}

	mergedRows := e.execHashJoin(leftRows, rightRows, hplan, subqueryResults, leftLen, rightLen, combinedCols)

	// Build column names
	colNames := make([]string, 0, len(leftSchema)+len(rightSchema))
	for _, c := range leftSchema {
		colNames = append(colNames, c.Name)
	}
	for _, c := range rightSchema {
		colNames = append(colNames, c.Name)
	}

	// Apply WHERE filter on merged rows
	if plan.Filter != nil {
		if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
			return nil, err
		}
		e.outerCols = combinedCols
		mergedRows = e.filterJoinRows(mergedRows, plan.Filter, combinedCols, subqueryResults)
	}

	// GROUP BY on merged join rows
	if plan.GroupByExprs != nil {
		engineRows := make([]*engineapi.Row, len(mergedRows))
		for i, row := range mergedRows {
			engineRows[i] = &engineapi.Row{Values: row}
		}

		grouped, err := e.groupByRowsForJoin(engineRows, plan, combinedCols)
		if err != nil {
			return nil, err
		}

		if plan.Having != nil {
			e.outerCols = combinedCols
			grouped = e.filterRows(grouped, plan.Having, combinedCols, subqueryResults)
		}

		if plan.OrderBy != nil {
			sortRawRows(grouped, plan.OrderBy)
		}

		if plan.Offset >= 0 && plan.Offset < len(grouped) {
			grouped = grouped[plan.Offset:]
		} else if plan.Offset >= len(grouped) {
			grouped = nil
		}

		if plan.Limit >= 0 && plan.Limit < len(grouped) {
			grouped = grouped[:plan.Limit]
		}

		rows := make([][]catalogapi.Value, len(grouped))
		for i, row := range grouped {
			rows[i] = row.Values
		}

		projCols := make([]string, len(plan.SelectColumns))
		for i, sc := range plan.SelectColumns {
			if ref, ok := sc.Expr.(*parserapi.ColumnRef); ok {
				if ref.Table != "" {
					projCols[i] = ref.Table + "." + ref.Column
				} else {
					projCols[i] = ref.Column
				}
			} else {
				projCols[i] = sc.Alias
			}
		}

		return &executorapi.Result{Columns: projCols, Rows: rows}, nil
	}

	if plan.OrderBy != nil {
		sortJoinRows(mergedRows, plan.OrderBy, combinedCols)
	}

	if plan.Offset >= 0 && plan.Offset < len(mergedRows) {
		mergedRows = mergedRows[plan.Offset:]
	} else if plan.Offset >= len(mergedRows) {
		mergedRows = nil
	}

	if plan.Limit >= 0 && plan.Limit < len(mergedRows) {
		mergedRows = mergedRows[:plan.Limit]
	}

	projected, projCols := projectJoinRows(mergedRows, colNames, plan)

	return &executorapi.Result{
		Columns: projCols,
		Rows:    projected,
	}, nil
}

// execIndexNestedLoopJoinSelect handles SELECT with IndexNestedLoopJoinPlan.
func (e *executor) execIndexNestedLoopJoinSelect(plan *plannerapi.SelectPlan, nlplan *plannerapi.IndexNestedLoopJoinPlan) (*executorapi.Result, error) {
	// Get table schemas
	outerTbl, err := e.catalog.GetTable(nlplan.OuterTable)
	if err != nil {
		return nil, fmt.Errorf("%w: get outer table: %v", executorapi.ErrExecFailed, err)
	}
	innerTbl, err := e.catalog.GetTable(nlplan.InnerTable)
	if err != nil {
		return nil, fmt.Errorf("%w: get inner table: %v", executorapi.ErrExecFailed, err)
	}

	// Execute outer scan (scan all outer rows)
	var outerScan plannerapi.ScanPlan
	switch s := nlplan.Outer.(type) {
	case plannerapi.ScanPlan:
		outerScan = s
	default:
		return nil, fmt.Errorf("execIndexNestedLoopJoinSelect: outer must be ScanPlan, got %T", nlplan.Outer)
	}
	outerRows, err := e.scanRows(outerTbl, outerScan, nil, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: scan outer: %v", executorapi.ErrExecFailed, err)
	}

	// Build combined schema for ON evaluation
	outerLen := len(nlplan.OuterSchema)
	innerLen := len(nlplan.InnerSchema)
	combinedCols := make([]catalogapi.ColumnDef, 0, outerLen+innerLen)
	for _, c := range nlplan.OuterSchema {
		col := *c
		if col.Table == "" {
			col.Table = nlplan.OuterTable
		}
		combinedCols = append(combinedCols, col)
	}
	for _, c := range nlplan.InnerSchema {
		col := *c
		if col.Table == "" {
			col.Table = nlplan.InnerTable
		}
		combinedCols = append(combinedCols, col)
	}

	// Prepare subquery results
	subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
	if nlplan.On != nil {
		e.walkExprForJoinSubqueries(nlplan.On, subqueryResults)
	}

	var result [][]catalogapi.Value
	isLeftJoin := nlplan.Type == "LEFT"

	for _, outerRow := range outerRows {
		// Extract join key from outer row
		joinKey := outerRow.Values[nlplan.OuterKeyIdx]

		// Index lookup on inner table
		rowIDIter, err := e.indexEngine.Scan(innerTbl.TableID, nlplan.InnerIndex.IndexID, encodingapi.OpEQ, joinKey)
		if err != nil {
			return nil, fmt.Errorf("%w: index lookup: %v", executorapi.ErrExecFailed, err)
		}

		found := false
		for rowIDIter.Next() {
			// Get full row from inner table
			innerRow, err := e.tableEngine.Get(innerTbl, rowIDIter.RowID())
			if err != nil {
				if err == engineapi.ErrRowNotFound {
					continue // stale index entry
				}
				return nil, fmt.Errorf("%w: get inner row: %v", executorapi.ErrExecFailed, err)
			}

			// Evaluate ON condition (if non-equi parts exist)
			if nlplan.On != nil {
				combinedVals := make([]catalogapi.Value, outerLen+innerLen)
				copy(combinedVals[:outerLen], outerRow.Values)
				copy(combinedVals[outerLen:], innerRow.Values)
				combinedRow := &engineapi.Row{Values: combinedVals}
				matchResult, err := evalExpr(nlplan.On, combinedRow, combinedCols, subqueryResults, e)
				if err != nil {
					rowIDIter.Close()
					return nil, err
				}
				if !isTruthy(matchResult) {
					continue
				}
			}

			// Merge rows
			merged := make([]catalogapi.Value, outerLen+innerLen)
			copy(merged[:outerLen], outerRow.Values)
			copy(merged[outerLen:], innerRow.Values)
			result = append(result, merged)
			found = true
		}
		rowIDIter.Close()

		// LEFT JOIN: emit with NULLs if no match
		if isLeftJoin && !found {
			merged := make([]catalogapi.Value, outerLen+innerLen)
			copy(merged[:outerLen], outerRow.Values)
			for i := outerLen; i < outerLen+innerLen; i++ {
				merged[i] = catalogapi.Value{IsNull: true}
			}
			result = append(result, merged)
		}
	}

	// Apply WHERE filter on merged rows
	if plan.Filter != nil {
		if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
			return nil, err
		}
		e.outerCols = combinedCols
		result = e.filterJoinRows(result, plan.Filter, combinedCols, subqueryResults)
	}

	// Build column names
	colNames := make([]string, 0, outerLen+innerLen)
	for _, c := range nlplan.OuterSchema {
		colNames = append(colNames, c.Name)
	}
	for _, c := range nlplan.InnerSchema {
		colNames = append(colNames, c.Name)
	}

	// GROUP BY on merged join rows
	if plan.GroupByExprs != nil {
		engineRows := make([]*engineapi.Row, len(result))
		for i, row := range result {
			engineRows[i] = &engineapi.Row{Values: row}
		}

		grouped, err := e.groupByRowsForJoin(engineRows, plan, combinedCols)
		if err != nil {
			return nil, err
		}

		if plan.Having != nil {
			e.outerCols = combinedCols
			grouped = e.filterRows(grouped, plan.Having, combinedCols, subqueryResults)
		}

		if plan.OrderBy != nil {
			sortRawRows(grouped, plan.OrderBy)
		}

		if plan.Offset >= 0 && plan.Offset < len(grouped) {
			grouped = grouped[plan.Offset:]
		} else if plan.Offset >= len(grouped) {
			grouped = nil
		}

		if plan.Limit >= 0 && plan.Limit < len(grouped) {
			grouped = grouped[:plan.Limit]
		}

		rows := make([][]catalogapi.Value, len(grouped))
		for i, row := range grouped {
			rows[i] = row.Values
		}

		projCols := make([]string, len(plan.SelectColumns))
		for i, sc := range plan.SelectColumns {
			if ref, ok := sc.Expr.(*parserapi.ColumnRef); ok {
				if ref.Table != "" {
					projCols[i] = ref.Table + "." + ref.Column
				} else {
					projCols[i] = ref.Column
				}
			} else {
				projCols[i] = sc.Alias
			}
		}

		return &executorapi.Result{Columns: projCols, Rows: rows}, nil
	}

	if plan.OrderBy != nil {
		sortJoinRows(result, plan.OrderBy, combinedCols)
	}

	if plan.Offset >= 0 && plan.Offset < len(result) {
		result = result[plan.Offset:]
	} else if plan.Offset >= len(result) {
		result = nil
	}

	if plan.Limit >= 0 && plan.Limit < len(result) {
		result = result[:plan.Limit]
	}

	projected, projCols := projectJoinRows(result, colNames, plan)

	return &executorapi.Result{
		Columns: projCols,
		Rows:    projected,
	}, nil
}

// execHashJoin performs the hash join algorithm.
// Build hash table on smaller table, probe with larger table.
// For outer joins, build on the "preserved" side to track unmatched rows correctly.
func (e *executor) execHashJoin(leftRows, rightRows []*engineapi.Row, hplan *plannerapi.HashJoinPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, leftLen, rightLen int, combinedCols []catalogapi.ColumnDef) [][]catalogapi.Value {

	// Determine build/probe tables based on join type and size
	var buildRows, probeRows []*engineapi.Row
	var buildKeyIdx, probeKeyIdx int
	var buildIsLeft bool // true if build side is the left table

	joinType := hplan.Type

	switch joinType {
	case "LEFT":
		// For LEFT JOIN: build on RIGHT so we can track unmatched LEFT rows
		buildRows, probeRows = rightRows, leftRows
		buildKeyIdx, probeKeyIdx = hplan.RightKeyIdx, hplan.LeftKeyIdx
		buildIsLeft = false
	case "RIGHT":
		// For RIGHT JOIN: build on LEFT so we can track unmatched RIGHT rows
		buildRows, probeRows = leftRows, rightRows
		buildKeyIdx, probeKeyIdx = hplan.LeftKeyIdx, hplan.RightKeyIdx
		buildIsLeft = true
	default:
		// INNER JOIN: build on smaller table for memory efficiency
		if len(leftRows) <= len(rightRows) {
			buildRows, probeRows = leftRows, rightRows
			buildKeyIdx, probeKeyIdx = hplan.LeftKeyIdx, hplan.RightKeyIdx
			buildIsLeft = true
		} else {
			buildRows, probeRows = rightRows, leftRows
			buildKeyIdx, probeKeyIdx = hplan.RightKeyIdx, hplan.LeftKeyIdx
			buildIsLeft = false
		}
	}

	// Build phase: create hash table
	hashTable := make(map[string][]*engineapi.Row)
	for _, row := range buildRows {
		key := hashKey(row.Values[buildKeyIdx])
		hashTable[key] = append(hashTable[key], row)
	}

	var merged [][]catalogapi.Value

	// Track unmatched rows for outer joins
	var matchedBuild map[*engineapi.Row]bool
	if joinType == "LEFT" || joinType == "RIGHT" {
		matchedBuild = make(map[*engineapi.Row]bool, len(buildRows))
	}

	// Pre-allocate combinedVals once per probe row, reused for all build matches
	combinedVals := make([]catalogapi.Value, leftLen+rightLen)

	// Probe phase
	for _, probe := range probeRows {
		key := hashKey(probe.Values[probeKeyIdx])
		buildMatches := hashTable[key]

		if len(buildMatches) == 0 {
			if joinType == "LEFT" {
				// LEFT JOIN: unmatched probe (left) → NULL right
				merged = append(merged, e.mergeLeftWithNull(probe, leftLen))
			} else if joinType == "RIGHT" {
				// RIGHT JOIN: unmatched probe (right) → NULL left
				merged = append(merged, e.mergeNullWithRight(probe, leftLen))
			}
			continue
		}

		// Fill probe values into combinedVals (right portion)
		if buildIsLeft {
			// build=left, probe=right → probe in right portion
			copy(combinedVals[leftLen:], probe.Values)
		} else {
			// build=right, probe=left → probe in left portion
			copy(combinedVals, probe.Values)
		}

		// For each build match, check if there's an ON condition match
		for _, build := range buildMatches {
			if hplan.On != nil {
				// Evaluate ON condition with combined row - reuse pre-allocated slice
				if buildIsLeft {
					// build=left, probe=right
					copy(combinedVals, build.Values)
				} else {
					// build=right, probe=left
					copy(combinedVals[leftLen:], build.Values)
				}
				combinedRow := &engineapi.Row{Values: combinedVals[:leftLen+rightLen]}
				result, err := evalExpr(hplan.On, combinedRow, combinedCols, subqueryResults, e)
				if err != nil || !isTruthy(result) {
					continue
				}
			}

			// Match found
			if matchedBuild != nil {
				matchedBuild[build] = true
			}

			var row []catalogapi.Value
			if buildIsLeft {
				row = e.mergeRows(build, probe, leftLen, rightLen)
			} else {
				row = e.mergeRows(probe, build, leftLen, rightLen)
			}
			merged = append(merged, row)
		}
	}

	// Emit unmatched build rows for outer joins
	// For LEFT JOIN: build=right, emit unmatched RIGHT with NULL left
	// For RIGHT JOIN: no unmatched build rows - unmatched RIGHT already handled in probe loop
	if joinType == "LEFT" {
		for _, build := range buildRows {
			if !matchedBuild[build] {
				merged = append(merged, e.mergeNullWithRight(build, leftLen))
			}
		}
	}
	// RIGHT JOIN: unmatched probe (RIGHT) rows are already handled in probe loop above

	return merged
}

// hashKey generates a string key for a value (for hash table lookup).
func hashKey(v catalogapi.Value) string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case catalogapi.TypeInt:
		return fmt.Sprintf("i:%d", v.Int)
	case catalogapi.TypeFloat:
		return fmt.Sprintf("f:%f", v.Float)
	case catalogapi.TypeText:
		return fmt.Sprintf("t:%s", v.Text)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// executeHashJoinPlan executes a HashJoinPlan and returns the merged rows.
func (e *executor) executeHashJoinPlan(hplan *plannerapi.HashJoinPlan, subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([][]catalogapi.Value, error) {
	// Execute left side
	var leftRows []*engineapi.Row
	switch left := hplan.Left.(type) {
	case plannerapi.ScanPlan:
		leftTbl, err := e.catalog.GetTable(hplan.LeftTable)
		if err != nil {
			return nil, fmt.Errorf("executeHashJoinPlan: get left table: %w", err)
		}
		rows, err := e.scanRows(leftTbl, left, nil, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("executeHashJoinPlan: scan left: %w", err)
		}
		leftRows = rows
	default:
		return nil, fmt.Errorf("executeHashJoinPlan: unsupported left type %T", hplan.Left)
	}

	// Execute right side
	rightTbl, err := e.catalog.GetTable(hplan.RightTable)
	if err != nil {
		return nil, fmt.Errorf("executeHashJoinPlan: get right table: %w", err)
	}
	rightRows, err := e.scanRows(rightTbl, hplan.Right, nil, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("executeHashJoinPlan: scan right: %w", err)
	}

	leftSchema := hplan.LeftSchema
	rightSchema := hplan.RightSchema
	leftLen := len(leftSchema)
	rightLen := len(rightSchema)

	// Build combined column definitions
	combinedCols := make([]catalogapi.ColumnDef, 0, leftLen+rightLen)
	for _, c := range leftSchema {
		combinedCols = append(combinedCols, *c)
	}
	for _, c := range rightSchema {
		combinedCols = append(combinedCols, *c)
	}

	// Execute hash join
	if subqueryResults == nil {
		subqueryResults = make(map[*parserapi.SubqueryExpr]interface{})
	}
	if hplan.On != nil {
		e.walkExprForJoinSubqueries(hplan.On, subqueryResults)
	}

	return e.execHashJoin(leftRows, rightRows, hplan, subqueryResults, leftLen, rightLen, combinedCols), nil
}

// execJoin handles a bare JoinPlan (used for EXPLAIN or subquery JOINs).
func (e *executor) execJoin(jplan *plannerapi.JoinPlan) (*executorapi.Result, error) {
	// Collect left rows — may be ScanPlan or nested *JoinPlan
	var leftRows []*engineapi.Row
	switch left := jplan.Left.(type) {
	case *plannerapi.JoinPlan:
		result, err := e.execJoin(left)
		if err != nil {
			return nil, err
		}
		for _, v := range result.Rows {
			leftRows = append(leftRows, &engineapi.Row{Values: v})
		}
	case plannerapi.ScanPlan:
		var err error
		leftRows, err = e.scanRows(jplan.LeftTable, left, nil, 0, 0)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("execJoin: unexpected left type %T", jplan.Left)
	}

	rightRows, err := e.scanRows(jplan.RightTable, jplan.Right, nil, 0, 0)
	if err != nil {
		return nil, err
	}

	// Build colNames and combinedCols with table names tagged
	colNames := make([]string, 0, len(jplan.LeftSchema)+len(jplan.RightSchema))
	combinedCols := make([]catalogapi.ColumnDef, 0, len(jplan.LeftSchema)+len(jplan.RightSchema))
	for _, c := range jplan.LeftSchema {
		colNames = append(colNames, c.Name)
		col := *c
		if col.Table == "" { col.Table = jplan.LeftTable.Name }
		combinedCols = append(combinedCols, col)
	}
	for _, c := range jplan.RightSchema {
		colNames = append(colNames, c.Name)
		col := *c
		col.Table = jplan.RightTable.Name
		combinedCols = append(combinedCols, col)
	}

	subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
	if jplan.On != nil {
		e.walkExprForJoinSubqueries(jplan.On, subqueryResults)
	}

	var mergedRows [][]catalogapi.Value
	leftLen := len(jplan.LeftSchema)
	rightLen := len(jplan.RightSchema)

	switch jplan.Type {
	case "LEFT":
		mergedRows = e.execLeftJoin(leftRows, rightRows, jplan, subqueryResults, leftLen, rightLen, combinedCols)
	case "RIGHT":
		mergedRows = e.execRightJoin(leftRows, rightRows, jplan, subqueryResults, leftLen, rightLen, combinedCols)
	default:
		mergedRows = e.execInnerJoin(leftRows, rightRows, jplan, subqueryResults, leftLen, rightLen, combinedCols)
	}

	return &executorapi.Result{
		Columns: colNames,
		Rows:    mergedRows,
	}, nil
}

// execInnerJoin emits only rows where ON=TRUE.
func (e *executor) execInnerJoin(leftRows, rightRows []*engineapi.Row, jplan *plannerapi.JoinPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, leftLen, rightLen int, combinedCols []catalogapi.ColumnDef) [][]catalogapi.Value {
	var merged [][]catalogapi.Value
	for _, left := range leftRows {
		for _, right := range rightRows {
			if e.joinMatch(left, right, jplan, subqueryResults, leftLen, combinedCols) {
				merged = append(merged, e.mergeRows(left, right, leftLen, rightLen))
			}
		}
	}
	return merged
}

// execLeftJoin emits all left rows; unmatched left rows get NULL for right columns.
func (e *executor) execLeftJoin(leftRows, rightRows []*engineapi.Row, jplan *plannerapi.JoinPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, leftLen, rightLen int, combinedCols []catalogapi.ColumnDef) [][]catalogapi.Value {
	var merged [][]catalogapi.Value
	for _, left := range leftRows {
		matched := false
		for _, right := range rightRows {
			if e.joinMatch(left, right, jplan, subqueryResults, leftLen, combinedCols) {
				matched = true
				merged = append(merged, e.mergeRows(left, right, leftLen, rightLen))
			}
		}
		if !matched {
			merged = append(merged, e.mergeLeftWithNull(left, rightLen))
		}
	}
	return merged
}

// execRightJoin emits all right rows; unmatched right rows get NULL for left columns.
func (e *executor) execRightJoin(leftRows, rightRows []*engineapi.Row, jplan *plannerapi.JoinPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, leftLen, rightLen int, combinedCols []catalogapi.ColumnDef) [][]catalogapi.Value {
	var merged [][]catalogapi.Value
	matchedRight := make([]bool, len(rightRows))

	for _, left := range leftRows {
		for j, right := range rightRows {
			if e.joinMatch(left, right, jplan, subqueryResults, leftLen, combinedCols) {
				matchedRight[j] = true
				merged = append(merged, e.mergeRows(left, right, leftLen, rightLen))
			}
		}
	}

	// Emit unmatched right rows with NULL left columns.
	for j, right := range rightRows {
		if !matchedRight[j] {
			merged = append(merged, e.mergeNullWithRight(right, leftLen))
		}
	}
	return merged
}

// mergeRows concatenates left and right values.
func (e *executor) mergeRows(left, right *engineapi.Row, leftLen, rightLen int) []catalogapi.Value {
	// Use actual left length for nested JOINs where left row has columns from
	// multiple tables (e.g. A JOIN B JOIN C: left has A+B columns, not just A).
	actualLeftLen := len(left.Values)
	result := make([]catalogapi.Value, actualLeftLen+rightLen)
	copy(result, left.Values)
	copy(result[actualLeftLen:], right.Values)
	return result
}

// mergeLeftWithNull produces left values + NULLs for right.
func (e *executor) mergeLeftWithNull(left *engineapi.Row, rightLen int) []catalogapi.Value {
	nullRight := make([]catalogapi.Value, rightLen)
	for i := range nullRight {
		nullRight[i] = catalogapi.Value{Type: catalogapi.TypeNull, IsNull: true}
	}
	result := make([]catalogapi.Value, len(left.Values)+rightLen)
	copy(result, left.Values)
	copy(result[len(left.Values):], nullRight)
	return result
}

// mergeNullWithRight produces NULLs for left + right values.
func (e *executor) mergeNullWithRight(right *engineapi.Row, leftLen int) []catalogapi.Value {
	nullLeft := make([]catalogapi.Value, leftLen)
	for i := range nullLeft {
		nullLeft[i] = catalogapi.Value{Type: catalogapi.TypeNull, IsNull: true}
	}
	result := make([]catalogapi.Value, leftLen+len(right.Values))
	copy(result, nullLeft)
	copy(result[leftLen:], right.Values)
	return result
}

// joinMatch evaluates the ON condition for a left/right row pair.
func (e *executor) joinMatch(left, right *engineapi.Row, jplan *plannerapi.JoinPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, leftLen int, combinedCols []catalogapi.ColumnDef) bool {
	if jplan.On == nil {
		return true
	}
	combinedVals := make([]catalogapi.Value, leftLen+len(right.Values))
	copy(combinedVals, left.Values)
	copy(combinedVals[leftLen:], right.Values)
	combinedRow := &engineapi.Row{Values: combinedVals}
	result, err := evalExpr(jplan.On, combinedRow, combinedCols, subqueryResults, e)
	if err != nil {
		return false
	}
	return isTruthy(result)
}

// collectRows executes a scan plan and returns all rows.
// LIMIT/OFFSET are not pushed down - this collects all matching rows.
func (e *executor) collectRows(table *catalogapi.TableSchema, scan plannerapi.ScanPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	return e.scanRows(table, scan, subqueryResults, 0, 0)
}

// walkExprForJoinSubqueries finds SubqueryExpr nodes and pre-computes them.
func (ex *executor) walkExprForJoinSubqueries(expr parserapi.Expr,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) {
	if expr == nil {
		return
	}
	switch node := expr.(type) {
	case *parserapi.SubqueryExpr:
		if _, ok := subqueryResults[node]; ok {
			return
		}
		subplan, err := ex.planner.Plan(node.Stmt)
		if err != nil {
			return
		}
		subResult, err := ex.Execute(subplan)
		if err != nil {
			return
		}
		var values []catalogapi.Value
		for _, row := range subResult.Rows {
			if len(row) > 0 {
				values = append(values, row[0])
			}
		}
		subqueryResults[node] = values
	case *parserapi.BinaryExpr:
		ex.walkExprForJoinSubqueries(node.Left, subqueryResults)
		ex.walkExprForJoinSubqueries(node.Right, subqueryResults)
	case *parserapi.UnaryExpr:
		ex.walkExprForJoinSubqueries(node.Operand, subqueryResults)
	case *parserapi.InExpr:
		ex.walkExprForJoinSubqueries(node.Expr, subqueryResults)
		for _, v := range node.Values {
			ex.walkExprForJoinSubqueries(v, subqueryResults)
		}
	case *parserapi.LikeExpr:
		ex.walkExprForJoinSubqueries(node.Expr, subqueryResults)
		// Pattern is a string literal, not an Expr
	case *parserapi.BetweenExpr:
		ex.walkExprForJoinSubqueries(node.Expr, subqueryResults)
		ex.walkExprForJoinSubqueries(node.Low, subqueryResults)
		ex.walkExprForJoinSubqueries(node.High, subqueryResults)
	case *parserapi.IsNullExpr:
		ex.walkExprForJoinSubqueries(node.Expr, subqueryResults)
	case *parserapi.ExistsExpr:
		// EXISTS subqueries are not precomputed — they need outer row context.
		// Just recurse into the inner SubqueryExpr to plan it if needed.
		if node.Subquery != nil && node.Subquery.Plan == nil {
			subplan, err := ex.planner.Plan(node.Subquery.Stmt)
			if err == nil {
				node.Subquery.Plan = subplan
			}
		}
	}
}

// filterJoinRows applies a WHERE filter to merged join rows.
func (e *executor) filterJoinRows(rows [][]catalogapi.Value, filter parserapi.Expr, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) [][]catalogapi.Value {
	if filter == nil || len(rows) == 0 {
		return rows
	}
	// Only set outerVals per row when the filter contains correlated subqueries.
	setOuterVals := hasCorrelatedSubquery(filter)
	filtered := rows[:0]
	for _, row := range rows {
		if setOuterVals {
			e.outerVals = row
		}
		engineRow := &engineapi.Row{Values: row}
		match, err := matchFilter(filter, engineRow, columns, subqueryResults, e)
		if err != nil {
			continue
		}
		if match {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// sortJoinRows sorts merged rows by ORDER BY columns.
// Uses lexicographic comparison: sort by first column, then second, etc.
func sortJoinRows(rows [][]catalogapi.Value, orderBy []*plannerapi.OrderByPlan, columns []catalogapi.ColumnDef) {
	if len(orderBy) == 0 || len(rows) == 0 {
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		for _, ob := range orderBy {
			colIdx := ob.ColumnIndex
			a := rows[i][colIdx]
			b := rows[j][colIdx]

			// NULL handling: NULLs sort first for ASC, last for DESC
			if a.IsNull && b.IsNull {
				continue // equal, check next column
			}
			if a.IsNull {
				return !ob.Desc
			}
			if b.IsNull {
				return ob.Desc
			}

			cmp := compareValues(a, b)
			if cmp < 0 {
				return !ob.Desc
			}
			if cmp > 0 {
				return ob.Desc
			}
			// equal, continue to next ORDER BY column
		}
		return false // all columns equal
	})
}

// compareValues compares two catalogapi.Value.
func compareValues(a, b catalogapi.Value) int {
	if a.Type != b.Type {
		return int(a.Type) - int(b.Type)
	}
	switch a.Type {
	case catalogapi.TypeInt:
		if a.Int < b.Int {
			return -1
		}
		if a.Int > b.Int {
			return 1
		}
		return 0
	case catalogapi.TypeFloat:
		if a.Float < b.Float {
			return -1
		}
		if a.Float > b.Float {
			return 1
		}
		return 0
	case catalogapi.TypeText:
		if a.Text < b.Text {
			return -1
		}
		if a.Text > b.Text {
			return 1
		}
		return 0
	}
	return 0
}

// projectJoinRows projects columns from merged rows.
// For JOIN queries (plan.Join != nil), plan.Columns contains per-table indices.
// Right-table columns need offset by plan.LeftColumnCount to find the
// correct position in the globally-merged row.
func projectJoinRows(rows [][]catalogapi.Value, colNames []string, plan *plannerapi.SelectPlan) ([][]catalogapi.Value, []string) {
	if len(plan.Columns) == 0 {
		return rows, colNames
	}
	projected := make([][]catalogapi.Value, len(rows))
	// For JOINs, plan.Columns contains indices into the merged row.
	// The merged row format is: [left cols...][right cols...]
	// So indices already account for the split — use them directly.
	for i, row := range rows {
		vals := make([]catalogapi.Value, len(plan.Columns))
		for j, idx := range plan.Columns {
			if idx < len(row) {
				vals[j] = row[idx]
			}
		}
		projected[i] = vals
	}
	projNames := make([]string, len(plan.Columns))
	for j, idx := range plan.Columns {
		if idx < len(colNames) {
			projNames[j] = colNames[idx]
		}
	}
	return projected, projNames
}

// scanHasFilter returns true if the scan plan includes any filter that is applied
// during or after scanning. When a filter exists, LIMIT cannot be safely pushed
// down to storage because the filter may discard rows after the limit is applied.
func scanHasFilter(scan plannerapi.ScanPlan) bool {
	switch s := scan.(type) {
	case *plannerapi.TableScanPlan:
		return s.Filter != nil
	case *plannerapi.IndexScanPlan:
		return s.ResidualFilter != nil
	case *plannerapi.IndexOnlyScanPlan:
		return s.ResidualFilter != nil
	case *plannerapi.IndexRangePlan:
		return s.ResidualFilter != nil
	case *plannerapi.FTSSearchPlan:
		return s.ResidualFilter != nil
	default:
		return false
	}
}

// hasCorrelatedSubquery checks whether an expression tree contains any correlated subquery.
func hasCorrelatedSubquery(expr parserapi.Expr) bool {
	if expr == nil {
		return false
	}
	found := false
	walkExpr(expr, func(e parserapi.Expr) {
		if found {
			return
		}
		if sq, ok := e.(*parserapi.SubqueryExpr); ok {
			if isCorrelatedSubquery(sq) {
				found = true
			}
		}
	})
	return found
}


// isCorrelatedSubquery checks whether a SubqueryExpr references columns from
// tables outside its own FROM clause (i.e., it is correlated with the outer query).
// A correlated subquery must NOT be precomputed — it must be re-evaluated per outer row.
func isCorrelatedSubquery(sq *parserapi.SubqueryExpr) bool {
	sel, ok := sq.Stmt.(*parserapi.SelectStmt)
	if !ok {
		return false
	}
	subTable := strings.ToUpper(sel.Table)
	correlated := false
	if sel.Where != nil {
		walkExpr(sel.Where, func(expr parserapi.Expr) {
			if correlated {
				return
			}
			ref, ok := expr.(*parserapi.ColumnRef)
			if !ok || ref.Table == "" {
				return
			}
			// If the column ref's table qualifier doesn't match the subquery's own FROM table,
			// it must be referencing an outer table → correlated.
			if !strings.EqualFold(ref.Table, subTable) {
				correlated = true
			}
		})
	}
	return correlated
}

// precomputeSubqueries finds all SubqueryExpr nodes in the plan's WHERE/HAVING
// and executes them, caching results in subqueryResults.
// Scalar subqueries (used in comparisons) store a single catalogapi.Value.
// List subqueries (used in IN) store a []catalogapi.Value.
// Correlated subqueries (referencing outer table columns) are SKIPPED here
// and evaluated on-demand per outer row via execSubquery during filterRows.
// This is called with the OUTER subqueryResults map so that nested subqueries
// don't re-execute infinitely (they share the parent's cache).
func (e *executor) precomputeSubqueries(plan *plannerapi.SelectPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) error {
	var multiErr error
	// Collect all expressions that might contain SubqueryExpr
	var exprs []parserapi.Expr
	if plan.Filter != nil {
		exprs = append(exprs, plan.Filter)
	}
	if plan.Having != nil {
		exprs = append(exprs, plan.Having)
	}

	for _, root := range exprs {
		// Check for ExistsExpr at the root level — skip precomputation since
		// EXISTS needs outer row context for correlated subqueries.
		if exists, ok := root.(*parserapi.ExistsExpr); ok {
			if exists.Subquery != nil && exists.Subquery.Plan == nil {
				subplan, err := e.planner.Plan(exists.Subquery.Stmt)
				if err == nil {
					exists.Subquery.Plan = subplan
				}
			}
			continue
		}
		walkExpr(root, func(expr parserapi.Expr) {
			sq, ok := expr.(*parserapi.SubqueryExpr)
			if !ok {
				return
			}
			// Already computed by an ancestor execSelect call?
			if _, exists := subqueryResults[sq]; exists {
				return
			}
			// Skip correlated subqueries for pre-computation — they must be evaluated
			// per outer row. But ensure they have a plan for execSubquery.
			if isCorrelatedSubquery(sq) {
				if sq.Plan == nil {
					subplan, err := e.planner.Plan(sq.Stmt)
					if err != nil {
						return
					}
					sq.Plan = subplan
				}
				return
			}
			// Plan and execute the subquery (may trigger nested execSelect with same map)
			// Planner already set sq.Plan — use it. Otherwise fallback to executor planning.
			var subplan plannerapi.Plan
			if sq.Plan != nil {
				subplan = sq.Plan.(plannerapi.Plan)
			} else {
				var err error
				subplan, err = e.planner.Plan(sq.Stmt)
				if err != nil {
					return
			}
			}
			result, err := e.Execute(subplan)
			if err != nil {
				return
			}
			// Determine scalar vs list context
			inList := isSubqueryInListContext(sq, root)
			if inList {
				// List context: collect all first-column values
				var vals []catalogapi.Value
				for _, row := range result.Rows {
					if len(row) > 0 {
						vals = append(vals, row[0])
					}
				}
				subqueryResults[sq] = vals
			} else {
				// Scalar context: expect 0 or 1 row/column
				if len(result.Rows) == 0 {
					subqueryResults[sq] = catalogapi.Value{IsNull: true}
				} else if len(result.Rows) >= 1 && len(result.Rows[0]) > 0 {
					if len(result.Rows) > 1 {
						multiErr = fmt.Errorf("scalar subquery returned more than 1 row")
						return
					}
					subqueryResults[sq] = result.Rows[0][0]
				} else {
					subqueryResults[sq] = catalogapi.Value{IsNull: true}
				}
			}
		})
		if multiErr != nil {
			return multiErr
		}
	}
	return nil
}

// columnNameFromExpr generates a meaningful column name from an expression.
// Returns the column name if it's a ColumnRef, the function name for aggregates,
// or "" for other expressions (caller should use "expr" or other default).
func columnNameFromExpr(expr parserapi.Expr) string {
	switch e := expr.(type) {
	case *parserapi.ColumnRef:
		return e.Column
	case *parserapi.AggregateCallExpr:
		if e.Arg != nil {
			if colRef, ok := e.Arg.(*parserapi.ColumnRef); ok {
				return e.Func + "_" + colRef.Column
			}
		}
		return e.Func + "()"
	case *parserapi.CoalesceExpr:
		return "COALESCE"
	case *parserapi.Literal:
		return "literal"
	}
	return ""
}

// execSelectFromCTE handles SELECT ... FROM cte_name [WHERE ...] for CTE references.
// Uses pre-materialized CTE results from execWith.
func (e *executor) execSelectFromCTE(plan *plannerapi.SelectPlan, cteResult *executorapi.Result) (*executorapi.Result, error) {
	// Convert pre-materialized CTE rows to engineapi.Row
	rows := make([]*engineapi.Row, len(cteResult.Rows))
	for i, rowVals := range cteResult.Rows {
		rows[i] = &engineapi.Row{
			RowID:  0,
			Values: rowVals,
		}
	}

	// Build column definitions from CTE result columns
	tableCols := make([]catalogapi.ColumnDef, len(cteResult.Columns))
	for i, name := range cteResult.Columns {
		tableCols[i] = catalogapi.ColumnDef{Name: name}
	}

	// Set outerCols for correlated subquery resolution
	savedOuterCols := e.outerCols
	savedOuterVals := e.outerVals
	e.outerCols = tableCols
	defer func() {
		e.outerCols = savedOuterCols
		e.outerVals = savedOuterVals
	}()

	// Apply WHERE filter
	filter := plan.Scan.(*plannerapi.DerivedTableScanPlan).Filter
	if filter != nil {
		subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
		if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
			return nil, err
		}
		rows = e.filterRows(rows, filter, tableCols, subqueryResults)
	}

	// Apply GROUP BY
	if plan.GroupByExprs != nil {
		grouped, err := e.groupByRows(rows, plan)
		if err != nil {
			return nil, err
		}
		rows = grouped

		// Apply HAVING
		if plan.Having != nil {
			subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
			if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
				return nil, err
			}
			rows = e.filterRows(rows, plan.Having, plan.Table.Columns, subqueryResults)
		}
	}

	// Project columns
	// If SelectColumns has expressions (like n+1), evaluate them
	projected := projectRows(rows, plan.Columns)
	colNames := buildColumnNames(plan.Table, plan.Columns)
	if plan.SelectColumns != nil && hasExpressions(plan.SelectColumns) {
		projected = make([][]catalogapi.Value, len(rows))
		colNames = make([]string, len(plan.SelectColumns))
		for i, sc := range plan.SelectColumns {
			if sc.Alias != "" {
				colNames[i] = sc.Alias
			} else if name := columnNameFromExpr(sc.Expr); name != "" {
				colNames[i] = name
			} else {
				colNames[i] = "expr"
			}
		}
		for rowIdx, row := range rows {
			projected[rowIdx] = make([]catalogapi.Value, len(plan.SelectColumns))
			for i, sc := range plan.SelectColumns {
				val, err := evalExpr(sc.Expr, row, tableCols, nil, e)
				if err != nil {
					return nil, err
				}
				projected[rowIdx][i] = val
			}
		}
	}

	// Apply DISTINCT
	if plan.Distinct {
		seen := make(map[uint64]bool)
		var deduped [][]catalogapi.Value
		for _, row := range projected {
			key := makeRowKey(row)
			if !seen[key] {
				seen[key] = true
				deduped = append(deduped, row)
			}
		}
		projected = deduped
	}

	return &executorapi.Result{
		Columns: colNames,
		Rows:    projected,
	}, nil
}

// execSelectFromDerived handles SELECT ... FROM (SELECT ...) AS alias [WHERE ...].
// It materializes the subquery result, then treats it as a regular table scan.
func (e *executor) execSelectFromDerived(plan *plannerapi.SelectPlan, dtScan *plannerapi.DerivedTableScanPlan) (*executorapi.Result, error) {
	// Step 1: Execute the subquery to get materialized rows.
	if plan.DerivedTableSubplan == nil {
		return nil, fmt.Errorf("%w: derived table subplan is missing", executorapi.ErrExecFailed)
	}
	subResult, err := e.Execute(plan.DerivedTableSubplan)
	if err != nil {
		return nil, fmt.Errorf("%w: execute derived table subquery: %v", executorapi.ErrExecFailed, err)
	}

	// Step 2: Convert subquery result rows to engineapi.Row (with RowID=0, no storage).
	rows := make([]*engineapi.Row, len(subResult.Rows))
	for i, rowVals := range subResult.Rows {
		rows[i] = &engineapi.Row{
			RowID:  0,
			Values: rowVals,
		}
	}

	// Step 3: Build column definitions with Table field set to alias.
	// The derived table schema already has the alias as Table name.
	tableCols := make([]catalogapi.ColumnDef, len(dtScan.Schema.Columns))
	for i, col := range dtScan.Schema.Columns {
		tableCols[i] = col
		if tableCols[i].Table == "" {
			tableCols[i].Table = dtScan.Schema.Name
		}
	}

	// Set outerCols for correlated subquery resolution.
	e.outerCols = tableCols

	// Step 4: Apply WHERE filter on materialized rows.
	if dtScan.Filter != nil {
		// Precompute subqueries in the WHERE filter.
		subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
		if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
			return nil, err
		}
		rows = e.filterRows(rows, dtScan.Filter, tableCols, subqueryResults)
	}

	// Step 5: GROUP BY
	if plan.GroupByExprs != nil {
		// Convert [][]catalogapi.Value back to []*engineapi.Row for groupByRows.
		// But groupByRows expects the rows to have len(plan.Table.Columns) values,
		// which matches the materialized subquery output.
		engineRows := make([]*engineapi.Row, len(rows))
		copy(engineRows, rows)

		grouped, err := e.groupByRows(engineRows, plan)
		if err != nil {
			return nil, err
		}
		rows2 := make([]*engineapi.Row, len(grouped))
		copy(rows2, grouped)

		// HAVING
		if plan.Having != nil {
			subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
			if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
				return nil, err
			}
			rows2 = e.filterRows(rows2, plan.Having, plan.Table.Columns, subqueryResults)
		}

		// ORDER BY
		if plan.OrderBy != nil {
			sortRawRows(rows2, plan.OrderBy)
		}

		// OFFSET
		if plan.Offset >= 0 && plan.Offset < len(rows2) {
			rows2 = rows2[plan.Offset:]
		} else if plan.Offset >= len(rows2) {
			rows2 = nil
		}

		// LIMIT
		if plan.Limit >= 0 && plan.Limit < len(rows2) {
			rows2 = rows2[:plan.Limit]
		}

		// Extract projected rows
		projected := make([][]catalogapi.Value, len(rows2))
		for i, row := range rows2 {
			projected[i] = row.Values
		}

		colNames := make([]string, len(plan.SelectColumns))
		for i, sc := range plan.SelectColumns {
			if name := columnNameFromExpr(sc.Expr); name != "" {
				colNames[i] = name
			} else {
				colNames[i] = "expr"
			}
		}

		return &executorapi.Result{Columns: colNames, Rows: projected}, nil
	}

	// Scalar aggregate in SELECT (no GROUP BY): compute across all rows.
	if plan.SelectColumns != nil && plan.GroupByExprs == nil {
		hasScalarAgg := false
		var nonAggCol string
		for _, sc := range plan.SelectColumns {
			if _, ok := sc.Expr.(*parserapi.AggregateCallExpr); ok {
				hasScalarAgg = true
			} else if _, ok := sc.Expr.(*parserapi.ColumnRef); ok {
				// Non-aggregate column ref — will be flagged below if no GROUP BY.
				// Literals (constants) are allowed alongside aggregates.
				if nonAggCol == "" {
					nonAggCol = columnNameFromExpr(sc.Expr)
				}
			}
		}
		if hasScalarAgg {
			// SQL standard: aggregate without GROUP BY requires ALL non-aggregate
			// columns to be functionally dependent — we don't track that, so reject.
			if nonAggCol != "" {
				return nil, fmt.Errorf("sql: aggregate function requires GROUP BY or must be the only column in SELECT")
			}
			vals := make([]catalogapi.Value, len(plan.SelectColumns))
			names := make([]string, len(plan.SelectColumns))
			for i, sc := range plan.SelectColumns {
				names[i] = sc.Alias
				if agg, ok := sc.Expr.(*parserapi.AggregateCallExpr); ok {
					val, err := computeAggregate(agg, rows, tableCols)
					if err != nil {
						return nil, err
					}
					vals[i] = val
				}
			}
			return &executorapi.Result{Columns: names, Rows: [][]catalogapi.Value{vals}}, nil
		}
	}

	// Step 6: ORDER BY
	if plan.OrderBy != nil {
		sortRawRows(rows, plan.OrderBy)
	}

	// Step 7: OFFSET
	if plan.Offset >= 0 && plan.Offset < len(rows) {
		rows = rows[plan.Offset:]
	} else if plan.Offset >= len(rows) {
		rows = nil
	}

	// Step 8: LIMIT
	if plan.Limit >= 0 && plan.Limit < len(rows) {
		rows = rows[:plan.Limit]
	}

	// Step 9: Project columns.
	projected := projectRows(rows, plan.Columns)
	colNames := buildColumnNames(plan.Table, plan.Columns)

	// Step 10: DISTINCT
	if plan.Distinct {
		seen := make(map[uint64]bool)
		var deduped [][]catalogapi.Value
		for _, row := range projected {
			key := makeRowKey(row)
			if !seen[key] {
				seen[key] = true
				deduped = append(deduped, row)
			}
		}
		projected = deduped
	}

	return &executorapi.Result{
		Columns: colNames,
		Rows:    projected,
	}, nil
}

func (e *executor) execSelect(plan *plannerapi.SelectPlan) (*executorapi.Result, error) {
	// Save outer context — a correlated subquery's inner execSelect must not
	// clobber the outer query's outerCols/outerVals that evalColumnRef needs
	// to resolve outer table references (e.g., users.id inside an orders subquery).
	savedOuterCols := e.outerCols
	savedOuterVals := e.outerVals
	defer func() {
		e.outerCols = savedOuterCols
		e.outerVals = savedOuterVals
	}()

	// Handle SELECT without FROM (constant expressions: SELECT 1, SELECT 'hello')
	if plan.Table == nil {
		// Evaluate SelectColumns expressions against an empty row
		emptyRow := &engineapi.Row{Values: []catalogapi.Value{}}
		emptyCols := []catalogapi.ColumnDef{}
		vals := make([]catalogapi.Value, len(plan.SelectColumns))
		names := make([]string, len(plan.SelectColumns))
		for i, sc := range plan.SelectColumns {
			name := sc.Alias
			if name == "" {
				name = "expr"
			}
			names[i] = name
			val, err := evalExpr(sc.Expr, emptyRow, emptyCols, nil, nil)
			if err != nil {
				return nil, err
			}
			vals[i] = val
		}
		return &executorapi.Result{Columns: names, Rows: [][]catalogapi.Value{vals}}, nil
	}

	// Handle CTE scan: use pre-materialized results from execWith
	if plan.Table != nil && plan.Scan != nil {
		if _, ok := plan.Scan.(*plannerapi.DerivedTableScanPlan); ok {
			// Check if this is a CTE (has pre-materialized results)
			if cteResult, ok := e.cteResults[plan.Table.Name]; ok {
				return e.execSelectFromCTE(plan, cteResult)
			}
		}
	}

	// Handle derived table (subquery in FROM clause): materialize the subquery first.
	// plan.Table is the derived table's schema (alias + columns from SELECT list).
	// plan.Scan is *DerivedTableScanPlan.
	// plan.DerivedTableSubplan contains the subquery's execution plan.
	if dtScan, ok := plan.Scan.(*plannerapi.DerivedTableScanPlan); ok {
		return e.execSelectFromDerived(plan, dtScan)
	}

	// Pre-compute subquery results BEFORE scanning — needed for filter during scan.
	subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
	if err := e.precomputeSubqueries(plan, subqueryResults); err != nil {
		return nil, err
	}

	// If the scan filter contains a correlated subquery, we must NOT evaluate it
	// during the scan (outer context isn't available yet). Strip the scan filter
	// and rely on plan.Filter in filterRows instead.
	scanPlan := plan.Scan
	if hasCorrelatedSubquery(plan.Filter) {
		switch s := scanPlan.(type) {
		case *plannerapi.TableScanPlan:
			stripped := *s
			stripped.Filter = nil
			scanPlan = &stripped
		}
	}

	// Collect matching rows via scan (filter during scan uses precomputed subquery results)
	// LIMIT/OFFSET pushdown: only safe when there's no ORDER BY, no GROUP BY,
	// no post-scan filter (plan.Filter), and no scan-level filter. A filter applied
	// after storage-level LIMIT would discard rows, returning fewer than requested.
	hasFilter := plan.Filter != nil || scanHasFilter(scanPlan)
	pushedDown := plan.OrderBy == nil && plan.GroupByExprs == nil && !hasFilter
	// Limit/offset to push down to storage. When pushedDown=false, pass 0 (no pushdown)
	storageLimit, storageOffset := plan.Limit, plan.Offset
	if !pushedDown {
		storageLimit, storageOffset = 0, 0
	}
	rows, err := e.scanRows(plan.Table, scanPlan, subqueryResults, storageLimit, storageOffset)
	if err != nil {
		return nil, err
	}

	// Apply row locking for SELECT FOR UPDATE
	// LockMode is set when the query has FOR UPDATE clause
	if plan.LockMode != parserapi.NoUpdate {
		var lockedRows []*engineapi.Row
		for _, row := range rows {
			locked, err := e.acquireRowLock(plan.Table.TableID, row.RowID, plan.LockMode, plan.LockWait)
			if err != nil {
				return nil, err // NOWAIT: lock contention error
			}
			if locked {
				lockedRows = append(lockedRows, row)
			}
			// SKIP LOCKED: if locked=false, row was skipped silently
		}
		rows = lockedRows
	}

	// Build columns with Table field populated so that evalColumnRef can
	// distinguish table-qualified references (e.g., orders.user_id vs users.id).
	tableCols := make([]catalogapi.ColumnDef, len(plan.Table.Columns))
	for i, col := range plan.Table.Columns {
		tableCols[i] = col
		if tableCols[i].Table == "" {
			tableCols[i].Table = plan.Table.Name
		}
	}

	// Set outerCols for correlated subquery resolution - used by filterRows to set outerVals per row.
	// Only set for the outermost query — if outerCols is already set (by a parent execSelect),
	// preserve it so the inner query can resolve outer table references (e.g., users.id).
	if e.outerCols == nil {
		e.outerCols = tableCols
	}

	// Apply residual filter from SelectPlan (handles index scan + residual)
	if plan.Filter != nil {
		rows = e.filterRows(rows, plan.Filter, tableCols, subqueryResults)
	}

	// ─── Window Functions ─────────────────────────────────────────────────────
	// Compute window functions AFTER filter but BEFORE GROUP BY and ORDER BY.
	// Window functions require the complete result set for correct computation.
	if hasWindowFunctions(plan.SelectColumns) {
		windowFuncs := extractWindowFuncExprs(plan.SelectColumns)
		if err := e.computeWindowFunctions(rows, tableCols, windowFuncs); err != nil {
			return nil, err
		}
	}


	// GROUP BY: group rows by encoded key, then compute aggregates per group.
	if plan.GroupByExprs != nil {
		grouped, err := e.groupByRows(rows, plan)
		if err != nil {
			return nil, err
		}
		rows = grouped

		// HAVING: filter grouped rows
		if plan.Having != nil {
			rows = e.filterRows(rows, plan.Having, plan.Table.Columns, subqueryResults)
		}
	}

	// ORDER BY (sort raw rows BEFORE projection so all columns are available)
	if plan.OrderBy != nil {
		sortRawRows(rows, plan.OrderBy)
	}

	// OFFSET and LIMIT: apply in executor if NOT pushed down to storage layer.
	// When pushedDown=true, storage already applied them; executor should not apply again.
	// When pushedDown=false, executor must apply them.
	if !pushedDown {
		// OFFSET (skip first N rows first)
		if plan.Offset > 0 && plan.Offset < len(rows) {
			rows = rows[plan.Offset:]
		} else if plan.Offset >= len(rows) {
			rows = nil
		}
		// LIMIT (take N from remaining)
		if plan.Limit > 0 && plan.Limit < len(rows) {
			rows = rows[:plan.Limit]
		}
	}

	// Scalar aggregate in SELECT (no GROUP BY): compute across all rows.
	if plan.SelectColumns != nil && plan.GroupByExprs == nil {
		hasScalarAgg := false
		var nonAggCol string
		for _, sc := range plan.SelectColumns {
			if _, ok := sc.Expr.(*parserapi.AggregateCallExpr); ok {
				hasScalarAgg = true
			} else if _, ok := sc.Expr.(*parserapi.ColumnRef); ok {
				// Non-aggregate column ref — will be flagged below if no GROUP BY.
				// Literals (constants) are allowed alongside aggregates.
				if nonAggCol == "" {
					nonAggCol = columnNameFromExpr(sc.Expr)
				}
			}
		}
		if hasScalarAgg {
			// SQL standard: aggregate without GROUP BY requires ALL non-aggregate
			// columns to be functionally dependent — we don't track that, so reject.
			if nonAggCol != "" {
				return nil, fmt.Errorf("sql: aggregate function requires GROUP BY or must be the only column in SELECT")
			}
			// rows is []*engineapi.Row at this point
			vals := make([]catalogapi.Value, len(plan.SelectColumns))
			names := make([]string, len(plan.SelectColumns))
			for i, sc := range plan.SelectColumns {
				names[i] = sc.Alias
				if agg, ok := sc.Expr.(*parserapi.AggregateCallExpr); ok {
					val, err := computeAggregate(agg, rows, plan.Table.Columns)
					if err != nil {
						return nil, err
					}
					vals[i] = val
				}
			}
			return &executorapi.Result{Columns: names, Rows: [][]catalogapi.Value{vals}}, nil
		}
	}

	// Project columns
	var projected [][]catalogapi.Value
	var colNames []string
	if plan.GroupByExprs != nil {
		// After GROUP BY, rows are already projected via projectGroupedRow
		// — just extract the pre-projected values and names
		projected = make([][]catalogapi.Value, len(rows))
		for i, row := range rows {
			projected[i] = row.Values
		}
		if plan.SelectColumns != nil {
			colNames = make([]string, len(plan.SelectColumns))
			for i, sc := range plan.SelectColumns {
				if name := columnNameFromExpr(sc.Expr); name != "" {
					colNames[i] = name
				} else {
					colNames[i] = "expr"
				}
			}
		} else {
			colNames = []string{"*"}
		}
	} else {
		colNames = buildColumnNames(plan.Table, plan.Columns)
		projected = projectRows(rows, plan.Columns)

		// If SelectColumns has expressions (like CoalesceExpr), evaluate them
		if plan.SelectColumns != nil && hasExpressions(plan.SelectColumns) {
			projected = make([][]catalogapi.Value, len(rows))
			colNames = make([]string, len(plan.SelectColumns))
			for i, sc := range plan.SelectColumns {
				if sc.Alias != "" {
					colNames[i] = sc.Alias
				} else if name := columnNameFromExpr(sc.Expr); name != "" {
					colNames[i] = name
				} else {
					colNames[i] = "expr"
				}
			}
			for rowIdx, row := range rows {
				projected[rowIdx] = make([]catalogapi.Value, len(plan.SelectColumns))
				for i, sc := range plan.SelectColumns {
					val, err := evalExpr(sc.Expr, row, plan.Table.Columns, nil, e)
					if err != nil {
						return nil, err
					}
					projected[rowIdx][i] = val
				}
			}
		}
	}

	// DISTINCT: deduplicate projected rows
	if plan.Distinct {
		seen := make(map[uint64]bool)
		var deduped [][]catalogapi.Value
		for _, row := range projected {
			key := makeRowKey(row)
			if !seen[key] {
				seen[key] = true
				deduped = append(deduped, row)
			}
		}
		projected = deduped
	}

	return &executorapi.Result{
		Columns: colNames,
		Rows:    projected,
	}, nil
}

// makeRowKey computes a FNV-64a hash of a row for deduplication.
// Eliminates per-row string allocation from distinctRowKey.
// Semantics match distinctRowKey: NULL keyed uniformly, type tag prevents
// int 1 vs text "1" collision. Hash collisions are astronomically unlikely.
func makeRowKey(row []catalogapi.Value) uint64 {
	h := fnv.New64a()
	for i, v := range row {
		if i > 0 {
			h.Write([]byte{0}) // column separator
		}
		if v.IsNull {
			h.Write([]byte("N"))
		} else {
			h.Write([]byte{byte('0' + v.Type), ':'})
			switch v.Type {
			case catalogapi.TypeInt:
				fmt.Fprintf(h, "%d", v.Int)
			case catalogapi.TypeFloat:
				fmt.Fprintf(h, "%g", v.Float)
			case catalogapi.TypeText:
				h.Write([]byte(v.Text))
			case catalogapi.TypeBlob:
				fmt.Fprintf(h, "%x", v.Blob)
			default:
				fmt.Fprintf(h, "%v", v.Int)
			}
		}
	}
	return h.Sum64()
}

func (e *executor) execUnion(plan *plannerapi.UnionPlan) (*executorapi.Result, error) {
	leftResult, err := e.Execute(plan.Left)
	if err != nil {
		return nil, err
	}
	rightResult, err := e.Execute(plan.Right)
	if err != nil {
		return nil, err
	}

	rows := append(leftResult.Rows, rightResult.Rows...)

	if !plan.UnionAll {
		seen := make(map[uint64]bool)
		var deduped [][]catalogapi.Value
		for _, row := range rows {
			key := makeRowKey(row)
			if !seen[key] {
				seen[key] = true
				deduped = append(deduped, row)
			}
		}
		rows = deduped
	}

	return &executorapi.Result{
		Rows:    rows,
		Columns: leftResult.Columns,
	}, nil
}

func (e *executor) execIntersect(plan *plannerapi.IntersectPlan) (*executorapi.Result, error) {
	leftResult, err := e.Execute(plan.Left)
	if err != nil {
		return nil, err
	}
	rightResult, err := e.Execute(plan.Right)
	if err != nil {
		return nil, err
	}

	// Build a hash set of right rows for O(1) lookup
	rightSet := make(map[uint64]bool)
	for _, row := range rightResult.Rows {
		rightSet[makeRowKey(row)] = true
	}

	// Keep rows that appear in both left and right (with dedup)
	seen := make(map[uint64]bool)
	var result [][]catalogapi.Value
	for _, row := range leftResult.Rows {
		keyStr := makeRowKey(row)
		if rightSet[keyStr] && !seen[keyStr] {
			seen[keyStr] = true
			result = append(result, row)
		}
	}

	return &executorapi.Result{
		Rows:    result,
		Columns: leftResult.Columns,
	}, nil
}

func (e *executor) execExcept(plan *plannerapi.ExceptPlan) (*executorapi.Result, error) {
	leftResult, err := e.Execute(plan.Left)
	if err != nil {
		return nil, err
	}
	rightResult, err := e.Execute(plan.Right)
	if err != nil {
		return nil, err
	}

	// Build a hash set of right rows for O(1) lookup
	rightSet := make(map[uint64]bool)
	for _, row := range rightResult.Rows {
		rightSet[makeRowKey(row)] = true
	}

	// Keep rows that appear in left but NOT in right (with dedup)
	seen := make(map[uint64]bool)
	var result [][]catalogapi.Value
	for _, row := range leftResult.Rows {
		keyStr := makeRowKey(row)
		if !rightSet[keyStr] && !seen[keyStr] {
			seen[keyStr] = true
			result = append(result, row)
		}
	}

	return &executorapi.Result{
		Rows:    result,
		Columns: leftResult.Columns,
	}, nil
}

func (e *executor) execWith(plan *plannerapi.WithPlan) (*executorapi.Result, error) {
	// Materialize each CTE into a temporary in-memory table
	for _, ctePlan := range plan.CTEs {
		var result *executorapi.Result
		var err error

		if ctePlan.IsRecursive && ctePlan.AnchorPlan != nil {
			// Recursive CTE: proper iterative execution
			result, err = e.execRecursiveCTE(ctePlan)
		} else {
			// Non-recursive CTE: simple materialization
			if ctePlan.SelectPlan != nil {
				result, err = e.Execute(ctePlan.SelectPlan)
			} else {
				// Fallback: execute the body plan directly
				result, err = e.Execute(ctePlan.AnchorPlan)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%w: executing CTE %s: %v", executorapi.ErrExecFailed, ctePlan.Name, err)
		}

		// Store CTE result in executor's CTE registry for lookup during main query
		e.cteResults[ctePlan.Name] = result
	}

	// Execute the main statement
	mainResult, err := e.Execute(plan.Statement)
	if err != nil {
		return nil, err
	}

	return mainResult, nil
}

// execRecursiveCTE executes a recursive CTE using the anchor-recursive split.
// The CTE body must be: anchor UNION ALL recursive_part
// The recursive part references the CTE name itself.
//
// Algorithm:
// 1. Execute anchor → prevRows (these become the CTE's data for iteration 1)
// 2. Loop:
//    a. Execute recursive part (sees prevRows in CTE)
//    b. If no new rows → break
//    c. Accumulate newRows into result
//    d. prevRows = newRows (for next iteration)
// 3. Return accumulated result
func (e *executor) execRecursiveCTE(ctePlan *plannerapi.CTEPlan) (*executorapi.Result, error) {
	var accumulated [][]catalogapi.Value
	var accumulatorColumns []string
	var prevRows [][]catalogapi.Value

	// Iteration 0: execute anchor
	anchorResult, err := e.Execute(ctePlan.AnchorPlan)
	if err != nil {
		return nil, fmt.Errorf("%w: recursive CTE anchor: %v", executorapi.ErrExecFailed, err)
	}
	if anchorResult != nil {
		accumulatorColumns = anchorResult.Columns
		prevRows = anchorResult.Rows
		accumulated = prevRows
	}

	// Iteration limit to prevent infinite loops
	const maxIterations = 1000

	for iteration := 1; iteration < maxIterations; iteration++ {
		// Set CTE's data to only the previous iteration's rows (NOT accumulated)
		e.cteResults[ctePlan.Name] = &executorapi.Result{
			Rows:    prevRows,
			Columns: accumulatorColumns,
		}

		// Execute recursive part
		result, err := e.Execute(ctePlan.RecursivePlan)
		if err != nil {
			return nil, fmt.Errorf("%w: recursive CTE iteration: %v", executorapi.ErrExecFailed, err)
		}

		newRows := result.Rows
		if len(newRows) == 0 {
			break // No new rows, we're done
		}

		// Append new rows to accumulated result
		accumulated = append(accumulated, newRows...)

		// Update prevRows for next iteration
		prevRows = newRows
	}

	// Return final accumulated result
	return &executorapi.Result{
		Rows:    accumulated,
		Columns: accumulatorColumns,
	}, nil
}

// execUpdateParameterized handles UPDATE with ParamRef expressions (resolved at execution time).
func (e *executor) execUpdateParameterized(plan *plannerapi.UpdatePlan) (*executorapi.Result, error) {
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Build set of changed column indices for index maintenance
	changedCols := make(map[int]bool, len(plan.ParamAssignments))
	for colIdx := range plan.ParamAssignments {
		changedCols[colIdx] = true
	}

	rows, err := e.scanRowsForDML(plan.Table, plan.Scan, nil)
	if err != nil {
		return nil, err
	}

	var count int64

	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for _, row := range rows {
			rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, row.RowID)
			oldRowVal := e.tableEngine.EncodeRow(row.Values)

			// Delete old index entries
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 || !changedCols[colIdx] {
					continue
				}
				oldVal, err := getIndexValue(idx, row, plan.Table.Columns, e)
				if err != nil {
					return nil, fmt.Errorf("%w: get old index value: %v", executorapi.ErrExecFailed, err)
				}
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, row.RowID)
				if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
					return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey, nil)
			}

			// Resolve parameter expressions
			newValues := make([]catalogapi.Value, len(row.Values))
			copy(newValues, row.Values)
			for colIdx, expr := range plan.ParamAssignments {
				val, err := evalExpr(expr, row, plan.Table.Columns, nil, e)
				if err != nil {
					return nil, fmt.Errorf("SET: %w", err)
				}
				if !val.IsNull {
					if err := checkType(val, plan.Table.Columns[colIdx].Type); err != nil {
						return nil, fmt.Errorf("SET %s: %w", plan.Table.Columns[colIdx].Name, err)
					}
				}
				newValues[colIdx] = val
			}

			// Check constraints
			if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
				return nil, err
			}
			if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues, row.RowID); err != nil {
				return nil, err
			}
			if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.CheckConstraints); err != nil {
				return nil, err
			}
			if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.ForeignKeys); err != nil {
				return nil, err
			}

			// Update row
			rowVal := e.tableEngine.EncodeRow(newValues)
			if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
				return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
			}
			e.txnCtx.AddPendingWrite(rowKey, oldRowVal)

			// Insert new index entries
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 || !changedCols[colIdx] {
					continue
				}
				newRow := &engineapi.Row{RowID: row.RowID, Values: newValues}
				newVal, err := getIndexValue(idx, newRow, plan.Table.Columns, e)
				if err != nil {
					return nil, fmt.Errorf("%w: get new index value: %v", executorapi.ErrExecFailed, err)
				}
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, row.RowID)
				if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
					return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey, nil)
			}
			count++
		}
		return &executorapi.Result{RowsAffected: count}, nil
	}

	// Non-transactional path
	batch := e.store.NewWriteBatch()
	for _, row := range rows {
		rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, row.RowID)

		// Delete old index entries
		for _, idx := range indexes {
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			oldVal, err := getIndexValue(idx, row, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get old index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, row.RowID)
			if err := batch.Delete(idxKey); err != nil && err != kvstoreapi.ErrKeyNotFound {
				return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
			}
		}

		// Resolve parameter expressions
		newValues := make([]catalogapi.Value, len(row.Values))
		copy(newValues, row.Values)
		for colIdx, expr := range plan.ParamAssignments {
			val, err := evalExpr(expr, row, plan.Table.Columns, nil, e)
			if err != nil {
				return nil, fmt.Errorf("SET: %w", err)
			}
			if !val.IsNull {
				if err := checkType(val, plan.Table.Columns[colIdx].Type); err != nil {
					return nil, fmt.Errorf("SET %s: %w", plan.Table.Columns[colIdx].Name, err)
				}
			}
			newValues[colIdx] = val
		}

		// Check constraints
		if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
			return nil, err
		}
		if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues, row.RowID); err != nil {
			return nil, err
		}
		if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.CheckConstraints); err != nil {
			return nil, err
		}
		if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.ForeignKeys); err != nil {
			return nil, err
		}

		// Update row
		rowVal := e.tableEngine.EncodeRow(newValues)
		if err := batch.Put(rowKey, rowVal); err != nil {
			return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
		}

		// Insert new index entries
		for _, idx := range indexes {
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			newRow := &engineapi.Row{RowID: row.RowID, Values: newValues}
			newVal, err := getIndexValue(idx, newRow, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get new index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, row.RowID)
			if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}
		count++
	}

	if err := batch.Commit(); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

// execDelete deletes rows from a table. It iterates until all rows are deleted,
// scanning in batches to handle tables with more rows than any single scan can return.
// This ensures DELETE without WHERE deletes ALL rows, not just the first batch.
// After each row deletion, FK ON DELETE actions are executed via executeFKDeleteActions.
func (e *executor) execDelete(plan *plannerapi.DeletePlan) (*executorapi.Result, error) {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Get referencing FKs for ON DELETE actions (RESTRICT check + CASCADE/SET NULL execution)
	var fks []catalogapi.ForeignKeySchema
	if plan.Table.PrimaryKey != "" {
		fks, err = e.catalog.GetReferencingFKs(plan.Table.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: get referencing FKs: %v", executorapi.ErrExecFailed, err)
		}
	}

	// Find the primary key column index for FK action key extraction
	var pkColIdx int = -1
	if plan.Table.PrimaryKey != "" {
		pkColIdx = findColumnIndexByName(plan.Table.Columns, plan.Table.PrimaryKey)
	}

	var count int64

	// When in a transaction, use DeleteWithXID so deletes share the transaction's XID.
	// Rollback marks entries as deleted with txnMax==txnXID → invisible.
	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for {
			// Scan a batch of rows to delete
			rows, err := e.scanRowsForDML(plan.Table, plan.Scan, nil)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 {
				break // No more rows to delete
			}
			for _, row := range rows {
				// Fire BEFORE DELETE triggers (before any changes)
				if err := e.fireTriggers(plan.Table.Name, "BEFORE", "DELETE", nil, nil, plan.Table.Columns, row.Values); err != nil {
					return nil, err
				}

				// Get old encoded row data for savepoint rollback.
				oldRowVal := e.tableEngine.EncodeRow(row.Values)

				// Delete index entries with transaction's XID.
				for _, idx := range indexes {
					colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
					if colIdx < 0 {
						continue
					}
					val := row.Values[colIdx]
					idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, row.RowID)
					if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
						return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
					}
					e.txnCtx.AddPendingWrite(idxKey, nil) // index rollback = delete
				}

				// Execute FK ON DELETE RESTRICT check BEFORE delete completes
				// This is deferred constraint checking - RESTRICT errors before the delete is visible
				if pkColIdx >= 0 && len(fks) > 0 {
					parentKeyValues := []catalogapi.Value{row.Values[pkColIdx]}
					if err := e.executeFKDeleteActions(plan.Table.Name, parentKeyValues, fks); err != nil {
						return nil, err
					}
				}

				// Delete row with transaction's XID.
				rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, row.RowID)
				if err := e.store.DeleteWithXID(rowKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
					return nil, fmt.Errorf("%w: delete: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(rowKey, oldRowVal) // store old row for rollback restore
				count++

				// Fire AFTER DELETE triggers (after row is deleted)
				if err := e.fireTriggers(plan.Table.Name, "AFTER", "DELETE", nil, nil, plan.Table.Columns, row.Values); err != nil {
					return nil, err
				}
			}
		}
		return &executorapi.Result{RowsAffected: count}, nil
	}

	// Non-transactional path: use WriteBatch for auto-commit per statement.
	for {
		batch := e.store.NewWriteBatch()

		// Scan a batch of rows to delete
		rows, err := e.scanRowsForDML(plan.Table, plan.Scan, nil)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break // No more rows to delete
		}

		for _, row := range rows {
			// Fire BEFORE DELETE triggers (before any changes)
			if err := e.fireTriggers(plan.Table.Name, "BEFORE", "DELETE", nil, nil, plan.Table.Columns, row.Values); err != nil {
				batch.Discard()
				return nil, err
			}

			// Delete index entries.
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 {
					continue
				}
				val := row.Values[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, row.RowID)
				if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
					batch.Discard()
					return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
			}

			// Delete row via batch.
			if err := e.tableEngine.DeleteFrom(plan.Table, batch, row.RowID); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: delete: %v", executorapi.ErrExecFailed, err)
			}
			count++

			// Fire AFTER DELETE triggers (after row is deleted)
			if err := e.fireTriggers(plan.Table.Name, "AFTER", "DELETE", nil, nil, plan.Table.Columns, row.Values); err != nil {
				batch.Discard()
				return nil, err
			}
		}

		// Execute RESTRICT checks BEFORE commit to prevent the delete.
		// CASCADE and SET NULL actions run after commit (when it's safe).
		if pkColIdx >= 0 && len(fks) > 0 {
			for _, row := range rows {
				parentKeyValues := []catalogapi.Value{row.Values[pkColIdx]}
				if err := e.checkFKRestrictBeforeDelete(plan.Table.Name, parentKeyValues, fks); err != nil {
					batch.Discard()
					return nil, err
				}
			}
		}

		if err := batch.Commit(); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
		}

		// Execute FK ON DELETE actions after batch commit (CASCADE/SET NULL)
		if pkColIdx >= 0 && len(fks) > 0 {
			for _, row := range rows {
				parentKeyValues := []catalogapi.Value{row.Values[pkColIdx]}
				if err := e.executeFKDeleteActions(plan.Table.Name, parentKeyValues, fks); err != nil {
					return nil, err
				}
			}
		}
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

// execTruncate deletes all rows from a table efficiently using DropTableData.
// This is faster than DELETE without WHERE because it doesn't scan individual rows.
// Truncate does NOT reset AUTOINCREMENT counter — that's SQLite behavior.
func (e *executor) execTruncate(plan *plannerapi.TruncatePlan) (*executorapi.Result, error) {
	// Use the efficient DropTableData to delete all rows at once
	if err := e.tableEngine.DropTableData(plan.TableID); err != nil {
		return nil, fmt.Errorf("%w: truncate %s: %v", executorapi.ErrExecFailed, plan.Table.Name, err)
	}
	return &executorapi.Result{RowsAffected: 0}, nil
}

func (e *executor) execUpdate(plan *plannerapi.UpdatePlan) (*executorapi.Result, error) {
	// Handle parameterized UPDATE (when ParamAssignments is set)
	if len(plan.ParamAssignments) > 0 {
		return e.execUpdateParameterized(plan)
	}

	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Get referencing FKs for ON UPDATE actions
	var fks []catalogapi.ForeignKeySchema
	var pkColIdx int = -1
	if plan.Table.PrimaryKey != "" {
		fks, err = e.catalog.GetReferencingFKs(plan.Table.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: get referencing FKs: %v", executorapi.ErrExecFailed, err)
		}
		pkColIdx = findColumnIndexByName(plan.Table.Columns, plan.Table.PrimaryKey)
	}

	// Defensive: validate that all assignment column indices are within bounds.
	// This guards against edge cases (e.g., planner bugs or concurrent schema changes).
	for colIdx := range plan.Assignments {
		if colIdx < 0 || colIdx >= len(plan.Table.Columns) {
			return nil, fmt.Errorf("%w: assignment column index %d out of bounds (table %q has %d columns)",
				executorapi.ErrExecFailed, colIdx, plan.Table.Name, len(plan.Table.Columns))
		}
	}

	// Build set of changed column indices for index maintenance
	changedCols := make(map[int]bool, len(plan.Assignments))
	for colIdx := range plan.Assignments {
		changedCols[colIdx] = true
	}

	// Check if primary key column is being updated
	pkChanged := pkColIdx >= 0 && changedCols[pkColIdx]

	rows, err := e.scanRowsForDML(plan.Table, plan.Scan, nil)
	if err != nil {
		return nil, err
	}

	var count int64

	// When in a transaction, use DeleteWithXID/PutWithXID for deferred-write.
	// All writes share the transaction's XID. Rollback marks them as deleted
	// with txnMax==txnXID → invisible.
	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for _, row := range rows {
			// Merge old values with new assignments (needed for trigger context)
			newValues := make([]catalogapi.Value, len(row.Values))
			copy(newValues, row.Values)
			for colIdx, val := range plan.Assignments {
				newValues[colIdx] = val
			}

			// Fire BEFORE UPDATE triggers (before any changes)
			if err := e.fireTriggers(plan.Table.Name, "BEFORE", "UPDATE", plan.Table.Columns, newValues, plan.Table.Columns, row.Values); err != nil {
				return nil, err
			}

			// Capture old row key and pre-value BEFORE any modifications.
			// This is needed for savepoint rollback to restore the original row.
			rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, row.RowID)
			oldRowVal := e.tableEngine.EncodeRow(row.Values)

			// Delete old index entries for changed columns with transaction's XID.
			for _, idx := range indexes {
				// Check if any indexed column changed
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 || !changedCols[colIdx] {
					continue
				}
				// Get index value (column or expression)
				oldVal, err := getIndexValue(idx, row, plan.Table.Columns, e)
				if err != nil {
					return nil, fmt.Errorf("%w: get old index value: %v", executorapi.ErrExecFailed, err)
				}
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, row.RowID)
				if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
					return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey, nil) // index rollback = delete
			}

			// Merge old values with new assignments (reconstruct after BEFORE trigger read-only access)
			newValues = make([]catalogapi.Value, len(row.Values))
			copy(newValues, row.Values)
			for colIdx, val := range plan.Assignments {
				newValues[colIdx] = val
			}

			// Check NOT NULL constraint before updating.
			if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
				return nil, err
			}

			// Check UNIQUE constraint before updating.
			// Pass row.RowID to exclude it — allows UPDATE to same value (idempotent).
			if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues, row.RowID); err != nil {
				return nil, err
			}

			// Check CHECK constraints before updating.
			if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.CheckConstraints); err != nil {
				return nil, err
			}

			// Check FOREIGN KEY constraints for changed columns.
			if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.ForeignKeys); err != nil {
				return nil, err
			}

			// Execute FK ON UPDATE actions BEFORE the update completes
			// Only for RESTRICT: check before allowing update
			if pkChanged && len(fks) > 0 {
				oldKeyValues := []catalogapi.Value{row.Values[pkColIdx]}
				newKeyValues := []catalogapi.Value{newValues[pkColIdx]}
				if err := e.executeFKUpdateActions(plan.Table.Name, oldKeyValues, newKeyValues, fks); err != nil {
					return nil, err
				}
			}

			// Update row with transaction's XID.
			rowVal := e.tableEngine.EncodeRow(newValues)
			if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
				return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
			}
			// Store old row as preValue so savepoint rollback can restore it.
			e.txnCtx.AddPendingWrite(rowKey, oldRowVal)

			// Insert new index entries for changed columns.
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 || !changedCols[colIdx] {
					continue
				}
				// Build new row with updated values for expression evaluation
				newRow := &engineapi.Row{RowID: row.RowID, Values: newValues}
				// Get index value (column or expression)
				newVal, err := getIndexValue(idx, newRow, plan.Table.Columns, e)
				if err != nil {
					return nil, fmt.Errorf("%w: get new index value: %v", executorapi.ErrExecFailed, err)
				}
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, row.RowID)
				if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
					return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey, nil) // index rollback = delete
			}

			count++

			// Fire AFTER UPDATE triggers (after row and indexes are updated)
			if err := e.fireTriggers(plan.Table.Name, "AFTER", "UPDATE", plan.Table.Columns, newValues, plan.Table.Columns, row.Values); err != nil {
				return nil, err
			}
		}
		return &executorapi.Result{RowsAffected: count}, nil
	}

	// Non-transactional path: use WriteBatch for auto-commit per statement.
	batch := e.store.NewWriteBatch()

	for _, row := range rows {
		// Merge old values with new assignments (needed for trigger context)
		newValues := make([]catalogapi.Value, len(row.Values))
		copy(newValues, row.Values)
		for colIdx, val := range plan.Assignments {
			newValues[colIdx] = val
		}

		// Fire BEFORE UPDATE triggers (before any changes)
		if err := e.fireTriggers(plan.Table.Name, "BEFORE", "UPDATE", plan.Table.Columns, newValues, plan.Table.Columns, row.Values); err != nil {
			batch.Discard()
			return nil, err
		}

		// Delete old index entries for changed columns.
		for _, idx := range indexes {
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			// Get index value (column or expression)
			oldVal, err := getIndexValue(idx, row, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get old index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, row.RowID)
			if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
			}
		}

		// Check NOT NULL constraint before updating.
		if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check UNIQUE constraint before updating.
		// Pass row.RowID to exclude it — allows UPDATE to same value (idempotent).
		if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues, row.RowID); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check CHECK constraints before updating.
		if err := e.checkCheckConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.CheckConstraints); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check FOREIGN KEY constraints for changed columns.
		if err := e.checkForeignKeyConstraints(plan.Table.Name, plan.Table.Columns, newValues, plan.Table.ForeignKeys); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check RESTRICT/NO ACTION BEFORE allowing update to complete.
		// This prevents the update if there are referencing child rows.
		if pkChanged && len(fks) > 0 {
			oldKeyValues := []catalogapi.Value{row.Values[pkColIdx]}
			newKeyValues := []catalogapi.Value{newValues[pkColIdx]}
			if err := e.checkFKRestrictBeforeUpdate(plan.Table.Name, oldKeyValues, fks); err != nil {
				batch.Discard()
				return nil, err
			}
			// Execute CASCADE/SET NULL actions
			if err := e.executeFKUpdateActions(plan.Table.Name, oldKeyValues, newKeyValues, fks); err != nil {
				batch.Discard()
				return nil, err
			}
		}

		// Update row via batch.
		if err := e.tableEngine.UpdateIn(plan.Table, batch, row.RowID, newValues); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
		}

		// Fire AFTER UPDATE triggers (after row is updated)
		if err := e.fireTriggers(plan.Table.Name, "AFTER", "UPDATE", plan.Table.Columns, newValues, plan.Table.Columns, row.Values); err != nil {
			batch.Discard()
			return nil, err
		}

		// Insert new index entries for changed columns.
		for _, idx := range indexes {
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			// Build new row with updated values for expression evaluation
			newRow := &engineapi.Row{RowID: row.RowID, Values: newValues}
			// Get index value (column or expression)
			newVal, err := getIndexValue(idx, newRow, plan.Table.Columns, e)
			if err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: get new index value: %v", executorapi.ErrExecFailed, err)
			}
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, row.RowID)
			if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}

		count++
	}

	if err := batch.Commit(); err != nil {
		batch.Discard()
		return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

// groupByRows groups input rows by GROUP BY expressions and computes aggregates.
// plan.SelectColumns tells us which output columns are group keys vs aggregates.
// Returns []*engineapi.Row where each row has len(SelectColumns) values.
func (e *executor) groupByRows(rows []*engineapi.Row, plan *plannerapi.SelectPlan) ([]*engineapi.Row, error) {
	// Phase 1: group rows by encoded key into a slice of groups (to preserve key for sorting)
	type group struct {
		key  []byte
		rows []*engineapi.Row
	}
	groups := make([]group, 0)
	groupMap := make(map[string]int) // key -> index in groups

	for _, row := range rows {
		key, err := e.encodeGroupKey(row, plan.GroupByExprs, plan.Table.Columns)
		if err != nil {
			return nil, fmt.Errorf("%w: group key: %v", executorapi.ErrExecFailed, err)
		}
		keyStr := string(key)
		if idx, ok := groupMap[keyStr]; ok {
			groups[idx].rows = append(groups[idx].rows, row)
		} else {
			groupMap[keyStr] = len(groups)
			groups = append(groups, group{key: key, rows: []*engineapi.Row{row}})
		}
	}

	// Phase 2: project each group and sort by encoded key
	result := make([]*engineapi.Row, 0, len(groups))
	for _, g := range groups {
		vals, err := projectGroupedRow(g.rows, plan)
		if err != nil {
			return nil, err
		}
		result = append(result, &engineapi.Row{Values: vals})
	}

	// Sort by encoded key (deterministic, stable)
	sort.SliceStable(result, func(i, j int) bool {
		return string(groups[i].key) < string(groups[j].key)
	})
	return result, nil
}


// groupByRowsForJoin is like groupByRows but uses combinedCols for column resolution.
// This is needed for JOIN queries where plan.Table.Columns only has left-table columns.
func (e *executor) groupByRowsForJoin(rows []*engineapi.Row, plan *plannerapi.SelectPlan, combinedCols []catalogapi.ColumnDef) ([]*engineapi.Row, error) {
	type group struct {
		key  []byte
		rows []*engineapi.Row
	}
	groups := make([]group, 0)
	groupMap := make(map[string]int)

	for _, row := range rows {
		key, err := e.encodeGroupKey(row, plan.GroupByExprs, combinedCols)
		if err != nil {
			return nil, fmt.Errorf("%w: group key: %v", executorapi.ErrExecFailed, err)
		}
		keyStr := string(key)
		if idx, ok := groupMap[keyStr]; ok {
			groups[idx].rows = append(groups[idx].rows, row)
		} else {
			groupMap[keyStr] = len(groups)
			groups = append(groups, group{key: key, rows: []*engineapi.Row{row}})
		}
	}

	result := make([]*engineapi.Row, 0, len(groups))
	for _, g := range groups {
		vals, err := projectGroupedRowForJoin(g.rows, plan, combinedCols)
		if err != nil {
			return nil, err
		}
		result = append(result, &engineapi.Row{Values: vals})
	}

	sort.SliceStable(result, func(i, j int) bool {
		return string(groups[i].key) < string(groups[j].key)
	})
	return result, nil
}

// projectGroupedRowForJoin is like projectGroupedRow but uses combinedCols for column resolution.
func projectGroupedRowForJoin(groupRows []*engineapi.Row, plan *plannerapi.SelectPlan, combinedCols []catalogapi.ColumnDef) ([]catalogapi.Value, error) {
	groupCols := groupKeyColIndices(plan)
	result := make([]catalogapi.Value, len(plan.SelectColumns))

	for i, sc := range plan.SelectColumns {
		switch expr := sc.Expr.(type) {
		case *parserapi.ColumnRef:
			if groupCols[plan.Columns[i]] {
				idx := plan.Columns[i]
				if idx >= 0 && idx < len(groupRows[0].Values) {
					result[i] = groupRows[0].Values[idx]
				} else {
					result[i] = catalogapi.Value{IsNull: true}
				}
			} else {
				result[i] = catalogapi.Value{IsNull: true}
			}
		case *parserapi.AggregateCallExpr:
			val, err := computeAggregate(expr, groupRows, combinedCols)
			if err != nil {
				return nil, err
			}
			result[i] = val
		case *parserapi.CoalesceExpr:
			val, err := evalExpr(expr, groupRows[0], combinedCols, nil, nil)
			if err != nil {
				return nil, err
			}
			result[i] = val
		default:
			return nil, fmt.Errorf("%w: GROUP BY SELECT expression must be column or aggregate", executorapi.ErrExecFailed)
		}
	}
	return result, nil
}

// encodeGroupKey computes a unique encoded key from GROUP BY expression values.
func (e *executor) encodeGroupKey(row *engineapi.Row, exprs []parserapi.Expr, columns []catalogapi.ColumnDef) ([]byte, error) {
	var buf []byte
	for _, expr := range exprs {
		val, err := evalExpr(expr, row, columns, nil, e)
		if err != nil {
			return nil, err
		}
		if val.IsNull {
			buf = append(buf, 0xFF) // NULL sentinel
		} else {
			buf = e.keyEncoder.EncodeValue(val)
		}
		buf = append(buf, 0) // separator between exprs
	}
	return buf, nil
}


// groupKeyColIndices returns the set of column indices that are group-key expressions.
func groupKeyColIndices(plan *plannerapi.SelectPlan) map[int]bool {
	groupCols := make(map[int]bool)
	for _, sc := range plan.SelectColumns {
		if ref, ok := sc.Expr.(*parserapi.ColumnRef); ok {
			for _, gb := range plan.GroupByExprs {
				if gbRef, ok := gb.(*parserapi.ColumnRef); ok {
					if strings.EqualFold(ref.Column, gbRef.Column) {
						idx := findColumnIndex(plan.Table, ref.Column)
						if idx >= 0 {
							groupCols[idx] = true
						}
					}
				}
			}
		}
	}
	return groupCols
}

// computeAggregate computes the aggregate value for an AggregateCallExpr across a group of rows.
func computeAggregate(agg *parserapi.AggregateCallExpr, rows []*engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	switch strings.ToUpper(agg.Func) {
	case "COUNT":
		if agg.Arg == nil {
			return catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(len(rows))}, nil
		}
		colRef, ok := agg.Arg.(*parserapi.ColumnRef)
		if !ok {
			return catalogapi.Value{}, fmt.Errorf("%w: COUNT argument must be a column", executorapi.ErrExecFailed)
		}
		idx := findColumnIndexByName(columns, colRef.Column)
		var count int64
		for _, row := range rows {
			if idx >= 0 && idx < len(row.Values) && !row.Values[idx].IsNull {
				count++
			}
		}
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: count}, nil

	case "SUM":
		colRef, ok := agg.Arg.(*parserapi.ColumnRef)
		if !ok {
			return catalogapi.Value{}, fmt.Errorf("%w: SUM argument must be a column", executorapi.ErrExecFailed)
		}
		idx := findColumnIndexByName(columns, colRef.Column)
		var sum int64
		var count int64
		for _, row := range rows {
			if idx >= 0 && idx < len(row.Values) {
				val := row.Values[idx]
				if !val.IsNull && val.Type == catalogapi.TypeInt {
					sum += val.Int
					count++
				}
			}
		}
		if count == 0 {
			return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
		}
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: sum}, nil

	case "AVG":
		colRef, ok := agg.Arg.(*parserapi.ColumnRef)
		if !ok {
			return catalogapi.Value{}, fmt.Errorf("%w: AVG argument must be a column", executorapi.ErrExecFailed)
		}
		idx := findColumnIndexByName(columns, colRef.Column)
		var sum float64
		var count int64
		for _, row := range rows {
			if idx >= 0 && idx < len(row.Values) {
				val := row.Values[idx]
				if !val.IsNull {
					if val.Type == catalogapi.TypeInt {
						sum += float64(val.Int)
					} else if val.Type == catalogapi.TypeFloat {
						sum += val.Float
					}
					count++
				}
			}
		}
		if count == 0 {
			return catalogapi.Value{Type: catalogapi.TypeFloat, IsNull: true}, nil
		}
		return catalogapi.Value{Type: catalogapi.TypeFloat, Float: sum / float64(count)}, nil

	case "MIN":
		colRef, ok := agg.Arg.(*parserapi.ColumnRef)
		if !ok {
			return catalogapi.Value{}, fmt.Errorf("%w: MIN argument must be a column", executorapi.ErrExecFailed)
		}
		idx := findColumnIndexByName(columns, colRef.Column)
		return minValueForColumn(rows, idx), nil

	case "MAX":
		colRef, ok := agg.Arg.(*parserapi.ColumnRef)
		if !ok {
			return catalogapi.Value{}, fmt.Errorf("%w: MAX argument must be a column", executorapi.ErrExecFailed)
		}
		idx := findColumnIndexByName(columns, colRef.Column)
		return maxValueForColumn(rows, idx), nil

	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported aggregate function %s", executorapi.ErrExecFailed, agg.Func)
	}
}

// minValueForColumn returns the MIN value for a column across rows (NULLs ignored).
func minValueForColumn(rows []*engineapi.Row, colIdx int) catalogapi.Value {
	var minInt int64 = 9223372036854775807
	var minFloat float64 = 1e300
	var minText string
	hasValue := false
	valType := catalogapi.Type(0)

	for _, row := range rows {
		if colIdx >= len(row.Values) {
			continue
		}
		val := row.Values[colIdx]
		if val.IsNull {
			continue
		}
		if valType == 0 {
			valType = val.Type
		}
		switch val.Type {
		case catalogapi.TypeInt:
			if val.Int < minInt {
				minInt = val.Int
			}
			hasValue = true
		case catalogapi.TypeFloat:
			if val.Float < minFloat {
				minFloat = val.Float
			}
			hasValue = true
		case catalogapi.TypeText:
			if !hasValue || val.Text < minText {
				minText = val.Text
			}
			hasValue = true
		}
	}

	if !hasValue {
		return catalogapi.Value{Type: valType, IsNull: true}
	}
	switch valType {
	case catalogapi.TypeInt:
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: minInt}
	case catalogapi.TypeFloat:
		return catalogapi.Value{Type: catalogapi.TypeFloat, Float: minFloat}
	case catalogapi.TypeText:
		return catalogapi.Value{Type: catalogapi.TypeText, Text: minText}
	}
	return catalogapi.Value{IsNull: true}
}

// maxValueForColumn returns the MAX value for a column across rows (NULLs ignored).
func maxValueForColumn(rows []*engineapi.Row, colIdx int) catalogapi.Value {
	var maxInt int64 = -9223372036854775808
	var maxFloat float64 = -1e300
	var maxText string
	hasValue := false
	valType := catalogapi.Type(0)

	for _, row := range rows {
		if colIdx >= len(row.Values) {
			continue
		}
		val := row.Values[colIdx]
		if val.IsNull {
			continue
		}
		if valType == 0 {
			valType = val.Type
		}
		switch val.Type {
		case catalogapi.TypeInt:
			if val.Int > maxInt {
				maxInt = val.Int
			}
			hasValue = true
		case catalogapi.TypeFloat:
			if val.Float > maxFloat {
				maxFloat = val.Float
			}
			hasValue = true
		case catalogapi.TypeText:
			if !hasValue || val.Text > maxText {
				maxText = val.Text
			}
			hasValue = true
		}
	}

	if !hasValue {
		return catalogapi.Value{Type: valType, IsNull: true}
	}
	switch valType {
	case catalogapi.TypeInt:
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: maxInt}
	case catalogapi.TypeFloat:
		return catalogapi.Value{Type: catalogapi.TypeFloat, Float: maxFloat}
	case catalogapi.TypeText:
		return catalogapi.Value{Type: catalogapi.TypeText, Text: maxText}
	}
	return catalogapi.Value{IsNull: true}
}

// encodeMetaKeyLocal returns the metadata key for a table's row counter.
// Matches tableEngine.encodeMetaKey: "t{tableID}m" (6 bytes).
func encodeMetaKeyLocal(tableID uint32) []byte {
	buf := make([]byte, 6)
	buf[0] = 't'
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = 'm'
	return buf
}

// nilValueForIndex returns an empty slice used as the value for index entries.
// Index entries store only the key (which encodes tableID, indexID, value, rowID);
// the value is always empty.
func nilValueForIndex() []byte {
	return []byte{}
}

// findColumnIndexByName returns the index of a column by name (case-insensitive).
func findColumnIndexByName(columns []catalogapi.ColumnDef, name string) int {
	upper := strings.ToUpper(name)
	for i, col := range columns {
		if strings.ToUpper(col.Name) == upper {
			return i
		}
	}
	return -1
}

// projectGroupedRow builds the output row for a group by evaluating each SelectColumn.
func projectGroupedRow(groupRows []*engineapi.Row, plan *plannerapi.SelectPlan) ([]catalogapi.Value, error) {
	groupCols := groupKeyColIndices(plan)
	result := make([]catalogapi.Value, len(plan.SelectColumns))

	for i, sc := range plan.SelectColumns {
		switch expr := sc.Expr.(type) {
		case *parserapi.ColumnRef:
			if groupCols[plan.Columns[i]] {
				idx := plan.Columns[i]
				if idx >= 0 && idx < len(groupRows[0].Values) {
					result[i] = groupRows[0].Values[idx]
				} else {
					result[i] = catalogapi.Value{IsNull: true}
				}
			} else {
				result[i] = catalogapi.Value{IsNull: true}
			}
		case *parserapi.AggregateCallExpr:
			val, err := computeAggregate(expr, groupRows, plan.Table.Columns)
			if err != nil {
				return nil, err
			}
			result[i] = val
		case *parserapi.CoalesceExpr:
			val, err := evalExpr(expr, groupRows[0], plan.Table.Columns, nil, nil)
			if err != nil {
				return nil, err
			}
			result[i] = val
		default:
			return nil, fmt.Errorf("%w: GROUP BY SELECT expression must be column or aggregate", executorapi.ErrExecFailed)
		}
	}
	return result, nil
}

// hasExpressions returns true if any SelectColumn contains a non-column, non-star expression.
func hasExpressions(cols []parserapi.SelectColumn) bool {
	for _, sc := range cols {
		switch sc.Expr.(type) {
		case *parserapi.ColumnRef, *parserapi.StarExpr:
			// These are handled by projectRows
		default:
			return true
		}
	}
	return false
}

// execExplain handles EXPLAIN and EXPLAIN ANALYZE.
// For EXPLAIN: returns the plan description without execution.
// For EXPLAIN ANALYZE: executes the inner plan and returns timing/row stats.
func (e *executor) execExplain(plan *plannerapi.ExplainPlan) (*executorapi.Result, error) {
	planDesc := explainPlanDescription(plan.Inner, plan.Analyze)
	planLines := strings.Split(planDesc, "\n")

	if !plan.Analyze {
		// EXPLAIN: just return the plan description as a single row.
		values := make([]catalogapi.Value, 1)
		values[0] = catalogapi.Value{Type: catalogapi.TypeText, Text: planDesc}
		return &executorapi.Result{
			Columns: []string{"QUERY PLAN"},
			Rows:    [][]catalogapi.Value{values},
		}, nil
	}

	// EXPLAIN ANALYZE: execute the inner plan and return stats.
	start := time.Now()
	innerResult, err := e.Execute(plan.Inner)
	elapsed := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("%w: EXPLAIN ANALYZE: %v", executorapi.ErrExecFailed, err)
	}

	// Build a detailed plan output with timing and execution stats.
	var sb strings.Builder
	sb.WriteString("┌")
	for i := 0; i < 70; i++ {
		sb.WriteString("─")
	}
	sb.WriteString("┐\n")

	// Header
	mode := "EXPLAIN ANALYZE"
	sb.WriteString(fmt.Sprintf("│ %-70s │\n", mode))

	// Separator
	sb.WriteString("├")
	for i := 0; i < 70; i++ {
		sb.WriteString("─")
	}
	sb.WriteString("┤\n")

	// Plan description
	for _, line := range planLines {
		if line == "" {
			continue
		}
		sb.WriteString(fmt.Sprintf("│ %-70s │\n", line))
	}

	// Separator
	sb.WriteString("├")
	for i := 0; i < 70; i++ {
		sb.WriteString("─")
	}
	sb.WriteString("┤\n")

	// Execution stats
	if innerResult != nil {
		sb.WriteString(fmt.Sprintf("│ %-70s │\n",
			fmt.Sprintf("actual rows=%d", len(innerResult.Rows))))
		if innerResult.RowsAffected > 0 {
			sb.WriteString(fmt.Sprintf("│ %-70s │\n",
				fmt.Sprintf("rows affected=%d", innerResult.RowsAffected)))
		}
	}

	// Timing
	sb.WriteString(fmt.Sprintf("│ %-70s │\n",
		fmt.Sprintf("actual time=%.3fms", float64(elapsed.Nanoseconds())/1e6)))

	// Bottom border
	sb.WriteString("└")
	for i := 0; i < 70; i++ {
		sb.WriteString("─")
	}
	sb.WriteString("┘")

	values := make([]catalogapi.Value, 1)
	values[0] = catalogapi.Value{Type: catalogapi.TypeText, Text: sb.String()}
	return &executorapi.Result{
		Columns: []string{"QUERY PLAN"},
		Rows:    [][]catalogapi.Value{values},
	}, nil
}

// explainPlanDescription returns a human-readable description of any plan.
func explainPlanDescription(plan plannerapi.Plan, analyze bool) string {
	prefix := "EXPLAIN"
	if analyze {
		prefix = "EXPLAIN ANALYZE"
	}
	
	var inner string
	switch p := plan.(type) {
	case *plannerapi.SelectPlan:
		inner = p.String()
	case *plannerapi.InsertPlan:
		inner = fmt.Sprintf("INSERT INTO %s", p.Table.Name)
	case *plannerapi.DeletePlan:
		inner = fmt.Sprintf("DELETE FROM %s", p.Table.Name)
	case *plannerapi.UpdatePlan:
		inner = fmt.Sprintf("UPDATE %s", p.Table.Name)
	case *plannerapi.CreateTablePlan:
		inner = fmt.Sprintf("CREATE TABLE %s", p.Schema.Name)
	case *plannerapi.DropTablePlan:
		inner = fmt.Sprintf("DROP TABLE %s", p.TableName)
	case *plannerapi.CreateIndexPlan:
		inner = fmt.Sprintf("CREATE INDEX ON %s(%s)", p.Schema.Table, p.Schema.Column)
	case *plannerapi.DropIndexPlan:
		inner = fmt.Sprintf("DROP INDEX %s.%s", p.TableName, p.IndexName)
	case *plannerapi.JoinPlan:
		inner = p.String()
	case *plannerapi.HashJoinPlan:
		inner = p.String()
	case *plannerapi.UnionPlan:
		inner = "UNION"
	case *plannerapi.IntersectPlan:
		inner = "INTERSECT"
	case *plannerapi.ExceptPlan:
		inner = "EXCEPT"
	case *plannerapi.InsertSelectPlan:
		inner = fmt.Sprintf("INSERT INTO %s SELECT ...", p.Table.Name)
	default:
		inner = fmt.Sprintf("%T", plan)
	}
	return prefix + "\n└─ " + inner
}

// checkType validates that a value's type matches the expected column type.
// NULL values always pass (they're valid for any column type in Phase 1).
func checkType(val catalogapi.Value, expected catalogapi.Type) error {
	if val.IsNull {
		return nil
	}
	if val.Type != expected {
		return fmt.Errorf("%w: expected %v, got %v", executorapi.ErrTypeMismatch, expected, val.Type)
	}
	return nil
}

// checkNotNullConstraint validates that all NOT NULL columns have non-NULL values.
// Returns nil if all constraints are satisfied, or ErrNotNullViolation if any
// NOT NULL column has a NULL value.
func checkNotNullConstraint(columns []catalogapi.ColumnDef, values []catalogapi.Value) error {
	for i, col := range columns {
		if col.NotNull && i < len(values) && values[i].IsNull {
			return sqlerrors.ErrNotNullViolation("", col.Name)
		}
	}
	return nil
}

// checkUniqueConstraint validates that values don't violate UNIQUE constraints.
// For each UNIQUE index, checks if the value already exists in the index.
// For UPDATE operations: pass the current row's rowID as excludeRowID to skip it.
// Returns nil if all UNIQUE constraints are satisfied, or ErrUniqueViolation
// if any UNIQUE column has a duplicate value.
func (e *executor) checkUniqueConstraint(tableName string, columns []catalogapi.ColumnDef, values []catalogapi.Value, excludeRowID uint64) error {
	indexes, err := e.catalog.ListIndexes(tableName)
	if err != nil {
		return err
	}

	// Get table schema for tableID
	table, err := e.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		if !idx.Unique {
			continue
		}

		// Find the column index for this index
		colIdx := findColumnIndexByName(columns, idx.Column)
		if colIdx < 0 {
			continue
		}

		val := values[colIdx]
		// NULL values don't violate UNIQUE constraint (SQL standard)
		if val.IsNull {
			continue
		}

		// Scan the index for any existing row with this value
		iter, err := e.indexEngine.Scan(table.TableID, idx.IndexID, encodingapi.OpEQ, val)
		if err != nil {
			return err
		}

		// Check each entry — skip excludeRowID for UPDATE (allows same value in same row)
		for iter.Next() {
			if iter.RowID() == excludeRowID {
				continue
			}
			iter.Close()
			return sqlerrors.ErrUniqueViolation(tableName, idx.Column)
		}
		err = iter.Err()
		iter.Close()

		if err != nil {
			return err
		}
	}
	return nil
}

// checkCheckConstraints validates that row values satisfy all CHECK constraints.
// It evaluates both column-level CHECKs (on each column) and table-level CHECKs.
// The tableName parameter is the table name for error messages.
// The tableChecks parameter contains table-level CHECK constraints from the schema.
func (e *executor) checkCheckConstraints(tableName string, columns []catalogapi.ColumnDef, values []catalogapi.Value, tableChecks []catalogapi.CheckConstraint) error {
	// Collect all CHECK constraints (column-level + table-level).
	var checks []catalogapi.CheckConstraint

	// Column-level checks.
	for _, col := range columns {
		if col.Check != nil {
			checks = append(checks, *col.Check)
		}
	}

	// Table-level checks from the schema.
	for _, check := range tableChecks {
		checks = append(checks, check)
	}

	// Evaluate each CHECK constraint by re-parsing and evaluating the expression.
	for _, check := range checks {
		if check.RawSQL == "" {
			continue
		}

		// Re-parse the CHECK expression in the context of this table.
		// Wrap in SELECT 1 WHERE to parse as a boolean expression.
		// e.g., "price > 0" becomes "SELECT 1 FROM table WHERE price > 0"
		// But simpler: parse the expression directly and evaluate with evalExpr.
		// Since CHECK expressions don't reference the table name, we prepend a fake
		// SELECT that we'll use to extract just the WHERE-like expression.
		// 
		// Strategy: wrap as "SELECT * WHERE <expr>" - the parser supports WHERE.
		sql := "SELECT * WHERE " + check.RawSQL
		stmt, err := e.parser.Parse(sql)
		if err != nil {
			// Re-parse failed - constraint is malformed.
			return fmt.Errorf("%w: malformed CHECK expression %q: %v", executorapi.ErrExecFailed, check.RawSQL, err)
		}

		sel, ok := stmt.(*parserapi.SelectStmt)
		if !ok {
			return fmt.Errorf("%w: CHECK expression %q did not parse as SELECT", executorapi.ErrExecFailed, check.RawSQL)
		}

		filter := sel.Where
		if filter == nil {
			// No WHERE clause - constraint always passes.
			continue
		}

		// Evaluate the filter expression against the row values.
		row := &engineapi.Row{Values: values}
		result, err := evalExpr(filter, row, columns, nil, e)
		if err != nil {
			return fmt.Errorf("%w: evaluating CHECK constraint %q: %v", executorapi.ErrExecFailed, check.RawSQL, err)
		}

		// CHECK constraint passes only if the expression evaluates to TRUE.
		// NULL or FALSE both fail the constraint.
		if !isTruthy(result) {
			return sqlerrors.ErrCheckViolation(tableName, check.RawSQL)
		}
	}

	return nil
}

// checkForeignKeyConstraints validates that row values satisfy all foreign key constraints.
// For INSERT: checks that FK column values reference existing rows in parent tables.
// For UPDATE: checks that new FK column values (if changed) reference existing rows.
// For any FK column that is NULL, the constraint is satisfied (SQL standard).
func (e *executor) checkForeignKeyConstraints(tableName string, columns []catalogapi.ColumnDef, values []catalogapi.Value, fks []catalogapi.ForeignKeySchema) error {
	if len(fks) == 0 {
		return nil
	}

	// Build a column name -> index map for efficient lookup
	colIdxMap := make(map[string]int, len(columns))
	for i, col := range columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	for _, fk := range fks {
		// Check each FK column (skip if value is NULL — NULL satisfies FK constraint)
		for i, colName := range fk.Columns {
			// Look up the column index
			idx, ok := colIdxMap[strings.ToLower(colName)]
			if !ok || idx < 0 || idx >= len(values) {
				continue
			}
			fkVal := values[idx]
			if fkVal.IsNull {
				continue // NULL FK always passes
			}

			// Get the parent table schema
			parentTable, err := e.catalog.GetTable(fk.ReferencedTable)
			if err != nil {
				if err == catalogapi.ErrTableNotFound {
					return sqlerrors.ErrForeignKeyViolation(
						fmt.Sprintf("references table %q which does not exist", fk.ReferencedTable))
				}
				return err
			}

			// Determine which column to look up in the parent table
			var parentColName string
			if i < len(fk.ReferencedColumns) {
				parentColName = fk.ReferencedColumns[i]
			}

			// Find the parent column index by name
			lookupColIdx := -1
			for j, col := range parentTable.Columns {
				if strings.EqualFold(col.Name, parentColName) {
					lookupColIdx = j
					break
				}
			}
			if lookupColIdx < 0 {
				return sqlerrors.ErrForeignKeyViolation(
					fmt.Sprintf("referenced column %q not found in table %q", parentColName, fk.ReferencedTable))
			}

			// Look up the FK value in the parent table.
			// First try to use index on the referenced column.
			found := false
			if lookupColIdx >= 0 {
				// Find the index on the referenced column (could be primary key or secondary index)
				indexes, err := e.catalog.ListIndexes(fk.ReferencedTable)
				if err == nil {
					var pkIndex *catalogapi.IndexSchema
					for _, idx := range indexes {
						// Find index on the referenced column
						colIdx := findColumnIndexByName(parentTable.Columns, parentColName)
						if colIdx >= 0 && strings.EqualFold(idx.Column, parentColName) {
							// Try to use primary key index first
							if idx.Unique && parentTable.PrimaryKey != "" && strings.EqualFold(idx.Column, parentTable.PrimaryKey) {
								pkIndex = idx
								break
							}
							// Fall back to any unique index on this column
							if idx.Unique && pkIndex == nil {
								pkIndex = idx
							}
						}
					}
					// Use the found index or skip if none
					if pkIndex != nil {
						iter, err := e.indexEngine.Scan(parentTable.TableID, pkIndex.IndexID, encodingapi.OpEQ, fkVal)
						if err == nil {
							found = iter.Next()
							iter.Close()
						}
					}
				}
				// Fall back to full table scan if not found via index
				if !found {
					found, err = e.scanFKParentTable(parentTable, lookupColIdx, fkVal)
					if err != nil {
						return err
					}
				}
			}

			if !found {
				return sqlerrors.ErrForeignKeyViolation(
					fmt.Sprintf("foreign key constraint violated: %s.%s references %s.%s, but %v does not exist",
						tableName, colName, fk.ReferencedTable, parentColName, fkVal))
			}
		}
	}

	return nil
}

// scanFKParentTable performs a full scan of the parent table to find a matching row.
// This is a fallback when index scan is not available or returns an error.
func (e *executor) scanFKParentTable(parentTable *catalogapi.TableSchema, lookupColIdx int, fkVal catalogapi.Value) (bool, error) {
	// Use the table engine's Scan to iterate all rows
	iter, err := e.tableEngine.Scan(parentTable)
	if err != nil {
		return false, fmt.Errorf("%w: scan parent table for FK check: %v", executorapi.ErrExecFailed, err)
	}
	defer iter.Close()

	for iter.Next() {
		// Get the row to check the column value
		row := iter.Row()
		if row == nil {
			continue
		}
		if lookupColIdx < len(row.Values) {
			cmp := compareValues(row.Values[lookupColIdx], fkVal)
			if cmp == 0 {
				return true, nil // found matching row in parent table
			}
		}
	}
	if err := iter.Err(); err != nil {
		return false, fmt.Errorf("%w: scan parent table: %v", executorapi.ErrExecFailed, err)
	}
	return false, nil // no matching row found
}

// ─── FK Action Execution ───────────────────────────────────────────────────────

// checkFKRestrictBeforeUpdate checks RESTRICT and NO ACTION constraints BEFORE update.
// This prevents the update from succeeding if there are referencing rows.
func (e *executor) checkFKRestrictBeforeUpdate(parentTableName string, parentKeyValues []catalogapi.Value, fks []catalogapi.ForeignKeySchema) error {
	for _, fk := range fks {
		action := strings.ToUpper(fk.OnUpdate)
		// Check RESTRICT and NO ACTION (both block the update)
		if action != "RESTRICT" && action != "NO ACTION" {
			continue
		}

		// Get the child table
		childTable, err := e.catalog.GetTable(fk.TableName)
		if err != nil {
			return fmt.Errorf("%w: get child table %q for FK check: %v", executorapi.ErrExecFailed, fk.TableName, err)
		}

		// Find all FK column indices in the child table
		fkColIdxs := make([]int, len(fk.Columns))
		for i, colName := range fk.Columns {
			fkColIdxs[i] = findColumnIndexByName(childTable.Columns, colName)
			if fkColIdxs[i] < 0 {
				return fmt.Errorf("%w: FK column %q not found in child table %q", executorapi.ErrExecFailed, colName, fk.TableName)
			}
		}

		// Check if any referencing rows exist
		hasRows, err := e.hasReferencingRows(childTable, fkColIdxs, parentKeyValues)
		if err != nil {
			return err
		}
		if hasRows {
			return sqlerrors.ErrForeignKeyViolation(
				fmt.Sprintf("foreign key constraint violated: cannot update parent row in %q - %s action prevents update with existing references from %q", parentTableName, action, fk.TableName))
		}
	}
	return nil
}

// checkFKRestrictBeforeDelete checks RESTRICT and NO ACTION constraints BEFORE delete.
// This prevents the delete from succeeding if there are referencing rows.
// Used in non-transactional path where FK actions run after commit.
func (e *executor) checkFKRestrictBeforeDelete(parentTableName string, parentKeyValues []catalogapi.Value, fks []catalogapi.ForeignKeySchema) error {
	for _, fk := range fks {
		action := strings.ToUpper(fk.OnDelete)
		// Check RESTRICT and NO ACTION (both block the delete)
		if action != "RESTRICT" && action != "NO ACTION" {
			continue
		}

		// Get the child table
		childTable, err := e.catalog.GetTable(fk.TableName)
		if err != nil {
			return fmt.Errorf("%w: get child table %q for FK check: %v", executorapi.ErrExecFailed, fk.TableName, err)
		}

		// Find all FK column indices in the child table
		fkColIdxs := make([]int, len(fk.Columns))
		for i, colName := range fk.Columns {
			fkColIdxs[i] = findColumnIndexByName(childTable.Columns, colName)
			if fkColIdxs[i] < 0 {
				return fmt.Errorf("%w: FK column %q not found in child table %q", executorapi.ErrExecFailed, colName, fk.TableName)
			}
		}

		// Check if any referencing rows exist
		hasRows, err := e.hasReferencingRows(childTable, fkColIdxs, parentKeyValues)
		if err != nil {
			return err
		}
		if hasRows {
			return sqlerrors.ErrForeignKeyViolation(
				fmt.Sprintf("foreign key constraint violated: cannot delete parent row in %q - %s action prevents deletion with existing references from %q", parentTableName, action, fk.TableName))
		}
	}
	return nil
}

// executeFKDeleteActions handles ON DELETE actions for a deleted parent row.
// parentKeyValues: the values of the parent's primary key (referenced columns).
// fks: foreign keys from child tables that reference this parent table.
func (e *executor) executeFKDeleteActions(parentTableName string, parentKeyValues []catalogapi.Value, fks []catalogapi.ForeignKeySchema) error {
	for _, fk := range fks {
		// Skip RESTRICT and NO ACTION (already checked by checkFKRestrictBeforeDelete)
		action := strings.ToUpper(fk.OnDelete)
		if action == "" || action == "RESTRICT" || action == "NO ACTION" {
			continue
		}

		// Get the child table
		childTable, err := e.catalog.GetTable(fk.TableName)
		if err != nil {
			return fmt.Errorf("%w: get child table %q for FK action: %v", executorapi.ErrExecFailed, fk.TableName, err)
		}

		// Find all FK column indices in the child table
		fkColIdxs := make([]int, len(fk.Columns))
		for i, colName := range fk.Columns {
			fkColIdxs[i] = findColumnIndexByName(childTable.Columns, colName)
			if fkColIdxs[i] < 0 {
				return fmt.Errorf("%w: FK column %q not found in child table %q", executorapi.ErrExecFailed, colName, fk.TableName)
			}
		}

		switch action {
		case "CASCADE":
			if err := e.cascadeDeleteChildRows(childTable, fkColIdxs, parentKeyValues); err != nil {
				return err
			}
		case "SET NULL":
			if err := e.setNullChildFKColumns(childTable, fkColIdxs, parentKeyValues); err != nil {
				return err
			}
		}
	}
	return nil
}

// executeFKUpdateActions handles ON UPDATE actions for an updated parent row.
// parentTableName: the parent table name (for error messages)
// oldKeyValues: the old values of the parent's primary key
// newKeyValues: the new values of the parent's primary key
// fks: foreign keys from child tables that reference this parent table
func (e *executor) executeFKUpdateActions(parentTableName string, oldKeyValues, newKeyValues []catalogapi.Value, fks []catalogapi.ForeignKeySchema) error {
	for _, fk := range fks {
		// Skip RESTRICT and NO ACTION (already checked by checkFKRestrictBeforeUpdate)
		action := strings.ToUpper(fk.OnUpdate)
		if action == "" || action == "RESTRICT" || action == "NO ACTION" {
			continue
		}

		// Get the child table
		childTable, err := e.catalog.GetTable(fk.TableName)
		if err != nil {
			return fmt.Errorf("%w: get child table %q for FK action: %v", executorapi.ErrExecFailed, fk.TableName, err)
		}

		// Find all FK column indices in the child table
		fkColIdxs := make([]int, len(fk.Columns))
		for i, colName := range fk.Columns {
			fkColIdxs[i] = findColumnIndexByName(childTable.Columns, colName)
			if fkColIdxs[i] < 0 {
				return fmt.Errorf("%w: FK column %q not found in child table %q", executorapi.ErrExecFailed, colName, fk.TableName)
			}
		}

		switch action {
		case "CASCADE":
			if err := e.cascadeUpdateChildFKValues(childTable, fkColIdxs, oldKeyValues, newKeyValues); err != nil {
				return err
			}
		case "SET NULL":
			if err := e.setNullChildFKColumns(childTable, fkColIdxs, oldKeyValues); err != nil {
				return err
			}
		}
	}
	return nil
}

// hasReferencingRows checks if child table has any rows referencing the given parent key values.
// Supports multi-column foreign keys by comparing all columns.
func (e *executor) hasReferencingRows(childTable *catalogapi.TableSchema, fkColIdxs []int, parentKeyVals []catalogapi.Value) (bool, error) {
	iter, err := e.tableEngine.Scan(childTable)
	if err != nil {
		return false, fmt.Errorf("%w: scan child table for FK check: %v", executorapi.ErrExecFailed, err)
	}
	defer iter.Close()

	for iter.Next() {
		row := iter.Row()
		if row == nil {
			continue
		}
		// Check if ALL FK columns match (multi-column FK support)
		if matchesAllFKColumns(row.Values, fkColIdxs, parentKeyVals) {
			return true, nil
		}
	}
	return false, iter.Err()
}

// matchesAllFKColumns checks if a row's FK columns all match the parent key values.
// Used for multi-column foreign key matching.
func matchesAllFKColumns(rowValues []catalogapi.Value, fkColIdxs []int, parentKeyVals []catalogapi.Value) bool {
	for i, colIdx := range fkColIdxs {
		if colIdx >= len(rowValues) {
			return false
		}
		if compareValues(rowValues[colIdx], parentKeyVals[i]) != 0 {
			return false
		}
	}
	return true
}

// cascadeDeleteChildRows deletes all rows in child table where FK columns match parent key.
// Supports multi-column foreign keys by comparing all columns.
func (e *executor) cascadeDeleteChildRows(childTable *catalogapi.TableSchema, fkColIdxs []int, parentKeyVals []catalogapi.Value) error {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(childTable.Name)
	if err != nil {
		return fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for {
			rows, err := e.scanChildRowsForFKAction(childTable, fkColIdxs, parentKeyVals)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				oldRowVal := e.tableEngine.EncodeRow(row.Values)

				// Delete index entries
				for _, idx := range indexes {
					colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
					if colIdx < 0 {
						continue
					}
					val := row.Values[colIdx]
					idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, val, row.RowID)
					if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
						return fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
					}
					e.txnCtx.AddPendingWrite(idxKey, nil)
				}

				// Delete row
				rowKey := e.keyEncoder.EncodeRowKey(childTable.TableID, row.RowID)
				if err := e.store.DeleteWithXID(rowKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
					return fmt.Errorf("%w: delete: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(rowKey, oldRowVal)
			}
		}
		return nil
	}

	// Non-transactional path
	for {
		batch := e.store.NewWriteBatch()
		rows, err := e.scanChildRowsForFKAction(childTable, fkColIdxs, parentKeyVals)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
				if colIdx < 0 {
					continue
				}
				val := row.Values[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, val, row.RowID)
				if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
					batch.Discard()
					return fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
			}

			if err := e.tableEngine.DeleteFrom(childTable, batch, row.RowID); err != nil {
				batch.Discard()
				return fmt.Errorf("%w: delete: %v", executorapi.ErrExecFailed, err)
			}
		}

		if err := batch.Commit(); err != nil {
			batch.Discard()
			return fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
		}
	}
	return nil
}

// setNullChildFKColumns sets the FK columns to NULL for all child rows referencing the parent key.
// Supports multi-column foreign keys by setting all FK columns to NULL.
func (e *executor) setNullChildFKColumns(childTable *catalogapi.TableSchema, fkColIdxs []int, parentKeyVals []catalogapi.Value) error {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(childTable.Name)
	if err != nil {
		return fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	nullValue := catalogapi.Value{IsNull: true}

	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for {
			rows, err := e.scanChildRowsForFKAction(childTable, fkColIdxs, parentKeyVals)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				oldRowVal := e.tableEngine.EncodeRow(row.Values)

				// Delete old index entries for the FK columns
				for _, idx := range indexes {
					colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
					if colIdx < 0 {
						continue
					}
					// Check if this index column is one of the FK columns
					isFKColumn := false
					for _, fkIdx := range fkColIdxs {
						if colIdx == fkIdx {
							isFKColumn = true
							break
						}
					}
					if !isFKColumn {
						continue
					}
					val := row.Values[colIdx]
					idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, val, row.RowID)
					if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
						return fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
					}
					e.txnCtx.AddPendingWrite(idxKey, nil)
				}

				// Update row: set all FK columns to NULL
				newValues := make([]catalogapi.Value, len(row.Values))
				copy(newValues, row.Values)
				for _, fkIdx := range fkColIdxs {
					newValues[fkIdx] = nullValue
				}

				rowKey := e.keyEncoder.EncodeRowKey(childTable.TableID, row.RowID)
				newRowVal := e.tableEngine.EncodeRow(newValues)
				if err := e.store.PutWithXID(rowKey, newRowVal, xid); err != nil {
					return fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(rowKey, oldRowVal)

				// Insert new index entries for the FK columns (now NULL - skip if index doesn't handle NULLs)
				for _, idx := range indexes {
					colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
					if colIdx < 0 {
						continue
					}
					// Check if this index column is one of the FK columns
					isFKColumn := false
					for _, fkIdx := range fkColIdxs {
						if colIdx == fkIdx {
							isFKColumn = true
							break
						}
					}
					if !isFKColumn {
						continue
					}
					// Index on NULL value - encode and insert
					idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, nullValue, row.RowID)
					if err := e.store.PutWithXID(idxKey, nil, xid); err != nil {
						return fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
					}
					e.txnCtx.AddPendingWrite(idxKey, nil)
				}
			}
		}
		return nil
	}

	// Non-transactional path
	for {
		batch := e.store.NewWriteBatch()
		rows, err := e.scanChildRowsForFKAction(childTable, fkColIdxs, parentKeyVals)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			// Delete old index entries
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
				if colIdx < 0 {
					continue
				}
				// Check if this index column is one of the FK columns
				isFKColumn := false
				for _, fkIdx := range fkColIdxs {
					if colIdx == fkIdx {
						isFKColumn = true
						break
					}
				}
				if !isFKColumn {
					continue
				}
				val := row.Values[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, val, row.RowID)
				if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
					batch.Discard()
					return fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
			}

			// Update row: set all FK columns to NULL
			newValues := make([]catalogapi.Value, len(row.Values))
			copy(newValues, row.Values)
			for _, fkIdx := range fkColIdxs {
				newValues[fkIdx] = nullValue
			}

			rowKey := e.keyEncoder.EncodeRowKey(childTable.TableID, row.RowID)
			rowVal := e.tableEngine.EncodeRow(newValues)
			batch.Put(rowKey, rowVal)
		}

		if err := batch.Commit(); err != nil {
			batch.Discard()
			return fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
		}
	}
	return nil
}

// cascadeUpdateChildFKValues updates all child rows' FK columns from old values to new values.
// Supports multi-column foreign keys by updating all FK columns.
func (e *executor) cascadeUpdateChildFKValues(childTable *catalogapi.TableSchema, fkColIdxs []int, oldKeyVals, newKeyVals []catalogapi.Value) error {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(childTable.Name)
	if err != nil {
		return fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for {
			rows, err := e.scanChildRowsForFKAction(childTable, fkColIdxs, oldKeyVals)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				oldRowVal := e.tableEngine.EncodeRow(row.Values)

				// Delete old index entries for the FK columns
				for _, idx := range indexes {
					colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
					if colIdx < 0 {
						continue
					}
					// Check if this index column is one of the FK columns
					isFKColumn := false
					for _, fkIdx := range fkColIdxs {
						if colIdx == fkIdx {
							isFKColumn = true
							break
						}
					}
					if !isFKColumn {
						continue
					}
					val := row.Values[colIdx]
					idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, val, row.RowID)
					if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
						return fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
					}
					e.txnCtx.AddPendingWrite(idxKey, nil)
				}

				// Update row: change all FK columns to new values
				newValues := make([]catalogapi.Value, len(row.Values))
				copy(newValues, row.Values)
				for i, fkIdx := range fkColIdxs {
					newValues[fkIdx] = newKeyVals[i]
				}

				rowKey := e.keyEncoder.EncodeRowKey(childTable.TableID, row.RowID)
				newRowVal := e.tableEngine.EncodeRow(newValues)
				if err := e.store.PutWithXID(rowKey, newRowVal, xid); err != nil {
					return fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(rowKey, oldRowVal)

				// Insert new index entries
				for _, idx := range indexes {
					colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
					if colIdx < 0 {
						continue
					}
					// Check if this index column is one of the FK columns
					isFKColumn := false
					for _, fkIdx := range fkColIdxs {
						if colIdx == fkIdx {
							isFKColumn = true
							break
						}
					}
					if !isFKColumn {
						continue
					}
					// Find the corresponding new value for this FK column
					var newVal catalogapi.Value
					for i, fkIdx := range fkColIdxs {
						if colIdx == fkIdx {
							newVal = newKeyVals[i]
							break
						}
					}
					idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, newVal, row.RowID)
					if err := e.store.PutWithXID(idxKey, nil, xid); err != nil {
						return fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
					}
					e.txnCtx.AddPendingWrite(idxKey, nil)
				}
			}
		}
		return nil
	}

	// Non-transactional path
	for {
		batch := e.store.NewWriteBatch()
		rows, err := e.scanChildRowsForFKAction(childTable, fkColIdxs, oldKeyVals)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			// Delete old index entries
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(childTable.Columns, idx.Column)
				if colIdx < 0 {
					continue
				}
				// Check if this index column is one of the FK columns
				isFKColumn := false
				for _, fkIdx := range fkColIdxs {
					if colIdx == fkIdx {
						isFKColumn = true
						break
					}
				}
				if !isFKColumn {
					continue
				}
				val := row.Values[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(childTable.TableID, idx.IndexID, val, row.RowID)
				if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
					batch.Discard()
					return fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
			}

			// Update row: change all FK columns to new values
			newValues := make([]catalogapi.Value, len(row.Values))
			copy(newValues, row.Values)
			for i, fkIdx := range fkColIdxs {
				newValues[fkIdx] = newKeyVals[i]
			}

			rowKey := e.keyEncoder.EncodeRowKey(childTable.TableID, row.RowID)
			rowVal := e.tableEngine.EncodeRow(newValues)
			batch.Put(rowKey, rowVal)
		}

		if err := batch.Commit(); err != nil {
			batch.Discard()
			return fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
		}
	}
	return nil
}
// Supports multi-column foreign keys by comparing all columns.
// Returns a batch of matching rows (up to DMLBatchSize).
func (e *executor) scanChildRowsForFKAction(childTable *catalogapi.TableSchema, fkColIdxs []int, parentKeyVals []catalogapi.Value) ([]*engineapi.Row, error) {
	iter, err := e.tableEngine.ScanWithLimit(childTable, DMLBatchSize, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: scan child table: %v", executorapi.ErrExecFailed, err)
	}
	defer iter.Close()

	var rows []*engineapi.Row
	for iter.Next() {
		row := iter.Row()
		if row == nil {
			continue
		}
		// Check if ALL FK columns match (multi-column FK support)
		if matchesAllFKColumns(row.Values, fkColIdxs, parentKeyVals) {
			rowCopy := &engineapi.Row{
				RowID:  row.RowID,
				Values: make([]catalogapi.Value, len(row.Values)),
			}
			copy(rowCopy.Values, row.Values)
			rows = append(rows, rowCopy)
		}
	}
	return rows, iter.Err()
}

// getIndexValue returns the value to index for a given index schema and row.
// For simple column indexes, returns the column value.
// For expression indexes (ExprSQL non-empty), re-parses and evaluates the expression.
func getIndexValue(idx *catalogapi.IndexSchema, row *engineapi.Row, columns []catalogapi.ColumnDef, e *executor) (catalogapi.Value, error) {
	if idx.ExprSQL == "" {
		// Simple column index - use column value
		colIdx := findColumnIndexByName(columns, idx.Column)
		if colIdx < 0 {
			return catalogapi.Value{}, fmt.Errorf("column %s not found", idx.Column)
		}
		return row.Values[colIdx], nil
	}

	// Expression index - re-parse and evaluate the expression
	// Wrap in SELECT statement so parser accepts it as a valid SQL statement
	exprSQL := "SELECT " + idx.ExprSQL

	stmt, err := e.parser.Parse(exprSQL)
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("re-parse index expression %q: %v", idx.ExprSQL, err)
	}

	if sel, ok := stmt.(*parserapi.SelectStmt); ok && len(sel.Columns) > 0 {
		val, err := evalExpr(sel.Columns[0].Expr, row, columns, nil, e)
		if err != nil {
			return catalogapi.Value{}, fmt.Errorf("eval expr %q: %v", idx.ExprSQL, err)
		}
		return val, nil
	}

	return catalogapi.Value{}, fmt.Errorf("could not extract expression from %q", idx.ExprSQL)
}

// ─── Window Functions ─────────────────────────────────────────────────────────

// WindowFunctionResult stores pre-computed window function values for all rows.
type WindowFunctionResult struct {
	Values      []catalogapi.Value       // one value per row in the result set
	rowIndexMap map[*engineapi.Row]int   // maps row pointer to result index
}

// windowFunctions stores pre-computed window function results during SELECT execution.
type windowFunctions map[*parserapi.WindowFuncExpr]*WindowFunctionResult

// hasWindowFunctions checks if any SelectColumn contains a window function.
func hasWindowFunctions(cols []parserapi.SelectColumn) bool {
	for _, sc := range cols {
		if _, ok := sc.Expr.(*parserapi.WindowFuncExpr); ok {
			return true
		}
	}
	return false
}

// extractWindowFuncExprs extracts window function expressions from SelectColumns.
func extractWindowFuncExprs(cols []parserapi.SelectColumn) []*parserapi.WindowFuncExpr {
	var funcs []*parserapi.WindowFuncExpr
	for _, sc := range cols {
		if wf, ok := sc.Expr.(*parserapi.WindowFuncExpr); ok {
			funcs = append(funcs, wf)
		}
	}
	return funcs
}

// computeWindowFunctions computes all window function values for all rows.
// It groups rows by PARTITION BY, sorts by ORDER BY, and evaluates each window function.
func (e *executor) computeWindowFunctions(rows []*engineapi.Row, columns []catalogapi.ColumnDef,
	windowFuncs []*parserapi.WindowFuncExpr) error {

	if len(windowFuncs) == 0 {
		return nil
	}

	// Initialize window results map
	e.windowResults = make(map[*parserapi.WindowFuncExpr]*WindowFunctionResult)

	for _, wf := range windowFuncs {
		result, err := e.computeSingleWindowFunction(wf, rows, columns)
		if err != nil {
			return err
		}
		e.windowResults[wf] = result
	}

	return nil
}

// computeSingleWindowFunction computes one window function for all rows.
func (e *executor) computeSingleWindowFunction(wf *parserapi.WindowFuncExpr,
	rows []*engineapi.Row, columns []catalogapi.ColumnDef) (*WindowFunctionResult, error) {

	// Partition rows if PARTITION BY is specified
	partitions, err := e.partitionRowsForWindow(wf.Window, rows, columns)
	if err != nil {
		return nil, err
	}

	result := make([]catalogapi.Value, len(rows))
	rowIndexMap := make(map[*engineapi.Row]int)

	for _, partition := range partitions {
		// Sort partition by ORDER BY if specified
		sortedRows := partition.rows
		if wf.Window != nil && len(wf.Window.OrderBy) > 0 {
			sortedRows = e.sortWindowPartition(partition.rows, wf.Window.OrderBy, columns)
		}

		// Compute the window function for each row in the partition
		values, err := e.computeWindowFunctionForRows(wf, sortedRows, columns)
		if err != nil {
			return nil, err
		}

		// Map results back to original row positions
		for i, origIdx := range partition.indices {
			result[origIdx] = values[i]
			if i < len(sortedRows) {
				rowIndexMap[sortedRows[i]] = origIdx
			}
		}
	}

	return &WindowFunctionResult{Values: result, rowIndexMap: rowIndexMap}, nil
}

// windowPartition represents a partition of rows with their original indices
type windowPartition struct {
	rows    []*engineapi.Row
	indices []int
}

// partitionRowsForWindow groups rows by PARTITION BY columns
func (e *executor) partitionRowsForWindow(spec *parserapi.WindowSpec,
	rows []*engineapi.Row, columns []catalogapi.ColumnDef) ([]windowPartition, error) {

	if spec == nil || len(spec.PartitionBy) == 0 {
		// No partition - all rows in one group
		indices := make([]int, len(rows))
		rowCopy := make([]*engineapi.Row, len(rows))
		for i := range rows {
			indices[i] = i
			rowCopy[i] = rows[i]
		}
		return []windowPartition{{rows: rowCopy, indices: indices}}, nil
	}

	// Group rows by partition key
	groups := make(map[string][]int)
	for i, row := range rows {
		key, err := e.computeWindowPartitionKey(row, spec.PartitionBy, columns)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], i)
	}

	// Build partitions preserving original order
	var partitions []windowPartition
	for _, indices := range groups {
		p := windowPartition{
			indices: indices,
			rows:    make([]*engineapi.Row, len(indices)),
		}
		for j, idx := range indices {
			p.rows[j] = rows[idx]
		}
		partitions = append(partitions, p)
	}

	return partitions, nil
}

// computeWindowPartitionKey computes a unique key for a row based on PARTITION BY expressions
func (e *executor) computeWindowPartitionKey(row *engineapi.Row, partitionBy []parserapi.Expr,
	columns []catalogapi.ColumnDef) (string, error) {

	var key []byte
	for _, expr := range partitionBy {
		val, err := evalExpr(expr, row, columns, nil, e)
		if err != nil {
			return "", err
		}
		if val.IsNull {
			key = append(key, 0xFF)
		} else {
			key = append(key, e.keyEncoder.EncodeValue(val)...)
		}
		key = append(key, 0)
	}
	return string(key), nil
}

// sortWindowPartition sorts rows by ORDER BY columns for window frame evaluation
func (e *executor) sortWindowPartition(rows []*engineapi.Row,
	orderBy []*parserapi.OrderByClause, columns []catalogapi.ColumnDef) []*engineapi.Row {

	if len(orderBy) == 0 || len(rows) == 0 {
		return rows
	}

	// Create a sortable copy
	type sortableRow struct {
		row      *engineapi.Row
		sortKeys []catalogapi.Value
	}

	sortable := make([]sortableRow, len(rows))
	for i, row := range rows {
		keys := make([]catalogapi.Value, len(orderBy))
		for j, ob := range orderBy {
			colIdx := findColumnIndexByName(columns, ob.Column)
			if colIdx >= 0 && colIdx < len(row.Values) {
				keys[j] = row.Values[colIdx]
			} else {
				keys[j] = catalogapi.Value{IsNull: true}
			}
		}
		sortable[i] = sortableRow{row: row, sortKeys: keys}
	}

	// Sort
	sort.Slice(sortable, func(i, j int) bool {
		for k, ob := range orderBy {
			a := sortable[i].sortKeys[k]
			b := sortable[j].sortKeys[k]

			// NULL handling
			if a.IsNull && b.IsNull {
				continue
			}
			if a.IsNull {
				return !ob.Desc
			}
			if b.IsNull {
				return ob.Desc
			}

			cmp := compareValues(a, b)
			if cmp < 0 {
				return !ob.Desc
			}
			if cmp > 0 {
				return ob.Desc
			}
		}
		return false
	})

	// Extract sorted rows
	result := make([]*engineapi.Row, len(rows))
	for i, s := range sortable {
		result[i] = s.row
	}
	return result
}

// computeWindowFunctionForRows computes a single window function for a sorted partition
func (e *executor) computeWindowFunctionForRows(wf *parserapi.WindowFuncExpr,
	rows []*engineapi.Row, columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	switch strings.ToUpper(wf.Func) {
	case "ROW_NUMBER":
		return e.windowRowNumber(rows)
	case "RANK":
		return e.windowRank(rows, wf, columns)
	case "DENSE_RANK":
		return e.windowDenseRank(rows, wf, columns)
	case "LAG":
		return e.windowLag(rows, wf, columns)
	case "LEAD":
		return e.windowLead(rows, wf, columns)
	case "FIRST_VALUE":
		return e.windowFirstValue(rows, wf, columns)
	case "LAST_VALUE":
		return e.windowLastValue(rows, wf, columns)
	case "SUM", "AVG", "COUNT", "MIN", "MAX":
		return e.windowAggregate(wf, rows, columns)
	default:
		return nil, fmt.Errorf("%w: unsupported window function %s", executorapi.ErrExecFailed, wf.Func)
	}
}

// windowRowNumber returns sequential row numbers starting from 1
func (e *executor) windowRowNumber(rows []*engineapi.Row) ([]catalogapi.Value, error) {
	result := make([]catalogapi.Value, len(rows))
	for i := range rows {
		result[i] = catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(i + 1)}
	}
	return result, nil
}

// windowRank computes rank with gaps (e.g., 1, 1, 3, 4, 4, 6)
func (e *executor) windowRank(rows []*engineapi.Row, wf *parserapi.WindowFuncExpr,
	columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 {
		return result, nil
	}

	if wf.Window == nil || len(wf.Window.OrderBy) == 0 {
		for i := range result {
			result[i] = catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}
		}
		return result, nil
	}

	orderCol := wf.Window.OrderBy[0].Column
	desc := wf.Window.OrderBy[0].Desc
	colIdx := findColumnIndexByName(columns, orderCol)

	rank := 1
	for i := range rows {
		if i > 0 {
			prev := rows[i-1].Values[colIdx]
			curr := rows[i].Values[colIdx]
			cmp := compareValues(prev, curr)
			if (desc && cmp > 0) || (!desc && cmp < 0) {
				rank = i + 1
			}
		}
		result[i] = catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(rank)}
	}
	return result, nil
}

// windowDenseRank computes rank without gaps (e.g., 1, 1, 2, 3, 3, 4)
func (e *executor) windowDenseRank(rows []*engineapi.Row, wf *parserapi.WindowFuncExpr,
	columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 {
		return result, nil
	}

	if wf.Window == nil || len(wf.Window.OrderBy) == 0 {
		for i := range result {
			result[i] = catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}
		}
		return result, nil
	}

	orderCol := wf.Window.OrderBy[0].Column
	desc := wf.Window.OrderBy[0].Desc
	colIdx := findColumnIndexByName(columns, orderCol)

	denseRank := 1
	for i := range rows {
		if i > 0 {
			prev := rows[i-1].Values[colIdx]
			curr := rows[i].Values[colIdx]
			cmp := compareValues(prev, curr)
			if (desc && cmp > 0) || (!desc && cmp < 0) {
				denseRank++
			}
		}
		result[i] = catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(denseRank)}
	}
	return result, nil
}

// windowLag returns the value from a preceding row
func (e *executor) windowLag(rows []*engineapi.Row, wf *parserapi.WindowFuncExpr,
	columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 || len(wf.Args) == 0 {
		return result, nil
	}

	offset := 1
	if len(wf.Args) >= 2 {
		offsetVal, err := evalExpr(wf.Args[1], nil, columns, nil, nil)
		if err == nil && !offsetVal.IsNull && offsetVal.Type == catalogapi.TypeInt {
			offset = int(offsetVal.Int)
			if offset < 0 {
				offset = 0
			}
		}
	}

	var defaultVal catalogapi.Value
	if len(wf.Args) >= 3 {
		defaultVal, _ = evalExpr(wf.Args[2], nil, columns, nil, nil)
	} else {
		defaultVal = catalogapi.Value{IsNull: true}
	}

	for i := range rows {
		if i >= offset {
			val, err := evalExpr(wf.Args[0], rows[i-offset], columns, nil, e)
			if err != nil || val.IsNull {
				result[i] = defaultVal
			} else {
				result[i] = val
			}
		} else {
			result[i] = defaultVal
		}
	}
	return result, nil
}

// windowLead returns the value from a following row
func (e *executor) windowLead(rows []*engineapi.Row, wf *parserapi.WindowFuncExpr,
	columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 || len(wf.Args) == 0 {
		return result, nil
	}

	offset := 1
	if len(wf.Args) >= 2 {
		offsetVal, err := evalExpr(wf.Args[1], nil, columns, nil, nil)
		if err == nil && !offsetVal.IsNull && offsetVal.Type == catalogapi.TypeInt {
			offset = int(offsetVal.Int)
			if offset < 0 {
				offset = 0
			}
		}
	}

	var defaultVal catalogapi.Value
	if len(wf.Args) >= 3 {
		defaultVal, _ = evalExpr(wf.Args[2], nil, columns, nil, nil)
	} else {
		defaultVal = catalogapi.Value{IsNull: true}
	}

	for i := range rows {
		if i+offset < len(rows) {
			val, err := evalExpr(wf.Args[0], rows[i+offset], columns, nil, e)
			if err != nil || val.IsNull {
				result[i] = defaultVal
			} else {
				result[i] = val
			}
		} else {
			result[i] = defaultVal
		}
	}
	return result, nil
}

// windowFirstValue returns the first value in the window frame
func (e *executor) windowFirstValue(rows []*engineapi.Row, wf *parserapi.WindowFuncExpr,
	columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 || len(wf.Args) == 0 {
		return result, nil
	}

	frameStart, frameEnd := e.getWindowFrameBounds(wf.Window, 0, len(rows))

	for i := range rows {
		if frameStart <= frameEnd && frameStart < len(rows) {
			val, err := evalExpr(wf.Args[0], rows[frameStart], columns, nil, e)
			if err != nil {
				result[i] = catalogapi.Value{IsNull: true}
			} else {
				result[i] = val
			}
		} else {
			result[i] = catalogapi.Value{IsNull: true}
		}
	}
	return result, nil
}

// windowLastValue returns the last value in the window frame
func (e *executor) windowLastValue(rows []*engineapi.Row, wf *parserapi.WindowFuncExpr,
	columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 || len(wf.Args) == 0 {
		return result, nil
	}

	for i := range rows {
		frameStart, frameEnd := e.getWindowFrameBounds(wf.Window, i, len(rows))
		if frameStart <= frameEnd && frameEnd < len(rows) {
			val, err := evalExpr(wf.Args[0], rows[frameEnd], columns, nil, e)
			if err != nil {
				result[i] = catalogapi.Value{IsNull: true}
			} else {
				result[i] = val
			}
		} else {
			result[i] = catalogapi.Value{IsNull: true}
		}
	}
	return result, nil
}

// getWindowFrameBounds returns the [start, end] inclusive bounds for a window frame
func (e *executor) getWindowFrameBounds(spec *parserapi.WindowSpec, currentRow int, totalRows int) (int, int) {
	frameStart := 0
	frameEnd := currentRow

	if spec != nil {
		if spec.FrameStart.Type != "" {
			switch spec.FrameStart.Type {
			case "UNBOUNDED PRECEDING":
				frameStart = 0
			case "CURRENT ROW":
				frameStart = currentRow
			case "PRECEDING":
				if spec.FrameStart.Expr != nil {
					offset := e.evalFrameOffset(spec.FrameStart.Expr)
					frameStart = currentRow - offset
				}
			}
		}

		if spec.FrameEnd.Type != "" {
			switch spec.FrameEnd.Type {
			case "UNBOUNDED FOLLOWING":
				frameEnd = totalRows - 1
			case "CURRENT ROW":
				frameEnd = currentRow
			case "FOLLOWING":
				if spec.FrameEnd.Expr != nil {
					offset := e.evalFrameOffset(spec.FrameEnd.Expr)
					frameEnd = currentRow + offset
				}
			}
		}
	}

	if frameStart < 0 {
		frameStart = 0
	}
	if frameEnd >= totalRows {
		frameEnd = totalRows - 1
	}
	if frameStart >= totalRows {
		frameStart = totalRows - 1
		frameEnd = totalRows - 1
	}

	return frameStart, frameEnd
}

// evalFrameOffset evaluates a frame offset expression to an integer
func (e *executor) evalFrameOffset(expr parserapi.Expr) int {
	if expr == nil {
		return 0
	}
	val, err := evalExpr(expr, nil, nil, nil, nil)
	if err != nil || val.IsNull || val.Type != catalogapi.TypeInt {
		return 0
	}
	offset := int(val.Int)
	if offset < 0 {
		offset = 0
	}
	return offset
}

// windowAggregate computes SUM/AVG/COUNT/MIN/MAX over the window frame
func (e *executor) windowAggregate(wf *parserapi.WindowFuncExpr,
	rows []*engineapi.Row, columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {

	result := make([]catalogapi.Value, len(rows))
	if len(rows) == 0 {
		return result, nil
	}

	for i := range rows {
		frameStart, frameEnd := e.getWindowFrameBounds(wf.Window, i, len(rows))
		if frameStart > frameEnd || frameStart >= len(rows) {
			result[i] = catalogapi.Value{IsNull: true}
			continue
		}
		if frameEnd >= len(rows) {
			frameEnd = len(rows) - 1
		}

		frameRows := rows[frameStart : frameEnd+1]
		var val catalogapi.Value
		var err error

		switch strings.ToUpper(wf.Func) {
		case "SUM":
			val, err = e.windowSum(wf.Args, frameRows, columns)
		case "AVG":
			val, err = e.windowAvg(wf.Args, frameRows, columns)
		case "COUNT":
			val, err = e.windowCount(wf.Args, frameRows, columns)
		case "MIN":
			val, err = e.windowMin(wf.Args, frameRows, columns)
		case "MAX":
			val, err = e.windowMax(wf.Args, frameRows, columns)
		}

		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

func (e *executor) windowSum(args []parserapi.Expr, rows []*engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	if len(args) == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	var sum int64
	var count int64
	for _, row := range rows {
		val, err := evalExpr(args[0], row, columns, nil, e)
		if err != nil || val.IsNull || val.Type != catalogapi.TypeInt {
			continue
		}
		sum += val.Int
		count++
	}
	if count == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	return catalogapi.Value{Type: catalogapi.TypeInt, Int: sum}, nil
}

func (e *executor) windowAvg(args []parserapi.Expr, rows []*engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	if len(args) == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	var sum float64
	var count int64
	for _, row := range rows {
		val, err := evalExpr(args[0], row, columns, nil, e)
		if err != nil || val.IsNull {
			continue
		}
		if val.Type == catalogapi.TypeInt {
			sum += float64(val.Int)
			count++
		} else if val.Type == catalogapi.TypeFloat {
			sum += val.Float
			count++
		}
	}
	if count == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	return catalogapi.Value{Type: catalogapi.TypeFloat, Float: sum / float64(count)}, nil
}

func (e *executor) windowCount(args []parserapi.Expr, rows []*engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	if len(args) == 0 {
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(len(rows))}, nil
	}
	var count int64
	for _, row := range rows {
		val, err := evalExpr(args[0], row, columns, nil, e)
		if err == nil && !val.IsNull {
			count++
		}
	}
	return catalogapi.Value{Type: catalogapi.TypeInt, Int: count}, nil
}

func (e *executor) windowMin(args []parserapi.Expr, rows []*engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	if len(args) == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	colIdx := -1
	if ref, ok := args[0].(*parserapi.ColumnRef); ok {
		colIdx = findColumnIndexByName(columns, ref.Column)
	}
	if colIdx < 0 {
		var minVal catalogapi.Value
		hasVal := false
		for _, row := range rows {
			val, err := evalExpr(args[0], row, columns, nil, e)
			if err != nil || val.IsNull {
				continue
			}
			if !hasVal {
				minVal = val
				hasVal = true
			} else if compareValues(val, minVal) < 0 {
				minVal = val
			}
		}
		if !hasVal {
			return catalogapi.Value{IsNull: true}, nil
		}
		return minVal, nil
	}
	return minValueForColumn(rows, colIdx), nil
}

func (e *executor) windowMax(args []parserapi.Expr, rows []*engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	if len(args) == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	colIdx := -1
	if ref, ok := args[0].(*parserapi.ColumnRef); ok {
		colIdx = findColumnIndexByName(columns, ref.Column)
	}
	if colIdx < 0 {
		var maxVal catalogapi.Value
		hasVal := false
		for _, row := range rows {
			val, err := evalExpr(args[0], row, columns, nil, e)
			if err != nil || val.IsNull {
				continue
			}
			if !hasVal {
				maxVal = val
				hasVal = true
			} else if compareValues(val, maxVal) > 0 {
				maxVal = val
			}
		}
		if !hasVal {
			return catalogapi.Value{IsNull: true}, nil
		}
		return maxVal, nil
	}
	return maxValueForColumn(rows, colIdx), nil
}

// ─── Trigger Execution ─────────────────────────────────────────────

const maxTriggerDepth = 16

// fireTriggers executes matching triggers for a given table and event.
// timing is "BEFORE" or "AFTER", event is "INSERT", "UPDATE", or "DELETE".
// newRow/newCols provide NEW.* values; oldRow/oldCols provide OLD.* values.
func (e *executor) fireTriggers(tableName, timing, event string, newCols []catalogapi.ColumnDef, newVals []catalogapi.Value, oldCols []catalogapi.ColumnDef, oldVals []catalogapi.Value) error {
	// Check re-entrancy limit
	if e.triggerDepth >= maxTriggerDepth {
		return fmt.Errorf("%w: trigger recursion limit exceeded (max %d)", executorapi.ErrExecFailed, maxTriggerDepth)
	}

	triggers, err := e.catalog.ListTriggers(tableName)
	if err != nil {
		return fmt.Errorf("%w: listing triggers: %v", executorapi.ErrExecFailed, err)
	}

	for _, trigger := range triggers {
		// Check if trigger matches timing and event
		if trigger.Timing != timing || trigger.Event != event {
			continue
		}

		// Set NEW/OLD context for this trigger evaluation
		e.triggerNewCols = newCols
		e.triggerNewVals = newVals
		e.triggerOldCols = oldCols
		e.triggerOldVals = oldVals

		// Evaluate WHEN condition if present
		if trigger.WhenCond != "" {
			matches, err := e.evalTriggerWhen(trigger.WhenCond, trigger.Table)
			if err != nil {
				return fmt.Errorf("%w: trigger %s WHEN condition: %v", executorapi.ErrExecFailed, trigger.Name, err)
			}
			if !matches {
				continue // WHEN condition evaluated to false - skip this trigger
			}
		}

		// Execute trigger body
		if err := e.execTriggerBody(trigger.Body, trigger.Table); err != nil {
			return fmt.Errorf("%w: trigger %s: %v", executorapi.ErrExecFailed, trigger.Name, err)
		}
	}

	// Clear trigger context
	e.triggerNewCols = nil
	e.triggerNewVals = nil
	e.triggerOldCols = nil
	e.triggerOldVals = nil

	return nil
}

// evalTriggerWhen evaluates a WHEN condition expression with trigger context.
// Returns true if condition is satisfied or has no WHEN clause.
func (e *executor) evalTriggerWhen(whenSQL, tableName string) (bool, error) {
	if whenSQL == "" {
		return true, nil
	}

	// Parse the WHEN expression - wrap in SELECT for expression-only input
	wrappedSQL := "SELECT " + whenSQL
	stmt, err := e.parser.Parse(wrappedSQL)
	if err != nil {
		return false, fmt.Errorf("parse WHEN condition: %v", err)
	}

	// Extract the expression from the SELECT statement
	var whereExpr parserapi.Expr
	if selectStmt, ok := stmt.(*parserapi.SelectStmt); ok {
		whereExpr = selectStmt.Where
	}

	if whereExpr == nil {
		return true, nil // No expression = always matches
	}

	// Create a dummy row with trigger values for evaluation
	// We need to set up context so NEW./OLD. column refs can be resolved
	cols := e.triggerNewCols
	vals := e.triggerNewVals
	if len(vals) == 0 {
		cols = e.triggerOldCols
		vals = e.triggerOldVals
	}
	if len(vals) == 0 {
		return true, nil // No values to evaluate against
	}

	// Evaluate the WHERE expression
	dummyRow := &engineapi.Row{Values: vals}
	result, err := evalExpr(whereExpr, dummyRow, cols, nil, e)
	if err != nil {
		return false, err
	}

	// WHEN condition is true if result is not NULL and not 0/false
	return !result.IsNull && result.Int != 0, nil
}

// execTriggerBody parses and executes the trigger body SQL.
// bodySQL contains one or more statements separated by semicolons.
func (e *executor) execTriggerBody(bodySQL, tableName string) error {
	// Increment trigger depth for re-entrancy protection
	e.triggerDepth++
	defer func() { e.triggerDepth-- }()

	// Split by semicolon and execute each statement
	// Note: Simple split - doesn't handle string literals with semicolons
	statements := splitStatements(bodySQL)
	for _, sql := range statements {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}

		// Parse the trigger statement
		stmt, err := e.parser.Parse(sql)
		if err != nil {
			return fmt.Errorf("parse trigger body: %v", err)
		}

		// Plan the statement
		plan, err := e.planner.Plan(stmt)
		if err != nil {
			return fmt.Errorf("plan trigger statement: %v", err)
		}

		// Execute the trigger statement
		if plan != nil {
			if _, err := e.Execute(plan); err != nil {
				return err
			}
		}
	}
	return nil
}

// splitStatements splits a SQL string into individual statements.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inString := false
	escaped := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if escaped {
			current.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\'' && !inString {
			inString = true
			current.WriteByte(ch)
			continue
		}
		if inString {
			if ch == '\'' {
				// Check for escaped quote ''
				if i+1 < len(sql) && sql[i+1] == '\'' {
					current.WriteByte(ch)
					current.WriteByte('\'')
					i++
					continue
				}
				inString = false
			}
			current.WriteByte(ch)
			continue
		}
		if ch == ';' {
			stmts = append(stmts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}

	// Don't forget the last statement
	if s := strings.TrimSpace(current.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}
