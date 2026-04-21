// Package internal implements the SQL executor.
package internal

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"

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
	planner     plannerapi.Planner
	// For correlated subquery support - outer query context set before filter evaluation
	outerCols []catalogapi.ColumnDef
	outerVals []catalogapi.Value
	// TxnContext provides row-level locking for SELECT FOR UPDATE
	txnCtx txnapi.TxnContext
}

// New creates a new Executor.
func New(store kvstoreapi.Store, catalog catalogapi.CatalogManager,
	tableEngine engineapi.TableEngine, indexEngine engineapi.IndexEngine,
	planner plannerapi.Planner) *executor {
	return &executor{
		store:       store,
		catalog:     catalog,
		tableEngine: tableEngine,
		indexEngine: indexEngine,
		keyEncoder:  encoding.NewKeyEncoder(),
		planner:     planner,
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
func (e *executor) ExecuteWithTxn(plan plannerapi.Plan, txnCtx txnapi.TxnContext) (*executorapi.Result, error) {
	// Set transaction context for row locking
	e.txnCtx = txnCtx
	// Panic recovery: rollback transaction and re-panic so caller knows.
	// Deferred in LIFO order, so this runs BEFORE the nil assignment below.
	defer func() {
		if r := recover(); r != nil {
			if e.txnCtx != nil {
				e.txnCtx.Rollback()
			}
			panic(r)
		}
	}()
	// Ensure cleanup on any exit path — runs after panic recovery.
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

	switch p := plan.(type) {
	case *plannerapi.CreateTablePlan:
		return e.execCreateTable(p)
	case *plannerapi.DropTablePlan:
		return e.execDropTable(p)
	case *plannerapi.CreateIndexPlan:
		return e.execCreateIndex(p)
	case *plannerapi.DropIndexPlan:
		return e.execDropIndex(p)
	case *plannerapi.InsertPlan:
		return e.execInsert(p)
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
	default:
		return nil, fmt.Errorf("%w: unsupported plan type %T", executorapi.ErrExecFailed, plan)
	}
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

func (e *executor) execCreateIndex(plan *plannerapi.CreateIndexPlan) (*executorapi.Result, error) {
	// I-C1: check existence BEFORE allocating ID to avoid wasting IDs.
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

	// Now safe to allocate ID.
	indexID, err := e.nextID(metaNextIndexID)
	if err != nil {
		return nil, err
	}

	schema := plan.Schema
	schema.IndexID = indexID

	// CR-C: Backfill FIRST, then create catalog entry. If crash during backfill,
	// catalog has no stale entry. A catalog entry always means fully-built index.
	tbl, err := e.catalog.GetTable(schema.Table)
	if err != nil {
		return nil, fmt.Errorf("%w: backfill get table: %v", executorapi.ErrExecFailed, err)
	}
	colIdx := findColumnIndex(tbl, schema.Column)
	if colIdx < 0 {
		return nil, fmt.Errorf("%w: backfill column %q not found", executorapi.ErrExecFailed, schema.Column)
	}
	existingRows, err := e.tableScan(tbl, nil, nil, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: backfill scan: %v", executorapi.ErrExecFailed, err)
	}
	batch := e.store.NewWriteBatch()
	for _, row := range existingRows {
		val := row.Values[colIdx]
		idxKey := e.indexEngine.EncodeIndexKey(tbl.TableID, schema.IndexID, val, row.RowID)
		if err := e.indexEngine.InsertBatch(idxKey, batch); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: backfill insert index: %v", executorapi.ErrExecFailed, err)
		}
	}
	// M2: Add catalog entry to the SAME batch as index entries. Both are
	// committed atomically — if batch.Commit() succeeds, both index data AND
	// catalog entry are persisted. No orphan index data possible.
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
	// Get indexes for this table (to maintain index entries)
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// When in a transaction, use PutWithXID so writes share the transaction's XID.
	// This enables own-write visibility (txnMin==s.XID) and rollback (txnMax==txnXID).
	// When NOT in a transaction, use WriteBatch for auto-commit per statement.
	if e.txnCtx != nil {
		xid := e.txnCtx.XID()
		for _, row := range plan.Rows {
			// Check NOT NULL constraint before inserting.
			if err := checkNotNullConstraint(plan.Table.Columns, row); err != nil {
				return nil, err
			}

			// Check UNIQUE constraint before inserting.
			if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, row); err != nil {
				return nil, err
			}

			// Allocate rowID via table engine (in-memory only, no persistence yet).
			rowID, err := e.tableEngine.AllocRowID(plan.Table.TableID)
			if err != nil {
				return nil, fmt.Errorf("%w: alloc rowid: %v", executorapi.ErrExecFailed, err)
			}

			// Write row data directly with transaction's XID.
			rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, rowID)
			rowVal := e.tableEngine.EncodeRow(row)
			if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
				return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
			}
			e.txnCtx.AddPendingWrite(rowKey)

			// Write index entries with same XID.
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 {
					continue
				}
				val := row[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, val, rowID)
				if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
					return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey)
			}
		}

		// Persist counter with transaction's XID.
		// In-line the counter persistence logic (tableEngine is not used for transactional writes).
		tableID := plan.Table.TableID
		e.tableEngine.IncrementCounter(tableID) // advance in-memory counter
		metaKey := encodeMetaKeyLocal(tableID)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, e.tableEngine.GetCounter(tableID))
		if err := e.store.PutWithXID(metaKey, buf, xid); err != nil {
			return nil, fmt.Errorf("%w: persist counter: %v", executorapi.ErrExecFailed, err)
		}
		e.txnCtx.AddPendingWrite(metaKey)

		return &executorapi.Result{RowsAffected: int64(len(plan.Rows))}, nil
	}

	// Non-transactional path: use WriteBatch for auto-commit per statement.
	batch := e.store.NewWriteBatch()

	for _, row := range plan.Rows {
		// Check NOT NULL constraint before inserting.
		if err := checkNotNullConstraint(plan.Table.Columns, row); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check UNIQUE constraint before inserting.
		if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, row); err != nil {
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
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 {
				continue
			}
			val := row[colIdx]
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
			colIdx := findColumnIndex(plan.Table, idx.Column)
			if colIdx < 0 {
				continue
			}
			val := row[colIdx]
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

		// For each build match, check if there's an ON condition match
		for _, build := range buildMatches {
			if hplan.On != nil {
				// Evaluate ON condition with combined row
				var combinedVals []catalogapi.Value
				if buildIsLeft {
					// build=left, probe=right
					combinedVals = make([]catalogapi.Value, leftLen+rightLen)
					copy(combinedVals, build.Values)
					copy(combinedVals[leftLen:], probe.Values)
				} else {
					// build=right, probe=left
					combinedVals = make([]catalogapi.Value, leftLen+rightLen)
					copy(combinedVals, probe.Values)
					copy(combinedVals[leftLen:], build.Values)
				}
				combinedRow := &engineapi.Row{Values: combinedVals}
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
	result := make([]catalogapi.Value, leftLen+rightLen)
	copy(result, left.Values)
	copy(result[leftLen:], right.Values)
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
		seen := make(map[string]bool)
		var deduped [][]catalogapi.Value
		for _, row := range projected {
			var key strings.Builder
			for _, v := range row {
				if v.IsNull {
					key.WriteString("NULL")
				} else {
					key.WriteString(fmt.Sprintf("%v", v))
				}
				key.WriteByte(0)
			}
			if !seen[key.String()] {
				seen[key.String()] = true
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
	// LIMIT/OFFSET pushdown: only push down if there's no ORDER BY (ORDER BY requires all rows first)
	pushedDown := plan.OrderBy == nil && plan.GroupByExprs == nil
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
					val, err := evalExpr(sc.Expr, row, plan.Table.Columns, nil, nil)
					if err != nil {
						return nil, err
					}
					projected[rowIdx][i] = val
				}
			}
		}
	}

	// DISTINCT: deduplicate projected rows by concatenating all column values into a key
	if plan.Distinct {
		seen := make(map[string]bool)
		var deduped [][]catalogapi.Value
		for _, row := range projected {
			var key strings.Builder
			for _, v := range row {
				if v.IsNull {
					// Use type-aware NULL key to follow SQL standard semantics.
					// Different NULL types (INT vs TEXT) are not equivalent in comparisons.
					key.WriteString(fmt.Sprintf("NULL:%d", v.Type))
				} else {
					key.WriteString(fmt.Sprintf("%v", v))
				}
				key.WriteByte(0) // separator between columns
			}
			if !seen[key.String()] {
				seen[key.String()] = true
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
		seen := make(map[string]bool)
		var deduped [][]catalogapi.Value
		for _, row := range rows {
			var key strings.Builder
			for _, v := range row {
				if v.IsNull {
					// Use type-aware NULL key to follow SQL standard semantics.
					// Different NULL types (INT vs TEXT) are not equivalent in comparisons.
					key.WriteString(fmt.Sprintf("NULL:%d", v.Type))
				} else {
					key.WriteString(fmt.Sprintf("%v", v))
				}
				key.WriteByte(0) // separator between columns
			}
			if !seen[key.String()] {
				seen[key.String()] = true
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
	rightSet := make(map[string]bool)
	for _, row := range rightResult.Rows {
		var key strings.Builder
		for _, v := range row {
			if v.IsNull {
				key.WriteString("NULL")
			} else {
				key.WriteString(fmt.Sprintf("%v", v))
			}
			key.WriteByte(0)
		}
		rightSet[key.String()] = true
	}

	// Keep rows that appear in both left and right (with dedup)
	seen := make(map[string]bool)
	var result [][]catalogapi.Value
	for _, row := range leftResult.Rows {
		var key strings.Builder
		for _, v := range row {
			if v.IsNull {
				key.WriteString("NULL")
			} else {
				key.WriteString(fmt.Sprintf("%v", v))
			}
			key.WriteByte(0)
		}
		keyStr := key.String()
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
	rightSet := make(map[string]bool)
	for _, row := range rightResult.Rows {
		var key strings.Builder
		for _, v := range row {
			if v.IsNull {
				key.WriteString("NULL")
			} else {
				key.WriteString(fmt.Sprintf("%v", v))
			}
			key.WriteByte(0)
		}
		rightSet[key.String()] = true
	}

	// Keep rows that appear in left but NOT in right (with dedup)
	seen := make(map[string]bool)
	var result [][]catalogapi.Value
	for _, row := range leftResult.Rows {
		var key strings.Builder
		for _, v := range row {
			if v.IsNull {
				key.WriteString("NULL")
			} else {
				key.WriteString(fmt.Sprintf("%v", v))
			}
			key.WriteByte(0)
		}
		keyStr := key.String()
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

// execDelete deletes rows from a table. It iterates until all rows are deleted,
// scanning in batches to handle tables with more rows than any single scan can return.
// This ensures DELETE without WHERE deletes ALL rows, not just the first batch.
func (e *executor) execDelete(plan *plannerapi.DeletePlan) (*executorapi.Result, error) {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
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
					e.txnCtx.AddPendingWrite(idxKey)
				}

				// Delete row with transaction's XID.
				rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, row.RowID)
				if err := e.store.DeleteWithXID(rowKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
					return nil, fmt.Errorf("%w: delete: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(rowKey)
				count++
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
		}

		if err := batch.Commit(); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: commit: %v", executorapi.ErrExecFailed, err)
		}
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

func (e *executor) execUpdate(plan *plannerapi.UpdatePlan) (*executorapi.Result, error) {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
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
			// Delete old index entries for changed columns with transaction's XID.
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 || !changedCols[colIdx] {
					continue
				}
				oldVal := row.Values[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, row.RowID)
				if err := e.store.DeleteWithXID(idxKey, xid); err != nil && err != kvstoreapi.ErrKeyNotFound {
					return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey)
			}

			// Merge old values with new assignments
			newValues := make([]catalogapi.Value, len(row.Values))
			copy(newValues, row.Values)
			for colIdx, val := range plan.Assignments {
				newValues[colIdx] = val
			}

			// Check NOT NULL constraint before updating.
			if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
				return nil, err
			}

			// Check UNIQUE constraint before updating.
			if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues); err != nil {
				return nil, err
			}

			// Update row with transaction's XID.
			rowKey := e.keyEncoder.EncodeRowKey(plan.Table.TableID, row.RowID)
			rowVal := e.tableEngine.EncodeRow(newValues)
			if err := e.store.PutWithXID(rowKey, rowVal, xid); err != nil {
				return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
			}
			e.txnCtx.AddPendingWrite(rowKey)

			// Insert new index entries for changed columns.
			for _, idx := range indexes {
				colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
				if colIdx < 0 || !changedCols[colIdx] {
					continue
				}
				newVal := newValues[colIdx]
				idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, newVal, row.RowID)
				if err := e.store.PutWithXID(idxKey, nilValueForIndex(), xid); err != nil {
					return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
				}
				e.txnCtx.AddPendingWrite(idxKey)
			}

			count++
		}
		return &executorapi.Result{RowsAffected: count}, nil
	}

	// Non-transactional path: use WriteBatch for auto-commit per statement.
	batch := e.store.NewWriteBatch()

	for _, row := range rows {
		// Delete old index entries for changed columns.
		for _, idx := range indexes {
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			oldVal := row.Values[colIdx]
			idxKey := e.indexEngine.EncodeIndexKey(plan.Table.TableID, idx.IndexID, oldVal, row.RowID)
			if err := e.indexEngine.DeleteBatch(idxKey, batch); err != nil {
				batch.Discard()
				return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
			}
		}

		// Merge old values with new assignments
		newValues := make([]catalogapi.Value, len(row.Values))
		copy(newValues, row.Values)
		for colIdx, val := range plan.Assignments {
			newValues[colIdx] = val
		}

		// Check NOT NULL constraint before updating.
		if err := checkNotNullConstraint(plan.Table.Columns, newValues); err != nil {
			batch.Discard()
			return nil, err
		}

		// Check UNIQUE constraint before updating.
		if err := e.checkUniqueConstraint(plan.Table.Name, plan.Table.Columns, newValues); err != nil {
			batch.Discard()
			return nil, err
		}

		// Update row via batch.
		if err := e.tableEngine.UpdateIn(plan.Table, batch, row.RowID, newValues); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
		}

		// Insert new index entries for changed columns.
		for _, idx := range indexes {
			colIdx := findColumnIndexByName(plan.Table.Columns, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			newVal := newValues[colIdx]
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
// Returns nil if all UNIQUE constraints are satisfied, or ErrUniqueViolation
// if any UNIQUE column has a duplicate value.
func (e *executor) checkUniqueConstraint(tableName string, columns []catalogapi.ColumnDef, values []catalogapi.Value) error {
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

		found := iter.Next()
		err = iter.Err()
		iter.Close()

		if err != nil {
			return err
		}

		if found {
			// Found a duplicate
			return sqlerrors.ErrUniqueViolation(tableName, idx.Column)
		}
	}
	return nil
}
