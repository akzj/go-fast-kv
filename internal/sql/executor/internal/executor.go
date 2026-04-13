// Package internal implements the SQL executor.
package internal

import (
	"encoding/binary"
	"fmt"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
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
	indexEngine engineapi.IndexEngine
}

// New creates a new Executor.
func New(store kvstoreapi.Store, catalog catalogapi.CatalogManager,
	tableEngine engineapi.TableEngine, indexEngine engineapi.IndexEngine) *executor {
	return &executor{
		store:       store,
		catalog:     catalog,
		tableEngine: tableEngine,
		indexEngine: indexEngine,
	}
}

// Execute dispatches a plan to the appropriate handler.
func (e *executor) Execute(plan plannerapi.Plan) (*executorapi.Result, error) {
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
	case *plannerapi.SelectPlan:
		return e.execSelect(p)
	case *plannerapi.DeletePlan:
		return e.execDelete(p)
	case *plannerapi.UpdatePlan:
		return e.execUpdate(p)
	default:
		return nil, fmt.Errorf("%w: unsupported plan type %T", executorapi.ErrExecFailed, plan)
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
	// Assign table ID
	tableID, err := e.nextID(metaNextTableID)
	if err != nil {
		return nil, err
	}

	schema := plan.Schema
	schema.TableID = tableID

	err = e.catalog.CreateTable(schema)
	if err != nil {
		if err == catalogapi.ErrTableExists && plan.IfNotExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
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
	indexID, err := e.nextID(metaNextIndexID)
	if err != nil {
		return nil, err
	}

	schema := plan.Schema
	schema.IndexID = indexID

	err = e.catalog.CreateIndex(schema)
	if err != nil {
		if err == catalogapi.ErrIndexExists && plan.IfNotExists {
			return &executorapi.Result{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	// Backfill: index all existing rows in the table.
	tbl, err := e.catalog.GetTable(schema.Table)
	if err != nil {
		return nil, fmt.Errorf("%w: backfill get table: %v", executorapi.ErrExecFailed, err)
	}
	colIdx := findColumnIndex(tbl, schema.Column)
	if colIdx < 0 {
		return nil, fmt.Errorf("%w: backfill column %q not found", executorapi.ErrExecFailed, schema.Column)
	}
	existingRows, err := e.tableScan(tbl, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: backfill scan: %v", executorapi.ErrExecFailed, err)
	}
	for _, row := range existingRows {
		val := row.Values[colIdx]
		if err := e.indexEngine.Insert(&schema, tbl.TableID, schema.IndexID, val, row.RowID); err != nil {
			return nil, fmt.Errorf("%w: backfill insert index: %v", executorapi.ErrExecFailed, err)
		}
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

	for _, row := range plan.Rows {
		rowID, err := e.tableEngine.Insert(plan.Table, row)
		if err != nil {
			return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
		}

		// Insert index entries
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
			if colIdx < 0 {
				continue
			}
			val := row[colIdx]
			if err := e.indexEngine.Insert(idx, plan.Table.TableID, idx.IndexID, val, rowID); err != nil {
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}
	}

	return &executorapi.Result{RowsAffected: int64(len(plan.Rows))}, nil
}

func (e *executor) execSelect(plan *plannerapi.SelectPlan) (*executorapi.Result, error) {
	// Collect matching rows
	rows, err := e.scanRows(plan.Table, plan.Scan, plan.Filter)
	if err != nil {
		return nil, err
	}

	// ORDER BY (sort raw rows BEFORE projection so all columns are available)
	if plan.OrderBy != nil {
		sortRawRows(rows, plan.OrderBy)
	}

	// LIMIT (apply before projection for efficiency)
	if plan.Limit >= 0 && plan.Limit < len(rows) {
		rows = rows[:plan.Limit]
	}

	// Project columns
	colNames := buildColumnNames(plan.Table, plan.Columns)
	projected := projectRows(rows, plan.Columns)

	return &executorapi.Result{
		Columns: colNames,
		Rows:    projected,
	}, nil
}

func (e *executor) execDelete(plan *plannerapi.DeletePlan) (*executorapi.Result, error) {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Scan for rows to delete
	rows, err := e.scanRowsForDML(plan.Table, plan.Scan)
	if err != nil {
		return nil, err
	}

	var count int64
	for _, row := range rows {
		// Delete index entries
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
			if colIdx < 0 {
				continue
			}
			val := row.Values[colIdx]
			if err := e.indexEngine.Delete(idx, plan.Table.TableID, idx.IndexID, val, row.RowID); err != nil {
				return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
			}
		}

		// Delete row
		if err := e.tableEngine.Delete(plan.Table, row.RowID); err != nil {
			return nil, fmt.Errorf("%w: delete: %v", executorapi.ErrExecFailed, err)
		}
		count++
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

func (e *executor) execUpdate(plan *plannerapi.UpdatePlan) (*executorapi.Result, error) {
	// Get indexes for cleanup
	indexes, err := e.catalog.ListIndexes(plan.Table.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: listing indexes: %v", executorapi.ErrExecFailed, err)
	}

	// Build set of changed column indices for index maintenance
	changedCols := make(map[int]bool, len(plan.Assignments))
	for colIdx := range plan.Assignments {
		changedCols[colIdx] = true
	}

	rows, err := e.scanRowsForDML(plan.Table, plan.Scan)
	if err != nil {
		return nil, err
	}

	var count int64
	for _, row := range rows {
		// Delete old index entries for changed columns
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			oldVal := row.Values[colIdx]
			if err := e.indexEngine.Delete(idx, plan.Table.TableID, idx.IndexID, oldVal, row.RowID); err != nil {
				return nil, fmt.Errorf("%w: index delete: %v", executorapi.ErrExecFailed, err)
			}
		}

		// Merge old values with new assignments
		newValues := make([]catalogapi.Value, len(row.Values))
		copy(newValues, row.Values)
		for colIdx, val := range plan.Assignments {
			newValues[colIdx] = val
		}

		// Update row
		if err := e.tableEngine.Update(plan.Table, row.RowID, newValues); err != nil {
			return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
		}

		// Insert new index entries for changed columns
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
			if colIdx < 0 || !changedCols[colIdx] {
				continue
			}
			newVal := newValues[colIdx]
			if err := e.indexEngine.Insert(idx, plan.Table.TableID, idx.IndexID, newVal, row.RowID); err != nil {
				return nil, fmt.Errorf("%w: index insert: %v", executorapi.ErrExecFailed, err)
			}
		}

		count++
	}

	return &executorapi.Result{RowsAffected: count}, nil
}

