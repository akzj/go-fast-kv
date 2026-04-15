// Package internal implements the SQL executor.
package internal

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
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
	existingRows, err := e.tableScan(tbl, nil, nil)
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

	// I-C3: use a single WriteBatch for all rows and index entries.
	batch := e.store.NewWriteBatch()

	for _, row := range plan.Rows {
		// Insert row into the shared batch.
		rowID, err := e.tableEngine.InsertInto(plan.Table, batch, row)
		if err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: insert: %v", executorapi.ErrExecFailed, err)
		}

		// Insert index entries into the same batch.
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
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
		leftRows, err = e.scanRows(jplan.LeftTable, left, nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("execJoinSelect: unexpected left type %T", jplan.Left)
	}

	rightRows, err := e.scanRows(jplan.RightTable, jplan.Right, nil)
	if err != nil {
		return nil, err
	}

	colNames := make([]string, 0, len(jplan.LeftSchema)+len(jplan.RightSchema))
	combinedCols := make([]catalogapi.ColumnDef, 0, len(jplan.LeftSchema)+len(jplan.RightSchema))
	for _, c := range jplan.LeftSchema {
		colNames = append(colNames, c.Name)
		col := *c
		col.Table = jplan.LeftTable.Name
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

	// Apply WHERE filter on merged rows
	if plan.Filter != nil {
		mergedRows = filterJoinRows(mergedRows, plan.Filter, combinedCols)
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
			grouped = filterRows(grouped, plan.Having, plan.Table.Columns, nil)
		}

		// ORDER BY on grouped rows
		if plan.OrderBy != nil {
			sortRawRows(grouped, plan.OrderBy)
		}

		// LIMIT on grouped rows
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

	if plan.Limit >= 0 && plan.Limit < len(mergedRows) {
		mergedRows = mergedRows[:plan.Limit]
	}

	projected, projCols := projectJoinRows(mergedRows, colNames, plan)

	return &executorapi.Result{
		Columns: projCols,
		Rows:    projected,
	}, nil
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
		leftRows, err = e.scanRows(jplan.LeftTable, left, nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("execJoin: unexpected left type %T", jplan.Left)
	}

	rightRows, err := e.scanRows(jplan.RightTable, jplan.Right, nil)
	if err != nil {
		return nil, err
	}

	// Build colNames and combinedCols with table names tagged
	colNames := make([]string, 0, len(jplan.LeftSchema)+len(jplan.RightSchema))
	combinedCols := make([]catalogapi.ColumnDef, 0, len(jplan.LeftSchema)+len(jplan.RightSchema))
	for _, c := range jplan.LeftSchema {
		colNames = append(colNames, c.Name)
		col := *c
		col.Table = jplan.LeftTable.Name
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
	result, err := evalExpr(jplan.On, combinedRow, combinedCols, subqueryResults)
	if err != nil {
		return false
	}
	return isTruthy(result)
}

// collectRows executes a scan plan and returns all rows.
func (e *executor) collectRows(table *catalogapi.TableSchema, scan plannerapi.ScanPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	return e.scanRows(table, scan, subqueryResults)
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
	}
}

// filterJoinRows applies a WHERE filter to merged join rows.
func filterJoinRows(rows [][]catalogapi.Value, filter parserapi.Expr, columns []catalogapi.ColumnDef) [][]catalogapi.Value {
	if filter == nil || len(rows) == 0 {
		return rows
	}
	filtered := rows[:0]
	for _, row := range rows {
		engineRow := &engineapi.Row{Values: row}
		match, err := matchFilter(filter, engineRow, columns, nil)
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
func sortJoinRows(rows [][]catalogapi.Value, orderBy *plannerapi.OrderByPlan, columns []catalogapi.ColumnDef) {
	if orderBy == nil || len(rows) == 0 {
		return
	}
	colIdx := orderBy.ColumnIndex
	desc := orderBy.Desc
	sort.Slice(rows, func(i, j int) bool {
		a := rows[i][colIdx]
		b := rows[j][colIdx]
		if a.IsNull && b.IsNull {
			return false
		}
		if a.IsNull {
			return true
		}
		if b.IsNull {
			return false
		}
		cmp := compareValues(a, b)
		if desc {
			cmp = -cmp
		}
		return cmp < 0
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

// precomputeSubqueries finds all SubqueryExpr nodes in the plan's WHERE/HAVING
// and executes them, caching results in subqueryResults.
// Scalar subqueries (used in comparisons) store a single catalogapi.Value.
// List subqueries (used in IN) store a []catalogapi.Value.
// This is called with the OUTER subqueryResults map so that nested subqueries
// don't re-execute infinitely (they share the parent's cache).
func (e *executor) precomputeSubqueries(plan *plannerapi.SelectPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) {
	// Collect all expressions that might contain SubqueryExpr
	var exprs []parserapi.Expr
	if plan.Filter != nil {
		exprs = append(exprs, plan.Filter)
	}
	if plan.Having != nil {
		exprs = append(exprs, plan.Having)
	}

	for _, root := range exprs {
		walkExpr(root, func(expr parserapi.Expr) {
			sq, ok := expr.(*parserapi.SubqueryExpr)
			if !ok {
				return
			}
			// Already computed by an ancestor execSelect call?
			if _, exists := subqueryResults[sq]; exists {
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
					subqueryResults[sq] = result.Rows[0][0]
				} else {
					subqueryResults[sq] = catalogapi.Value{IsNull: true}
				}
			}
		})
	}
}

func (e *executor) execSelect(plan *plannerapi.SelectPlan) (*executorapi.Result, error) {
	// Pre-compute subquery results BEFORE scanning — needed for filter during scan.
	subqueryResults := make(map[*parserapi.SubqueryExpr]interface{})
	e.precomputeSubqueries(plan, subqueryResults)

	// Collect matching rows via scan (filter during scan uses precomputed subquery results)
	rows, err := e.scanRows(plan.Table, plan.Scan, subqueryResults)
	if err != nil {
		return nil, err
	}

	// Apply residual filter from SelectPlan (handles index scan + residual)
	if plan.Filter != nil {
		rows = filterRows(rows, plan.Filter, plan.Table.Columns, subqueryResults)
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
			rows = filterRows(rows, plan.Having, plan.Table.Columns, subqueryResults)
		}
	}

	// ORDER BY (sort raw rows BEFORE projection so all columns are available)
	if plan.OrderBy != nil {
		sortRawRows(rows, plan.OrderBy)
	}

	// LIMIT (apply before projection for efficiency)
	if plan.Limit >= 0 && plan.Limit < len(rows) {
		rows = rows[:plan.Limit]
	}

	// Scalar aggregate in SELECT (no GROUP BY): compute across all rows.
	if plan.SelectColumns != nil && plan.GroupByExprs == nil {
		hasScalarAgg := false
		for _, sc := range plan.SelectColumns {
			if _, ok := sc.Expr.(*parserapi.AggregateCallExpr); ok {
				hasScalarAgg = true
				break
			}
		}
		if hasScalarAgg {
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
				} else if ref, ok := sc.Expr.(*parserapi.ColumnRef); ok {
					idx := findColumnIndexByName(plan.Table.Columns, ref.Column)
					if len(rows) > 0 && idx >= 0 && idx < len(rows[0].Values) {
						vals[i] = rows[0].Values[idx]
					}
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
				if ref, ok := sc.Expr.(*parserapi.ColumnRef); ok {
					colNames[i] = ref.Column
				} else {
					colNames[i] = "?"
				}
			}
		} else {
			colNames = []string{"?"}
		}
	} else {
		colNames = buildColumnNames(plan.Table, plan.Columns)
		projected = projectRows(rows, plan.Columns)
	}

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
	rows, err := e.scanRowsForDML(plan.Table, plan.Scan, nil)
	if err != nil {
		return nil, err
	}

	// F-W3: use a single WriteBatch for all deletes.
	batch := e.store.NewWriteBatch()

	var count int64
	for _, row := range rows {
		// Delete index entries (auto-commit per entry — see TODO in execInsert).
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
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

	rows, err := e.scanRowsForDML(plan.Table, plan.Scan, nil)
	if err != nil {
		return nil, err
	}

	// F-W3: use a single WriteBatch for all updates.
	batch := e.store.NewWriteBatch()

	var count int64
	for _, row := range rows {
		// Delete old index entries for changed columns.
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
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

		// Update row via batch.
		if err := e.tableEngine.UpdateIn(plan.Table, batch, row.RowID, newValues); err != nil {
			batch.Discard()
			return nil, fmt.Errorf("%w: update: %v", executorapi.ErrExecFailed, err)
		}

		// Insert new index entries for changed columns.
		for _, idx := range indexes {
			colIdx := findColumnIndex(plan.Table, idx.Column)
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
		val, err := evalExpr(expr, row, columns, nil)
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
		for _, row := range rows {
			if idx >= 0 && idx < len(row.Values) {
				val := row.Values[idx]
				if !val.IsNull && val.Type == catalogapi.TypeInt {
					sum += val.Int
				}
			}
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
		default:
			return nil, fmt.Errorf("%w: GROUP BY SELECT expression must be column or aggregate", executorapi.ErrExecFailed)
		}
	}
	return result, nil
}
