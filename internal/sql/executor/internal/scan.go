package internal

import (
	"fmt"
	"sort"
	"strings"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// ─── Scan Helpers ───────────────────────────────────────────────────

// scanRows collects rows matching a scan plan.
func (e *executor) scanRows(table *catalogapi.TableSchema, scan plannerapi.ScanPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	switch s := scan.(type) {
	case *plannerapi.TableScanPlan:
		return e.tableScan(table, s.Filter, subqueryResults)
	case *plannerapi.IndexScanPlan:
		return e.indexScan(table, s, subqueryResults)
	case *plannerapi.IndexOnlyScanPlan:
		return e.indexOnlyScan(table, s, subqueryResults)
	case *plannerapi.IndexRangePlan:
		return e.indexRangeScan(s, subqueryResults)
	case *plannerapi.DerivedTableScanPlan:
		// DerivedTableScanPlan is handled in execSelect before scanRows is called.
		// This case should not be reached, but return empty rows to be safe.
		return []*engineapi.Row{}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported scan type %T", executorapi.ErrExecFailed, scan)
	}
}

// scanRowsForDML delegates to scanRows (consolidation: S1).
func (e *executor) scanRowsForDML(table *catalogapi.TableSchema, scan plannerapi.ScanPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	return e.scanRows(table, scan, subqueryResults)
}

// filterRows applies a residual filter to already-scanned rows (used by execSelect for SelectPlan.Filter).
func (e *executor) filterRows(rows []*engineapi.Row, filter parserapi.Expr, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) []*engineapi.Row {
	if filter == nil || len(rows) == 0 {
		return rows
	}
	// Only set outerVals per row when the filter contains correlated subqueries.
	// This is the mechanism by which the outer row's values are propagated to
	// the subquery's evalColumnRef. Without correlated subqueries, setting
	// outerVals would corrupt the outer context for nested executions.
	setOuterVals := hasCorrelatedSubquery(filter)
	filtered := rows[:0]
	for _, row := range rows {
		if setOuterVals {
			e.outerVals = row.Values
		}
		match, err := matchFilter(filter, row, columns, subqueryResults, e)
		if err != nil {
			continue
		}
		if match {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// tableScan iterates all rows and applies a filter.
func (e *executor) tableScan(table *catalogapi.TableSchema, filter parserapi.Expr,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	iter, err := e.tableEngine.Scan(table)
	if err != nil {
		return nil, fmt.Errorf("%w: scan: %v", executorapi.ErrExecFailed, err)
	}
	defer iter.Close()

	// Build columns with Table field populated so that evalColumnRef can
	// distinguish table-qualified references (e.g., orders.user_id vs users.id).
	scanCols := make([]catalogapi.ColumnDef, len(table.Columns))
	for i, col := range table.Columns {
		scanCols[i] = col
		if scanCols[i].Table == "" {
			scanCols[i].Table = table.Name
		}
	}

	var rows []*engineapi.Row
	for iter.Next() {
		row := iter.Row()

		// Apply filter if present
		if filter != nil {
			pass, err := matchFilter(filter, row, scanCols, subqueryResults, e)
			if err != nil {
				return nil, err
			}
			if !pass {
				continue
			}
		}

		// Copy row to avoid iterator reuse issues
		rowCopy := &engineapi.Row{
			RowID:  row.RowID,
			Values: make([]catalogapi.Value, len(row.Values)),
		}
		copy(rowCopy.Values, row.Values)
		rows = append(rows, rowCopy)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan iteration: %v", executorapi.ErrExecFailed, err)
	}

	return rows, nil
}

// indexScan uses an index to find matching rows.
func (e *executor) indexScan(table *catalogapi.TableSchema, scan *plannerapi.IndexScanPlan, subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	rowIDIter, err := e.indexEngine.Scan(scan.TableID, scan.IndexID, scan.Op, scan.Value)
	if err != nil {
		return nil, fmt.Errorf("%w: index scan: %v", executorapi.ErrExecFailed, err)
	}
	defer rowIDIter.Close()

	var rows []*engineapi.Row
	for rowIDIter.Next() {
		rowID := rowIDIter.RowID()
		row, err := e.tableEngine.Get(table, rowID)
		if err != nil {
			if err == engineapi.ErrRowNotFound {
				continue // stale index entry
			}
			return nil, fmt.Errorf("%w: get row: %v", executorapi.ErrExecFailed, err)
		}

		// Apply residual filter
		if scan.ResidualFilter != nil {
			pass, err := matchFilter(scan.ResidualFilter, row, table.Columns, subqueryResults, e)
			if err != nil {
				return nil, err
			}
			if !pass {
				continue
			}
		}

		rows = append(rows, row)
	}
	if err := rowIDIter.Err(); err != nil {
		return nil, fmt.Errorf("%w: index scan iteration: %v", executorapi.ErrExecFailed, err)
	}

	return rows, nil
}

// indexOnlyScan uses an index to satisfy a query without touching table pages.
// All required columns are available in the index itself.
func (e *executor) indexOnlyScan(table *catalogapi.TableSchema, scan *plannerapi.IndexOnlyScanPlan,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	rowIDIter, err := e.indexEngine.Scan(scan.TableID, scan.IndexID, scan.Op, scan.Value)
	if err != nil {
		return nil, fmt.Errorf("%w: index only scan: %v", executorapi.ErrExecFailed, err)
	}
	defer rowIDIter.Close()

	// Get raw key for decoding - the iterator has Key() method
	kvIter, ok := rowIDIter.(interface{ Key() []byte })
	if !ok {
		return nil, fmt.Errorf("%w: iterator does not support key access", executorapi.ErrExecFailed)
	}

	var rows []*engineapi.Row
	for rowIDIter.Next() {
		// Decode index key to extract the indexed column value
		_, _, colValue, _, err := e.keyEncoder.DecodeIndexKey(kvIter.Key())
		if err != nil {
			return nil, fmt.Errorf("%w: decode index key: %v", executorapi.ErrExecFailed, err)
		}

		// Build row with just the indexed column value
		row := &engineapi.Row{
			RowID:  rowIDIter.RowID(),
			Values: []catalogapi.Value{colValue},
		}

		// Apply residual filter if any
		if scan.ResidualFilter != nil {
			pass, err := matchFilter(scan.ResidualFilter, row, table.Columns, subqueryResults, e)
			if err != nil {
				return nil, err
			}
			if !pass {
				continue
			}
		}

		rows = append(rows, row)
	}
	return rows, rowIDIter.Err()
}

// indexRangeScan uses an index range scan for LIKE 'prefix%' optimization.
func (e *executor) indexRangeScan(scan *plannerapi.IndexRangePlan, subqueryResults map[*parserapi.SubqueryExpr]interface{}) ([]*engineapi.Row, error) {
	startVal := catalogapi.Value{Type: catalogapi.TypeText, Text: scan.StartPrefix}
	endVal := catalogapi.Value{Type: catalogapi.TypeText, Text: scan.EndPrefix}
	iter, err := e.indexEngine.ScanRange(scan.TableID, scan.IndexID, &startVal, &endVal)
	if err != nil {
		return nil, fmt.Errorf("%w: index range scan: %v", executorapi.ErrExecFailed, err)
	}
	defer iter.Close()

	table, err := e.catalog.GetTable(scan.Index.Table)
	if err != nil {
		return nil, err
	}

	var rows []*engineapi.Row
	for iter.Next() {
		rowID := iter.RowID()
		row, err := e.tableEngine.Get(table, rowID)
		if err != nil {
			if err == engineapi.ErrRowNotFound {
				continue // stale index entry
			}
			return nil, fmt.Errorf("%w: get row: %v", executorapi.ErrExecFailed, err)
		}
		if scan.ResidualFilter != nil {
			pass, err := matchFilter(scan.ResidualFilter, row, table.Columns, subqueryResults, e)
			if err != nil {
				return nil, err
			}
			if !pass {
				continue
			}
		}
		rows = append(rows, row)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("%w: index range scan iteration: %v", executorapi.ErrExecFailed, err)
	}
	return rows, nil
}

// ─── Projection & Sorting ───────────────────────────────────────────

// buildColumnNames returns column names for the result set.
func buildColumnNames(table *catalogapi.TableSchema, colIndices []int) []string {
	if len(colIndices) == 0 {
		// SELECT * — all columns
		names := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			names[i] = col.Name
		}
		return names
	}
	names := make([]string, len(colIndices))
	for i, idx := range colIndices {
		if idx < 0 || idx >= len(table.Columns) {
			names[i] = "?"
		} else {
			names[i] = table.Columns[idx].Name
		}
	}
	return names
}

// projectRows extracts selected columns from rows.
func projectRows(rows []*engineapi.Row, colIndices []int) [][]catalogapi.Value {
	result := make([][]catalogapi.Value, len(rows))
	for i, row := range rows {
		if len(colIndices) == 0 {
			// SELECT * — all columns
			vals := make([]catalogapi.Value, len(row.Values))
			copy(vals, row.Values)
			result[i] = vals
		} else {
			vals := make([]catalogapi.Value, len(colIndices))
			for j, idx := range colIndices {
				if idx >= 0 && idx < len(row.Values) {
					vals[j] = row.Values[idx]
				} else {
					vals[j] = catalogapi.Value{IsNull: true}
				}
			}
			result[i] = vals
		}
	}
	return result
}

// sortRawRows sorts raw engine rows by the ORDER BY column BEFORE projection.
// This ensures ORDER BY works even when the sort column is not in the SELECT list.
func sortRawRows(rows []*engineapi.Row, orderBy *plannerapi.OrderByPlan) {
	colIdx := orderBy.ColumnIndex

	sort.SliceStable(rows, func(i, j int) bool {
		var va, vb catalogapi.Value
		if colIdx < len(rows[i].Values) {
			va = rows[i].Values[colIdx]
		} else {
			va = catalogapi.Value{IsNull: true}
		}
		if colIdx < len(rows[j].Values) {
			vb = rows[j].Values[colIdx]
		} else {
			vb = catalogapi.Value{IsNull: true}
		}

		// NULL sorts first (ASC) or last (DESC)
		if va.IsNull && vb.IsNull {
			return false
		}
		if va.IsNull {
			return !orderBy.Desc
		}
		if vb.IsNull {
			return orderBy.Desc
		}

		cmp, err := encoding.CompareValues(va, vb)
		if err != nil {
			return false
		}
		if orderBy.Desc {
			return cmp > 0
		}
		return cmp < 0
	})
}

// findColumnIndex returns the index of a column in a table schema, or -1.
func findColumnIndex(tbl *catalogapi.TableSchema, name string) int {
	upper := strings.ToUpper(name)
	for i, c := range tbl.Columns {
		if strings.ToUpper(c.Name) == upper {
			return i
		}
	}
	return -1
}
