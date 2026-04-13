// Package executor executes SQL plans.
package executor

import (
	"fmt"
	"strings"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
	"github.com/akzj/go-fast-kv/internal/sql/value"
)

// Executor executes plan nodes.
type Executor struct {
	kv   kvstoreapi.Store
	cata *catalog.Catalog
}

// New creates a new Executor.
func New(kv kvstoreapi.Store) *Executor {
	return &Executor{
		kv:   kv,
		cata: catalog.New(kv),
	}
}

// Exec executes a plan that modifies data. Returns rows affected.
func (e *Executor) Exec(plan planner.PlanNode) (int64, error) {
	switch p := plan.(type) {
	case *planner.CreateTablePlan:
		return e.execCreateTable(p)
	case *planner.CreateIndexPlan:
		return e.execCreateIndex(p)
	case *planner.DropIndexPlan:
		return e.execDropIndex(p)
	case *planner.InsertPlan:
		return e.execInsert(p)
	case *planner.UpdatePlan:
		return e.execUpdate(p)
	case *planner.DeletePlan:
		return e.execDelete(p)
	default:
		return 0, fmt.Errorf("executor: unsupported plan type %T", plan)
	}
}

// Query executes a plan that returns rows.
func (e *Executor) Query(plan planner.PlanNode) (Iterator, error) {
	switch p := plan.(type) {
	case *planner.TableScanPlan:
		return e.execTableScan(p)
	case *planner.IndexScanPlan:
		return e.execIndexScan(p)
	default:
		return nil, fmt.Errorf("executor: unsupported query plan type %T", plan)
	}
}

// Iterator is the interface for iterating over result rows.
type Iterator interface {
	Next() bool
	Row() Row
	Err() error
	Close()
}

// Row represents a result row.
type Row struct {
	Columns []string
	Values  []value.Value
}

// execCreateTable creates a table.
func (e *Executor) execCreateTable(plan *planner.CreateTablePlan) (int64, error) {
	err := e.cata.CreateTable(plan.Schema)
	if err != nil {
		return 0, err
	}
	return 0, nil
}

// execCreateIndex creates an index.
func (e *Executor) execCreateIndex(plan *planner.CreateIndexPlan) (int64, error) {
	err := e.cata.CreateIndex(plan.Schema)
	if err != nil {
		return 0, err
	}
	return 0, nil
}

// execDropIndex drops an index.
func (e *Executor) execDropIndex(plan *planner.DropIndexPlan) (int64, error) {
	err := e.cata.DropIndex(plan.TableName, plan.IndexName)
	if err != nil {
		return 0, err
	}
	return 0, nil
}

// execInsert inserts a row.
func (e *Executor) execInsert(plan *planner.InsertPlan) (int64, error) {
	table, err := e.cata.GetTable(plan.Table)
	if err != nil {
		return 0, err
	}

	// Encode row key: "{table}:{col0}:{col1}:..."
	rowKey := plan.Table + ":"
	for i := range plan.Columns {
		if i > 0 {
			rowKey += ":"
		}
		rowKey += encodeValue(plan.Values[i], table.Columns[i].Type)
	}

	// Encode row data
	data := encodeRow(plan.Values)

	err = e.kv.Put([]byte(rowKey), data)
	if err != nil {
		return 0, err
	}

	// Update indexes
	indexes, err := e.cata.ListIndexesByTable(plan.Table)
	if err == nil {
		for _, idx := range indexes {
			// Find column index
			colIdx := -1
			for ci, col := range table.Columns {
				if strings.EqualFold(col.Name, idx.Column) {
					colIdx = ci
					break
				}
			}
			if colIdx >= 0 && colIdx < len(plan.Values) {
				// Store index entry: "{table}:__idx:{indexName}:{colVal}" -> "{rowKey}"
				idxKey := fmt.Sprintf("%s:__idx:%s:%s", plan.Table, idx.Name,
					encodeValue(plan.Values[colIdx], table.Columns[colIdx].Type))
				e.kv.Put([]byte(idxKey), []byte(rowKey))
			}
		}
	}

	return 1, nil
}

// execUpdate updates rows.
func (e *Executor) execUpdate(plan *planner.UpdatePlan) (int64, error) {
	// Look up table schema
	table, err := e.cata.GetTable(plan.Table)
	if err != nil {
		return 0, err
	}

	// Find column index (case-insensitive)
	colIdx := -1
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, plan.Column) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return 0, fmt.Errorf("executor: column %s not found", plan.Column)
	}

	// Check if this column has an index
	indexedCol := false
	var indexName string
	indexes, _ := e.cata.ListIndexesByTable(plan.Table)
	for _, idx := range indexes {
		if strings.EqualFold(idx.Column, plan.Column) {
			indexedCol = true
			indexName = idx.Name
			break
		}
	}

	// Scan and update matching rows
	var count int64
	prefix := plan.Table + ":"
	start := []byte(prefix)
	end := []byte(prefix + ":\xff")

	iter := e.kv.Scan(start, end)
	defer iter.Close()

	for iter.Next() {
		// Decode row
		rowValues, err := decodeRow(table.Columns, iter.Value())
		if err != nil {
			continue
		}

		// Check WHERE condition
		if plan.Where != nil && !matchCondition(rowValues, table.Columns, plan.Where) {
			continue
		}

		// Delete old index entry if column is indexed
		if indexedCol {
			oldIdxKey := fmt.Sprintf("%s:__idx:%s:%s", plan.Table, indexName,
				encodeValue(rowValues[colIdx], table.Columns[colIdx].Type))
			e.kv.Delete([]byte(oldIdxKey))
		}

		// Update the column
		rowValues[colIdx] = plan.Value

		// Re-encode and store
		newData := encodeRow(rowValues)
		if err := e.kv.Put(iter.Key(), newData); err != nil {
			return count, err
		}

		// Insert new index entry if column is indexed
		if indexedCol {
			newIdxKey := fmt.Sprintf("%s:__idx:%s:%s", plan.Table, indexName,
				encodeValue(plan.Value, table.Columns[colIdx].Type))
			e.kv.Put([]byte(newIdxKey), iter.Key())
		}

		count++
	}

	return count, iter.Err()
}

// execDelete deletes rows.
func (e *Executor) execDelete(plan *planner.DeletePlan) (int64, error) {
	// Look up table schema
	table, err := e.cata.GetTable(plan.Table)
	if err != nil {
		return 0, err
	}

	// Scan and delete matching rows
	var count int64
	prefix := plan.Table + ":"
	start := []byte(prefix)
	end := []byte(prefix + ":\xff")

	iter := e.kv.Scan(start, end)
	defer iter.Close()

	for iter.Next() {
		// Decode row
		rowValues, err := decodeRow(table.Columns, iter.Value())
		if err != nil {
			continue
		}

		// Check WHERE condition
		if plan.Where != nil && !matchCondition(rowValues, table.Columns, plan.Where) {
			continue
		}

		// Delete all index entries pointing to this row
		indexes, _ := e.cata.ListIndexesByTable(plan.Table)
		for _, idx := range indexes {
			// Find column index
			colIdx := -1
			for ci, col := range table.Columns {
				if strings.EqualFold(col.Name, idx.Column) {
					colIdx = ci
					break
				}
			}
			if colIdx >= 0 && colIdx < len(rowValues) {
				idxKey := fmt.Sprintf("%s:__idx:%s:%s", plan.Table, idx.Name,
					encodeValue(rowValues[colIdx], table.Columns[colIdx].Type))
				e.kv.Delete([]byte(idxKey))
			}
		}

		// Delete the row
		if err := e.kv.Delete(iter.Key()); err != nil {
			return count, err
		}
		count++
	}

	return count, iter.Err()
}

// execTableScan performs a full table scan.
func (e *Executor) execTableScan(plan *planner.TableScanPlan) (Iterator, error) {
	// Look up table schema for column definitions
	table, err := e.cata.GetTable(plan.Table)
	if err != nil {
		return nil, err
	}

	prefix := plan.Table + ":"
	start := []byte(prefix)
	end := []byte(prefix + ":\xff")

	iter := e.kv.Scan(start, end)
	return &tableScanIter{
		kv:      e.kv,
		iter:    iter,
		table:   plan.Table,
		where:   plan.Where,
		columns: table.Columns,
	}, nil
}

// execIndexScan performs an index scan to find matching rows.
// Index format: "{table}:__idx:{indexName}:{columnValue}" -> "{rowKey}"
func (e *Executor) execIndexScan(plan *planner.IndexScanPlan) (Iterator, error) {
	// Look up table schema
	table, err := e.cata.GetTable(plan.Table)
	if err != nil {
		return nil, err
	}

	// Look up index to get column position
	idx, err := e.cata.GetIndex(plan.Table, plan.Index)
	if err != nil {
		return nil, err
	}

	// Find column index
	colIdx := -1
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, idx.Column) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, fmt.Errorf("execIndexScan: column %s not found in table", idx.Column)
	}

	// Scan index: "{table}:__idx:{indexName}:{prefix}"
	prefix := fmt.Sprintf("%s:__idx:%s:", plan.Table, plan.Index)
	start := []byte(prefix)
	end := []byte(prefix + "\xff")

	iter := e.kv.Scan(start, end)
	return &indexScanIter{
		kv:      e.kv,
		iter:    iter,
		table:   plan.Table,
		columns: table.Columns,
		colIdx:  colIdx,
		op:      plan.Op,
		value:   plan.Value,
	}, nil
}

type indexScanIter struct {
	kv      kvstoreapi.Store
	iter    kvstoreapi.Iterator
	table   string
	columns []catalog.ColumnDef
	colIdx  int
	op      string
	value   value.Value
	row     Row
	err     error
}

func (i *indexScanIter) Next() bool {
	for i.iter.Next() {
		// Index entry: key = "{table}:__idx:{idx}:{colVal}", value = "{rowKey}"
		// We need to find rows where column matches the condition

		// First, extract the column value from the index key
		// Index key format: "{table}:__idx:{indexName}:{columnValue}"
		key := string(i.iter.Key())
		parts := strings.Split(key, ":")
		if len(parts) < 4 {
			continue
		}

		// parts[3] is the column value stored in index
		idxColValue := parts[len(parts)-1]

		// Check if it matches our condition
		condVal := i.value.String()
		if !compareIndexValue(idxColValue, condVal, i.op) {
			continue
		}

		// Get row key from index value
		rowKey := string(i.iter.Value())

		// Fetch the actual row
		data, err := i.kv.Get([]byte(rowKey))
		if err != nil {
			// Row might have been deleted
			continue
		}

		// Decode row
		rowValues, err := decodeRow(i.columns, data)
		if err != nil {
			i.err = err
			continue
		}

		i.row = Row{
			Values: rowValues,
		}
		return true
	}

	i.err = i.iter.Err()
	return false
}

func compareIndexValue(idxVal, condVal, op string) bool {
	switch op {
	case "=":
		return idxVal == condVal
	case "!=":
		return idxVal != condVal
	case ">":
		return idxVal > condVal
	case "<":
		return idxVal < condVal
	case ">=":
		return idxVal >= condVal
	case "<=":
		return idxVal <= condVal
	default:
		return false
	}
}

func (i *indexScanIter) Row() Row {
	return i.row
}

func (i *indexScanIter) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.iter.Err()
}

func (i *indexScanIter) Close() {
	i.iter.Close()
}

type tableScanIter struct {
	kv      kvstoreapi.Store
	iter    kvstoreapi.Iterator
	table   string
	where   *parser.Condition
	columns []catalog.ColumnDef
	row     Row
	err     error
}

func (i *tableScanIter) Next() bool {
	for i.iter.Next() {
		// Decode row
		rowValues, err := decodeRow(i.columns, i.iter.Value())
		if err != nil {
			i.err = err
			continue
		}

		// Apply WHERE filter (case-insensitive column matching)
		if i.where != nil && !matchCondition(rowValues, i.columns, i.where) {
			continue
		}

		i.row = Row{
			Values: rowValues,
		}
		return true
	}

	i.err = i.iter.Err()
	return false
}

func (i *tableScanIter) Row() Row {
	return i.row
}

func (i *tableScanIter) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.iter.Err()
}

func (i *tableScanIter) Close() {
	i.iter.Close()
}

// encodeValue encodes a value for use in row key.
func encodeValue(v value.Value, targetType value.Type) string {
	switch targetType {
	case value.TypeInt:
		return fmt.Sprintf("%d", v.AsInt())
	case value.TypeText:
		return v.AsText()
	default:
		return v.AsText()
	}
}

// encodeRow encodes values into binary format.
// Format: values separated by '\x00'
func encodeRow(values []value.Value) []byte {
	result := make([]byte, 0, len(values)*16)
	for _, v := range values {
		switch v.Type {
		case value.TypeInt:
			result = append(result, []byte(fmt.Sprintf("%d\x00", v.Int))...)
		case value.TypeFloat:
			result = append(result, []byte(fmt.Sprintf("%f\x00", v.Float))...)
		case value.TypeText:
			result = append(result, []byte(v.Text+"\x00")...)
		default:
			result = append(result, "NULL\x00"...)
		}
	}
	return result
}

// decodeRow decodes values from binary format.
func decodeRow(columns []catalog.ColumnDef, data []byte) ([]value.Value, error) {
	if len(data) == 0 {
		return make([]value.Value, len(columns)), nil
	}

	// Split by null delimiter
	parts := strings.Split(string(data), "\x00")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}

	values := make([]value.Value, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		if i < len(parts) {
			switch columns[i].Type {
			case value.TypeInt:
				var vi int64
				fmt.Sscanf(parts[i], "%d", &vi)
				values = append(values, value.NewInt(vi))
			case value.TypeFloat:
				var vf float64
				fmt.Sscanf(parts[i], "%f", &vf)
				values = append(values, value.NewFloat(vf))
			default:
				values = append(values, value.NewText(parts[i]))
			}
		} else {
			values = append(values, value.NewText(""))
		}
	}

	return values, nil
}

// matchCondition checks if a row matches the WHERE condition.
// Uses case-insensitive column name matching.
func matchCondition(rowValues []value.Value, columns []catalog.ColumnDef, cond *parser.Condition) bool {
	// Find column index (case-insensitive)
	colIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col.Name, cond.Column) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 || colIdx >= len(rowValues) {
		return false
	}

	rowVal := rowValues[colIdx]
	condVal := cond.Value

	switch cond.Op {
	case "=":
		return rowVal.Compare(condVal) == 0
	case "!=":
		return rowVal.Compare(condVal) != 0
	case ">":
		return rowVal.Compare(condVal) > 0
	case "<":
		return rowVal.Compare(condVal) < 0
	case ">=":
		return rowVal.Compare(condVal) >= 0
	case "<=":
		return rowVal.Compare(condVal) <= 0
	default:
		return false
	}
}
