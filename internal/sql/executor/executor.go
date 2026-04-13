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

	// TODO: Update indexes

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

		// Update the column
		rowValues[colIdx] = plan.Value

		// Re-encode and store
		newData := encodeRow(rowValues)
		if err := e.kv.Put(iter.Key(), newData); err != nil {
			return count, err
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
