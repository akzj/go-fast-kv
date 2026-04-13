// Package executor executes SQL plans.
package executor

import (
	"fmt"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
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
	// TODO: Implement update
	return 0, nil
}

// execDelete deletes rows.
func (e *Executor) execDelete(plan *planner.DeletePlan) (int64, error) {
	// TODO: Implement delete
	return 0, nil
}

// execTableScan performs a full table scan.
func (e *Executor) execTableScan(plan *planner.TableScanPlan) (Iterator, error) {
	prefix := plan.Table + ":"
	start := []byte(prefix)
	end := []byte(prefix + ":\xff")

	iter := e.kv.Scan(start, end)
	return &tableScanIter{
		kv:    e.kv,
		iter:  iter,
		table: plan.Table,
	}, nil
}

type tableScanIter struct {
	kv    kvstoreapi.Store
	iter  kvstoreapi.Iterator
	table string
	row   Row
	err   error
}

func (i *tableScanIter) Next() bool {
	if !i.iter.Next() {
		return false
	}

	// Decode row
	// TODO: proper decoding

	return true
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
// Format: [numCols:1][type1:1][len1:2][data1:N]...
func encodeRow(values []value.Value) []byte {
	// Simple text encoding for now: values separated by '\x00'
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
