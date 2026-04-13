// Package internal implements the SQL planner.
package internal

import (
	"fmt"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// Compile-time interface check.
var _ plannerapi.Planner = (*planner)(nil)

type planner struct {
	catalog catalogapi.CatalogManager
}

// New creates a new Planner backed by the given catalog.
func New(catalog catalogapi.CatalogManager) *planner {
	return &planner{catalog: catalog}
}

// Plan converts a parsed AST statement into an execution plan.
func (p *planner) Plan(stmt parserapi.Statement) (plannerapi.Plan, error) {
	switch s := stmt.(type) {
	case *parserapi.CreateTableStmt:
		return p.planCreateTable(s)
	case *parserapi.DropTableStmt:
		return p.planDropTable(s)
	case *parserapi.CreateIndexStmt:
		return p.planCreateIndex(s)
	case *parserapi.DropIndexStmt:
		return p.planDropIndex(s)
	case *parserapi.InsertStmt:
		return p.planInsert(s)
	case *parserapi.SelectStmt:
		return p.planSelect(s)
	case *parserapi.DeleteStmt:
		return p.planDelete(s)
	case *parserapi.UpdateStmt:
		return p.planUpdate(s)
	default:
		return nil, fmt.Errorf("%w: unsupported statement type %T", plannerapi.ErrInvalidPlan, stmt)
	}
}

// ─── DDL Planning ───────────────────────────────────────────────────

func (p *planner) planCreateTable(stmt *parserapi.CreateTableStmt) (*plannerapi.CreateTablePlan, error) {
	if len(stmt.Columns) == 0 {
		return nil, plannerapi.ErrEmptyTable
	}

	cols := make([]catalogapi.ColumnDef, len(stmt.Columns))
	for i, c := range stmt.Columns {
		t, err := resolveTypeName(c.TypeName)
		if err != nil {
			return nil, err
		}
		cols[i] = catalogapi.ColumnDef{Name: c.Name, Type: t}
	}

	pk := stmt.PrimaryKey
	if pk == "" {
		for _, c := range stmt.Columns {
			if c.PrimaryKey {
				pk = c.Name
				break
			}
		}
	}

	if pk != "" {
		found := false
		for _, c := range cols {
			if c.Name == pk {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: primary key column %q", plannerapi.ErrColumnNotFound, pk)
		}
	}

	return &plannerapi.CreateTablePlan{
		Schema: catalogapi.TableSchema{
			Name:       stmt.Table,
			Columns:    cols,
			PrimaryKey: pk,
		},
		IfNotExists: stmt.IfNotExists,
	}, nil
}

func (p *planner) planDropTable(stmt *parserapi.DropTableStmt) (*plannerapi.DropTablePlan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound && stmt.IfExists {
			return &plannerapi.DropTablePlan{
				TableName: stmt.Table,
				TableID:   0,
				IfExists:  true,
			}, nil
		}
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	return &plannerapi.DropTablePlan{
		TableName: stmt.Table,
		TableID:   tbl.TableID,
		IfExists:  stmt.IfExists,
	}, nil
}

func (p *planner) planCreateIndex(stmt *parserapi.CreateIndexStmt) (*plannerapi.CreateIndexPlan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	if findColumnIndex(tbl, stmt.Column) < 0 {
		return nil, fmt.Errorf("%w: %s.%s", plannerapi.ErrColumnNotFound, stmt.Table, stmt.Column)
	}

	return &plannerapi.CreateIndexPlan{
		Schema: catalogapi.IndexSchema{
			Name:   stmt.Index,
			Table:  stmt.Table,
			Column: stmt.Column,
			Unique: stmt.Unique,
		},
		IfNotExists: stmt.IfNotExists,
	}, nil
}

func (p *planner) planDropIndex(stmt *parserapi.DropIndexStmt) (*plannerapi.DropIndexPlan, error) {
	// Validate table exists
	_, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	// Validate index exists (unless IF EXISTS)
	if !stmt.IfExists {
		_, err := p.catalog.GetIndex(stmt.Table, stmt.Index)
		if err == catalogapi.ErrIndexNotFound {
			return nil, catalogapi.ErrIndexNotFound
		}
		if err != nil {
			return nil, err
		}
	}
	return &plannerapi.DropIndexPlan{
		IndexName: stmt.Index,
		TableName: stmt.Table,
		IfExists:  stmt.IfExists,
	}, nil
}

// ─── DML Planning ───────────────────────────────────────────────────

func (p *planner) planInsert(stmt *parserapi.InsertStmt) (*plannerapi.InsertPlan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	rows := make([][]catalogapi.Value, len(stmt.Values))
	for i, exprRow := range stmt.Values {
		resolved, err := p.resolveInsertRow(tbl, stmt.Columns, exprRow)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
		rows[i] = resolved
	}

	return &plannerapi.InsertPlan{Table: tbl, Rows: rows}, nil
}

func (p *planner) resolveInsertRow(tbl *catalogapi.TableSchema, columns []string, exprs []parserapi.Expr) ([]catalogapi.Value, error) {
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
			val, err := resolveExprToValue(exprs[i])
			if err != nil {
				return nil, err
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
		val, err := resolveExprToValue(expr)
		if err != nil {
			return nil, err
		}
		if !val.IsNull {
			if err := checkType(val, tbl.Columns[i].Type); err != nil {
				return nil, fmt.Errorf("column %s: %w", tbl.Columns[i].Name, err)
			}
		}
		values[i] = val
	}
	return values, nil
}

func (p *planner) planSelect(stmt *parserapi.SelectStmt) (*plannerapi.SelectPlan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	colIndices, err := p.resolveSelectColumns(tbl, stmt.Columns)
	if err != nil {
		return nil, err
	}

	scan, residualFilter, err := p.planScan(tbl, stmt.Where)
	if err != nil {
		return nil, err
	}

	var orderBy *plannerapi.OrderByPlan
	if stmt.OrderBy != nil {
		idx := findColumnIndex(tbl, stmt.OrderBy.Column)
		if idx < 0 {
			return nil, fmt.Errorf("%w: ORDER BY %s", plannerapi.ErrColumnNotFound, stmt.OrderBy.Column)
		}
		orderBy = &plannerapi.OrderByPlan{ColumnIndex: idx, Desc: stmt.OrderBy.Desc}
	}

	limit := -1
	if stmt.Limit != nil {
		val, err := resolveExprToValue(stmt.Limit)
		if err != nil {
			return nil, fmt.Errorf("LIMIT: %w", err)
		}
		if val.IsNull || val.Type != catalogapi.TypeInt {
			return nil, fmt.Errorf("LIMIT: %w: expected integer", plannerapi.ErrTypeMismatch)
		}
		limit = int(val.Int)
	}

	return &plannerapi.SelectPlan{
		Table: tbl, Scan: scan, Columns: colIndices,
		Filter: residualFilter, OrderBy: orderBy, Limit: limit,
	}, nil
}

func (p *planner) planDelete(stmt *parserapi.DeleteStmt) (*plannerapi.DeletePlan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	scan, _, err := p.planScan(tbl, stmt.Where)
	if err != nil {
		return nil, err
	}

	return &plannerapi.DeletePlan{Table: tbl, Scan: scan}, nil
}

func (p *planner) planUpdate(stmt *parserapi.UpdateStmt) (*plannerapi.UpdatePlan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	assignments := make(map[int]catalogapi.Value, len(stmt.Assignments))
	for _, a := range stmt.Assignments {
		idx := findColumnIndex(tbl, a.Column)
		if idx < 0 {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrColumnNotFound, a.Column)
		}
		val, err := resolveExprToValue(a.Value)
		if err != nil {
			return nil, fmt.Errorf("SET %s: %w", a.Column, err)
		}
		if !val.IsNull {
			if err := checkType(val, tbl.Columns[idx].Type); err != nil {
				return nil, fmt.Errorf("SET %s: %w", a.Column, err)
			}
		}
		assignments[idx] = val
	}

	scan, _, err := p.planScan(tbl, stmt.Where)
	if err != nil {
		return nil, err
	}

	return &plannerapi.UpdatePlan{Table: tbl, Assignments: assignments, Scan: scan}, nil
}

// ─── Scan Planning ──────────────────────────────────────────────────

func (p *planner) planScan(tbl *catalogapi.TableSchema, where parserapi.Expr) (plannerapi.ScanPlan, parserapi.Expr, error) {
	if where == nil {
		return &plannerapi.TableScanPlan{TableID: tbl.TableID, Filter: nil}, nil, nil
	}

	conditions := flattenAnd(where)

	var bestCandidate *indexCandidate
	var bestIdx int = -1

	for i, cond := range conditions {
		cand := p.extractIndexCandidate(tbl, cond)
		if cand == nil {
			continue
		}
		if bestCandidate == nil ||
			(cand.op == encodingapi.OpEQ && bestCandidate.op != encodingapi.OpEQ) {
			bestCandidate = cand
			bestIdx = i
		}
	}

	if bestCandidate != nil {
		var residualParts []parserapi.Expr
		for i, cond := range conditions {
			if i != bestIdx {
				residualParts = append(residualParts, cond)
			}
		}
		var residual parserapi.Expr
		if len(residualParts) == 1 {
			residual = residualParts[0]
		} else if len(residualParts) > 1 {
			residual = buildAndChain(residualParts)
		}

		return &plannerapi.IndexScanPlan{
			TableID: tbl.TableID, IndexID: bestCandidate.index.IndexID,
			Index: bestCandidate.index, Op: bestCandidate.op,
			Value: bestCandidate.value, ResidualFilter: residual,
		}, residual, nil
	}

	return &plannerapi.TableScanPlan{TableID: tbl.TableID, Filter: where}, where, nil
}

type indexCandidate struct {
	index *catalogapi.IndexSchema
	op    encodingapi.CompareOp
	value catalogapi.Value
}

func (p *planner) extractIndexCandidate(tbl *catalogapi.TableSchema, expr parserapi.Expr) *indexCandidate {
	bin, ok := expr.(*parserapi.BinaryExpr)
	if !ok {
		return nil
	}

	op, ok := binOpToCompareOp(bin.Op)
	if !ok {
		return nil
	}

	colRef, colOk := bin.Left.(*parserapi.ColumnRef)
	lit, litOk := bin.Right.(*parserapi.Literal)

	if !colOk || !litOk {
		lit, litOk = bin.Left.(*parserapi.Literal)
		colRef, colOk = bin.Right.(*parserapi.ColumnRef)
		if !colOk || !litOk {
			return nil
		}
		op = flipCompareOp(op)
	}

	// W2: GetIndexByColumn returns (nil, nil) when no index exists
	idx, err := p.catalog.GetIndexByColumn(tbl.Name, colRef.Column)
	if err != nil || idx == nil {
		return nil
	}

	return &indexCandidate{index: idx, op: op, value: lit.Value}
}

// ─── Column Resolution ──────────────────────────────────────────────

func (p *planner) resolveSelectColumns(tbl *catalogapi.TableSchema, cols []parserapi.SelectColumn) ([]int, error) {
	if len(cols) == 1 {
		if _, ok := cols[0].Expr.(*parserapi.StarExpr); ok {
			return nil, nil
		}
	}

	indices := make([]int, len(cols))
	for i, sc := range cols {
		ref, ok := sc.Expr.(*parserapi.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("%w: SELECT expression must be a column reference", plannerapi.ErrUnsupportedExpr)
		}
		idx := findColumnIndex(tbl, ref.Column)
		if idx < 0 {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrColumnNotFound, ref.Column)
		}
		indices[i] = idx
	}
	return indices, nil
}
