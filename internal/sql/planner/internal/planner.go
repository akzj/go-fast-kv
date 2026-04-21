// Package internal implements the SQL planner.
package internal

import (
	"fmt"
	"strings"

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
	case *parserapi.ExplainStmt:
		// Wrap the inner plan in ExplainPlan instead of unwrapping it.
		innerPlan, err := p.Plan(s.Statement)
		if err != nil {
			return nil, err
		}
		return &plannerapi.ExplainPlan{Inner: innerPlan, Analyze: s.Analyze}, nil
	case *parserapi.UnionStmt:
		return p.planUnion(s)
	case *parserapi.IntersectStmt:
		return p.planIntersect(s)
	case *parserapi.ExceptStmt:
		return p.planExcept(s)
	// Transaction control statements — nil plan signals "transaction control"
	// to sql.DB.Exec(), which manages the transaction lifecycle.
	case *parserapi.BeginStmt:
		return nil, nil
	case *parserapi.CommitStmt:
		return nil, nil
	case *parserapi.RollbackStmt:
		return nil, nil
	case *parserapi.AlterTableStmt:
		return p.planAlterTable(s)
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
		cols[i] = catalogapi.ColumnDef{Name: c.Name, Type: t, NotNull: c.NotNull}
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

	// Collect indexes for UNIQUE columns (auto-created during table creation)
	var uniqueIndexes []catalogapi.IndexSchema
	for _, c := range stmt.Columns {
		if c.Unique {
			// Generate index name: uq_<table>_<column>
			idxName := fmt.Sprintf("uq_%s_%s", strings.ToLower(stmt.Table), strings.ToLower(c.Name))
			uniqueIndexes = append(uniqueIndexes, catalogapi.IndexSchema{
				Name:   idxName,
				Table:  stmt.Table,
				Column: c.Name,
				Unique: true,
			})
		}
	}

	return &plannerapi.CreateTablePlan{
		Schema: catalogapi.TableSchema{
			Name:       stmt.Table,
			Columns:    cols,
			PrimaryKey: pk,
		},
		IfNotExists:  stmt.IfNotExists,
		UniqueIndexes: uniqueIndexes,
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

func (p *planner) planAlterTable(stmt *parserapi.AlterTableStmt) (*plannerapi.AlterTablePlan, error) {
	// Validate table exists
	_, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	plan := &plannerapi.AlterTablePlan{
		TableName:  stmt.Table,
		Operation:  stmt.Operation,
		ColumnName: stmt.Column,
		ColumnNew:  stmt.ColumnNew,
		TypeName:   stmt.TypeName,
		NotNull:    stmt.NotNull,
		Unique:     stmt.Unique,
	}

	// For ADD COLUMN, resolve type
	if stmt.Operation == parserapi.AlterAddColumn {
		t, err := resolveTypeName(stmt.TypeName)
		if err != nil {
			return nil, err
		}
		// Convert catalogapi.Type back to string for plan
		switch t {
		case catalogapi.TypeInt:
			plan.TypeName = "INT"
		case catalogapi.TypeFloat:
			plan.TypeName = "FLOAT"
		case catalogapi.TypeText:
			plan.TypeName = "TEXT"
		case catalogapi.TypeBlob:
			plan.TypeName = "BLOB"
		default:
			plan.TypeName = "NULL"
		}
	}

	return plan, nil
}

// ─── DML Planning ───────────────────────────────────────────────────

func (p *planner) planInsert(stmt *parserapi.InsertStmt) (plannerapi.Plan, error) {
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	// INSERT ... SELECT
	if stmt.SelectStmt != nil {
		subPlan, err := p.planSelect(stmt.SelectStmt)
		if err != nil {
			return nil, err
		}
		// If SELECT has explicit columns (not *), validate count against target columns.
		// Skip validation for SELECT * — executor's projectRows expands * to all table columns.
		// For SELECT *, INSERT INTO dst SELECT * FROM src: dst table columns govern expected count.
		if !hasStarExpr(subPlan.SelectColumns) {
			if len(stmt.Columns) > 0 && len(subPlan.SelectColumns) != len(stmt.Columns) {
				return nil, plannerapi.ErrColumnCountMismatch
			}
			if len(stmt.Columns) == 0 && len(subPlan.SelectColumns) != len(tbl.Columns) {
				return nil, plannerapi.ErrColumnCountMismatch
			}
		}
		return &plannerapi.InsertSelectPlan{
			Table:      tbl,
			SelectPlan: subPlan,
			Columns:    stmt.Columns,
		}, nil
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

func (p *planner) planUnion(s *parserapi.UnionStmt) (plannerapi.Plan, error) {
	leftPlan, err := p.Plan(s.Left)
	if err != nil {
		return nil, err
	}
	rightPlan, err := p.Plan(s.Right)
	if err != nil {
		return nil, err
	}
	return &plannerapi.UnionPlan{Left: leftPlan, Right: rightPlan, UnionAll: s.UnionAll}, nil
}

func (p *planner) planIntersect(s *parserapi.IntersectStmt) (plannerapi.Plan, error) {
	leftPlan, err := p.Plan(s.Left)
	if err != nil {
		return nil, err
	}
	rightPlan, err := p.Plan(s.Right)
	if err != nil {
		return nil, err
	}
	return &plannerapi.IntersectPlan{Left: leftPlan, Right: rightPlan}, nil
}

func (p *planner) planExcept(s *parserapi.ExceptStmt) (plannerapi.Plan, error) {
	leftPlan, err := p.Plan(s.Left)
	if err != nil {
		return nil, err
	}
	rightPlan, err := p.Plan(s.Right)
	if err != nil {
		return nil, err
	}
	return &plannerapi.ExceptPlan{Left: leftPlan, Right: rightPlan}, nil
}

func (p *planner) planSelect(stmt *parserapi.SelectStmt) (*plannerapi.SelectPlan, error) {
	// Handle derived table (subquery in FROM clause)
	if stmt.DerivedTable != nil {
		return p.planDerivedTableSelect(stmt)
	}

	// Handle JOIN queries
	if stmt.Join != nil {
		return p.planJoinSelect(stmt)
	}

	// Handle SELECT without FROM (constant expressions)
	if stmt.Table == "" {
		// SELECT 1, SELECT 1+1, SELECT 'hello'
		// No table scan needed — evaluate expressions directly
		// ORDER BY on constants: sort the single row (no-op but wire through)
		var orderBy []*plannerapi.OrderByPlan
	if len(stmt.OrderBy) > 0 {
		// For constant SELECT, ORDER BY just returns the single row
		// We still need a column index — resolve against SELECT columns
		for _, ob := range stmt.OrderBy {
			orderBy = append(orderBy, &plannerapi.OrderByPlan{ColumnIndex: 0, Desc: ob.Desc})
		}
	}
		limit := -1
		if stmt.Limit != nil {
			val, err := resolveExprToValue(stmt.Limit)
			if err == nil && !val.IsNull && val.Type == catalogapi.TypeInt {
				limit = int(val.Int)
			}
		}
		offset := -1
		if stmt.Offset != nil {
			val, err := resolveExprToValue(stmt.Offset)
			if err == nil && !val.IsNull && val.Type == catalogapi.TypeInt {
				offset = int(val.Int)
			}
		}
		return &plannerapi.SelectPlan{
			Table:         nil,
			Scan:          nil,
			Columns:       nil,
			SelectColumns: stmt.Columns,
			Filter:        nil,
			GroupByExprs:  nil,
			Having:        nil,
			OrderBy:       orderBy,
			Limit:         limit,
			Offset:        offset,
			Distinct:      stmt.Distinct,
			LockMode:      stmt.LockMode,
			LockWait:      stmt.LockWait,
		}, nil
	}

	// Single-table SELECT
	tbl, err := p.catalog.GetTable(stmt.Table)
	if err != nil {
		if err == catalogapi.ErrTableNotFound {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, stmt.Table)
		}
		return nil, err
	}

	colIndices, err := p.resolveSelectColumns(tbl, stmt.Columns, stmt.GroupBy)
	if err != nil {
		return nil, err
	}

	scan, residualFilter, err := p.planScan(tbl, stmt.Where)
	if err != nil {
		return nil, err
	}

	// Try to optimize IndexScanPlan to IndexOnlyScanPlan (covering index).
	// If all required columns (SELECT, ORDER BY) are available in the index itself,
	// we can skip reading table pages entirely.
	if indexScan, ok := scan.(*plannerapi.IndexScanPlan); ok {
		if isCoveringIndex(tbl, stmt.Columns, stmt.OrderBy, indexScan.Index) {
			// Find which column index in SELECT refers to the indexed column.
			// For SELECT col FROM t WHERE col = 1, IndexedColumnIdx = 0.
			idxColIdx := -1
			for i, sc := range stmt.Columns {
				if colRef, ok := sc.Expr.(*parserapi.ColumnRef); ok {
					if colRef.Column == indexScan.Index.Column {
						idxColIdx = i
						break
					}
				}
			}
			scan = &plannerapi.IndexOnlyScanPlan{
				TableID:           indexScan.TableID,
				IndexID:          indexScan.IndexID,
				Index:            indexScan.Index,
				Op:               indexScan.Op,
				Value:            indexScan.Value,
				ResidualFilter:   residualFilter,
				IndexedColumnIdx: idxColIdx,
			}
			// ResidualFilter was already extracted from planScan
			residualFilter = nil
		}
	}

	var orderBy []*plannerapi.OrderByPlan
	if len(stmt.OrderBy) > 0 {
		for _, ob := range stmt.OrderBy {
			idx := findColumnIndex(tbl, ob.Column)
			if idx < 0 {
				return nil, fmt.Errorf("%w: ORDER BY %s", plannerapi.ErrColumnNotFound, ob.Column)
			}
			orderBy = append(orderBy, &plannerapi.OrderByPlan{ColumnIndex: idx, Desc: ob.Desc})
		}
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
	offset := -1
	if stmt.Offset != nil {
		val, err := resolveExprToValue(stmt.Offset)
		if err != nil {
			return nil, fmt.Errorf("OFFSET: %w", err)
		}
		if val.IsNull || val.Type != catalogapi.TypeInt {
			return nil, fmt.Errorf("OFFSET: %w: expected integer", plannerapi.ErrTypeMismatch)
		}
		offset = int(val.Int)
	}

	// NEW: plan subqueries in WHERE and HAVING
	if err := p.planSubqueries(residualFilter); err != nil {
		return nil, fmt.Errorf("planning subquery in WHERE: %w", err)
	}
	if err := p.planSubqueries(stmt.Having); err != nil {
		return nil, fmt.Errorf("planning subquery in HAVING: %w", err)
	}
	// HAVING requires GROUP BY
	if stmt.Having != nil && len(stmt.GroupBy) == 0 {
		return nil, fmt.Errorf("HAVING requires GROUP BY")
	}
	return &plannerapi.SelectPlan{
		Table: tbl, Scan: scan, Columns: colIndices,
		SelectColumns: stmt.Columns,
		Filter: residualFilter, GroupByExprs: stmt.GroupBy,
		Having: stmt.Having, OrderBy: orderBy, Limit: limit, Offset: offset,
		Distinct: stmt.Distinct,
		LockMode: stmt.LockMode,
		LockWait: stmt.LockWait,
	}, nil
}


// planDerivedTableSelect handles SELECT ... FROM (SELECT ...) AS alias [WHERE ...]
// The derived table produces a virtual table with its SELECT list as the schema.
func (p *planner) planDerivedTableSelect(stmt *parserapi.SelectStmt) (*plannerapi.SelectPlan, error) {
	dt := stmt.DerivedTable

	// Plan the subquery first
	subStmt, ok := dt.Subquery.Stmt.(*parserapi.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("%w: derived table subquery must be SELECT", plannerapi.ErrInvalidPlan)
	}
	subPlan, err := p.planSelect(subStmt)
	if err != nil {
		return nil, fmt.Errorf("plan derived table subquery: %w", err)
	}

	// Build the derived table schema from the subquery's SELECT list.
	// This schema has NO catalog backing — executor materializes it as a temp table.
	derivedSchema := buildDerivedSchema(dt.Alias, subPlan.SelectColumns)

	// Resolve select columns against the derived table's schema.
	colIndices, err := p.resolveSelectColumnsFromDerived(stmt.Columns, derivedSchema, stmt.GroupBy)
	if err != nil {
		return nil, err
	}

	// Resolve WHERE filter — may reference columns from the derived table via alias.
	scan, residualFilter, err := p.planScanForDerived(derivedSchema, stmt.Where)
	if err != nil {
		return nil, err
	}

	// Plan subqueries in WHERE and HAVING
	if err := p.planSubqueries(residualFilter); err != nil {
		return nil, fmt.Errorf("planning subquery in WHERE: %w", err)
	}
	if err := p.planSubqueries(stmt.Having); err != nil {
		return nil, fmt.Errorf("planning subquery in HAVING: %w", err)
	}

	// HAVING requires GROUP BY
	if stmt.Having != nil && len(stmt.GroupBy) == 0 {
		return nil, fmt.Errorf("HAVING requires GROUP BY")
	}

	// Resolve ORDER BY against derived schema (supports multiple columns)
	var orderBy []*plannerapi.OrderByPlan
	if len(stmt.OrderBy) > 0 {
		for _, ob := range stmt.OrderBy {
			orderCol := ob.Column
			orderTable := ""
			if dot := strings.LastIndex(orderCol, "."); dot >= 0 {
				orderTable = orderCol[:dot]
				orderCol = orderCol[dot+1:]
			}
			idx := -1
			for i, c := range derivedSchema.Columns {
				if strings.EqualFold(c.Name, orderCol) {
					if orderTable == "" || strings.EqualFold(c.Table, orderTable) {
						idx = i
						break
					}
					if idx < 0 {
						idx = i
					}
				}
			}
			if idx < 0 {
				return nil, fmt.Errorf("%w: ORDER BY %s", plannerapi.ErrColumnNotFound, ob.Column)
			}
			// Map ORDER BY index from derivedSchema to projected output
			if len(colIndices) > 0 {
				mappedIdx := -1
				for selectPos, derivedIdx := range colIndices {
					if derivedIdx == idx {
						mappedIdx = selectPos
						break
					}
				}
				if mappedIdx >= 0 {
					idx = mappedIdx
				}
			}
			orderBy = append(orderBy, &plannerapi.OrderByPlan{ColumnIndex: idx, Desc: ob.Desc})
		}
	}
	// Resolve LIMIT
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

	// Resolve OFFSET
	offset := -1
	if stmt.Offset != nil {
		val, err := resolveExprToValue(stmt.Offset)
		if err != nil {
			return nil, fmt.Errorf("OFFSET: %w", err)
		}
		if val.IsNull || val.Type != catalogapi.TypeInt {
			return nil, fmt.Errorf("OFFSET: %w: expected integer", plannerapi.ErrTypeMismatch)
		}
		offset = int(val.Int)
	}

	return &plannerapi.SelectPlan{
		Table:               derivedSchema,
		Scan:                scan,
		Columns:             colIndices,
		SelectColumns:       stmt.Columns,
		Filter:              residualFilter,
		GroupByExprs:        stmt.GroupBy,
		Having:              stmt.Having,
		OrderBy:             orderBy,
		Limit:               limit,
		Offset:              offset,
		Distinct:            stmt.Distinct,
		LockMode:            stmt.LockMode,
		LockWait:            stmt.LockWait,
		DerivedTableSubplan: subPlan,
		DerivedTableAlias:   dt.Alias,
	}, nil
}

// buildDerivedSchema builds a catalogapi.TableSchema for the derived table.
func buildDerivedSchema(alias string, selectCols []parserapi.SelectColumn) *catalogapi.TableSchema {
	columns := make([]catalogapi.ColumnDef, len(selectCols))
	for i, sc := range selectCols {
		col := catalogapi.ColumnDef{
			Name:  deriveColumnName(sc),
			Table: alias,
		}
		col.Type = inferTypeFromExpr(sc.Expr)
		columns[i] = col
	}
	return &catalogapi.TableSchema{
		Name:    alias,
		Columns: columns,
	}
}

// deriveColumnName returns the column name for a SELECT column.
func deriveColumnName(sc parserapi.SelectColumn) string {
	if sc.Alias != "" {
		return sc.Alias
	}
	switch expr := sc.Expr.(type) {
	case *parserapi.ColumnRef:
		if expr.Column != "" {
			// Return lowercase for column references (SQL standard)
			return strings.ToLower(expr.Column)
		}
	case *parserapi.AggregateCallExpr:
		return strings.ToUpper(expr.Func)
	case *parserapi.Literal:
		return "literal"
	case *parserapi.StarExpr:
		return "*"
	}
	return "col"
}

// inferTypeFromExpr attempts to infer the catalog type from an expression.
func inferTypeFromExpr(expr parserapi.Expr) catalogapi.Type {
	switch e := expr.(type) {
	case *parserapi.Literal:
		return e.Value.Type
	case *parserapi.ColumnRef:
		return catalogapi.TypeText
	case *parserapi.AggregateCallExpr:
		switch strings.ToUpper(e.Func) {
		case "COUNT":
			return catalogapi.TypeInt
		case "SUM", "AVG":
			return catalogapi.TypeFloat
		case "MIN", "MAX":
			return catalogapi.TypeText
		}
	case *parserapi.BinaryExpr:
		if inferTypeFromExpr(e.Left) == catalogapi.TypeInt &&
			inferTypeFromExpr(e.Right) == catalogapi.TypeInt {
			return catalogapi.TypeInt
		}
		return catalogapi.TypeFloat
	case *parserapi.CaseExpr:
		if len(e.Whens) > 0 {
			return inferTypeFromExpr(e.Whens[0].Val)
		}
		if e.Else != nil {
			return inferTypeFromExpr(e.Else)
		}
	case *parserapi.CoalesceExpr:
		if len(e.Args) > 0 {
			return inferTypeFromExpr(e.Args[0])
		}
	}
	return catalogapi.TypeText
}

// resolveSelectColumnsFromDerived resolves SELECT columns against a derived table schema.
func (p *planner) resolveSelectColumnsFromDerived(cols []parserapi.SelectColumn, tbl *catalogapi.TableSchema, groupByExprs []parserapi.Expr) ([]int, error) {
	if len(cols) == 1 {
		if _, ok := cols[0].Expr.(*parserapi.StarExpr); ok {
			return nil, nil
		}
	}

	indices := make([]int, len(cols))
	for i, sc := range cols {
		switch expr := sc.Expr.(type) {
		case *parserapi.ColumnRef:
			if len(groupByExprs) > 0 && !isInGroupBy(expr.Column, expr.Table, groupByExprs) {
				return nil, fmt.Errorf("%w: column %q must appear in the GROUP BY clause or be used in an aggregate function", plannerapi.ErrUnsupportedExpr, expr.Column)
			}
			idx := findColumnIndex(tbl, expr.Column)
			if idx < 0 {
				return nil, fmt.Errorf("%w: %s", plannerapi.ErrColumnNotFound, expr.Column)
			}
			indices[i] = idx
		case *parserapi.AggregateCallExpr:
			indices[i] = -1
		case *parserapi.CoalesceExpr:
			indices[i] = -1
		case *parserapi.CaseExpr:
			indices[i] = -1
		case *parserapi.BinaryExpr:
			indices[i] = -1
		default:
			return nil, fmt.Errorf("%w: SELECT expression must be a column reference or aggregate", plannerapi.ErrUnsupportedExpr)
		}
	}
	return indices, nil
}

// planScanForDerived creates a scan plan for a derived table.
func (p *planner) planScanForDerived(tbl *catalogapi.TableSchema, where parserapi.Expr) (plannerapi.ScanPlan, parserapi.Expr, error) {
	if where == nil {
		return &plannerapi.DerivedTableScanPlan{Schema: tbl, Filter: nil}, nil, nil
	}
	return &plannerapi.DerivedTableScanPlan{Schema: tbl, Filter: where}, where, nil
}

// planJoinSelect handles SELECT ... FROM t1 JOIN t2 ON ...
func (p *planner) planJoinSelect(stmt *parserapi.SelectStmt) (*plannerapi.SelectPlan, error) {
	j := stmt.Join

	// Get right table (always a string)
	rightTbl, err := p.catalog.GetTable(j.Right)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, j.Right)
	}

	// Determine left plan, left table, and combined schema based on j.Left type
	var leftPlan plannerapi.Plan
	var leftTbl *catalogapi.TableSchema
	var leftSchema []*catalogapi.ColumnDef

	switch left := j.Left.(type) {
	case string:
		// Base case: left is a table name
		leftTbl, err = p.catalog.GetTable(left)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", plannerapi.ErrTableNotFound, left)
		}
		leftScan, _, err := p.planScan(leftTbl, nil)
		if err != nil {
			return nil, fmt.Errorf("join left scan: %w", err)
		}
		leftPlan = leftScan
		leftSchema = colsToPtr(leftTbl.Columns)
		// Set Table field on base-case left columns for executor's evalColumnRef
		for i := range leftSchema {
			leftSchema[i].Table = leftTbl.Name
		}

	case *parserapi.JoinExpr:
		// Nested join: recursively plan the left side
		leftNested, err := p.planJoinSelect(&parserapi.SelectStmt{Join: left})
		if err != nil {
			return nil, fmt.Errorf("plan nested join: %w", err)
		}
		// leftPlan is the nested JoinPlan
		leftPlan = leftNested.Join
		// Build combined schema from nested join's left + right
		leftSchema = append(leftSchema, leftNested.Join.GetLeftSchema()...)
		leftSchema = append(leftSchema, leftNested.Join.GetRightSchema()...)
		// leftTable is the leftmost table (from nested join)
		// Type-assert to get LeftTable since nested joins use *JoinPlan
		if jp, ok := leftNested.Join.(*plannerapi.JoinPlan); ok {
			leftTbl = jp.LeftTable
		} else {
			// For HashJoinPlan nested (shouldn't happen), use table name
			leftTbl = &catalogapi.TableSchema{Name: leftNested.Join.GetLeftTableName()}
		}

	default:
		return nil, fmt.Errorf("invalid left in join: %T", j.Left)
	}

	// Plan right scan
	rightScan, _, err := p.planScan(rightTbl, nil)
	if err != nil {
		return nil, fmt.Errorf("join right scan: %w", err)
	}

	// Collect all table names for validation (including nested joins)
	allTables := collectJoinTableNames(j.Left)
	allTables = append(allTables, j.Right)

	// Validate ON condition
	if err := validateJoinOn(j.On, allTables); err != nil {
		return nil, fmt.Errorf("join ON: %w", err)
	}

	// Plan subqueries in ON
	if err := p.planSubqueries(j.On); err != nil {
		return nil, fmt.Errorf("planning subquery in ON: %w", err)
	}

	joinPlan := &plannerapi.JoinPlan{
		Left:        leftPlan,
		Right:       rightScan,
		LeftSchema:  leftSchema,
		RightSchema: colsToPtrWithTable(rightTbl.Columns, rightTbl.Name),
		LeftTable:   leftTbl,
		RightTable:  rightTbl,
		On:          j.On,
		Type:        string(j.Type),
	}

	// Build combined schema for column resolution (with Table field set)
	combinedSchema := make([]*catalogapi.ColumnDef, 0, len(leftSchema)+len(rightTbl.Columns))
	combinedSchema = append(combinedSchema, leftSchema...)
	// Right columns need Table field set
	for _, c := range rightTbl.Columns {
		col := c
		col.Table = rightTbl.Name
		combinedSchema = append(combinedSchema, &col)
	}

	// Resolve select columns using combined schema
	colIndices := []int{}
	for _, col := range stmt.Columns {
		if colExpr, ok := col.Expr.(*parserapi.ColumnRef); ok {
			idx := -1
			// Search in combined schema by column name AND table qualifier
			for i, c := range combinedSchema {
				if strings.EqualFold(c.Name, colExpr.Column) {
					if colExpr.Table == "" || strings.EqualFold(c.Table, colExpr.Table) {
						idx = i
						break
					}
				}
			}
			if idx < 0 {
				return nil, fmt.Errorf("%w: %s", plannerapi.ErrColumnNotFound, colExpr.Column)
			}
			colIndices = append(colIndices, idx)
		}
	}

	// ORDER BY for JOIN — resolve against combined schema with table qualifier support
	// Supports multiple columns: ORDER BY col1, col2 DESC
	var orderBy []*plannerapi.OrderByPlan
	if len(stmt.OrderBy) > 0 {
		for _, ob := range stmt.OrderBy {
			idx := -1
			// Split qualified name (e.g., "users.name") into table and column
			orderCol := ob.Column
			orderTable := ""
			if dot := strings.LastIndex(orderCol, "."); dot >= 0 {
				orderTable = orderCol[:dot]
				orderCol = orderCol[dot+1:]
			}
			for i, c := range combinedSchema {
				if strings.EqualFold(c.Name, orderCol) {
					if orderTable == "" || strings.EqualFold(c.Table, orderTable) {
						idx = i
						break
					}
					// Unqualified: remember first match but keep looking for qualified match
					if idx < 0 {
						idx = i
					}
				}
			}
			if idx < 0 {
				return nil, fmt.Errorf("%w: ORDER BY %s", plannerapi.ErrColumnNotFound, ob.Column)
			}
			// If GROUP BY is present, map the ORDER BY index from combinedSchema
			// to the position in SELECT columns. After GROUP BY, the result only
			// contains SELECT columns, not the full combined schema.
			if stmt.GroupBy != nil {
				mappedIdx := -1
				for selectPos, combinedIdx := range colIndices {
					if combinedIdx == idx {
						mappedIdx = selectPos
						break
					}
				}
				if mappedIdx >= 0 {
					idx = mappedIdx
				}
			}
			orderBy = append(orderBy, &plannerapi.OrderByPlan{ColumnIndex: idx, Desc: ob.Desc})
		}
	}
	// LIMIT for JOIN
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

	// OFFSET for JOIN
	offset := -1
	if stmt.Offset != nil {
		val, err := resolveExprToValue(stmt.Offset)
		if err != nil {
			return nil, fmt.Errorf("OFFSET: %w", err)
		}
		if val.IsNull || val.Type != catalogapi.TypeInt {
			return nil, fmt.Errorf("OFFSET: %w: expected integer", plannerapi.ErrTypeMismatch)
		}
		offset = int(val.Int)
	}

	// Check for equi-join and convert to optimized plan if applicable
	if hashLeftIdx, hashRightIdx, isEqui := detectEquiJoin(j.On, leftTbl.Name, rightTbl.Name, leftTbl, rightTbl); isEqui {
		// Check for index availability on both join columns
		leftIdx, _ := p.catalog.GetIndexByColumn(leftTbl.Name, leftTbl.Columns[hashLeftIdx].Name)
		rightIdx, _ := p.catalog.GetIndexByColumn(rightTbl.Name, rightTbl.Columns[hashRightIdx].Name)

		if leftIdx != nil {
			// LEFT table has index on join column → scan RIGHT (outer), lookup LEFT (inner)
			return &plannerapi.SelectPlan{
				Table:            leftTbl,
				Scan:             nil,
				Join: &plannerapi.IndexNestedLoopJoinPlan{
					Outer:       rightScan,
					Inner:       leftPlan,
					OuterSchema: colsToPtrWithTable(rightTbl.Columns, rightTbl.Name),
					InnerSchema: leftSchema,
					OuterTable:  rightTbl.Name,
					InnerTable:  leftTbl.Name,
					InnerIndex:  leftIdx,
					OuterKeyIdx: hashRightIdx,
					InnerKeyIdx: hashLeftIdx,
					On:          j.On,
					Type:        string(j.Type),
				},
				Columns:          colIndices,
				SelectColumns:    stmt.Columns,
				Filter:           stmt.Where,
				GroupByExprs:     stmt.GroupBy,
				Having:           stmt.Having,
				OrderBy:          orderBy,
				Limit:            limit,
				Offset:           offset,
				LeftColumnCount:  len(leftSchema),
				Distinct:         stmt.Distinct,
				LockMode:         stmt.LockMode,
				LockWait:         stmt.LockWait,
			}, nil
		}

		if rightIdx != nil {
			// RIGHT table has index on join column → scan LEFT, lookup RIGHT
			return &plannerapi.SelectPlan{
				Table:            leftTbl,
				Scan:             nil,
				Join: &plannerapi.IndexNestedLoopJoinPlan{
					Outer:       leftPlan,
					Inner:       rightScan,
					OuterSchema: leftSchema,
					InnerSchema: colsToPtrWithTable(rightTbl.Columns, rightTbl.Name),
					OuterTable:  leftTbl.Name,
					InnerTable:  rightTbl.Name,
					InnerIndex:  rightIdx,
					OuterKeyIdx: hashLeftIdx,
					InnerKeyIdx: hashRightIdx,
					On:          j.On,
					Type:        string(j.Type),
				},
				Columns:          colIndices,
				SelectColumns:    stmt.Columns,
				Filter:           stmt.Where,
				GroupByExprs:     stmt.GroupBy,
				Having:           stmt.Having,
				OrderBy:          orderBy,
				Limit:            limit,
				Offset:           offset,
				LeftColumnCount:  len(leftSchema),
				Distinct:         stmt.Distinct,
				LockMode:         stmt.LockMode,
				LockWait:         stmt.LockWait,
			}, nil
		}

		// No index on either side → fall back to HashJoin
		return &plannerapi.SelectPlan{
			Table:            leftTbl,
			Scan:             nil,
			Join: &plannerapi.HashJoinPlan{
				Left:        leftPlan,
				Right:       rightScan,
				LeftSchema:  leftSchema,
				RightSchema: colsToPtrWithTable(rightTbl.Columns, rightTbl.Name),
				LeftTable:   leftTbl.Name,
				RightTable:  rightTbl.Name,
				LeftKeyIdx:  hashLeftIdx,
				RightKeyIdx: hashRightIdx,
				On:          j.On,
				Type:        string(j.Type),
			},
			Columns:          colIndices,
			SelectColumns:    stmt.Columns,
			Filter:           stmt.Where,
			GroupByExprs:     stmt.GroupBy,
			Having:           stmt.Having,
			OrderBy:          orderBy,
			Limit:            limit,
			Offset:           offset,
			LeftColumnCount:  len(leftSchema),
			Distinct:         stmt.Distinct,
			LockMode:         stmt.LockMode,
		}, nil
	}

	return &plannerapi.SelectPlan{
		Table:            leftTbl,
		Scan:             nil,
		Join:             joinPlan,
		Columns:          colIndices,
		SelectColumns:    stmt.Columns,
		Filter:           stmt.Where, // WHERE applied on merged rows in executor
		GroupByExprs:     stmt.GroupBy,
		Having:           stmt.Having,
		OrderBy:          orderBy,
		Limit:            limit,
		Offset:           offset,
		LeftColumnCount:  len(leftSchema),
		Distinct:         stmt.Distinct,
		LockMode:         stmt.LockMode,
		LockWait:         stmt.LockWait,
	}, nil
}

// collectJoinTableNames recursively collects all table names from a JoinExpr
// (which may have a nested JoinExpr on its left side).
func collectJoinTableNames(left interface{}) []string {
	var tables []string
	switch l := left.(type) {
	case string:
		tables = append(tables, l)
	case *parserapi.JoinExpr:
		tables = append(tables, collectJoinTableNames(l.Left)...)
		tables = append(tables, l.Right)
	}
	return tables
}

// validateJoinOn checks that all column references in the ON condition
// are qualified with a table name and that the table is one of the join tables.
func validateJoinOn(expr parserapi.Expr, tableNames []string) error {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *parserapi.ColumnRef:
		if e.Table == "" {
			return fmt.Errorf("join ON must use qualified column names (table.column), got %q", e.Column)
		}
		found := false
		for _, t := range tableNames {
			if strings.EqualFold(e.Table, t) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("join ON references unknown table %q", e.Table)
		}
	case *parserapi.BinaryExpr:
		if err := validateJoinOn(e.Left, tableNames); err != nil {
			return err
		}
		return validateJoinOn(e.Right, tableNames)
	case *parserapi.UnaryExpr:
		return validateJoinOn(e.Operand, tableNames)
	case *parserapi.InExpr:
		if err := validateJoinOn(e.Expr, tableNames); err != nil {
			return err
		}
		for _, v := range e.Values {
			if err := validateJoinOn(v, tableNames); err != nil {
				return err
			}
		}
	case *parserapi.LikeExpr:
		if err := validateJoinOn(e.Expr, tableNames); err != nil {
			return err
		}
		// LikeExpr.Pattern is a string, not an Expr — no recursive validation needed
	case *parserapi.BetweenExpr:
		if err := validateJoinOn(e.Expr, tableNames); err != nil {
			return err
		}
		if err := validateJoinOn(e.Low, tableNames); err != nil {
			return err
		}
		return validateJoinOn(e.High, tableNames)
	case *parserapi.IsNullExpr:
		return validateJoinOn(e.Expr, tableNames)
	case *parserapi.SubqueryExpr, *parserapi.AggregateCallExpr, *parserapi.Literal:
	}
	return nil
}

// colsToPtr converts []ColumnDef to []*ColumnDef.
func colsToPtr(cols []catalogapi.ColumnDef) []*catalogapi.ColumnDef {
	result := make([]*catalogapi.ColumnDef, len(cols))
	for i := range cols {
		result[i] = &cols[i]
	}
	return result
}

// colsToPtrWithTable converts []ColumnDef to []*ColumnDef and sets the Table field.
func colsToPtrWithTable(cols []catalogapi.ColumnDef, tableName string) []*catalogapi.ColumnDef {
	result := make([]*catalogapi.ColumnDef, len(cols))
	for i := range cols {
		col := cols[i] // copy
		col.Table = tableName
		result[i] = &col
	}
	return result
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

	// First: check for LIKE prefix candidates (highest priority — more specific than EQ)
	for i, cond := range conditions {
		cand := p.extractLikeIndexCandidate(tbl, cond)
		if cand != nil {
			var residualParts []parserapi.Expr
			for j, c := range conditions {
				if j != i {
					residualParts = append(residualParts, c)
				}
			}
			var residual parserapi.Expr
			if len(residualParts) == 1 {
				residual = residualParts[0]
			} else if len(residualParts) > 1 {
				residual = buildAndChain(residualParts)
			}
			return &plannerapi.IndexRangePlan{
				TableID: tbl.TableID, IndexID: cand.index.IndexID,
				Index: cand.index,
				StartPrefix: cand.startPrefix, EndPrefix: cand.endPrefix,
				ResidualFilter: residual,
			}, residual, nil
		}
	}

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

// isCoveringIndex checks if the given index can satisfy the query without reading table pages.
// For a covering index, all required columns (SELECT, WHERE, ORDER BY) must be the indexed column.
func isCoveringIndex(tbl *catalogapi.TableSchema, selectCols []parserapi.SelectColumn, orderBy []*parserapi.OrderByClause, index *catalogapi.IndexSchema) bool {
	indexedColumn := index.Column

	// Check ORDER BY columns (if present) — index must cover ALL ORDER BY columns
	if len(orderBy) > 0 {
		for _, ob := range orderBy {
			if ob.Column != indexedColumn {
				return false
			}
		}
	}

	// Check SELECT columns
	for _, sc := range selectCols {
		switch expr := sc.Expr.(type) {
		case *parserapi.ColumnRef:
			if expr.Column != indexedColumn {
				return false
			}
		case *parserapi.StarExpr:
			// SELECT * requires all columns — not covering
			return false
		default:
			// Aggregates, expressions, etc. — not covering (may need other columns)
			return false
		}
	}

	return true
}

type indexCandidate struct {
	index *catalogapi.IndexSchema
	op    encodingapi.CompareOp
	value catalogapi.Value
}

type likeIndexCandidate struct {
	index       *catalogapi.IndexSchema
	startPrefix string
	endPrefix   string
}

// extractLikeIndexCandidate checks if expr is a LIKE 'prefix%' on an indexed column.
func (p *planner) extractLikeIndexCandidate(tbl *catalogapi.TableSchema, expr parserapi.Expr) *likeIndexCandidate {
	like, ok := expr.(*parserapi.LikeExpr)
	if !ok {
		return nil
	}
	// LikeExpr.Pattern is directly a string (verified from parser/api/api.go)
	pattern := like.Pattern
	if !isPrefixPattern(pattern) {
		return nil
	}
	// Strip trailing % to get prefix
	prefix := pattern
	if len(prefix) > 0 && prefix[len(prefix)-1] == '%' {
		prefix = prefix[:len(prefix)-1]
	}
	endPrefix := nextLexicographic(prefix)

	colRef, ok := like.Expr.(*parserapi.ColumnRef)
	if !ok {
		return nil
	}
	idx, err := p.catalog.GetIndexByColumn(tbl.Name, colRef.Column)
	if err != nil || idx == nil {
		return nil
	}
	// Index column must be TEXT type
	idxColIdx := -1
	for i, col := range tbl.Columns {
		if strings.EqualFold(col.Name, colRef.Column) {
			idxColIdx = i
			break
		}
	}
	if idxColIdx < 0 || tbl.Columns[idxColIdx].Type != catalogapi.TypeText {
		return nil
	}

	return &likeIndexCandidate{index: idx, startPrefix: prefix, endPrefix: endPrefix}
}

// isPrefixPattern returns true if pattern is LIKE 'prefix%' (all wildcards at end).
func isPrefixPattern(pattern string) bool {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '%' {
			for j := i; j < len(pattern); j++ {
				if pattern[j] != '%' {
					return false
				}
			}
			return true
		}
	}
	return false
}

// nextLexicographic returns the smallest string lexicographically greater than s.
func nextLexicographic(s string) string {
	if s == "" {
		return ""
	}
	b := []byte(s)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 255 {
			b[i]++
			return string(b)
		}
		b[i] = 0
	}
	return string([]byte{0}) + s + string([]byte{0})
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

func (p *planner) resolveSelectColumns(tbl *catalogapi.TableSchema, cols []parserapi.SelectColumn, groupByExprs []parserapi.Expr) ([]int, error) {
	if len(cols) == 1 {
		if _, ok := cols[0].Expr.(*parserapi.StarExpr); ok {
			return nil, nil
		}
	}

	indices := make([]int, len(cols))
	for i, sc := range cols {
		switch expr := sc.Expr.(type) {
		case *parserapi.ColumnRef:
			// CR-E: if GROUP BY present, validate column is in GROUP BY or is an aggregate
			if len(groupByExprs) > 0 && !isInGroupBy(expr.Column, expr.Table, groupByExprs) {
				return nil, fmt.Errorf("%w: column %q must appear in the GROUP BY clause or be used in an aggregate function", plannerapi.ErrUnsupportedExpr, expr.Column)
			}
			idx := findColumnIndex(tbl, expr.Column)
			if idx < 0 {
				return nil, fmt.Errorf("%w: %s", plannerapi.ErrColumnNotFound, expr.Column)
			}
			indices[i] = idx
		case *parserapi.AggregateCallExpr:
			// Aggregates are not column indices; set -1 as sentinel (executor handles them via SelectColumns).
			indices[i] = -1
		case *parserapi.CoalesceExpr:
			// CoalesceExpr is evaluated by the executor; set -1 as sentinel.
			indices[i] = -1
		case *parserapi.CaseExpr:
			// CaseExpr is evaluated by the executor; set -1 as sentinel.
			indices[i] = -1
		case *parserapi.BinaryExpr:
			// Binary expressions (e.g., 1+1, a+b) are evaluated by the executor; set -1 as sentinel.
			indices[i] = -1
		default:
			return nil, fmt.Errorf("%w: SELECT expression must be a column reference or aggregate", plannerapi.ErrUnsupportedExpr)
		}
	}
	return indices, nil
}

// isInGroupBy checks if a column reference appears in the GROUP BY expression list.
// If table qualifier is non-empty, it must also match.
func isInGroupBy(colName, table string, groupByExprs []parserapi.Expr) bool {
	for _, gb := range groupByExprs {
		if gbRef, ok := gb.(*parserapi.ColumnRef); ok {
			if !strings.EqualFold(colName, gbRef.Column) {
				continue
			}
			// If SELECT column has table qualifier, GROUP BY must match it
			if table != "" && gbRef.Table != "" && !strings.EqualFold(table, gbRef.Table) {
				continue
			}
			return true
		}
	}
	return false
}

// planSubqueries walks expr AST and plans each SubqueryExpr found.
func (p *planner) planSubqueries(expr parserapi.Expr) error {
	return walkExprForSubqueries(expr, p)
}

// walkExprForSubqueries traverses an expression AST and sets sq.Plan for each SubqueryExpr.
func walkExprForSubqueries(expr parserapi.Expr, p *planner) error {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *parserapi.SubqueryExpr:
		// Already planned? (nested call during subquery planning)
		if e.Plan != nil {
			return nil
		}
		subplan, err := p.Plan(e.Stmt)
		if err != nil {
			return err
		}
		e.Plan = subplan
	case *parserapi.ExistsExpr:
		// Recurse into the subquery to plan it
		return walkExprForSubqueries(&parserapi.SubqueryExpr{Stmt: e.Subquery.Stmt, Plan: e.Subquery.Plan}, p)
	case *parserapi.BinaryExpr:
		if err := walkExprForSubqueries(e.Left, p); err != nil {
			return err
		}
		return walkExprForSubqueries(e.Right, p)
	case *parserapi.UnaryExpr:
		return walkExprForSubqueries(e.Operand, p)
	case *parserapi.InExpr:
		if err := walkExprForSubqueries(e.Expr, p); err != nil {
			return err
		}
		for _, v := range e.Values {
			if err := walkExprForSubqueries(v, p); err != nil {
				return err
			}
		}
	case *parserapi.LikeExpr:
		// Pattern is a string literal, not a subquery
		return walkExprForSubqueries(e.Expr, p)
	case *parserapi.BetweenExpr:
		if err := walkExprForSubqueries(e.Expr, p); err != nil {
			return err
		}
		if err := walkExprForSubqueries(e.Low, p); err != nil {
			return err
		}
		return walkExprForSubqueries(e.High, p)
	case *parserapi.IsNullExpr:
		return walkExprForSubqueries(e.Expr, p)
	case *parserapi.CaseExpr:
		for _, w := range e.Whens {
			if err := walkExprForSubqueries(w.Cond, p); err != nil {
				return err
			}
			if err := walkExprForSubqueries(w.Val, p); err != nil {
				return err
			}
		}
		if e.Else != nil {
			if err := walkExprForSubqueries(e.Else, p); err != nil {
				return err
			}
		}
	case *parserapi.AggregateCallExpr:
		// No subquery inside aggregates
	case *parserapi.Literal, *parserapi.ColumnRef, *parserapi.StarExpr:
		// Leaf nodes
	}
	return nil
}

// detectEquiJoin checks if the ON clause is an equi-join (t1.col = t2.col).
// Returns left and right key column indices if it's an equi-join, or -1/-1 if not.
// Handles both orderings: t1.col = t2.col AND t2.col = t1.col.
func detectEquiJoin(on parserapi.Expr, leftTable, rightTable string, leftTableSchema, rightTableSchema *catalogapi.TableSchema) (leftIdx, rightIdx int, isEqui bool) {
	if on == nil {
		return -1, -1, false
	}

	bin, ok := on.(*parserapi.BinaryExpr)
	if !ok || bin.Op != parserapi.BinEQ {
		return -1, -1, false
	}

	leftRef, leftOK := bin.Left.(*parserapi.ColumnRef)
	rightRef, rightOK := bin.Right.(*parserapi.ColumnRef)

	if !leftOK || !rightOK {
		return -1, -1, false
	}

	// Case 1: leftTable.col = rightTable.col (normal ordering)
	if strings.EqualFold(leftRef.Table, leftTable) && strings.EqualFold(rightRef.Table, rightTable) {
		leftIdx = findColumnIndex(leftTableSchema, leftRef.Column)
		rightIdx = findColumnIndex(rightTableSchema, rightRef.Column)
		if leftIdx >= 0 && rightIdx >= 0 {
			return leftIdx, rightIdx, true
		}
	}

	// Case 2: rightTable.col = leftTable.col (reversed ordering)
	if strings.EqualFold(leftRef.Table, rightTable) && strings.EqualFold(rightRef.Table, leftTable) {
		// For reversed ordering, we swap: build on right, probe on left
		rightIdx = findColumnIndex(rightTableSchema, leftRef.Column)  // key in right table
		leftIdx = findColumnIndex(leftTableSchema, rightRef.Column)   // key in left table
		if leftIdx >= 0 && rightIdx >= 0 {
			return leftIdx, rightIdx, true
		}
	}

	return -1, -1, false
}

// hasStarExpr returns true if any column expression is a StarExpr (SELECT *).
func hasStarExpr(cols []parserapi.SelectColumn) bool {
	for _, c := range cols {
		if _, ok := c.Expr.(*parserapi.StarExpr); ok {
			return true
		}
	}
	return false
}
