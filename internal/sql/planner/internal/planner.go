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
		return p.Plan(s.Statement)
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
	// Handle JOIN queries
	if stmt.Join != nil {
		return p.planJoinSelect(stmt)
	}

	// Handle SELECT without FROM (constant expressions)
	if stmt.Table == "" {
		// SELECT 1, SELECT 1+1, SELECT 'hello'
		// No table scan needed — evaluate expressions directly
		// ORDER BY on constants: sort the single row (no-op but wire through)
		var orderBy *plannerapi.OrderByPlan
		if stmt.OrderBy != nil {
			// For constant SELECT, ORDER BY just returns the single row
			// We still need a column index — resolve against SELECT columns
			orderBy = &plannerapi.OrderByPlan{ColumnIndex: 0, Desc: stmt.OrderBy.Desc}
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
	}, nil
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
		if lt, ok := leftNested.Join.Left.(*plannerapi.JoinPlan); ok {
			// Nested-nested: use its schemas directly (Table already set by its executor call)
			leftSchema = append(leftSchema, lt.LeftSchema...)
			leftSchema = append(leftSchema, lt.RightSchema...)
		} else {
			// Base case left: LeftTable columns, set Table field
			for _, c := range leftNested.Join.LeftTable.Columns {
				c := c
				c.Table = leftNested.Join.LeftTable.Name
				leftSchema = append(leftSchema, &c)
			}
		}
		// Add nested join's right schema, set Table field
		for _, c := range leftNested.Join.RightTable.Columns {
			c := c
			c.Table = leftNested.Join.RightTable.Name
			leftSchema = append(leftSchema, &c)
		}
		// leftTable is the leftmost table (from nested join)
		leftTbl = leftNested.Join.LeftTable

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
		RightSchema: colsToPtr(rightTbl.Columns),
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
	var orderBy *plannerapi.OrderByPlan
	if stmt.OrderBy != nil {
		idx := -1
		// Split qualified name (e.g., "users.name") into table and column
		orderCol := stmt.OrderBy.Column
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
			return nil, fmt.Errorf("%w: ORDER BY %s", plannerapi.ErrColumnNotFound, stmt.OrderBy.Column)
		}
		// If GROUP BY is present, map the ORDER BY index from combinedSchema
		// to the position in SELECT columns. After GROUP BY, the result only
		// contains SELECT columns,not the full combined schema.
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
		orderBy = &plannerapi.OrderByPlan{ColumnIndex: idx, Desc: stmt.OrderBy.Desc}
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
			if len(groupByExprs) > 0 && !isInGroupBy(expr.Column, groupByExprs) {
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
		case *parserapi.BinaryExpr:
			// Binary expressions (e.g., 1+1, a+b) are evaluated by the executor; set -1 as sentinel.
			indices[i] = -1
		default:
			return nil, fmt.Errorf("%w: SELECT expression must be a column reference or aggregate", plannerapi.ErrUnsupportedExpr)
		}
	}
	return indices, nil
}

// isInGroupBy checks if a column name appears in the GROUP BY expression list.
func isInGroupBy(colName string, groupByExprs []parserapi.Expr) bool {
	for _, gb := range groupByExprs {
		if gbRef, ok := gb.(*parserapi.ColumnRef); ok {
			if strings.EqualFold(colName, gbRef.Column) {
				return true
			}
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
	case *parserapi.AggregateCallExpr:
		// No subquery inside aggregates
	case *parserapi.Literal, *parserapi.ColumnRef, *parserapi.StarExpr:
		// Leaf nodes
	}
	return nil
}
