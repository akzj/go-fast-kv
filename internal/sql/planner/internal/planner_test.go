package internal

import (
	"errors"
	"strings"
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// setupPlanner creates a planner with a real catalog backed by kvstore.
// It pre-creates a USERS table with columns: ID(INT), NAME(TEXT), AGE(INT).
func setupPlanner(t *testing.T) *planner {
	t.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cat := catalog.New(store)
	err = cat.CreateTable(catalogapi.TableSchema{
		Name: "USERS",
		Columns: []catalogapi.ColumnDef{
			{Name: "ID", Type: catalogapi.TypeInt},
			{Name: "NAME", Type: catalogapi.TypeText},
			{Name: "AGE", Type: catalogapi.TypeInt},
		},
		PrimaryKey: "ID",
		TableID:    1,
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	return New(cat)
}

// setupPlannerWithIndex creates a planner with USERS table + index on AGE.
func setupPlannerWithIndex(t *testing.T) *planner {
	t.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cat := catalog.New(store)
	err = cat.CreateTable(catalogapi.TableSchema{
		Name: "USERS",
		Columns: []catalogapi.ColumnDef{
			{Name: "ID", Type: catalogapi.TypeInt},
			{Name: "NAME", Type: catalogapi.TypeText},
			{Name: "AGE", Type: catalogapi.TypeInt},
		},
		PrimaryKey: "ID",
		TableID:    1,
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	err = cat.CreateIndex(catalogapi.IndexSchema{
		Name:    "IDX_AGE",
		Table:   "USERS",
		Column:  "AGE",
		IndexID: 10,
	})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}

	return New(cat)
}

func TestPlan_CreateTable(t *testing.T) {
	p := setupPlanner(t)

	stmt := &parserapi.CreateTableStmt{
		Table: "PRODUCTS",
		Columns: []parserapi.ColumnDef{
			{Name: "ID", TypeName: "INT", PrimaryKey: true},
			{Name: "TITLE", TypeName: "TEXT"},
			{Name: "PRICE", TypeName: "FLOAT"},
		},
		IfNotExists: true,
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	cp, ok := plan.(*plannerapi.CreateTablePlan)
	if !ok {
		t.Fatalf("expected CreateTablePlan, got %T", plan)
	}
	if cp.Schema.Name != "PRODUCTS" {
		t.Errorf("expected table name PRODUCTS, got %s", cp.Schema.Name)
	}
	if len(cp.Schema.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(cp.Schema.Columns))
	}
	if cp.Schema.Columns[0].Type != catalogapi.TypeInt {
		t.Errorf("expected column 0 type INT, got %v", cp.Schema.Columns[0].Type)
	}
	if cp.Schema.Columns[2].Type != catalogapi.TypeFloat {
		t.Errorf("expected column 2 type FLOAT, got %v", cp.Schema.Columns[2].Type)
	}
	if cp.Schema.PrimaryKey != "ID" {
		t.Errorf("expected PK=ID, got %s", cp.Schema.PrimaryKey)
	}
	if !cp.IfNotExists {
		t.Error("expected IfNotExists=true")
	}
}

func TestPlan_DropTable(t *testing.T) {
	p := setupPlanner(t)

	t.Run("ExistingTable", func(t *testing.T) {
		stmt := &parserapi.DropTableStmt{Table: "USERS"}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		dp, ok := plan.(*plannerapi.DropTablePlan)
		if !ok {
			t.Fatalf("expected DropTablePlan, got %T", plan)
		}
		if dp.TableName != "USERS" {
			t.Errorf("expected USERS, got %s", dp.TableName)
		}
		if dp.TableID != 1 {
			t.Errorf("expected TableID=1, got %d", dp.TableID)
		}
	})

	t.Run("NonExistentIfExists", func(t *testing.T) {
		stmt := &parserapi.DropTableStmt{Table: "NOPE", IfExists: true}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		dp := plan.(*plannerapi.DropTablePlan)
		if dp.TableID != 0 {
			t.Errorf("expected TableID=0 for non-existent, got %d", dp.TableID)
		}
		if !dp.IfExists {
			t.Error("expected IfExists=true")
		}
	})

	t.Run("NonExistentNoIfExists", func(t *testing.T) {
		stmt := &parserapi.DropTableStmt{Table: "NOPE"}
		_, err := p.Plan(stmt)
		if err == nil {
			t.Fatal("expected error for non-existent table without IF EXISTS")
		}
		if !errors.Is(err, plannerapi.ErrTableNotFound) {
			t.Errorf("expected ErrTableNotFound, got: %v", err)
		}
	})
}

func TestPlan_AlterTable(t *testing.T) {
	p := setupPlanner(t)

	t.Run("AddColumn_Int", func(t *testing.T) {
		stmt := &parserapi.AlterTableStmt{
			Table:     "USERS",
			Operation: parserapi.AlterAddColumn,
			Column:    "AGE",
			TypeName:  "INT",
		}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		at, ok := plan.(*plannerapi.AlterTablePlan)
		if !ok {
			t.Fatalf("expected AlterTablePlan, got %T", plan)
		}
		if at.TableName != "USERS" {
			t.Errorf("table: expected USERS, got %s", at.TableName)
		}
		if at.Operation != parserapi.AlterAddColumn {
			t.Errorf("operation: expected AlterAddColumn, got %v", at.Operation)
		}
		if at.ColumnName != "AGE" {
			t.Errorf("column: expected AGE, got %s", at.ColumnName)
		}
		if at.TypeName != "INT" {
			t.Errorf("type: expected INT, got %s", at.TypeName)
		}
	})

	t.Run("AddColumn_Text", func(t *testing.T) {
		stmt := &parserapi.AlterTableStmt{
			Table:     "USERS",
			Operation: parserapi.AlterAddColumn,
			Column:    "EMAIL",
			TypeName:  "TEXT",
		}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		at := plan.(*plannerapi.AlterTablePlan)
		if at.TypeName != "TEXT" {
			t.Errorf("type: expected TEXT, got %s", at.TypeName)
		}
	})

	t.Run("AddColumn_NotNull", func(t *testing.T) {
		stmt := &parserapi.AlterTableStmt{
			Table:     "USERS",
			Operation: parserapi.AlterAddColumn,
			Column:    "NICK",
			TypeName:  "TEXT",
			NotNull:   true,
		}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		at := plan.(*plannerapi.AlterTablePlan)
		if !at.NotNull {
			t.Error("expected NotNull=true")
		}
	})

	t.Run("DropColumn", func(t *testing.T) {
		stmt := &parserapi.AlterTableStmt{
			Table:     "USERS",
			Operation: parserapi.AlterDropColumn,
			Column:    "AGE",
		}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		at, ok := plan.(*plannerapi.AlterTablePlan)
		if !ok {
			t.Fatalf("expected AlterTablePlan, got %T", plan)
		}
		if at.Operation != parserapi.AlterDropColumn {
			t.Errorf("operation: expected AlterDropColumn, got %v", at.Operation)
		}
	})

	t.Run("RenameColumn", func(t *testing.T) {
		stmt := &parserapi.AlterTableStmt{
			Table:      "USERS",
			Operation:  parserapi.AlterRenameColumn,
			Column:     "OLD_NAME",
			ColumnNew:  "NEW_NAME",
		}
		plan, err := p.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}
		at := plan.(*plannerapi.AlterTablePlan)
		if at.Operation != parserapi.AlterRenameColumn {
			t.Errorf("operation: expected AlterRenameColumn, got %v", at.Operation)
		}
		if at.ColumnNew != "NEW_NAME" {
			t.Errorf("columnNew: expected NEW_NAME, got %s", at.ColumnNew)
		}
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		stmt := &parserapi.AlterTableStmt{
			Table:     "NOPE",
			Operation: parserapi.AlterAddColumn,
			Column:    "COL",
			TypeName:  "INT",
		}
		_, err := p.Plan(stmt)
		if err == nil {
			t.Fatal("expected error for non-existent table")
		}
		if !errors.Is(err, plannerapi.ErrTableNotFound) {
			t.Errorf("expected ErrTableNotFound, got: %v", err)
		}
	})
}

func TestPlan_Insert(t *testing.T) {
	p := setupPlanner(t)

	stmt := &parserapi.InsertStmt{
		Table: "USERS",
		Values: [][]parserapi.Expr{
			{
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}},
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: "Alice"}},
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 30}},
			},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	ip, ok := plan.(*plannerapi.InsertPlan)
	if !ok {
		t.Fatalf("expected InsertPlan, got %T", plan)
	}
	if ip.Table.Name != "USERS" {
		t.Errorf("expected table USERS, got %s", ip.Table.Name)
	}
	if len(ip.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(ip.Rows))
	}
	if len(ip.Rows[0]) != 3 {
		t.Fatalf("expected 3 values, got %d", len(ip.Rows[0]))
	}
	if ip.Rows[0][0].Int != 1 {
		t.Errorf("expected ID=1, got %d", ip.Rows[0][0].Int)
	}
	if ip.Rows[0][1].Text != "Alice" {
		t.Errorf("expected NAME=Alice, got %s", ip.Rows[0][1].Text)
	}
}

func TestPlan_InsertColumnReorder(t *testing.T) {
	p := setupPlanner(t)

	// Specify columns in different order: NAME, ID, AGE
	stmt := &parserapi.InsertStmt{
		Table:   "USERS",
		Columns: []string{"NAME", "ID", "AGE"},
		Values: [][]parserapi.Expr{
			{
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: "Bob"}},
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 2}},
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 25}},
			},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	ip := plan.(*plannerapi.InsertPlan)
	row := ip.Rows[0]
	// Values should be reordered to match table columns: ID, NAME, AGE
	if row[0].Int != 2 {
		t.Errorf("expected ID=2 at index 0, got %d", row[0].Int)
	}
	if row[1].Text != "Bob" {
		t.Errorf("expected NAME=Bob at index 1, got %s", row[1].Text)
	}
	if row[2].Int != 25 {
		t.Errorf("expected AGE=25 at index 2, got %d", row[2].Int)
	}
}

func TestPlan_SelectTableScan(t *testing.T) {
	p := setupPlanner(t)

	// SELECT NAME, AGE FROM USERS WHERE AGE > 18
	// No index on AGE → should produce TableScanPlan
	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.ColumnRef{Column: "NAME"}},
			{Expr: &parserapi.ColumnRef{Column: "AGE"}},
		},
		Where: &parserapi.BinaryExpr{
			Left:  &parserapi.ColumnRef{Column: "AGE"},
			Op:    parserapi.BinGT,
			Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 18}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	if len(sp.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(sp.Columns))
	}
	if sp.Columns[0] != 1 { // NAME is index 1
		t.Errorf("expected column index 1 (NAME), got %d", sp.Columns[0])
	}
	if sp.Columns[1] != 2 { // AGE is index 2
		t.Errorf("expected column index 2 (AGE), got %d", sp.Columns[1])
	}

	ts, ok := sp.Scan.(*plannerapi.TableScanPlan)
	if !ok {
		t.Fatalf("expected TableScanPlan, got %T", sp.Scan)
	}
	if ts.TableID != 1 {
		t.Errorf("expected TableID=1, got %d", ts.TableID)
	}
	if ts.Filter == nil {
		t.Error("expected filter on TableScanPlan")
	}
	if sp.Limit != -1 {
		t.Errorf("expected Limit=-1, got %d", sp.Limit)
	}
}

func TestPlan_SelectIndexScan(t *testing.T) {
	p := setupPlannerWithIndex(t)

	// SELECT * FROM USERS WHERE AGE = 25
	// Index on AGE → should produce IndexScanPlan
	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.StarExpr{}},
		},
		Where: &parserapi.BinaryExpr{
			Left:  &parserapi.ColumnRef{Column: "AGE"},
			Op:    parserapi.BinEQ,
			Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 25}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	// SELECT * → empty columns
	if len(sp.Columns) != 0 {
		t.Errorf("expected empty columns for *, got %d", len(sp.Columns))
	}

	is, ok := sp.Scan.(*plannerapi.IndexScanPlan)
	if !ok {
		t.Fatalf("expected IndexScanPlan, got %T", sp.Scan)
	}
	if is.IndexID != 10 {
		t.Errorf("expected IndexID=10, got %d", is.IndexID)
	}
	if is.Op != encodingapi.OpEQ {
		t.Errorf("expected OpEQ, got %v", is.Op)
	}
	if is.Value.Int != 25 {
		t.Errorf("expected scan value 25, got %d", is.Value.Int)
	}
	if is.ResidualFilter != nil {
		t.Error("expected no residual filter for single equality scan")
	}
}

func TestPlan_SelectIndexOnlyScan(t *testing.T) {
	p := setupPlannerWithIndex(t)

	// SELECT AGE FROM USERS WHERE AGE = 25
	// Index on AGE → should produce IndexOnlyScanPlan because:
	// - SELECT only references AGE (indexed column)
	// - WHERE only references AGE (indexed column)
	// - No ORDER BY
	// All columns are in the index → covering index → no table access needed.
	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.ColumnRef{Column: "AGE"}},
		},
		Where: &parserapi.BinaryExpr{
			Left:  &parserapi.ColumnRef{Column: "AGE"},
			Op:    parserapi.BinEQ,
			Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 25}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	// SELECT AGE → column index 2 (AGE is the 3rd column: ID=0, NAME=1, AGE=2)
	if len(sp.Columns) != 1 || sp.Columns[0] != 2 {
		t.Errorf("expected Columns=[2], got %v", sp.Columns)
	}

	is, ok := sp.Scan.(*plannerapi.IndexOnlyScanPlan)
	if !ok {
		t.Fatalf("expected IndexOnlyScanPlan, got %T", sp.Scan)
	}
	if is.IndexID != 10 {
		t.Errorf("expected IndexID=10, got %d", is.IndexID)
	}
	if is.Op != encodingapi.OpEQ {
		t.Errorf("expected OpEQ, got %v", is.Op)
	}
	if is.Value.Int != 25 {
		t.Errorf("expected scan value 25, got %d", is.Value.Int)
	}
	if is.IndexedColumnIdx != 0 {
		t.Errorf("expected IndexedColumnIdx=0 (first column in SELECT), got %d", is.IndexedColumnIdx)
	}
	if is.ResidualFilter != nil {
		t.Error("expected no residual filter for single equality scan")
	}
	// Verify EXPLAIN output shows "INDEX ONLY SCAN"
	explain := sp.String()
	if !strings.Contains(explain, "INDEX ONLY SCAN") {
		t.Errorf("expected EXPLAIN to contain 'INDEX ONLY SCAN', got:\n%s", explain)
	}
}

func TestPlan_SelectIndexOnlyScan_FallbackToIndexScan(t *testing.T) {
	p := setupPlannerWithIndex(t)

	// SELECT NAME FROM USERS WHERE AGE = 25
	// Index on AGE, but SELECT references NAME (not indexed) → NOT covering.
	// Should fall back to IndexScanPlan (not IndexOnlyScanPlan).
	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.ColumnRef{Column: "NAME"}},
		},
		Where: &parserapi.BinaryExpr{
			Left:  &parserapi.ColumnRef{Column: "AGE"},
			Op:    parserapi.BinEQ,
			Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 25}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	// Should be IndexScanPlan (uses index, but still needs table for NAME)
	_, ok := sp.Scan.(*plannerapi.IndexScanPlan)
	if !ok {
		t.Fatalf("expected IndexScanPlan (not covering), got %T", sp.Scan)
	}
	// Should NOT be IndexOnlyScanPlan
	if _, bad := sp.Scan.(*plannerapi.IndexOnlyScanPlan); bad {
		t.Error("expected IndexScanPlan, not IndexOnlyScanPlan (NAME not in index)")
	}
	// Verify EXPLAIN output shows "INDEX SCAN" (not "INDEX ONLY SCAN")
	explain := sp.String()
	if !strings.Contains(explain, "INDEX SCAN") {
		t.Errorf("expected EXPLAIN to contain 'INDEX SCAN', got:\n%s", explain)
	}
	if strings.Contains(explain, "INDEX ONLY SCAN") {
		t.Errorf("expected EXPLAIN NOT to contain 'INDEX ONLY SCAN', got:\n%s", explain)
	}
}

func TestPlan_SelectStar(t *testing.T) {
	p := setupPlanner(t)

	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.StarExpr{}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	if len(sp.Columns) != 0 {
		t.Errorf("expected empty columns for *, got %v", sp.Columns)
	}
	if _, ok := sp.Scan.(*plannerapi.TableScanPlan); !ok {
		t.Errorf("expected TableScanPlan for no-WHERE, got %T", sp.Scan)
	}
}

func TestPlan_SelectOrderByLimit(t *testing.T) {
	p := setupPlanner(t)

	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.StarExpr{}},
		},
		OrderBy: []*parserapi.OrderByClause{{Column: "AGE", Desc: true}},
		Limit:   &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 10}},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	if len(sp.OrderBy) == 0 {
		t.Fatal("expected OrderBy plan")
	}
	if sp.OrderBy[0].ColumnIndex != 2 { // AGE is index 2
		t.Errorf("expected ORDER BY column index 2, got %d", sp.OrderBy[0].ColumnIndex)
	}
	if !sp.OrderBy[0].Desc {
		t.Error("expected DESC=true")
	}
	if sp.Limit != 10 {
		t.Errorf("expected Limit=10, got %d", sp.Limit)
	}
}

func TestPlan_Delete(t *testing.T) {
	p := setupPlannerWithIndex(t)

	// DELETE FROM USERS WHERE AGE = 30
	stmt := &parserapi.DeleteStmt{
		Table: "USERS",
		Where: &parserapi.BinaryExpr{
			Left:  &parserapi.ColumnRef{Column: "AGE"},
			Op:    parserapi.BinEQ,
			Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 30}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	dp := plan.(*plannerapi.DeletePlan)
	if dp.Table.Name != "USERS" {
		t.Errorf("expected USERS, got %s", dp.Table.Name)
	}
	// Should use index scan since AGE has an index
	is, ok := dp.Scan.(*plannerapi.IndexScanPlan)
	if !ok {
		t.Fatalf("expected IndexScanPlan, got %T", dp.Scan)
	}
	if is.Op != encodingapi.OpEQ {
		t.Errorf("expected OpEQ, got %v", is.Op)
	}
}

func TestPlan_Update(t *testing.T) {
	p := setupPlanner(t)

	// UPDATE USERS SET NAME = 'Charlie', AGE = 35 WHERE ID = 1
	stmt := &parserapi.UpdateStmt{
		Table: "USERS",
		Assignments: []parserapi.Assignment{
			{Column: "NAME", Value: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: "Charlie"}}},
			{Column: "AGE", Value: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 35}}},
		},
		Where: &parserapi.BinaryExpr{
			Left:  &parserapi.ColumnRef{Column: "ID"},
			Op:    parserapi.BinEQ,
			Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	up := plan.(*plannerapi.UpdatePlan)
	if len(up.Assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(up.Assignments))
	}
	// NAME is column index 1
	if v, ok := up.Assignments[1]; !ok || v.Text != "Charlie" {
		t.Errorf("expected NAME=Charlie at index 1, got %v", up.Assignments[1])
	}
	// AGE is column index 2
	if v, ok := up.Assignments[2]; !ok || v.Int != 35 {
		t.Errorf("expected AGE=35 at index 2, got %v", up.Assignments[2])
	}
}

func TestPlan_UpdateRejectColumnRef(t *testing.T) {
	p := setupPlanner(t)

	// UPDATE USERS SET NAME = AGE → should fail (Phase 1: literals only)
	stmt := &parserapi.UpdateStmt{
		Table: "USERS",
		Assignments: []parserapi.Assignment{
			{Column: "NAME", Value: &parserapi.ColumnRef{Column: "AGE"}},
		},
	}

	_, err := p.Plan(stmt)
	if err == nil {
		t.Fatal("expected error for column ref in SET")
	}
	if !errors.Is(err, plannerapi.ErrUnsupportedExpr) {
		t.Errorf("expected ErrUnsupportedExpr, got: %v", err)
	}
}

func TestPlan_Errors(t *testing.T) {
	p := setupPlanner(t)

	t.Run("TableNotFound", func(t *testing.T) {
		stmt := &parserapi.SelectStmt{
			Table:   "NONEXISTENT",
			Columns: []parserapi.SelectColumn{{Expr: &parserapi.StarExpr{}}},
		}
		_, err := p.Plan(stmt)
		if !errors.Is(err, plannerapi.ErrTableNotFound) {
			t.Errorf("expected ErrTableNotFound, got: %v", err)
		}
	})

	t.Run("ColumnNotFound", func(t *testing.T) {
		stmt := &parserapi.SelectStmt{
			Table: "USERS",
			Columns: []parserapi.SelectColumn{
				{Expr: &parserapi.ColumnRef{Column: "NONEXISTENT"}},
			},
		}
		_, err := p.Plan(stmt)
		if !errors.Is(err, plannerapi.ErrColumnNotFound) {
			t.Errorf("expected ErrColumnNotFound, got: %v", err)
		}
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		// INSERT text value into INT column
		stmt := &parserapi.InsertStmt{
			Table: "USERS",
			Values: [][]parserapi.Expr{
				{
					&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: "notanint"}},
					&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: "Alice"}},
					&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 30}},
				},
			},
		}
		_, err := p.Plan(stmt)
		if !errors.Is(err, plannerapi.ErrTypeMismatch) {
			t.Errorf("expected ErrTypeMismatch, got: %v", err)
		}
	})

	t.Run("ColumnCountMismatch", func(t *testing.T) {
		stmt := &parserapi.InsertStmt{
			Table: "USERS",
			Values: [][]parserapi.Expr{
				{
					&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}},
					// only 2 values for 3-column table
					&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeText, Text: "Alice"}},
				},
			},
		}
		_, err := p.Plan(stmt)
		if !errors.Is(err, plannerapi.ErrColumnCountMismatch) {
			t.Errorf("expected ErrColumnCountMismatch, got: %v", err)
		}
	})

	t.Run("OrderByColumnNotFound", func(t *testing.T) {
		stmt := &parserapi.SelectStmt{
			Table:   "USERS",
			Columns: []parserapi.SelectColumn{{Expr: &parserapi.StarExpr{}}},
			OrderBy: []*parserapi.OrderByClause{{Column: "NONEXISTENT"}},
		}
		_, err := p.Plan(stmt)
		if !errors.Is(err, plannerapi.ErrColumnNotFound) {
			t.Errorf("expected ErrColumnNotFound, got: %v", err)
		}
	})

	t.Run("CreateTableEmpty", func(t *testing.T) {
		stmt := &parserapi.CreateTableStmt{Table: "EMPTY"}
		_, err := p.Plan(stmt)
		if !errors.Is(err, plannerapi.ErrEmptyTable) {
			t.Errorf("expected ErrEmptyTable, got: %v", err)
		}
	})
}

func TestPlan_CreateIndex(t *testing.T) {
	p := setupPlanner(t)

	stmt := &parserapi.CreateIndexStmt{
		Index:       "IDX_NAME",
		Table:       "USERS",
		Column:      "NAME",
		Unique:      true,
		IfNotExists: true,
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	cp, ok := plan.(*plannerapi.CreateIndexPlan)
	if !ok {
		t.Fatalf("expected CreateIndexPlan, got %T", plan)
	}
	if cp.Schema.Name != "IDX_NAME" {
		t.Errorf("expected IDX_NAME, got %s", cp.Schema.Name)
	}
	if cp.Schema.Table != "USERS" {
		t.Errorf("expected table USERS, got %s", cp.Schema.Table)
	}
	if cp.Schema.Column != "NAME" {
		t.Errorf("expected column NAME, got %s", cp.Schema.Column)
	}
	if !cp.Schema.Unique {
		t.Error("expected Unique=true")
	}
	if !cp.IfNotExists {
		t.Error("expected IfNotExists=true")
	}
}

func TestPlan_DropIndex(t *testing.T) {
	p := setupPlanner(t)

	stmt := &parserapi.DropIndexStmt{
		Index:    "IDX_AGE",
		Table:    "USERS",
		IfExists: true,
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	dp, ok := plan.(*plannerapi.DropIndexPlan)
	if !ok {
		t.Fatalf("expected DropIndexPlan, got %T", plan)
	}
	if dp.IndexName != "IDX_AGE" {
		t.Errorf("expected IDX_AGE, got %s", dp.IndexName)
	}
	if dp.TableName != "USERS" {
		t.Errorf("expected USERS, got %s", dp.TableName)
	}
	if !dp.IfExists {
		t.Error("expected IfExists=true")
	}
}

func TestPlan_SelectIndexPreference(t *testing.T) {
	// Test that '=' is preferred over range when both are available
	p := setupPlannerWithIndex(t)

	// WHERE AGE > 10 AND AGE = 25 → should pick AGE = 25 (equality)
	stmt := &parserapi.SelectStmt{
		Table: "USERS",
		Columns: []parserapi.SelectColumn{
			{Expr: &parserapi.StarExpr{}},
		},
		Where: &parserapi.BinaryExpr{
			Left: &parserapi.BinaryExpr{
				Left:  &parserapi.ColumnRef{Column: "AGE"},
				Op:    parserapi.BinGT,
				Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 10}},
			},
			Op: parserapi.BinAnd,
			Right: &parserapi.BinaryExpr{
				Left:  &parserapi.ColumnRef{Column: "AGE"},
				Op:    parserapi.BinEQ,
				Right: &parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 25}},
			},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	is, ok := sp.Scan.(*plannerapi.IndexScanPlan)
	if !ok {
		t.Fatalf("expected IndexScanPlan, got %T", sp.Scan)
	}
	if is.Op != encodingapi.OpEQ {
		t.Errorf("expected equality scan preferred, got op %v", is.Op)
	}
	if is.Value.Int != 25 {
		t.Errorf("expected scan value 25, got %d", is.Value.Int)
	}
	// The range condition should be residual
	if is.ResidualFilter == nil {
		t.Error("expected residual filter for the range condition")
	}
}

func TestPlan_DeleteAll(t *testing.T) {
	p := setupPlanner(t)

	// DELETE FROM USERS (no WHERE)
	stmt := &parserapi.DeleteStmt{Table: "USERS"}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	dp := plan.(*plannerapi.DeletePlan)
	ts, ok := dp.Scan.(*plannerapi.TableScanPlan)
	if !ok {
		t.Fatalf("expected TableScanPlan for delete-all, got %T", dp.Scan)
	}
	if ts.Filter != nil {
		t.Error("expected nil filter for delete-all")
	}
}

func TestPlan_InsertNull(t *testing.T) {
	p := setupPlanner(t)

	// INSERT INTO USERS VALUES (1, NULL, 30)
	stmt := &parserapi.InsertStmt{
		Table: "USERS",
		Values: [][]parserapi.Expr{
			{
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}},
				&parserapi.Literal{Value: catalogapi.Value{IsNull: true}},
				&parserapi.Literal{Value: catalogapi.Value{Type: catalogapi.TypeInt, Int: 30}},
			},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	ip := plan.(*plannerapi.InsertPlan)
	if !ip.Rows[0][1].IsNull {
		t.Error("expected NULL for column 1")
	}
}

// setupPlannerWithJoinTables creates a planner with two tables for join testing.
func setupPlannerWithJoinTables(t *testing.T) *planner {
	t.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cat := catalog.New(store)

	// Create t1 table
	err = cat.CreateTable(catalogapi.TableSchema{
		Name: "t1",
		Columns: []catalogapi.ColumnDef{
			{Name: "id", Type: catalogapi.TypeInt},
			{Name: "x", Type: catalogapi.TypeInt},
		},
		PrimaryKey: "id",
		TableID:    1,
	})
	if err != nil {
		t.Fatalf("create t1 table: %v", err)
	}

	// Create t2 table
	err = cat.CreateTable(catalogapi.TableSchema{
		Name: "t2",
		Columns: []catalogapi.ColumnDef{
			{Name: "id", Type: catalogapi.TypeInt},
			{Name: "y", Type: catalogapi.TypeInt},
		},
		PrimaryKey: "id",
		TableID:    2,
	})
	if err != nil {
		t.Fatalf("create t2 table: %v", err)
	}

	return New(cat)
}

func TestPlan_IndexNestedLoopJoin(t *testing.T) {
	p := setupPlannerWithJoinTables(t)

	// Create index on t1.id
	err := p.catalog.CreateIndex(catalogapi.IndexSchema{
		Name:   "idx_t1_id",
		Table:  "t1",
		Column: "id",
		IndexID: 10,
	})
	if err != nil {
		t.Fatalf("create index on t1.id: %v", err)
	}

	// Query: SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
	stmt := &parserapi.SelectStmt{
		Join: &parserapi.JoinExpr{
			Type: parserapi.JoinType("INNER"),
			Left: "t1",
			Right: "t2",
			On: &parserapi.BinaryExpr{
				Left:  &parserapi.ColumnRef{Table: "t1", Column: "id"},
				Op:    parserapi.BinEQ,
				Right: &parserapi.ColumnRef{Table: "t2", Column: "id"},
			},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	if sp.Join == nil {
		t.Fatal("expected Join plan")
	}

	// Should use IndexNestedLoopJoinPlan since t1.id has an index
	inlPlan, ok := sp.Join.(*plannerapi.IndexNestedLoopJoinPlan)
	if !ok {
		t.Fatalf("expected IndexNestedLoopJoinPlan, got %T", sp.Join)
	}

	// Verify the plan structure
	if inlPlan.OuterTable != "t2" {
		t.Errorf("OuterTable = %q, want t2", inlPlan.OuterTable)
	}
	if inlPlan.InnerTable != "t1" {
		t.Errorf("InnerTable = %q, want t1", inlPlan.InnerTable)
	}
	if inlPlan.InnerIndex == nil {
		t.Error("InnerIndex should not be nil")
	}
	if inlPlan.Type != "INNER" {
		t.Errorf("Type = %q, want INNER", inlPlan.Type)
	}

	// Verify EXPLAIN output shows "INDEX NESTED LOOP JOIN"
	explain := sp.String()
	if !strings.Contains(explain, "INDEX NESTED LOOP JOIN") {
		t.Errorf("expected EXPLAIN to contain 'INDEX NESTED LOOP JOIN', got:\n%s", explain)
	}
}

func TestPlan_IndexNestedLoopJoin_FallbackToHashJoin(t *testing.T) {
	p := setupPlannerWithJoinTables(t)

	// NO index on either join column - should fall back to HashJoin

	stmt := &parserapi.SelectStmt{
		Join: &parserapi.JoinExpr{
			Type: parserapi.JoinType("INNER"),
			Left: "t1",
			Right: "t2",
			On: &parserapi.BinaryExpr{
				Left:  &parserapi.ColumnRef{Table: "t1", Column: "id"},
				Op:    parserapi.BinEQ,
				Right: &parserapi.ColumnRef{Table: "t2", Column: "id"},
			},
		},
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	sp := plan.(*plannerapi.SelectPlan)
	if sp.Join == nil {
		t.Fatal("expected Join plan")
	}

	// Should fall back to HashJoinPlan since no index exists
	hashPlan, ok := sp.Join.(*plannerapi.HashJoinPlan)
	if !ok {
		t.Fatalf("expected HashJoinPlan when no index, got %T", sp.Join)
	}

	// Verify the plan structure
	if hashPlan.LeftTable != "t1" {
		t.Errorf("LeftTable = %q, want t1", hashPlan.LeftTable)
	}
	if hashPlan.RightTable != "t2" {
		t.Errorf("RightTable = %q, want t2", hashPlan.RightTable)
	}
	if hashPlan.Type != "INNER" {
		t.Errorf("Type = %q, want INNER", hashPlan.Type)
	}

	// Verify EXPLAIN output shows "HASH JOIN" (not "INDEX NESTED LOOP JOIN")
	explain := sp.String()
	if strings.Contains(explain, "INDEX NESTED LOOP JOIN") {
		t.Errorf("expected EXPLAIN NOT to contain 'INDEX NESTED LOOP JOIN', got:\n%s", explain)
	}
	if !strings.Contains(explain, "HASH JOIN") {
		t.Errorf("expected EXPLAIN to contain 'HASH JOIN', got:\n%s", explain)
	}
}
