package internal

import (
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
)

// ─── Test helpers ───────────────────────────────────────────────────

func openTestStore(t *testing.T) kvstoreapi.Store {
	t.Helper()
	dir := t.TempDir()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestEngines(t *testing.T) (engineapi.TableEngine, engineapi.IndexEngine, kvstoreapi.Store) {
	t.Helper()
	store := openTestStore(t)
	encoder := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	te := NewTableEngine(store, encoder, codec)
	ie := NewIndexEngine(store, encoder)
	return te, ie, store
}

func testTable() *catalogapi.TableSchema {
	return &catalogapi.TableSchema{
		Name: "users",
		Columns: []catalogapi.ColumnDef{
			{Name: "id", Type: catalogapi.TypeInt},
			{Name: "name", Type: catalogapi.TypeText},
			{Name: "age", Type: catalogapi.TypeInt},
		},
		TableID: 1,
	}
}

func testTableWithPK() *catalogapi.TableSchema {
	return &catalogapi.TableSchema{
		Name:       "users",
		Columns:    []catalogapi.ColumnDef{
			{Name: "id", Type: catalogapi.TypeInt},
			{Name: "name", Type: catalogapi.TypeText},
			{Name: "age", Type: catalogapi.TypeInt},
		},
		PrimaryKey: "id",
		TableID:    2,
	}
}

func intVal(v int64) catalogapi.Value {
	return catalogapi.Value{Type: catalogapi.TypeInt, Int: v}
}

func textVal(v string) catalogapi.Value {
	return catalogapi.Value{Type: catalogapi.TypeText, Text: v}
}

func nullVal() catalogapi.Value {
	return catalogapi.Value{IsNull: true}
}

// ─── TableEngine Tests ──────────────────────────────────────────────

func TestTableEngine_InsertGet(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	values := []catalogapi.Value{intVal(1), textVal("Alice"), intVal(30)}
	rowID, err := te.Insert(table, values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected non-zero rowID")
	}

	row, err := te.Get(table, rowID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if row.RowID != rowID {
		t.Errorf("expected rowID %d, got %d", rowID, row.RowID)
	}
	if len(row.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(row.Values))
	}
	if row.Values[0].Int != 1 {
		t.Errorf("expected id=1, got %d", row.Values[0].Int)
	}
	if row.Values[1].Text != "Alice" {
		t.Errorf("expected name=Alice, got %q", row.Values[1].Text)
	}
	if row.Values[2].Int != 30 {
		t.Errorf("expected age=30, got %d", row.Values[2].Int)
	}
}

func TestTableEngine_InsertAutoIncrement(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	var ids []uint64
	for i := 0; i < 5; i++ {
		values := []catalogapi.Value{intVal(int64(i)), textVal("user"), intVal(20)}
		rowID, err := te.Insert(table, values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
		ids = append(ids, rowID)
	}

	// RowIDs should be monotonically increasing.
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("rowID[%d]=%d not > rowID[%d]=%d", i, ids[i], i-1, ids[i-1])
		}
	}
}

func TestTableEngine_InsertWithPK(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTableWithPK()

	values := []catalogapi.Value{intVal(42), textVal("Bob"), intVal(25)}
	rowID, err := te.Insert(table, values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowID != 42 {
		t.Errorf("expected rowID=42 (from PK), got %d", rowID)
	}

	row, err := te.Get(table, 42)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if row.Values[1].Text != "Bob" {
		t.Errorf("expected name=Bob, got %q", row.Values[1].Text)
	}
}

func TestTableEngine_InsertDuplicatePK(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTableWithPK()

	values1 := []catalogapi.Value{intVal(1), textVal("Alice"), intVal(30)}
	_, err := te.Insert(table, values1)
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	values2 := []catalogapi.Value{intVal(1), textVal("Bob"), intVal(25)}
	_, err = te.Insert(table, values2)
	if err != engineapi.ErrDuplicateKey {
		t.Errorf("expected ErrDuplicateKey, got %v", err)
	}
}

func TestTableEngine_Scan(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	// Insert 5 rows.
	for i := 0; i < 5; i++ {
		values := []catalogapi.Value{intVal(int64(i + 1)), textVal("user"), intVal(int64(20 + i))}
		_, err := te.Insert(table, values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Scan all rows.
	iter, err := te.Scan(table)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	defer iter.Close()

	count := 0
	var prevRowID uint64
	for iter.Next() {
		row := iter.Row()
		if row.RowID <= prevRowID && count > 0 {
			t.Errorf("rows not in order: %d <= %d", row.RowID, prevRowID)
		}
		prevRowID = row.RowID
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Scan iteration error: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

func TestTableEngine_Delete(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	values := []catalogapi.Value{intVal(1), textVal("Alice"), intVal(30)}
	rowID, err := te.Insert(table, values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = te.Delete(table, rowID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = te.Get(table, rowID)
	if err != engineapi.ErrRowNotFound {
		t.Errorf("expected ErrRowNotFound after delete, got %v", err)
	}

	// Delete non-existent row.
	err = te.Delete(table, 99999)
	if err != engineapi.ErrRowNotFound {
		t.Errorf("expected ErrRowNotFound for non-existent row, got %v", err)
	}
}

func TestTableEngine_Update(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	values := []catalogapi.Value{intVal(1), textVal("Alice"), intVal(30)}
	rowID, err := te.Insert(table, values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	newValues := []catalogapi.Value{intVal(1), textVal("Alice Updated"), intVal(31)}
	err = te.Update(table, rowID, newValues)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	row, err := te.Get(table, rowID)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if row.Values[1].Text != "Alice Updated" {
		t.Errorf("expected name='Alice Updated', got %q", row.Values[1].Text)
	}
	if row.Values[2].Int != 31 {
		t.Errorf("expected age=31, got %d", row.Values[2].Int)
	}

	// Update non-existent row.
	err = te.Update(table, 99999, newValues)
	if err != engineapi.ErrRowNotFound {
		t.Errorf("expected ErrRowNotFound for non-existent row, got %v", err)
	}
}

func TestTableEngine_DropTableData(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	// Insert 3 rows.
	for i := 0; i < 3; i++ {
		values := []catalogapi.Value{intVal(int64(i + 1)), textVal("user"), intVal(20)}
		_, err := te.Insert(table, values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Drop all data.
	err := te.DropTableData(table.TableID)
	if err != nil {
		t.Fatalf("DropTableData failed: %v", err)
	}

	// Scan should return nothing.
	iter, err := te.Scan(table)
	if err != nil {
		t.Fatalf("Scan after drop failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 rows after drop, got %d", count)
	}
}

func TestTableEngine_NullValues(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	// Insert row with NULL values.
	values := []catalogapi.Value{intVal(1), nullVal(), nullVal()}
	rowID, err := te.Insert(table, values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	row, err := te.Get(table, rowID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !row.Values[1].IsNull {
		t.Error("expected name to be NULL")
	}
	if !row.Values[2].IsNull {
		t.Error("expected age to be NULL")
	}
}

// ─── IndexEngine Tests ──────────────────────────────────────────────

func TestIndexEngine_InsertScan(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	// Insert index entries for ages 10, 20, 30, 20, 40.
	entries := []struct {
		age   int64
		rowID uint64
	}{
		{10, 1}, {20, 2}, {30, 3}, {20, 4}, {40, 5},
	}
	for _, e := range entries {
		err := ie.Insert(idx, tableID, indexID, intVal(e.age), e.rowID)
		if err != nil {
			t.Fatalf("Insert index entry failed: %v", err)
		}
	}

	// Scan for age == 20 → should find rowIDs 2 and 4.
	iter, err := ie.Scan(tableID, indexID, encodingapi.OpEQ, intVal(20))
	if err != nil {
		t.Fatalf("Scan OpEQ failed: %v", err)
	}
	defer iter.Close()

	var found []uint64
	for iter.Next() {
		found = append(found, iter.RowID())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Scan iteration error: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 results for age==20, got %d: %v", len(found), found)
	}
	// Should contain rowIDs 2 and 4 (in order).
	if found[0] != 2 || found[1] != 4 {
		t.Errorf("expected rowIDs [2, 4], got %v", found)
	}
}

func TestIndexEngine_RangeScan(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	// Insert ages: 10, 20, 30, 40, 50.
	for i, age := range []int64{10, 20, 30, 40, 50} {
		err := ie.Insert(idx, tableID, indexID, intVal(age), uint64(i+1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	tests := []struct {
		name     string
		op       encodingapi.CompareOp
		value    int64
		expected []uint64
	}{
		{"GT 25", encodingapi.OpGT, 25, []uint64{3, 4, 5}},   // 30, 40, 50
		{"GE 30", encodingapi.OpGE, 30, []uint64{3, 4, 5}},   // 30, 40, 50
		{"LT 30", encodingapi.OpLT, 30, []uint64{1, 2}},       // 10, 20
		{"LE 30", encodingapi.OpLE, 30, []uint64{1, 2, 3}},    // 10, 20, 30
		{"EQ 30", encodingapi.OpEQ, 30, []uint64{3}},           // 30
		{"GT 50", encodingapi.OpGT, 50, []uint64{}},             // nothing
		{"LT 10", encodingapi.OpLT, 10, []uint64{}},             // nothing
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := ie.Scan(tableID, indexID, tt.op, intVal(tt.value))
			if err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			defer iter.Close()

			var found []uint64
			for iter.Next() {
				found = append(found, iter.RowID())
			}
			if err := iter.Err(); err != nil {
				t.Fatalf("iteration error: %v", err)
			}
			if len(found) != len(tt.expected) {
				t.Fatalf("expected %d results, got %d: %v", len(tt.expected), len(found), found)
			}
			for i, id := range tt.expected {
				if found[i] != id {
					t.Errorf("result[%d]: expected rowID %d, got %d", i, id, found[i])
				}
			}
		})
	}
}

func TestIndexEngine_Delete(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	// Insert an entry.
	err := ie.Insert(idx, tableID, indexID, intVal(25), 1)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify it exists.
	iter, err := ie.Scan(tableID, indexID, encodingapi.OpEQ, intVal(25))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 1 {
		t.Fatalf("expected 1 entry before delete, got %d", count)
	}

	// Delete it.
	err = ie.Delete(idx, tableID, indexID, intVal(25), 1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone.
	iter, err = ie.Scan(tableID, indexID, encodingapi.OpEQ, intVal(25))
	if err != nil {
		t.Fatalf("Scan after delete failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("expected 0 entries after delete, got %d", count)
	}
}

func TestIndexEngine_DropIndexData(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	// Insert several entries.
	for i := 0; i < 5; i++ {
		err := ie.Insert(idx, tableID, indexID, intVal(int64(i*10)), uint64(i+1))
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Drop all.
	err := ie.DropIndexData(tableID, indexID)
	if err != nil {
		t.Fatalf("DropIndexData failed: %v", err)
	}

	// Scan should return nothing.
	iter, err := ie.ScanRange(tableID, indexID, nil, nil)
	if err != nil {
		t.Fatalf("ScanRange after drop failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 entries after drop, got %d", count)
	}
}

func TestIndexEngine_ScanRange(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	// Insert ages: 10, 20, 30, 40, 50.
	for i, age := range []int64{10, 20, 30, 40, 50} {
		err := ie.Insert(idx, tableID, indexID, intVal(age), uint64(i+1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// ScanRange [20, 40) → should find 20, 30.
	startVal := intVal(20)
	endVal := intVal(40)
	iter, err := ie.ScanRange(tableID, indexID, &startVal, &endVal)
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}
	defer iter.Close()

	var found []uint64
	for iter.Next() {
		found = append(found, iter.RowID())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 results for [20,40), got %d: %v", len(found), found)
	}
	if found[0] != 2 || found[1] != 3 {
		t.Errorf("expected rowIDs [2, 3], got %v", found)
	}

	// ScanRange [nil, nil) → all entries.
	iter2, err := ie.ScanRange(tableID, indexID, nil, nil)
	if err != nil {
		t.Fatalf("ScanRange all failed: %v", err)
	}
	defer iter2.Close()

	count := 0
	for iter2.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("expected 5 entries for full scan, got %d", count)
	}
}

func TestTableEngine_InsertNegativePK(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTableWithPK()

	// F-W2: negative PK should be rejected.
	values := []catalogapi.Value{intVal(-1), textVal("Bad"), intVal(99)}
	_, err := te.Insert(table, values)
	if err == nil {
		t.Fatal("expected error for negative PK, got nil")
	}
	// Verify row was NOT inserted.
	_, err = te.Get(table, 0) // rowID 0 doesn't exist
	if err != engineapi.ErrRowNotFound {
		t.Fatalf("expected ErrRowNotFound for rowID 0, got %v", err)
	}
}
