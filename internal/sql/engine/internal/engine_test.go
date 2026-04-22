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

// ─── FTS Engine Tests ───────────────────────────────────────────────

func TestFTSEngine_NewFTSEngine(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)
	if fts == nil {
		t.Fatal("expected non-nil FTSEngine")
	}
	var _ engineapi.FTSEngine = fts
}

func TestFTSEngine_IndexDocument(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	err := fts.IndexDocument("users", 1, []string{"Hello world", "SQL database engine"}, "")
	if err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}
	err = fts.IndexDocument("users", 2, []string{"World of databases"}, "")
	if err != nil {
		t.Fatalf("IndexDocument doc 2 failed: %v", err)
	}

	docIDs, err := fts.Search("users", "world")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(docIDs) != 2 {
		t.Errorf("expected 2 docIDs for 'world', got %d: %v", len(docIDs), docIDs)
	}

	docIDs, err = fts.Search("users", "hello")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1] for 'hello', got %v", docIDs)
	}

	docIDs, err = fts.Search("users", "nonexistent")
	if err != nil {
		t.Fatalf("Search for nonexistent failed: %v", err)
	}
	if len(docIDs) != 0 {
		t.Errorf("expected 0 docIDs for nonexistent, got %d", len(docIDs))
	}
}

func TestFTSEngine_IndexDocument_EmptyTexts(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	err := fts.IndexDocument("users", 1, []string{}, "")
	if err != nil {
		t.Fatalf("IndexDocument with empty texts failed: %v", err)
	}
	err = fts.IndexDocument("users", 2, []string{"", "   ", ""}, "")
	if err != nil {
		t.Fatalf("IndexDocument with whitespace-only texts failed: %v", err)
	}
	docIDs, err := fts.Search("users", "anything")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(docIDs) != 0 {
		t.Errorf("expected 0 docIDs for empty docs, got %d", len(docIDs))
	}
}

func TestFTSEngine_IndexDocument_WithPorterStemmer(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	// Index with porter stemmer
	err := fts.IndexDocument("posts", 1, []string{"running", "databases"}, "porter")
	if err != nil {
		t.Fatalf("IndexDocument with porter failed: %v", err)
	}

	// Porter stemmer stems "running" -> "runn" (not "run")
	// Search for the stemmed form
	docIDs, err := fts.Search("posts", "runn")
	if err != nil {
		t.Fatalf("Search for 'runn' failed: %v", err)
	}
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1] for 'runn' (stemmed from 'running'), got %v", docIDs)
	}

	// Verify non-existent queries return empty
	docIDs, err = fts.Search("posts", "nonexistent")
	if err != nil {
		t.Fatalf("Search for nonexistent failed: %v", err)
	}
	if len(docIDs) != 0 {
		t.Errorf("expected 0 for nonexistent, got %d", len(docIDs))
	}
}

func TestFTSEngine_RemoveDocument(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	err := fts.IndexDocument("users", 1, []string{"Hello world"}, "")
	if err != nil {
		t.Fatalf("IndexDocument doc 1 failed: %v", err)
	}
	err = fts.IndexDocument("users", 2, []string{"Hello database"}, "")
	if err != nil {
		t.Fatalf("IndexDocument doc 2 failed: %v", err)
	}

	docIDs, err := fts.Search("users", "hello")
	if err != nil {
		t.Fatalf("Search before remove failed: %v", err)
	}
	if len(docIDs) != 2 {
		t.Fatalf("expected 2 docIDs before remove, got %d", len(docIDs))
	}

	err = fts.RemoveDocument("users", 1, []string{"Hello world"}, "")
	if err != nil {
		t.Fatalf("RemoveDocument failed: %v", err)
	}
	docIDs, err = fts.Search("users", "hello")
	if err != nil {
		t.Fatalf("Search after remove failed: %v", err)
	}
	if len(docIDs) != 1 || docIDs[0] != 2 {
		t.Errorf("expected [2] after remove, got %v", docIDs)
	}
}

func TestFTSEngine_Search_AND(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	fts.IndexDocument("docs", 1, []string{"SQL database"}, "")
	fts.IndexDocument("docs", 2, []string{"NoSQL database"}, "")
	fts.IndexDocument("docs", 3, []string{"SQL server"}, "")

	docIDs, err := fts.Search("docs", "SQL AND database")
	if err != nil {
		t.Fatalf("Search AND failed: %v", err)
	}
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1] for SQL AND database, got %v", docIDs)
	}
}

func TestFTSEngine_Search_OR(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	fts.IndexDocument("docs", 1, []string{"SQL database"}, "")
	fts.IndexDocument("docs", 2, []string{"NoSQL database"}, "")
	fts.IndexDocument("docs", 3, []string{"SQL server"}, "")

	docIDs, err := fts.Search("docs", "SQL OR server")
	if err != nil {
		t.Fatalf("Search OR failed: %v", err)
	}
	if len(docIDs) != 2 {
		t.Errorf("expected 2 docIDs for SQL OR server, got %d: %v", len(docIDs), docIDs)
	}
}

func TestFTSEngine_Search_NOT(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	fts.IndexDocument("docs", 1, []string{"SQL database"}, "")
	fts.IndexDocument("docs", 2, []string{"NoSQL database"}, "")

	docIDs, err := fts.Search("docs", "database NOT SQL")
	if err != nil {
		t.Fatalf("Search NOT failed: %v", err)
	}
	if len(docIDs) != 1 || docIDs[0] != 2 {
		t.Errorf("expected [2] for database NOT SQL, got %v", docIDs)
	}
}

func TestFTSEngine_Search_EmptyQuery(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	fts.IndexDocument("docs", 1, []string{"SQL"}, "")

	docIDs, err := fts.Search("docs", "")
	if err != nil {
		t.Fatalf("Search empty failed: %v", err)
	}
	if docIDs != nil && len(docIDs) != 0 {
		t.Errorf("expected empty/nil for empty query, got %v", docIDs)
	}

	docIDs, err = fts.Search("docs", "   ")
	if err != nil {
		t.Fatalf("Search whitespace failed: %v", err)
	}
}

func TestFTSEngine_DropFTSData(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	fts.IndexDocument("users", 1, []string{"Alice data"}, "")
	fts.IndexDocument("users", 2, []string{"Bob data"}, "")
	fts.IndexDocument("posts", 1, []string{"Post content"}, "")

	err := fts.DropFTSData("users")
	if err != nil {
		t.Fatalf("DropFTSData failed: %v", err)
	}

	docIDs, err := fts.Search("users", "alice")
	if err != nil {
		t.Fatalf("Search users after drop failed: %v", err)
	}
	if len(docIDs) != 0 {
		t.Errorf("expected 0 docIDs for users after drop, got %d", len(docIDs))
	}

	docIDs, err = fts.Search("posts", "post")
	if err != nil {
		t.Fatalf("Search posts after drop failed: %v", err)
	}
	if len(docIDs) != 1 {
		t.Errorf("expected 1 docID for posts, got %d", len(docIDs))
	}
}

func TestFTSEngine_CaseInsensitive(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	fts.IndexDocument("docs", 1, []string{"Hello WORLD"}, "")

	docIDs, err := fts.Search("docs", "HELLO")
	if err != nil {
		t.Fatalf("Search uppercase failed: %v", err)
	}
	if len(docIDs) != 1 {
		t.Errorf("expected 1 for HELLO, got %d", len(docIDs))
	}

	docIDs, err = fts.Search("docs", "world")
	if err != nil {
		t.Fatalf("Search lowercase failed: %v", err)
	}
	if len(docIDs) != 1 {
		t.Errorf("expected 1 for world, got %d", len(docIDs))
	}
}

func TestFTSEngine_Tokenize(t *testing.T) {
	store := openTestStore(t)
	fts := NewFTSEngine(store)

	// Test that alphanumeric tokens work correctly
	texts := []string{"Hello world", "SQL database"}
	err := fts.IndexDocument("docs", 1, texts, "")
	if err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}

	// Single term searches should work
	docIDs, _ := fts.Search("docs", "hello")
	if len(docIDs) != 1 {
		t.Errorf("expected 'hello' to match doc 1, got %d", len(docIDs))
	}
	docIDs, _ = fts.Search("docs", "world")
	if len(docIDs) != 1 {
		t.Errorf("expected 'world' to match doc 1, got %d", len(docIDs))
	}
	docIDs, _ = fts.Search("docs", "sql")
	if len(docIDs) != 1 {
		t.Errorf("expected 'sql' to match doc 1, got %d", len(docIDs))
	}
	docIDs, _ = fts.Search("docs", "database")
	if len(docIDs) != 1 {
		t.Errorf("expected 'database' to match doc 1, got %d", len(docIDs))
	}

	// Non-existent term
	docIDs, _ = fts.Search("docs", "nonexistent")
	if len(docIDs) != 0 {
		t.Errorf("expected 0 for nonexistent, got %d", len(docIDs))
	}
}

// ─── TableEngine Batch API Tests ────────────────────────────────────

func TestTableEngine_InsertInto(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTable()

	batch := store.NewWriteBatch()
	values := []catalogapi.Value{intVal(1), textVal("Alice"), intVal(30)}
	rowID, err := te.InsertInto(table, batch, values)
	if err != nil {
		t.Fatalf("InsertInto failed: %v", err)
	}
	if rowID != 1 {
		t.Errorf("expected rowID 1, got %d", rowID)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	row, err := te.Get(table, rowID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if row.Values[1].Text != "Alice" {
		t.Errorf("expected name=Alice, got %q", row.Values[1].Text)
	}
}

func TestTableEngine_InsertInto_AutoIncrement(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTable()

	for i := 0; i < 3; i++ {
		batch := store.NewWriteBatch()
		values := []catalogapi.Value{intVal(0), textVal("user"), intVal(20)}
		rowID, err := te.InsertInto(table, batch, values)
		if err != nil {
			t.Fatalf("InsertInto %d failed: %v", i, err)
		}
		if err := batch.Commit(); err != nil {
			t.Fatalf("Commit %d failed: %v", i, err)
		}
		if rowID == 0 {
			t.Errorf("expected non-zero rowID, got 0")
		}
	}
}

func TestTableEngine_InsertInto_WithPK(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTableWithPK()

	batch := store.NewWriteBatch()
	values := []catalogapi.Value{intVal(100), textVal("Bob"), intVal(25)}
	rowID, err := te.InsertInto(table, batch, values)
	if err != nil {
		t.Fatalf("InsertInto with PK failed: %v", err)
	}
	if rowID != 100 {
		t.Errorf("expected rowID=100 from PK, got %d", rowID)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	row, _ := te.Get(table, 100)
	if row.Values[1].Text != "Bob" {
		t.Errorf("expected Bob, got %q", row.Values[1].Text)
	}
}

func TestTableEngine_InsertInto_DuplicatePK(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTableWithPK()

	batch1 := store.NewWriteBatch()
	_, err := te.InsertInto(table, batch1, []catalogapi.Value{intVal(1), textVal("A"), intVal(1)})
	if err != nil {
		t.Fatalf("First InsertInto failed: %v", err)
	}
	if err := batch1.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	batch2 := store.NewWriteBatch()
	_, err = te.InsertInto(table, batch2, []catalogapi.Value{intVal(1), textVal("B"), intVal(2)})
	if err != engineapi.ErrDuplicateKey {
		t.Errorf("expected ErrDuplicateKey, got %v", err)
	}
	batch2.Discard()
}

func TestTableEngine_InsertInto_NegativePK(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTableWithPK()

	batch := store.NewWriteBatch()
	_, err := te.InsertInto(table, batch, []catalogapi.Value{intVal(-5), textVal("Bad"), intVal(99)})
	if err == nil {
		t.Fatal("expected error for negative PK, got nil")
	}
	batch.Discard()
}

func TestTableEngine_DeleteFrom(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTable()

	rowID, _ := te.Insert(table, []catalogapi.Value{intVal(0), textVal("Alice"), intVal(30)})

	batch := store.NewWriteBatch()
	err := te.DeleteFrom(table, batch, rowID)
	if err != nil {
		t.Fatalf("DeleteFrom failed: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	_, err = te.Get(table, rowID)
	if err != engineapi.ErrRowNotFound {
		t.Errorf("expected ErrRowNotFound, got %v", err)
	}
}

func TestTableEngine_UpdateIn(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTable()

	rowID, _ := te.Insert(table, []catalogapi.Value{intVal(0), textVal("Alice"), intVal(30)})

	batch := store.NewWriteBatch()
	newValues := []catalogapi.Value{intVal(0), textVal("Alice Updated"), intVal(31)}
	err := te.UpdateIn(table, batch, rowID, newValues)
	if err != nil {
		t.Fatalf("UpdateIn failed: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	row, _ := te.Get(table, rowID)
	if row.Values[1].Text != "Alice Updated" {
		t.Errorf("expected Alice Updated, got %q", row.Values[1].Text)
	}
}

func TestTableEngine_ScanWithLimit(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	for i := 0; i < 10; i++ {
		te.Insert(table, []catalogapi.Value{intVal(int64(i)), textVal("user"), intVal(int64(20 + i))})
	}

	iter, err := te.ScanWithLimit(table, 3, 0)
	if err != nil {
		t.Fatalf("ScanWithLimit failed: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 3 {
		t.Errorf("expected 3 rows with limit=3, got %d", count)
	}

	iter, err = te.ScanWithLimit(table, 5, 3)
	if err != nil {
		t.Fatalf("ScanWithLimit with offset failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 5 {
		t.Errorf("expected 5 rows with limit=5 offset=3, got %d", count)
	}

	iter, err = te.ScanWithLimit(table, 5, 20)
	if err != nil {
		t.Fatalf("ScanWithLimit offset beyond data failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("expected 0 rows when offset beyond data, got %d", count)
	}
}

// ─── TableEngine Counter Tests ──────────────────────────────────────

func TestTableEngine_CounterOperations(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	counter := te.NextRowID(table.TableID)
	if counter != 0 {
		t.Errorf("expected initial counter=0, got %d", counter)
	}

	te.Insert(table, []catalogapi.Value{intVal(0), textVal("A"), intVal(1)})
	counter = te.NextRowID(table.TableID)
	if counter != 2 {
		t.Errorf("expected counter=2 after 1 insert, got %d", counter)
	}

	got := te.GetCounter(table.TableID)
	if got != counter {
		t.Errorf("GetCounter=%d, NextRowID=%d", got, counter)
	}
}

func TestTableEngine_IncrementCounter(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	te.IncrementCounter(table.TableID)
	te.IncrementCounter(table.TableID)

	counter := te.GetCounter(table.TableID)
	if counter != 2 {
		t.Errorf("expected counter=2 after 2 increments, got %d", counter)
	}

	te.Insert(table, []catalogapi.Value{intVal(0), textVal("A"), intVal(1)})
	counter = te.GetCounter(table.TableID)
	if counter != 3 {
		t.Errorf("expected counter=3 after insert, got %d", counter)
	}
}

func TestTableEngine_AllocRowID(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	id1, err := te.AllocRowID(table.TableID)
	if err != nil {
		t.Fatalf("AllocRowID 1 failed: %v", err)
	}
	if id1 != 1 {
		t.Errorf("expected first AllocRowID=1, got %d", id1)
	}

	id2, err := te.AllocRowID(table.TableID)
	if err != nil {
		t.Fatalf("AllocRowID 2 failed: %v", err)
	}
	if id2 != 2 {
		t.Errorf("expected second AllocRowID=2, got %d", id2)
	}

	te.Insert(table, []catalogapi.Value{intVal(0), textVal("A"), intVal(1)})
	id3, _ := te.AllocRowID(table.TableID)
	if id3 != 3 {
		t.Errorf("expected third AllocRowID=3, got %d", id3)
	}
}

func TestTableEngine_PersistCounter(t *testing.T) {
	te, _, store := newTestEngines(t)
	table := testTable()

	te.Insert(table, []catalogapi.Value{intVal(0), textVal("A"), intVal(1)})
	te.Insert(table, []catalogapi.Value{intVal(0), textVal("B"), intVal(2)})

	counter := te.GetCounter(table.TableID)
	if counter != 3 {
		t.Errorf("expected counter=3, got %d", counter)
	}

	// Force counter to load from KV before persisting.
	_, err := te.AllocRowID(table.TableID)
	if err != nil {
		t.Fatalf("AllocRowID failed: %v", err)
	}

	batch := store.NewWriteBatch()
	err = te.PersistCounter(batch, table.TableID)
	if err != nil {
		t.Fatalf("PersistCounter failed: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// New engine reads persisted counter from KV.
	encoder := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	te2 := NewTableEngine(store, encoder, codec)

	// Verify counter is NOT loaded yet (lazy load)
	counter2 := te2.GetCounter(table.TableID)
	if counter2 != 0 {
		t.Errorf("expected counter=0 (not loaded yet), got %d", counter2)
	}

	// Trigger lazy load from KV by calling AllocRowID
	id, err := te2.AllocRowID(table.TableID)
	if err != nil {
		t.Fatalf("AllocRowID failed: %v", err)
	}
	// PersistCounter persisted 3, so AllocRowID returns 3 and increments to 4
	if id != 3 {
		t.Errorf("expected AllocRowID=3 (persisted value), got %d", id)
	}

	counter3 := te2.GetCounter(table.TableID)
	if counter3 != 4 {
		t.Errorf("expected counter=4 after AllocRowID, got %d", counter3)
	}
}

func TestTableEngine_EncodeRow(t *testing.T) {
	te, _, _ := newTestEngines(t)

	values := []catalogapi.Value{intVal(42), textVal("test"), intVal(99)}
	encoded := te.EncodeRow(values)
	if len(encoded) == 0 {
		t.Error("expected non-empty encoded row")
	}

	codec := encoding.NewRowCodec()
	cols := []catalogapi.ColumnDef{
		{Name: "id", Type: catalogapi.TypeInt},
		{Name: "name", Type: catalogapi.TypeText},
		{Name: "age", Type: catalogapi.TypeInt},
	}
	decoded, err := codec.DecodeRow(encoded, cols)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}
	if decoded[0].Int != 42 || decoded[1].Text != "test" || decoded[2].Int != 99 {
		t.Errorf("decoded values mismatch: %v", decoded)
	}
}

// ─── IndexEngine Batch API Tests ────────────────────────────────────

func TestIndexEngine_BatchInsert(t *testing.T) {
	_, ie, store := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	batch := store.NewWriteBatch()
	key := ie.EncodeIndexKey(tableID, indexID, intVal(25), 1)
	err := ie.InsertBatch(key, batch)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
	key2 := ie.EncodeIndexKey(tableID, indexID, intVal(30), 2)
	err = ie.InsertBatch(key2, batch)
	if err != nil {
		t.Fatalf("InsertBatch 2 failed: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

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
		t.Errorf("expected 1 entry for age=25, got %d", count)
	}
	_ = idx
}

func TestIndexEngine_BatchDelete(t *testing.T) {
	_, ie, store := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	ie.Insert(idx, tableID, indexID, intVal(25), 1)
	ie.Insert(idx, tableID, indexID, intVal(25), 2)

	batch := store.NewWriteBatch()
	key := ie.EncodeIndexKey(tableID, indexID, intVal(25), 1)
	err := ie.DeleteBatch(key, batch)
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	key2 := ie.EncodeIndexKey(tableID, indexID, intVal(25), 2)
	err = ie.DeleteBatch(key2, batch)
	if err != nil {
		t.Fatalf("DeleteBatch 2 failed: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	iter, err := ie.Scan(tableID, indexID, encodingapi.OpEQ, intVal(25))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("expected 0 entries after batch delete, got %d", count)
	}
}

func TestIndexEngine_EncodeIndexKey(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	key := ie.EncodeIndexKey(1, 2, intVal(42), 99)
	if len(key) == 0 {
		t.Error("expected non-empty key")
	}

	key2 := ie.EncodeIndexKey(1, 2, intVal(43), 99)
	if string(key) == string(key2) {
		t.Error("different values should produce different keys")
	}

	key3 := ie.EncodeIndexKey(1, 2, intVal(42), 100)
	if string(key) == string(key3) {
		t.Error("different rowIDs should produce different keys")
	}
}

// ─── IndexEngine Scan Boundary Tests ───────────────────────────────

func TestIndexEngine_Scan_AllBoundaryCases(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	idx := &catalogapi.IndexSchema{Name: "idx_age", Table: "users", Column: "age"}
	tableID := uint32(1)
	indexID := uint32(1)

	for i, age := range []int64{10, 20, 30, 40, 50} {
		ie.Insert(idx, tableID, indexID, intVal(age), uint64(i+1))
	}

	iter, err := ie.Scan(tableID, indexID, encodingapi.OpGT, intVal(50))
	if err != nil {
		t.Fatalf("Scan GT max failed: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("GT 50: expected 0, got %d", count)
	}

	iter, err = ie.Scan(tableID, indexID, encodingapi.OpLT, intVal(10))
	if err != nil {
		t.Fatalf("Scan LT min failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("LT 10: expected 0, got %d", count)
	}

	iter, err = ie.Scan(tableID, indexID, encodingapi.OpLE, intVal(10))
	if err != nil {
		t.Fatalf("Scan LE min failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 1 {
		t.Errorf("LE 10: expected 1, got %d", count)
	}

	iter, err = ie.Scan(tableID, indexID, encodingapi.OpGE, intVal(50))
	if err != nil {
		t.Fatalf("Scan GE max failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 1 {
		t.Errorf("GE 50: expected 1, got %d", count)
	}

	iter, err = ie.Scan(tableID, indexID, encodingapi.OpEQ, intVal(99))
	if err != nil {
		t.Fatalf("Scan EQ nonexistent failed: %v", err)
	}
	count = 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("EQ 99: expected 0, got %d", count)
	}
}

// ─── RowIterator Tests ───────────────────────────────────────────────

func TestRowIterator_Empty(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	iter, err := te.Scan(table)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if iter.Next() {
		t.Error("expected no rows on empty scan")
	}
	if err := iter.Err(); err != nil {
		t.Errorf("unexpected error on empty scan: %v", err)
	}
	iter.Close()
}

func TestRowIterator_KeyAndValue(t *testing.T) {
	te, _, _ := newTestEngines(t)
	table := testTable()

	te.Insert(table, []catalogapi.Value{intVal(0), textVal("A"), intVal(1)})
	te.Insert(table, []catalogapi.Value{intVal(0), textVal("B"), intVal(2)})

	iter, _ := te.Scan(table)
	defer iter.Close()

	count := 0
	for iter.Next() {
		row := iter.Row()
		if row == nil {
			t.Error("expected non-nil row")
		}
		if row.RowID == 0 {
			t.Error("expected non-zero rowID")
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// ─── TableEngine Error Cases ────────────────────────────────────────

func TestTableEngine_TableIDNotSet(t *testing.T) {
	te, _, _ := newTestEngines(t)

	noIDTable := &catalogapi.TableSchema{
		Name: "notable",
		Columns: []catalogapi.ColumnDef{
			{Name: "id", Type: catalogapi.TypeInt},
		},
	}

	_, err := te.Insert(noIDTable, []catalogapi.Value{intVal(1)})
	if err != engineapi.ErrTableIDNotSet {
		t.Errorf("expected ErrTableIDNotSet for Insert, got %v", err)
	}

	_, err = te.Get(noIDTable, 1)
	if err != engineapi.ErrTableIDNotSet {
		t.Errorf("expected ErrTableIDNotSet for Get, got %v", err)
	}

	_, err = te.Scan(noIDTable)
	if err != engineapi.ErrTableIDNotSet {
		t.Errorf("expected ErrTableIDNotSet for Scan, got %v", err)
	}

	err = te.Delete(noIDTable, 1)
	if err != engineapi.ErrTableIDNotSet {
		t.Errorf("expected ErrTableIDNotSet for Delete, got %v", err)
	}

	err = te.Update(noIDTable, 1, []catalogapi.Value{intVal(1)})
	if err != engineapi.ErrTableIDNotSet {
		t.Errorf("expected ErrTableIDNotSet for Update, got %v", err)
	}
}

func TestIndexEngine_TableIDNotSet(t *testing.T) {
	_, ie, _ := newTestEngines(t)

	iter, err := ie.Scan(0, 0, encodingapi.OpEQ, intVal(1))
	if err != nil {
		t.Fatalf("Scan with zero IDs should not error: %v", err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != 0 {
		t.Errorf("expected 0 results for zero IDs, got %d", count)
	}
}
