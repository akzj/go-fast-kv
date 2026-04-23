package internal

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// ─── Leaf Page Basics ───────────────────────────────────────────────

func TestNewLeafPage(t *testing.T) {
	p := NewLeafPage()
	if !p.IsLeaf() {
		t.Fatal("expected leaf page")
	}
	if p.Count() != 0 {
		t.Fatalf("expected count 0, got %d", p.Count())
	}
	if p.Next() != 0 {
		t.Fatalf("expected next 0, got %d", p.Next())
	}
	if p.HighKey() != nil {
		t.Fatalf("expected nil high key, got %v", p.HighKey())
	}
	if p.freeEnd() != btreeapi.PageSize {
		t.Fatalf("expected freeEnd %d, got %d", btreeapi.PageSize, p.freeEnd())
	}
	// FreeSpace for empty leaf: slotArrayStart=16, slotArrayEnd=16
	// FreeSpace = 4096 - 16 = 4080
	if p.FreeSpace() != btreeapi.PageSize-16 {
		t.Fatalf("expected FreeSpace %d, got %d", btreeapi.PageSize-16, p.FreeSpace())
	}
	if len(p.Data()) != btreeapi.PageSize {
		t.Fatalf("expected data len %d, got %d", btreeapi.PageSize, len(p.Data()))
	}
}

func TestNewInternalPage(t *testing.T) {
	p := NewInternalPage()
	if p.IsLeaf() {
		t.Fatal("expected internal page")
	}
	if p.Count() != 0 {
		t.Fatalf("expected count 0, got %d", p.Count())
	}
	// Internal: slotArrayStart=24 (16+8 for child0)
	if p.FreeSpace() != btreeapi.PageSize-24 {
		t.Fatalf("expected FreeSpace %d, got %d", btreeapi.PageSize-24, p.FreeSpace())
	}
}

func TestSetHighKey(t *testing.T) {
	p := NewLeafPage()
	hk := []byte("highkey123")
	p.SetHighKey(hk)
	got := p.HighKey()
	if !bytes.Equal(got, hk) {
		t.Fatalf("expected highkey %q, got %q", hk, got)
	}
	// slotArrayStart should NOT change (highKey is in cell area now)
	if p.slotArrayStart() != 16 {
		t.Fatalf("expected slotArrayStart 16, got %d", p.slotArrayStart())
	}
}

func TestSetHighKey_AfterEntries(t *testing.T) {
	// Key test: SetHighKey after entries should NOT corrupt slot array
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	p.InsertLeafEntry(1, []byte("bbb"), 2, math.MaxUint64, []byte("v2"), 0)

	// Set highKey after entries exist
	p.SetHighKey([]byte("zzz"))

	// Entries should still be readable
	if string(p.EntryKey(0)) != "aaa" {
		t.Fatalf("expected key aaa, got %q", p.EntryKey(0))
	}
	if string(p.EntryKey(1)) != "bbb" {
		t.Fatalf("expected key bbb, got %q", p.EntryKey(1))
	}
	if string(p.HighKey()) != "zzz" {
		t.Fatalf("expected highkey zzz, got %q", p.HighKey())
	}
}

func TestSetNext(t *testing.T) {
	p := NewLeafPage()
	p.SetNext(42)
	if p.Next() != 42 {
		t.Fatalf("expected next 42, got %d", p.Next())
	}
}

// ─── Leaf Entry Insert & Read ───────────────────────────────────────

func TestInsertLeafEntry_Single(t *testing.T) {
	p := NewLeafPage()
	key := []byte("hello")
	val := []byte("world")
	txnMin := uint64(100)
	txnMax := uint64(math.MaxUint64)

	err := p.InsertLeafEntry(0, key, txnMin, txnMax, val, 0)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if p.Count() != 1 {
		t.Fatalf("expected count 1, got %d", p.Count())
	}

	gotKey := p.EntryKey(0)
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("expected key %q, got %q", key, gotKey)
	}
	if p.EntryTxnMin(0) != txnMin {
		t.Fatalf("expected txnMin %d, got %d", txnMin, p.EntryTxnMin(0))
	}
	if p.EntryTxnMax(0) != txnMax {
		t.Fatalf("expected txnMax %d, got %d", txnMax, p.EntryTxnMax(0))
	}
	if p.EntryValueType(0) != 0 {
		t.Fatalf("expected inline value type 0, got %d", p.EntryValueType(0))
	}
	gotVal := p.EntryInlineValue(0)
	if !bytes.Equal(gotVal, val) {
		t.Fatalf("expected value %q, got %q", val, gotVal)
	}
}

func TestInsertLeafEntry_BlobRef(t *testing.T) {
	p := NewLeafPage()
	key := []byte("blobkey")
	blobID := uint64(999)

	err := p.InsertLeafEntry(0, key, 1, math.MaxUint64, nil, blobID)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if p.EntryValueType(0) != 1 {
		t.Fatalf("expected blobRef type 1, got %d", p.EntryValueType(0))
	}
	if p.EntryBlobID(0) != blobID {
		t.Fatalf("expected blobID %d, got %d", blobID, p.EntryBlobID(0))
	}
}

func TestInsertLeafEntry_MultipleInOrder(t *testing.T) {
	p := NewLeafPage()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for i, k := range keys {
		err := p.InsertLeafEntry(i, []byte(k), uint64(i+1), math.MaxUint64, []byte(fmt.Sprintf("val%d", i)), 0)
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	if p.Count() != len(keys) {
		t.Fatalf("expected count %d, got %d", len(keys), p.Count())
	}

	for i, k := range keys {
		got := p.EntryKey(i)
		if !bytes.Equal(got, []byte(k)) {
			t.Fatalf("entry %d: expected key %q, got %q", i, k, got)
		}
	}
}

func TestInsertLeafEntry_InsertInMiddle(t *testing.T) {
	p := NewLeafPage()

	p.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	p.InsertLeafEntry(1, []byte("ccc"), 2, math.MaxUint64, []byte("v3"), 0)
	p.InsertLeafEntry(1, []byte("bbb"), 3, math.MaxUint64, []byte("v2"), 0)

	if p.Count() != 3 {
		t.Fatalf("expected count 3, got %d", p.Count())
	}

	expected := []string{"aaa", "bbb", "ccc"}
	for i, k := range expected {
		got := string(p.EntryKey(i))
		if got != k {
			t.Fatalf("entry %d: expected %q, got %q", i, k, got)
		}
	}
}

func TestSetEntryTxnMax(t *testing.T) {
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("key"), 1, math.MaxUint64, []byte("val"), 0)

	if p.EntryTxnMax(0) != math.MaxUint64 {
		t.Fatalf("expected TxnMax MaxUint64, got %d", p.EntryTxnMax(0))
	}

	p.SetEntryTxnMax(0, 42)
	if p.EntryTxnMax(0) != 42 {
		t.Fatalf("expected TxnMax 42, got %d", p.EntryTxnMax(0))
	}
}

func TestEntryValue(t *testing.T) {
	p := NewLeafPage()

	p.InsertLeafEntry(0, []byte("k1"), 1, math.MaxUint64, []byte("inline-val"), 0)
	v := p.EntryValue(0)
	if !v.IsInline() {
		t.Fatal("expected inline value")
	}
	if !bytes.Equal(v.Inline, []byte("inline-val")) {
		t.Fatalf("expected inline value %q, got %q", "inline-val", v.Inline)
	}

	p.InsertLeafEntry(1, []byte("k2"), 2, math.MaxUint64, nil, 777)
	v2 := p.EntryValue(1)
	if v2.IsInline() {
		t.Fatal("expected blob ref")
	}
	if v2.BlobID != 777 {
		t.Fatalf("expected blobID 777, got %d", v2.BlobID)
	}
}

// ─── Delete ─────────────────────────────────────────────────────────

func TestDeleteLeafEntry(t *testing.T) {
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	p.InsertLeafEntry(1, []byte("bbb"), 2, math.MaxUint64, []byte("v2"), 0)
	p.InsertLeafEntry(2, []byte("ccc"), 3, math.MaxUint64, []byte("v3"), 0)

	p.DeleteLeafEntry(1)

	if p.Count() != 2 {
		t.Fatalf("expected count 2, got %d", p.Count())
	}
	if string(p.EntryKey(0)) != "aaa" {
		t.Fatalf("expected first key aaa, got %q", p.EntryKey(0))
	}
	if string(p.EntryKey(1)) != "ccc" {
		t.Fatalf("expected second key ccc, got %q", p.EntryKey(1))
	}
}

func TestDeleteLeafEntry_First(t *testing.T) {
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	p.InsertLeafEntry(1, []byte("bbb"), 2, math.MaxUint64, []byte("v2"), 0)

	p.DeleteLeafEntry(0)

	if p.Count() != 1 {
		t.Fatalf("expected count 1, got %d", p.Count())
	}
	if string(p.EntryKey(0)) != "bbb" {
		t.Fatalf("expected key bbb, got %q", p.EntryKey(0))
	}
}

func TestDeleteLeafEntry_Last(t *testing.T) {
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	p.InsertLeafEntry(1, []byte("bbb"), 2, math.MaxUint64, []byte("v2"), 0)

	p.DeleteLeafEntry(1)

	if p.Count() != 1 {
		t.Fatalf("expected count 1, got %d", p.Count())
	}
	if string(p.EntryKey(0)) != "aaa" {
		t.Fatalf("expected key aaa, got %q", p.EntryKey(0))
	}
}

// ─── Search ─────────────────────────────────────────────────────────

func TestSearchLeaf(t *testing.T) {
	p := NewLeafPage()
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, k := range keys {
		p.InsertLeafEntry(i, []byte(k), uint64(i+1), math.MaxUint64, []byte("v"), 0)
	}

	tests := []struct {
		search string
		want   int
	}{
		{"apple", 0},
		{"banana", 1},
		{"cherry", 2},
		{"date", 3},
		{"elderberry", 4},
		{"aaa", 0},
		{"fig", 5},
		{"cat", 2},
		{"banana1", 2},
	}

	for _, tt := range tests {
		got := p.SearchLeaf([]byte(tt.search))
		if got != tt.want {
			t.Errorf("SearchLeaf(%q) = %d, want %d", tt.search, got, tt.want)
		}
	}
}

func TestFindInsertPos_MVCC(t *testing.T) {
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("key"), 10, math.MaxUint64, []byte("v10"), 0)
	p.InsertLeafEntry(1, []byte("key"), 5, math.MaxUint64, []byte("v5"), 0)

	pos := p.FindInsertPos([]byte("key"), 15)
	if pos != 0 {
		t.Fatalf("expected pos 0 for txnMin=15, got %d", pos)
	}

	pos = p.FindInsertPos([]byte("key"), 7)
	if pos != 1 {
		t.Fatalf("expected pos 1 for txnMin=7, got %d", pos)
	}

	pos = p.FindInsertPos([]byte("key"), 1)
	if pos != 2 {
		t.Fatalf("expected pos 2 for txnMin=1, got %d", pos)
	}
}

// ─── Internal Page ──────────────────────────────────────────────────

func TestInternalPage_Basic(t *testing.T) {
	p := NewInternalPage()
	p.SetChild0(100)

	if p.Child0() != 100 {
		t.Fatalf("expected child0 100, got %d", p.Child0())
	}

	p.InsertInternalEntry(0, []byte("mmm"), 200)
	p.InsertInternalEntry(1, []byte("zzz"), 300)

	if p.Count() != 2 {
		t.Fatalf("expected count 2, got %d", p.Count())
	}

	if string(p.InternalKey(0)) != "mmm" {
		t.Fatalf("expected key mmm, got %q", p.InternalKey(0))
	}
	if p.InternalChild(0) != 200 {
		t.Fatalf("expected child 200, got %d", p.InternalChild(0))
	}
	if string(p.InternalKey(1)) != "zzz" {
		t.Fatalf("expected key zzz, got %q", p.InternalKey(1))
	}
	if p.InternalChild(1) != 300 {
		t.Fatalf("expected child 300, got %d", p.InternalChild(1))
	}
}

func TestInternalPage_FindChild(t *testing.T) {
	p := NewInternalPage()
	p.SetChild0(10)
	p.InsertInternalEntry(0, []byte("ddd"), 20)
	p.InsertInternalEntry(1, []byte("mmm"), 30)

	tests := []struct {
		key  string
		want uint64
	}{
		{"aaa", 10},
		{"ccc", 10},
		{"ddd", 20},
		{"fff", 20},
		{"mmm", 30},
		{"zzz", 30},
	}

	for _, tt := range tests {
		got := p.FindChild([]byte(tt.key))
		if got != tt.want {
			t.Errorf("FindChild(%q) = %d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestSetInternalChild(t *testing.T) {
	p := NewInternalPage()
	p.SetChild0(10)
	p.InsertInternalEntry(0, []byte("key"), 20)

	p.SetInternalChild(0, 99)
	if p.InternalChild(0) != 99 {
		t.Fatalf("expected child 99, got %d", p.InternalChild(0))
	}
}

// ─── Page Full ──────────────────────────────────────────────────────

func TestInsertLeafEntry_PageFull(t *testing.T) {
	p := NewLeafPage()

	i := 0
	for {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-that-is-somewhat-long-%04d", i))
		err := p.InsertLeafEntry(i, key, uint64(i+1), math.MaxUint64, val, 0)
		if err != nil {
			if err != ErrPageFull {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
		i++
		if i > 200 {
			t.Fatal("should have hit page full by now")
		}
	}

	if i == 0 {
		t.Fatal("should have inserted at least one entry")
	}

	for j := 0; j < i; j++ {
		expected := fmt.Sprintf("key-%04d", j)
		got := string(p.EntryKey(j))
		if got != expected {
			t.Fatalf("entry %d: expected %q, got %q", j, expected, got)
		}
	}
}

// ─── Split Leaf ─────────────────────────────────────────────────────

func TestSplitLeaf(t *testing.T) {
	p := NewLeafPage()

	n := 20
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		val := []byte(fmt.Sprintf("val-%02d", i))
		err := p.InsertLeafEntry(i, key, uint64(i+1), math.MaxUint64, val, 0)
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	mid := n / 2
	splitKey, right := p.SplitLeaf(mid)

	if p.Count() != mid {
		t.Fatalf("left count: expected %d, got %d", mid, p.Count())
	}
	if right.Count() != n-mid {
		t.Fatalf("right count: expected %d, got %d", n-mid, right.Count())
	}

	rightFirstKey := right.EntryKey(0)
	if !bytes.Equal(splitKey, rightFirstKey) {
		t.Fatalf("splitKey %q != right first key %q", splitKey, rightFirstKey)
	}

	for i := 0; i < mid; i++ {
		expected := fmt.Sprintf("key-%02d", i)
		got := string(p.EntryKey(i))
		if got != expected {
			t.Fatalf("left entry %d: expected %q, got %q", i, expected, got)
		}
	}

	for i := 0; i < n-mid; i++ {
		expected := fmt.Sprintf("key-%02d", i+mid)
		got := string(right.EntryKey(i))
		if got != expected {
			t.Fatalf("right entry %d: expected %q, got %q", i, expected, got)
		}
	}
}

// ─── Split Internal ─────────────────────────────────────────────────

func TestSplitInternal(t *testing.T) {
	p := NewInternalPage()
	p.SetChild0(100)

	n := 10
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		err := p.InsertInternalEntry(i, key, uint64(200+i))
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	mid := n / 2
	splitKey, right := p.SplitInternal(mid)

	expectedSplitKey := fmt.Sprintf("key-%02d", mid)
	if string(splitKey) != expectedSplitKey {
		t.Fatalf("splitKey: expected %q, got %q", expectedSplitKey, splitKey)
	}

	if p.Count() != mid {
		t.Fatalf("left count: expected %d, got %d", mid, p.Count())
	}
	if right.Count() != n-mid-1 {
		t.Fatalf("right count: expected %d, got %d", n-mid-1, right.Count())
	}
	if p.Child0() != 100 {
		t.Fatalf("left child0: expected 100, got %d", p.Child0())
	}
	if right.Child0() != uint64(200+mid) {
		t.Fatalf("right child0: expected %d, got %d", 200+mid, right.Child0())
	}

	for i := 0; i < mid; i++ {
		expected := fmt.Sprintf("key-%02d", i)
		got := string(p.InternalKey(i))
		if got != expected {
			t.Fatalf("left key %d: expected %q, got %q", i, expected, got)
		}
	}

	for i := 0; i < n-mid-1; i++ {
		expected := fmt.Sprintf("key-%02d", i+mid+1)
		got := string(right.InternalKey(i))
		if got != expected {
			t.Fatalf("right key %d: expected %q, got %q", i, expected, got)
		}
	}
}

// ─── Compact ────────────────────────────────────────────────────────

func TestCompact(t *testing.T) {
	p := NewLeafPage()

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		val := []byte(fmt.Sprintf("val-%02d", i))
		p.InsertLeafEntry(i, key, uint64(i+1), math.MaxUint64, val, 0)
	}

	freeSpaceBefore := p.FreeSpace()

	p.DeleteLeafEntry(3)
	p.DeleteLeafEntry(1)

	freeSpaceAfterDelete := p.FreeSpace()

	if freeSpaceAfterDelete <= freeSpaceBefore {
		t.Fatal("expected more free space after delete")
	}

	p.Compact()

	freeSpaceAfterCompact := p.FreeSpace()

	if freeSpaceAfterCompact < freeSpaceAfterDelete {
		t.Fatalf("expected more free space after compact: before=%d, after=%d",
			freeSpaceAfterDelete, freeSpaceAfterCompact)
	}

	if p.Count() != 3 {
		t.Fatalf("expected count 3, got %d", p.Count())
	}

	expected := []string{"key-00", "key-02", "key-04"}
	for i, k := range expected {
		got := string(p.EntryKey(i))
		if got != k {
			t.Fatalf("entry %d: expected %q, got %q", i, k, got)
		}
	}
}

// ─── PageFromBytes ──────────────────────────────────────────────────

func TestPageFromBytes(t *testing.T) {
	p1 := NewLeafPage()
	p1.InsertLeafEntry(0, []byte("hello"), 1, math.MaxUint64, []byte("world"), 0)

	p2 := PageFromBytes(p1.Data())

	if !p2.IsLeaf() {
		t.Fatal("expected leaf")
	}
	if p2.Count() != 1 {
		t.Fatalf("expected count 1, got %d", p2.Count())
	}
	if string(p2.EntryKey(0)) != "hello" {
		t.Fatalf("expected key hello, got %q", p2.EntryKey(0))
	}
	if string(p2.EntryInlineValue(0)) != "world" {
		t.Fatalf("expected value world, got %q", p2.EntryInlineValue(0))
	}
}

// ─── Clone ──────────────────────────────────────────────────────────

func TestClone(t *testing.T) {
	p := NewLeafPage()
	p.InsertLeafEntry(0, []byte("key"), 1, math.MaxUint64, []byte("val"), 0)

	c := p.Clone()

	p.SetEntryTxnMax(0, 42)

	if c.EntryTxnMax(0) != math.MaxUint64 {
		t.Fatalf("clone should not be affected by original modification")
	}
}

// ─── HighKey with entries ───────────────────────────────────────────

func TestHighKeyWithEntries(t *testing.T) {
	p := NewLeafPage()
	p.SetHighKey([]byte("zzz"))

	p.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	p.InsertLeafEntry(1, []byte("bbb"), 2, math.MaxUint64, []byte("v2"), 0)

	if p.Count() != 2 {
		t.Fatalf("expected count 2, got %d", p.Count())
	}
	if string(p.HighKey()) != "zzz" {
		t.Fatalf("expected highkey zzz, got %q", p.HighKey())
	}
	if string(p.EntryKey(0)) != "aaa" {
		t.Fatalf("expected key aaa, got %q", p.EntryKey(0))
	}
	if string(p.EntryKey(1)) != "bbb" {
		t.Fatalf("expected key bbb, got %q", p.EntryKey(1))
	}
}

// ─── UsedBytes ──────────────────────────────────────────────────────

func TestUsedBytes(t *testing.T) {
	p := NewLeafPage()
	initialUsed := p.UsedBytes()
	if initialUsed != 16 {
		t.Fatalf("expected initial UsedBytes 16, got %d", initialUsed)
	}

	p.InsertLeafEntry(0, []byte("key"), 1, math.MaxUint64, []byte("val"), 0)
	afterInsert := p.UsedBytes()
	if afterInsert <= initialUsed {
		t.Fatalf("expected UsedBytes to increase after insert")
	}
}

// ─── Empty value ────────────────────────────────────────────────────

func TestInsertLeafEntry_EmptyValue(t *testing.T) {
	p := NewLeafPage()
	err := p.InsertLeafEntry(0, []byte("key"), 1, math.MaxUint64, nil, 0)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if p.EntryValueType(0) != 0 {
		t.Fatalf("expected inline type, got %d", p.EntryValueType(0))
	}
	v := p.EntryInlineValue(0)
	if v != nil {
		t.Fatalf("expected nil inline value, got %v", v)
	}
}

// ─── Stress: Fill and verify ────────────────────────────────────────

func TestFillPageAndVerify(t *testing.T) {
	p := NewLeafPage()

	var inserted int
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := []byte(fmt.Sprintf("v%04d", i))
		err := p.InsertLeafEntry(i, key, uint64(i+1), math.MaxUint64, val, 0)
		if err != nil {
			break
		}
		inserted++
	}

	for i := 0; i < inserted; i++ {
		expectedKey := fmt.Sprintf("k%04d", i)
		expectedVal := fmt.Sprintf("v%04d", i)
		gotKey := string(p.EntryKey(i))
		gotVal := string(p.EntryInlineValue(i))
		if gotKey != expectedKey {
			t.Fatalf("entry %d: key expected %q, got %q", i, expectedKey, gotKey)
		}
		if gotVal != expectedVal {
			t.Fatalf("entry %d: val expected %q, got %q", i, expectedVal, gotVal)
		}
		if p.EntryTxnMin(i) != uint64(i+1) {
			t.Fatalf("entry %d: txnMin expected %d, got %d", i, i+1, p.EntryTxnMin(i))
		}
	}
}

// ─── Internal Page Insert in Middle ─────────────────────────────────

func TestInternalPage_InsertInMiddle(t *testing.T) {
	p := NewInternalPage()
	p.SetChild0(10)

	p.InsertInternalEntry(0, []byte("aaa"), 20)
	p.InsertInternalEntry(1, []byte("ccc"), 40)
	p.InsertInternalEntry(1, []byte("bbb"), 30)

	if p.Count() != 3 {
		t.Fatalf("expected count 3, got %d", p.Count())
	}

	expected := []struct {
		key   string
		child uint64
	}{
		{"aaa", 20},
		{"bbb", 30},
		{"ccc", 40},
	}

	for i, e := range expected {
		gotKey := string(p.InternalKey(i))
		gotChild := p.InternalChild(i)
		if gotKey != e.key {
			t.Fatalf("key %d: expected %q, got %q", i, e.key, gotKey)
		}
		if gotChild != e.child {
			t.Fatalf("child %d: expected %d, got %d", i, e.child, gotChild)
		}
	}
}

// ─── FreeSpace consistency ──────────────────────────────────────────

func TestFreeSpace_WithHighKey(t *testing.T) {
	p := NewLeafPage()
	freeNoHK := p.FreeSpace()

	p2 := NewLeafPage()
	hk := []byte("highkey-10-bytes")
	p2.SetHighKey(hk)
	freeWithHK := p2.FreeSpace()

	// HighKey is stored in cell area, so it reduces freeEnd → reduces FreeSpace
	diff := freeNoHK - freeWithHK
	expectedDiff := 2 + len(hk) // highKeyLen(2) + highKey bytes
	if diff != expectedDiff {
		t.Fatalf("expected FreeSpace diff of %d, got %d", expectedDiff, diff)
	}
}

func TestInternalFreeSpace_WithHighKey(t *testing.T) {
	p := NewInternalPage()
	freeNoHK := p.FreeSpace()

	p2 := NewInternalPage()
	p2.SetHighKey([]byte("hk"))
	freeWithHK := p2.FreeSpace()

	diff := freeNoHK - freeWithHK
	expectedDiff := 2 + 2 // highKeyLen(2) + "hk"(2)
	if diff != expectedDiff {
		t.Fatalf("expected FreeSpace diff of %d, got %d", expectedDiff, diff)
	}
}
