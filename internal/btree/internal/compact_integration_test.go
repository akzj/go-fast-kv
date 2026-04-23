package internal

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/pagestore"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// ─── Helpers ────────────────────────────────────────────────────────

func newTestCompactPageStore(t *testing.T) pagestoreapi.PageStore {
	t.Helper()
	dir := t.TempDir()
	segMgr, err := segment.New(segmentapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("segment.New: %v", err)
	}
	t.Cleanup(func() { segMgr.Close() })
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	t.Cleanup(func() { ps.Close() })
	return ps
}

// ─── Integration Tests ──────────────────────────────────────────────

func TestCompactIntegration_LeafPage(t *testing.T) {
	ps := newTestCompactPageStore(t)

	page := NewLeafPage()
	page.SetNext(42)
	page.SetHighKey([]byte("zzz"))

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	vals := []string{"v-apple", "v-banana", "v-cherry", "v-date", "v-elderberry"}
	for i := range keys {
		err := page.InsertLeafEntry(i, []byte(keys[i]), uint64(i+1), math.MaxUint64, []byte(vals[i]), 0)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	compact := page.SerializeCompact()
	t.Logf("Leaf: full=%d, compact=%d (%.0f%% savings)",
		btreeapi.PageSize, len(compact),
		100*(1-float64(len(compact))/float64(btreeapi.PageSize)))

	pid := ps.Alloc()
	entry, err := ps.WriteCompact(pid, compact)
	if err != nil {
		t.Fatalf("WriteCompact: %v", err)
	}

	segID, offset, recordLen := segmentapi.UnpackPageVAddr(entry.VAddr)
	expectedRecordLen := uint16(pagestoreapi.PageRecordOverhead + len(compact))
	if recordLen != expectedRecordLen {
		t.Fatalf("RecordLen: got %d, want %d", recordLen, expectedRecordLen)
	}
	t.Logf("VAddr: segID=%d, offset=%d, recordLen=%d", segID, offset, recordLen)

	readBack, err := ps.ReadCompact(pid)
	if err != nil {
		t.Fatalf("ReadCompact: %v", err)
	}
	if !bytes.Equal(readBack, compact) {
		t.Fatal("ReadCompact data mismatch")
	}

	restored := DeserializeCompact(readBack)
	if !restored.IsLeaf() || restored.Count() != len(keys) {
		t.Fatalf("restored: isLeaf=%v, count=%d", restored.IsLeaf(), restored.Count())
	}
	if restored.Next() != 42 {
		t.Fatalf("next: got %d", restored.Next())
	}
	if string(restored.HighKey()) != "zzz" {
		t.Fatalf("highkey: got %q", restored.HighKey())
	}
	for i := range keys {
		if string(restored.EntryKey(i)) != keys[i] {
			t.Fatalf("key %d: got %q, want %q", i, restored.EntryKey(i), keys[i])
		}
		if string(restored.EntryInlineValue(i)) != vals[i] {
			t.Fatalf("val %d: got %q, want %q", i, restored.EntryInlineValue(i), vals[i])
		}
	}
	if err := restored.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestCompactIntegration_InternalPage(t *testing.T) {
	ps := newTestCompactPageStore(t)

	page := NewInternalPage()
	page.SetChild0(100)
	page.SetNext(200)
	page.SetHighKey([]byte("mmm"))
	page.InsertInternalEntry(0, []byte("ddd"), 300)
	page.InsertInternalEntry(1, []byte("ggg"), 400)
	page.InsertInternalEntry(2, []byte("kkk"), 500)

	compact := page.SerializeCompact()
	t.Logf("Internal: full=%d, compact=%d (%.0f%% savings)",
		btreeapi.PageSize, len(compact),
		100*(1-float64(len(compact))/float64(btreeapi.PageSize)))

	pid := ps.Alloc()
	_, err := ps.WriteCompact(pid, compact)
	if err != nil {
		t.Fatalf("WriteCompact: %v", err)
	}

	readBack, err := ps.ReadCompact(pid)
	if err != nil {
		t.Fatalf("ReadCompact: %v", err)
	}

	restored := DeserializeCompact(readBack)
	if restored.IsLeaf() || restored.Count() != 3 {
		t.Fatalf("restored: isLeaf=%v, count=%d", restored.IsLeaf(), restored.Count())
	}
	if restored.Child0() != 100 || restored.Next() != 200 {
		t.Fatalf("child0=%d, next=%d", restored.Child0(), restored.Next())
	}
	if string(restored.HighKey()) != "mmm" {
		t.Fatalf("highkey: got %q", restored.HighKey())
	}
}

func TestCompactIntegration_PostSplit(t *testing.T) {
	ps := newTestCompactPageStore(t)

	page := NewLeafPage()
	n := 20
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		val := []byte(fmt.Sprintf("val-%02d", i))
		page.InsertLeafEntry(i, key, uint64(i+1), math.MaxUint64, val, 0)
	}

	_, right := page.SplitLeaf(n / 2)

	leftPID := ps.Alloc()
	leftCompact := page.SerializeCompact()
	_, err := ps.WriteCompact(leftPID, leftCompact)
	if err != nil {
		t.Fatalf("WriteCompact left: %v", err)
	}

	rightPID := ps.Alloc()
	rightCompact := right.SerializeCompact()
	_, err = ps.WriteCompact(rightPID, rightCompact)
	if err != nil {
		t.Fatalf("WriteCompact right: %v", err)
	}

	t.Logf("Post-split: left=%d, right=%d (full=%d)",
		len(leftCompact), len(rightCompact), btreeapi.PageSize)

	leftData, _ := ps.ReadCompact(leftPID)
	leftRestored := DeserializeCompact(leftData)
	if leftRestored.Count() != n/2 {
		t.Fatalf("left count: got %d, want %d", leftRestored.Count(), n/2)
	}

	rightData, _ := ps.ReadCompact(rightPID)
	rightRestored := DeserializeCompact(rightData)
	if rightRestored.Count() != n-n/2 {
		t.Fatalf("right count: got %d, want %d", rightRestored.Count(), n-n/2)
	}

	for i := 0; i < n/2; i++ {
		expected := fmt.Sprintf("key-%02d", i)
		if string(leftRestored.EntryKey(i)) != expected {
			t.Fatalf("left key %d: got %q", i, leftRestored.EntryKey(i))
		}
	}
	for i := 0; i < n-n/2; i++ {
		expected := fmt.Sprintf("key-%02d", i+n/2)
		if string(rightRestored.EntryKey(i)) != expected {
			t.Fatalf("right key %d: got %q", i, rightRestored.EntryKey(i))
		}
	}
}

func TestCompactIntegration_ManyPages(t *testing.T) {
	ps := newTestCompactPageStore(t)

	type pageInfo struct {
		pid   pagestoreapi.PageID
		keys  []string
		count int
	}
	var pages []pageInfo

	for p := 0; p < 100; p++ {
		page := NewLeafPage()
		numEntries := 1 + (p % 20)
		var keys []string
		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("p%03d-k%02d", p, i)
			val := fmt.Sprintf("v%03d-%02d", p, i)
			if err := page.InsertLeafEntry(i, []byte(key), uint64(i+1), math.MaxUint64, []byte(val), 0); err != nil {
				break
			}
			keys = append(keys, key)
		}

		pid := ps.Alloc()
		compact := page.SerializeCompact()
		if _, err := ps.WriteCompact(pid, compact); err != nil {
			t.Fatalf("WriteCompact page %d: %v", p, err)
		}
		pages = append(pages, pageInfo{pid: pid, keys: keys, count: len(keys)})
	}

	for _, pi := range pages {
		data, err := ps.ReadCompact(pi.pid)
		if err != nil {
			t.Fatalf("ReadCompact page %d: %v", pi.pid, err)
		}
		restored := DeserializeCompact(data)
		if restored.Count() != pi.count {
			t.Fatalf("page %d: count=%d, want %d", pi.pid, restored.Count(), pi.count)
		}
		for i, key := range pi.keys {
			if string(restored.EntryKey(i)) != key {
				t.Fatalf("page %d entry %d: got %q", pi.pid, i, restored.EntryKey(i))
			}
		}
	}
}

func TestCompactIntegration_SpaceSavings(t *testing.T) {
	var totalFull, totalCompact int

	for entries := 1; entries <= 50; entries++ {
		page := NewLeafPage()
		for i := 0; i < entries; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			val := []byte(fmt.Sprintf("value-%04d", i))
			if err := page.InsertLeafEntry(i, key, uint64(i+1), math.MaxUint64, val, 0); err != nil {
				break
			}
		}
		compact := page.SerializeCompact()
		totalFull += btreeapi.PageSize
		totalCompact += len(compact)
	}

	savings := 100 * (1 - float64(totalCompact)/float64(totalFull))
	t.Logf("Space savings: full=%d, compact=%d (%.1f%% savings)", totalFull, totalCompact, savings)

	if savings < 30 {
		t.Errorf("Expected at least 30%% savings, got %.1f%%", savings)
	}
}

func TestCompactIntegration_MutateAfterRoundTrip(t *testing.T) {
	ps := newTestCompactPageStore(t)

	page := NewLeafPage()
	page.InsertLeafEntry(0, []byte("aaa"), 1, math.MaxUint64, []byte("v1"), 0)
	page.InsertLeafEntry(1, []byte("ccc"), 2, math.MaxUint64, []byte("v3"), 0)

	pid := ps.Alloc()
	compact := page.SerializeCompact()
	ps.WriteCompact(pid, compact)

	data, _ := ps.ReadCompact(pid)
	restored := DeserializeCompact(data)

	// Mutate the restored page
	err := restored.InsertLeafEntry(1, []byte("bbb"), 3, math.MaxUint64, []byte("v2"), 0)
	if err != nil {
		t.Fatalf("insert into restored: %v", err)
	}
	if restored.Count() != 3 {
		t.Fatalf("count after insert: %d", restored.Count())
	}

	// Write it back compact
	compact2 := restored.SerializeCompact()
	ps.WriteCompact(pid, compact2)

	data2, _ := ps.ReadCompact(pid)
	restored2 := DeserializeCompact(data2)

	expected := []string{"aaa", "bbb", "ccc"}
	for i, k := range expected {
		if string(restored2.EntryKey(i)) != k {
			t.Fatalf("entry %d: got %q, want %q", i, restored2.EntryKey(i), k)
		}
	}
}
