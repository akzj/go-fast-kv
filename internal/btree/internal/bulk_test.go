package internal

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// TestBulkLoadEmpty tests that empty bulk load returns error.
func TestBulkLoadEmpty(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	_, err := loader.Build()
	if err != ErrBulkEmpty {
		t.Errorf("expected ErrBulkEmpty, got %v", err)
	}
}

// TestBulkLoadSingleEntry tests bulk loading a single entry.
func TestBulkLoadSingleEntry(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.Add([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	if rootPID == 0 {
		t.Fatal("expected non-zero root page ID")
	}

	tree.SetRootPageID(rootPID)

	// Verify we can read the entry
	val, err := tree.Get([]byte("key1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("expected 'value1', got '%s'", string(val))
	}
}

// TestBulkLoadMultipleEntries tests bulk loading multiple entries.
func TestBulkLoadMultipleEntries(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	// Create sorted entries
	entries := make([]btreeapi.KVPair, 100)
	for i := 0; i < 100; i++ {
		entries[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.AddSorted(entries); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// Verify all entries can be read
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		expected := []byte(fmt.Sprintf("value%03d", i))

		val, err := tree.Get(key, 0)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}
		if !bytes.Equal(val, expected) {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, val)
		}
	}
}

// TestBulkLoadUnsortedEntries tests that unsorted entries are automatically sorted.
func TestBulkLoadUnsortedEntries(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	// Create unsorted entries
	entries := []btreeapi.KVPair{
		{Key: []byte("zebra"), Value: []byte("z")},
		{Key: []byte("apple"), Value: []byte("a")},
		{Key: []byte("banana"), Value: []byte("b")},
		{Key: []byte("mango"), Value: []byte("m")},
	}

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.AddSorted(entries); err != nil {
		t.Fatal(err)
	}

	// Verify not sorted yet
	if loader.IsSorted() {
		t.Error("expected unsorted")
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// All entries should be readable regardless of insertion order
	for _, e := range entries {
		val, err := tree.Get(e.Key, 0)
		if err != nil {
			t.Errorf("failed to get key %s: %v", e.Key, err)
			continue
		}
		if !bytes.Equal(val, e.Value) {
			t.Errorf("key %s: expected '%s', got '%s'", e.Key, e.Value, val)
		}
	}
}

// TestBulkLoadScan verifies that all entries are found via Scan.
func TestBulkLoadScan(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	// Create sorted entries
	entries := make([]btreeapi.KVPair, 50)
	for i := 0; i < 50; i++ {
		entries[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.AddSorted(entries); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// Scan all entries
	found := make(map[string]string)
	iter := tree.Scan(nil, nil, 0)
	for iter.Next() {
		found[string(iter.Key())] = string(iter.Value())
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}

	if len(found) != 50 {
		t.Errorf("expected 50 entries, got %d", len(found))
	}

	// Verify order is correct
	prev := ""
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%03d", i)
		val, ok := found[key]
		if !ok {
			t.Errorf("missing key %s", key)
			continue
		}
		if prev != "" && bytes.Compare([]byte(prev), []byte(key)) >= 0 {
			t.Error("scan order is incorrect")
		}
		expectedVal := fmt.Sprintf("value%03d", i)
		if val != expectedVal {
			t.Errorf("key %s: expected '%s', got '%s'", key, expectedVal, val)
		}
		prev = key
	}
}

// TestBulkLoadLargeValue tests bulk loading with large values (blob).
func TestBulkLoadLargeValue(t *testing.T) {
	tree := New(btreeapi.Config{InlineThreshold: 32}, NewMemPageProvider(), nil)
	defer tree.Close()

	// Create a large value (> 256 bytes inline threshold)
	largeValue := bytes.Repeat([]byte("x"), 1000)

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.Add([]byte("largekey"), largeValue); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// Verify the large value can be read back
	val, err := tree.Get([]byte("largekey"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, largeValue) {
		t.Error("large value mismatch")
	}
}

// TestBulkLoadMVCCMode tests bulk loading with MVCC mode.
func TestBulkLoadMVCCMode(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	entries := []btreeapi.KVPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}

	loader := tree.NewBulkLoaderWithTxn(btreeapi.BulkModeMVCC, 100)
	if err := loader.AddSorted(entries); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// txnID 100 should see the entries
	val, err := tree.Get([]byte("a"), 100)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("1")) {
		t.Error("expected value 1")
	}

	// txnID 50 should NOT see the entries (before txnID 100)
	_, err = tree.Get([]byte("a"), 50)
	if err != btreeapi.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound for txn 50, got %v", err)
	}
}

// TestBulkLoadDuplicateBuild tests that calling Build twice returns error.
func TestBulkLoadDuplicateBuild(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	loader.Add([]byte("key1"), []byte("value1"))

	_, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	_, err = loader.Build()
	if err != ErrBulkAlreadyDone {
		t.Errorf("expected ErrBulkAlreadyDone, got %v", err)
	}
}

// TestBulkLoadKeyTooLarge tests that oversized keys are rejected.
func TestBulkLoadKeyTooLarge(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	tooLargeKey := bytes.Repeat([]byte("x"), btreeapi.MaxKeySize+1)

	err := loader.Add(tooLargeKey, []byte("value"))
	if err != btreeapi.ErrKeyTooLarge {
		t.Errorf("expected ErrKeyTooLarge, got %v", err)
	}
}

// TestBulkLoad10KEntries tests bulk loading a larger dataset.
func TestBulkLoad10KEntries(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	// Create 10,000 sorted entries
	entries := make([]btreeapi.KVPair, 10000)
	for i := 0; i < 10000; i++ {
		entries[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%06d", i)),
			Value: []byte(fmt.Sprintf("value%06d", i)),
		}
	}

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.AddSorted(entries); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// Verify first, middle, and last entries
	testCases := []struct {
		keyIndex int
	}{
		{0},
		{4999},
		{9999},
	}

	for _, tc := range testCases {
		key := []byte(fmt.Sprintf("key%06d", tc.keyIndex))
		expected := []byte(fmt.Sprintf("value%06d", tc.keyIndex))

		val, err := tree.Get(key, 0)
		if err != nil {
			t.Errorf("failed to get key at index %d: %v", tc.keyIndex, err)
			continue
		}
		if !bytes.Equal(val, expected) {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, val)
		}
	}
}

// TestBulkLoadScanWithRange tests scanning a range of bulk-loaded entries.
func TestBulkLoadScanWithRange(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	// Create 100 sorted entries
	entries := make([]btreeapi.KVPair, 100)
	for i := 0; i < 100; i++ {
		entries[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	if err := loader.AddSorted(entries); err != nil {
		t.Fatal(err)
	}

	rootPID, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	tree.SetRootPageID(rootPID)

	// Scan range [key020, key050)
	start := []byte("key020")
	end := []byte("key050")

	count := 0
	iter := tree.Scan(start, end, 0)
	for iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}

	// Should get 30 entries (020-049)
	if count != 30 {
		t.Errorf("expected 30 entries in range, got %d", count)
	}
}

// TestBulkLoadEntryCount tests the EntryCount method.
func TestBulkLoadEntryCount(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)

	if loader.EntryCount() != 0 {
		t.Error("expected 0 entries initially")
	}

	loader.Add([]byte("a"), []byte("1"))
	if loader.EntryCount() != 1 {
		t.Error("expected 1 entry")
	}

	loader.Add([]byte("b"), []byte("2"))
	if loader.EntryCount() != 2 {
		t.Error("expected 2 entries")
	}

	// Build to capture count before close
	_, err := loader.Build()
	if err != nil {
		t.Fatal(err)
	}

	// After Build, entries may be cleared but EntryCount should reflect final count
	count := loader.EntryCount()
	if count != 2 {
		t.Errorf("expected 2 entries after Build, got %d", count)
	}
}

// TestBulkLoadSort tests the Sort method.
func TestBulkLoadSort(t *testing.T) {
	loader := &BulkLoader{
		entries: []btreeapi.KVPair{
			{Key: []byte("c"), Value: []byte("3")},
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
		},
	}

	if loader.IsSorted() {
		t.Error("expected unsorted initially")
	}

	loader.Sort()

	if !loader.IsSorted() {
		t.Error("expected sorted after Sort()")
	}

	// Verify order
	expected := []string{"a", "b", "c"}
	for i, exp := range expected {
		if string(loader.entries[i].Key) != exp {
			t.Errorf("at index %d: expected '%s', got '%s'", i, exp, loader.entries[i].Key)
		}
	}
}

// TestBulkLoadClose tests the Close method.
func TestBulkLoadClose(t *testing.T) {
	tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)
	defer tree.Close()

	loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
	loader.Add([]byte("key1"), []byte("value1"))

	// Close should prevent further Add
	err := loader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = loader.Add([]byte("key2"), []byte("value2"))
	if err != ErrBulkAlreadyDone {
		t.Errorf("expected ErrBulkAlreadyDone after Close, got %v", err)
	}
}

// BenchmarkBulkLoad10K benchmarks bulk loading 10,000 entries.
func BenchmarkBulkLoad10K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)

		entries := make([]btreeapi.KVPair, 10000)
		for j := 0; j < 10000; j++ {
			entries[j] = btreeapi.KVPair{
				Key:   []byte(fmt.Sprintf("key%06d", j)),
				Value: []byte(fmt.Sprintf("value%06d", j)),
			}
		}

		loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
		if err := loader.AddSorted(entries); err != nil {
			b.Fatal(err)
		}

		rootPID, err := loader.Build()
		if err != nil {
			b.Fatal(err)
		}

		tree.SetRootPageID(rootPID)
		tree.Close()
	}
}

// BenchmarkBulkLoad100K benchmarks bulk loading 100,000 entries.
func BenchmarkBulkLoad100K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)

		entries := make([]btreeapi.KVPair, 100000)
		for j := 0; j < 100000; j++ {
			entries[j] = btreeapi.KVPair{
				Key:   []byte(fmt.Sprintf("key%06d", j)),
				Value: []byte(fmt.Sprintf("value%06d", j)),
			}
		}

		loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
		if err := loader.AddSorted(entries); err != nil {
			b.Fatal(err)
		}

		rootPID, err := loader.Build()
		if err != nil {
			b.Fatal(err)
		}

		tree.SetRootPageID(rootPID)
		tree.Close()
	}
}

// BenchmarkBulkLoadSortedAlready benchmarks when entries are already sorted.
func BenchmarkBulkLoadSortedAlready(b *testing.B) {
	// Pre-generate sorted entries
	entries := make([]btreeapi.KVPair, 10000)
	for j := 0; j < 10000; j++ {
		entries[j] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%06d", j)),
			Value: []byte(fmt.Sprintf("value%06d", j)),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)

		loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
		if err := loader.AddSorted(entries); err != nil {
			b.Fatal(err)
		}

		rootPID, err := loader.Build()
		if err != nil {
			b.Fatal(err)
		}

		tree.SetRootPageID(rootPID)
		tree.Close()
	}
}

// BenchmarkBulkLoadUnsorted benchmarks when entries need sorting.
func BenchmarkBulkLoadUnsorted(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree := New(btreeapi.Config{}, NewMemPageProvider(), nil)

		// Create unsorted entries
		entries := make([]btreeapi.KVPair, 10000)
		for j := 0; j < 10000; j++ {
			entries[j] = btreeapi.KVPair{
				Key:   []byte(fmt.Sprintf("key%06d", j)),
				Value: []byte(fmt.Sprintf("value%06d", j)),
			}
		}
		// Shuffle
		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].Key, entries[j].Key) > 0
		})

		loader := tree.NewBulkLoader(btreeapi.BulkModeFast)
		if err := loader.AddSorted(entries); err != nil {
			b.Fatal(err)
		}

		rootPID, err := loader.Build()
		if err != nil {
			b.Fatal(err)
		}

		tree.SetRootPageID(rootPID)
		tree.Close()
	}
}
