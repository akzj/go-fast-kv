package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// TestBulkLoadEmpty tests that empty bulk load succeeds.
func TestBulkLoadEmpty(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	err = store.BulkLoad(nil)
	if err != nil {
		t.Fatal(err)
	}
}

// TestBulkLoadSingleEntry tests bulk loading a single entry.
func TestBulkLoadSingleEntry(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	pairs := []btreeapi.KVPair{
		{Key: []byte("key1"), Value: []byte("value1")},
	}

	err = store.BulkLoad(pairs)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the entry is readable
	val, err := store.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got '%s'", string(val))
	}
}

// TestBulkLoadMultipleEntries tests bulk loading multiple entries.
func TestBulkLoadMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create sorted entries
	pairs := make([]btreeapi.KVPair, 100)
	for i := 0; i < 100; i++ {
		pairs[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	err = store.BulkLoad(pairs)
	if err != nil {
		t.Fatal(err)
	}

	// Verify first, middle, and last entries
	testCases := []int{0, 50, 99}
	for _, idx := range testCases {
		key := fmt.Sprintf("key%03d", idx)
		expected := fmt.Sprintf("value%03d", idx)

		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(val))
		}
	}
}

// TestBulkLoadScan verifies all entries are found via Scan.
func TestBulkLoadScan(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create 50 sorted entries
	pairs := make([]btreeapi.KVPair, 50)
	for i := 0; i < 50; i++ {
		pairs[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	err = store.BulkLoad(pairs)
	if err != nil {
		t.Fatal(err)
	}

	// Scan all
	count := 0
	iter := store.Scan(nil, nil)
	defer iter.Close()
	for iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 50 {
		t.Errorf("expected 50 entries, got %d", count)
	}
}

// TestBulkLoadScanWithRange tests scanning a range after bulk load.
func TestBulkLoadScanWithRange(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create 100 sorted entries
	pairs := make([]btreeapi.KVPair, 100)
	for i := 0; i < 100; i++ {
		pairs[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	err = store.BulkLoad(pairs)
	if err != nil {
		t.Fatal(err)
	}

	// Scan range [key020, key050)
	start := []byte("key020")
	end := []byte("key050")

	count := 0
	iter := store.Scan(start, end)
	defer iter.Close()
	for iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 30 {
		t.Errorf("expected 30 entries in range, got %d", count)
	}
}

// TestBulkLoadMVCC tests bulk loading with MVCC mode.
func TestBulkLoadMVCC(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	pairs := []btreeapi.KVPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}

	// BulkLoadMVCC with startTxnID=100
	err = store.BulkLoadMVCC(pairs, 100)
	if err != nil {
		t.Fatal(err)
	}

	// Entries should be visible (txnID 100+)
	val, err := store.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "1" {
		t.Error("expected value 1")
	}
}

// TestBulkLoadPersistence tests that bulk loaded data survives checkpoint and reopen.
func TestBulkLoadPersistence(t *testing.T) {
	dir := t.TempDir()

	// First run: bulk load data
	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	pairs := make([]btreeapi.KVPair, 100)
	for i := 0; i < 100; i++ {
		pairs[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: []byte(fmt.Sprintf("value%03d", i)),
		}
	}

	err = store.BulkLoad(pairs)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}

	err = store.Checkpoint()
	if err != nil {
		store.Close()
		t.Fatal(err)
	}

	err = store.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen and verify data
	store, err = Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Check a few entries
	for _, idx := range []int{0, 50, 99} {
		key := fmt.Sprintf("key%03d", idx)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("after reopen: failed to get %s: %v", key, err)
			continue
		}
		expected := fmt.Sprintf("value%03d", idx)
		if string(val) != expected {
			t.Errorf("after reopen: key %s: expected '%s', got '%s'", key, expected, string(val))
		}
	}
}

// TestBulkLoadThenPut tests mixing bulk load with regular puts.
func TestBulkLoadThenPut(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Bulk load first batch
	batch1 := []btreeapi.KVPair{
		{Key: []byte("aaa"), Value: []byte("bulk1")},
		{Key: []byte("bbb"), Value: []byte("bulk2")},
	}
	err = store.BulkLoad(batch1)
	if err != nil {
		t.Fatal(err)
	}

	// Then put additional entries
	err = store.Put([]byte("ccc"), []byte("put1"))
	if err != nil {
		t.Fatal(err)
	}

	err = store.Put([]byte("ddd"), []byte("put2"))
	if err != nil {
		t.Fatal(err)
	}

	// Verify all entries are accessible
	for _, test := range []struct {
		key, expected string
	}{
		{"aaa", "bulk1"},
		{"bbb", "bulk2"},
		{"ccc", "put1"},
		{"ddd", "put2"},
	} {
		val, err := store.Get([]byte(test.key))
		if err != nil {
			t.Errorf("failed to get %s: %v", test.key, err)
			continue
		}
		if string(val) != test.expected {
			t.Errorf("key %s: expected '%s', got '%s'", test.key, test.expected, string(val))
		}
	}
}

// TestBulkLoadThenDelete tests bulk load followed by delete.
func TestBulkLoadThenDelete(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Bulk load entries
	pairs := []btreeapi.KVPair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	err = store.BulkLoad(pairs)
	if err != nil {
		t.Fatal(err)
	}

	// Delete one entry
	err = store.Delete([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}

	// key1 should be gone
	_, err = store.Get([]byte("key1"))
	if err != kvstoreapi.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound for key1, got %v", err)
	}

	// key2 should still exist
	val, err := store.Get([]byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value2" {
		t.Error("expected value2 for key2")
	}
}

// TestBulkLoad10K tests bulk loading 10,000 entries.
func TestBulkLoad10K(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create 10,000 sorted entries
	pairs := make([]btreeapi.KVPair, 10000)
	for i := 0; i < 10000; i++ {
		pairs[i] = btreeapi.KVPair{
			Key:   []byte(fmt.Sprintf("key%06d", i)),
			Value: []byte(fmt.Sprintf("value%06d", i)),
		}
	}

	err = store.BulkLoad(pairs)
	if err != nil {
		t.Fatal(err)
	}

	// Sample some entries
	for _, idx := range []int{0, 5000, 9999} {
		key := fmt.Sprintf("key%06d", idx)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get %s: %v", key, err)
			continue
		}
		expected := fmt.Sprintf("value%06d", idx)
		if string(val) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(val))
		}
	}

	// Count via scan
	count := 0
	iter := store.Scan(nil, nil)
	defer iter.Close()
	for iter.Next() {
		count++
	}
	if count != 10000 {
		t.Errorf("expected 10000 entries, got %d", count)
	}
}

// BenchmarkBulkLoad1000 benchmarks bulk loading 1,000 entries.
func BenchmarkBulkLoad1000(b *testing.B) {
	dir := b.TempDir()
	defer os.RemoveAll(dir)

	for i := 0; i < b.N; i++ {
		os.RemoveAll(dir)
		store, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			b.Fatal(err)
		}

		pairs := make([]btreeapi.KVPair, 1000)
		for j := 0; j < 1000; j++ {
			pairs[j] = btreeapi.KVPair{
				Key:   []byte(fmt.Sprintf("key%06d", j)),
				Value: []byte(fmt.Sprintf("value%06d", j)),
			}
		}

		if err := store.BulkLoad(pairs); err != nil {
			b.Fatal(err)
		}
		store.Close()
	}
}

// BenchmarkBulkLoad10K benchmarks bulk loading 10,000 entries.
func BenchmarkBulkLoad10K(b *testing.B) {
	dir := b.TempDir()
	defer os.RemoveAll(dir)

	for i := 0; i < b.N; i++ {
		os.RemoveAll(dir)
		store, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			b.Fatal(err)
		}

		pairs := make([]btreeapi.KVPair, 10000)
		for j := 0; j < 10000; j++ {
			pairs[j] = btreeapi.KVPair{
				Key:   []byte(fmt.Sprintf("key%06d", j)),
				Value: []byte(fmt.Sprintf("value%06d", j)),
			}
		}

		if err := store.BulkLoad(pairs); err != nil {
			b.Fatal(err)
		}
		store.Close()
	}
}

// BenchmarkBulkLoad100K benchmarks bulk loading 100,000 entries.
func BenchmarkBulkLoad100K(b *testing.B) {
	dir := b.TempDir()
	defer os.RemoveAll(dir)

	for i := 0; i < b.N; i++ {
		os.RemoveAll(dir)
		store, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			b.Fatal(err)
		}

		pairs := make([]btreeapi.KVPair, 100000)
		for j := 0; j < 100000; j++ {
			pairs[j] = btreeapi.KVPair{
				Key:   []byte(fmt.Sprintf("key%06d", j)),
				Value: []byte(fmt.Sprintf("value%06d", j)),
			}
		}

		if err := store.BulkLoad(pairs); err != nil {
			b.Fatal(err)
		}
		store.Close()
	}
}

// Ensure test file path exists for the temp dir pattern
var _ = filepath.Join

// Debug test - will be removed
func TestBulkLoadDebug(t *testing.T) {
    dir := t.TempDir()
    
    store, err := Open(kvstoreapi.Config{Dir: dir})
    if err != nil {
        t.Fatal(err)
    }
    defer store.Close()
    
    pairs := []btreeapi.KVPair{
        {Key: []byte("key1"), Value: []byte("value1")},
        {Key: []byte("key2"), Value: []byte("value2")},
    }
    
    t.Log("Calling BulkLoad...")
    if err := store.BulkLoad(pairs); err != nil {
        t.Fatal(err)
    }
    t.Log("BulkLoad done")
    
    val, err := store.Get([]byte("key1"))
    if err != nil {
        t.Fatalf("Get key1 failed: %v", err)
    }
    t.Logf("Get key1: %s", string(val))
    
    val, err = store.Get([]byte("key2"))
    if err != nil {
        t.Fatalf("Get key2 failed: %v", err)
    }
    t.Logf("Get key2: %s", string(val))
}
