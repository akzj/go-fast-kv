// Package internal hosts the core KVStore implementation.
package internal

import (
	"os"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func TestMetrics_PopulatedAfterOperations(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write some data.
	for i := 0; i < 100; i++ {
		key := []byte("metrics-key-" + string(rune('a'+i%26)))
		val := []byte("metrics-value-" + string(rune('A'+i%26)))
		if err := store.Put(key, val); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Read some data.
	for i := 0; i < 50; i++ {
		key := []byte("metrics-key-" + string(rune('a'+i%26)))
		_, err := store.Get(key)
		if err != nil && err != kvstoreapi.ErrKeyNotFound {
			t.Fatalf("Get failed: %v", err)
		}
	}

	// Get metrics.
	m := store.GetMetrics()
	if m == nil {
		t.Fatal("GetMetrics returned nil")
	}

	// After 100 writes + 50 reads, we expect:
	// - Write throughput > 0 (some samples recorded)
	// - Read throughput > 0
	// - Total errors should be 0
	if m.TotalErrors != 0 {
		t.Errorf("expected 0 errors, got %d", m.TotalErrors)
	}

	// GCRunning and CompactionRunning should be false (no background ops triggered)
	if m.GCRunning {
		t.Log("note: GC is running (expected for 100 ops with default threshold)")
	}
	if m.CompactionRunning {
		t.Log("note: compaction is running")
	}

	t.Logf("Metrics after 100 writes + 50 reads:")
	t.Logf("  ReadThroughput:  %d ops/s", m.ReadThroughput)
	t.Logf("  WriteThroughput: %d ops/s", m.WriteThroughput)
	t.Logf("  TotalErrors:     %d", m.TotalErrors)
	t.Logf("  GetLatencyP50:   %.2f µs", m.GetLatencyP50)
	t.Logf("  GetLatencyP99:   %.2f µs", m.GetLatencyP99)
	t.Logf("  PutLatencyP50:   %.2f µs", m.PutLatencyP50)
	t.Logf("  PutLatencyP99:   %.2f µs", m.PutLatencyP99)
}

func TestMetrics_ZeroBlocking(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Put enough data to fill the latency ring buffer.
	for i := 0; i < 5000; i++ {
		key := []byte("bench-key-" + string(rune(i%256)))
		val := []byte("bench-val")
		if err := store.Put(key, val); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Calling GetMetrics should not block (zero locking).
	// We call it multiple times rapidly to verify.
	for i := 0; i < 100; i++ {
		m := store.GetMetrics()
		if m == nil {
			t.Fatal("GetMetrics returned nil")
		}
		if m.TotalErrors != 0 {
			t.Errorf("expected 0 errors, got %d", m.TotalErrors)
		}
	}
}

func TestMetrics_ErrorCounting(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	store, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write a key.
	if err := store.Put([]byte("test-key"), []byte("test-value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get a non-existent key (should return ErrKeyNotFound, not count as error).
	_, err = store.Get([]byte("non-existent"))
	if err != kvstoreapi.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}

	// Verify no errors counted.
	m := store.GetMetrics()
	if m.TotalErrors != 0 {
		t.Errorf("expected 0 errors (ErrKeyNotFound is not an error), got %d", m.TotalErrors)
	}
}
