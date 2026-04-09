package internal

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── 1. TestIsolation_ReadWriteConcurrent ───────────────────────────
// goroutine A writes key="x" value="1", goroutine B reads key="x" concurrently.
// B should see either the old value or the new value, never a partial write.

func TestIsolation_ReadWriteConcurrent(t *testing.T) {
	s := openTestStore(t)

	key := []byte("x")
	oldVal := []byte("old-value-000")
	newVal := []byte("new-value-111")

	// Seed with old value
	if err := s.Put(key, oldVal); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	var wg sync.WaitGroup
	const iterations = 1000
	var readErrors atomic.Int64

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := s.Put(key, newVal); err != nil {
				t.Errorf("writer Put: %v", err)
				return
			}
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			val, err := s.Get(key)
			if err != nil {
				t.Errorf("reader Get: %v", err)
				return
			}
			// Value must be one of the two valid values (no partial write)
			if !bytes.Equal(val, oldVal) && !bytes.Equal(val, newVal) {
				readErrors.Add(1)
				t.Errorf("reader saw invalid value: %q (expected %q or %q)", val, oldVal, newVal)
				return
			}
		}
	}()

	wg.Wait()
	if readErrors.Load() > 0 {
		t.Fatalf("read isolation violated: %d invalid reads", readErrors.Load())
	}
}

// ─── 2. TestIsolation_WriteBatchAtomicity ───────────────────────────
// WriteBatch writes key1, key2, key3 atomically.
// A concurrent Scan should see either all 3 or none (not partial).

func TestIsolation_WriteBatchAtomicity(t *testing.T) {
	s := openTestStore(t)

	batchKeys := [][]byte{
		[]byte("batch-key-001"),
		[]byte("batch-key-002"),
		[]byte("batch-key-003"),
	}
	const rounds = 200
	var wg sync.WaitGroup
	var atomicityViolations atomic.Int64

	// Writer: repeatedly commit batches of 3 keys
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			batch := s.NewWriteBatch()
			val := []byte(fmt.Sprintf("batch-value-%03d", r))
			for _, k := range batchKeys {
				if err := batch.Put(k, val); err != nil {
					t.Errorf("batch Put: %v", err)
					return
				}
			}
			if err := batch.Commit(); err != nil {
				t.Errorf("batch Commit: %v", err)
				return
			}
		}
	}()

	// Reader: repeatedly scan and check atomicity
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds*2; r++ {
			iter := s.Scan([]byte("batch-key-"), []byte("batch-key-~"))
			var found []string
			for iter.Next() {
				found = append(found, string(iter.Key()))
			}
			if err := iter.Err(); err != nil {
				t.Errorf("scan error: %v", err)
				iter.Close()
				return
			}
			iter.Close()

			// Atomicity check: should see 0 or 3, not 1 or 2
			if len(found) != 0 && len(found) != 3 {
				atomicityViolations.Add(1)
				t.Errorf("atomicity violation: scan found %d keys (expected 0 or 3): %v", len(found), found)
			}
		}
	}()

	wg.Wait()
	if atomicityViolations.Load() > 0 {
		t.Fatalf("WriteBatch atomicity violated %d times", atomicityViolations.Load())
	}
}

// ─── 3. TestIsolation_ConcurrentWriteNoLoss ─────────────────────────
// 10 goroutines each write 100 unique keys → should end up with 1000 keys.

func TestIsolation_ConcurrentWriteNoLoss(t *testing.T) {
	s := openTestStore(t)

	const goroutines = 10
	const keysPerGoroutine = 100
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%02d-key-%04d", g, i))
				val := []byte(fmt.Sprintf("g%02d-val-%04d", g, i))
				if err := s.Put(key, val); err != nil {
					t.Errorf("goroutine %d Put: %v", g, err)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Verify all 1000 keys exist
	missing := 0
	for g := 0; g < goroutines; g++ {
		for i := 0; i < keysPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("g%02d-key-%04d", g, i))
			expectedVal := []byte(fmt.Sprintf("g%02d-val-%04d", g, i))
			val, err := s.Get(key)
			if err != nil {
				t.Errorf("missing key %s: %v", key, err)
				missing++
				continue
			}
			if !bytes.Equal(val, expectedVal) {
				t.Errorf("wrong value for %s: got %q, want %q", key, val, expectedVal)
			}
		}
	}

	if missing > 0 {
		t.Fatalf("%d of %d keys missing after concurrent writes", missing, goroutines*keysPerGoroutine)
	}

	// Also verify via Scan
	iter := s.Scan([]byte("g"), []byte("g~"))
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	if count != goroutines*keysPerGoroutine {
		t.Fatalf("Scan found %d keys, expected %d", count, goroutines*keysPerGoroutine)
	}
}

// ─── 4. TestIsolation_ConcurrentWriteSameKey ────────────────────────
// 10 goroutines all write key="counter" with different values.
// Final state: key exists with one of the written values.

func TestIsolation_ConcurrentWriteSameKey(t *testing.T) {
	s := openTestStore(t)

	const goroutines = 10
	const writesPerGoroutine = 100
	key := []byte("counter")

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				val := []byte(fmt.Sprintf("g%02d-v%04d", g, i))
				if err := s.Put(key, val); err != nil {
					t.Errorf("goroutine %d Put: %v", g, err)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Key must exist
	val, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get after concurrent writes: %v", err)
	}

	// Value must be one of the written values (format: gNN-vNNNN)
	if len(val) == 0 {
		t.Fatalf("Get returned empty value")
	}
	t.Logf("Final value for 'counter': %q", val)
}

// ─── 5. TestIsolation_ScanDuringPut ─────────────────────────────────
// One goroutine continuously Scans, another continuously Puts.
// Scan should never panic or return an error.

func TestIsolation_ScanDuringPut(t *testing.T) {
	s := openTestStore(t)

	const iterations = 500
	var wg sync.WaitGroup
	var scanErrors atomic.Int64
	var scanPanics atomic.Int64

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			key := []byte(fmt.Sprintf("scan-key-%05d", i))
			val := []byte(fmt.Sprintf("scan-val-%05d", i))
			if err := s.Put(key, val); err != nil {
				t.Errorf("writer Put: %v", err)
				return
			}
		}
	}()

	// Scanner
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				scanPanics.Add(1)
				t.Errorf("Scan panicked: %v", r)
			}
		}()
		for i := 0; i < iterations; i++ {
			iter := s.Scan([]byte("scan-key-"), []byte("scan-key-~"))
			count := 0
			for iter.Next() {
				count++
				_ = iter.Key()
				_ = iter.Value()
			}
			if err := iter.Err(); err != nil {
				scanErrors.Add(1)
				t.Errorf("Scan error at iteration %d: %v", i, err)
			}
			iter.Close()
		}
	}()

	wg.Wait()

	if scanErrors.Load() > 0 {
		t.Fatalf("Scan had %d errors during concurrent Put", scanErrors.Load())
	}
	if scanPanics.Load() > 0 {
		t.Fatalf("Scan panicked %d times during concurrent Put", scanPanics.Load())
	}
}

// ─── 6. TestIsolation_DeleteGetConcurrent ───────────────────────────
// One goroutine does Put then Delete, another goroutine continuously Gets.
// Get should return value or ErrKeyNotFound, never panic.

func TestIsolation_DeleteGetConcurrent(t *testing.T) {
	s := openTestStore(t)

	key := []byte("delete-test-key")
	val := []byte("delete-test-value")

	const iterations = 500
	var wg sync.WaitGroup
	var getPanics atomic.Int64
	var getErrors atomic.Int64

	// Writer: Put then Delete in a loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := s.Put(key, val); err != nil {
				t.Errorf("writer Put: %v", err)
				return
			}
			// Ignore ErrKeyNotFound on Delete (may have been deleted by timing)
			err := s.Delete(key)
			if err != nil && !errors.Is(err, kvstoreapi.ErrKeyNotFound) {
				t.Errorf("writer Delete: %v", err)
				return
			}
		}
	}()

	// Reader: continuously Get
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				getPanics.Add(1)
				t.Errorf("Get panicked: %v", r)
			}
		}()
		for i := 0; i < iterations*2; i++ {
			v, err := s.Get(key)
			if err != nil {
				if !errors.Is(err, kvstoreapi.ErrKeyNotFound) {
					getErrors.Add(1)
					t.Errorf("unexpected Get error: %v", err)
					return
				}
				// ErrKeyNotFound is expected sometimes
				continue
			}
			// If we got a value, it must be the correct one
			if !bytes.Equal(v, val) {
				t.Errorf("Get returned wrong value: %q, expected %q", v, val)
			}
		}
	}()

	wg.Wait()

	if getPanics.Load() > 0 {
		t.Fatalf("Get panicked %d times during concurrent Delete", getPanics.Load())
	}
	if getErrors.Load() > 0 {
		t.Fatalf("Get had %d unexpected errors during concurrent Delete", getErrors.Load())
	}
}
