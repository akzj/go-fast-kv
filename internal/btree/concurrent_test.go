package btree

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// concTxnID is a large txnID used for "see everything" reads in concurrent tests.
const concTxnID = math.MaxUint64 - 1

func concKey(i int) []byte   { return []byte(fmt.Sprintf("ckey-%06d", i)) }
func concValue(i int) []byte { return []byte(fmt.Sprintf("cval-%06d", i)) }

// ─── 1. TestConcurrentGet ───────────────────────────────────────────

func TestConcurrentGet(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Pre-populate 200 keys
	n := 200
	for i := 0; i < n; i++ {
		if err := tree.Put(concKey(i), concValue(i), 1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// 10 goroutines reading all keys simultaneously
	var wg sync.WaitGroup
	errs := make(chan error, 10*n)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				val, err := tree.Get(concKey(i), concTxnID)
				if err != nil {
					errs <- fmt.Errorf("Get(%d): %v", i, err)
					return
				}
				if !bytes.Equal(val, concValue(i)) {
					errs <- fmt.Errorf("Get(%d): got %q, want %q", i, val, concValue(i))
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// ─── 2. TestConcurrentPut ───────────────────────────────────────────

func TestConcurrentPut(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// 10 goroutines each writing 100 unique keys (disjoint key ranges)
	numGoroutines := 10
	keysPerGoroutine := 100
	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := gid * keysPerGoroutine
			for i := 0; i < keysPerGoroutine; i++ {
				k := concKey(base + i)
				v := concValue(base + i)
				if err := tree.Put(k, v, uint64(gid+1)); err != nil {
					errs <- fmt.Errorf("goroutine %d Put(%d): %v", gid, base+i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Verify all keys
	total := numGoroutines * keysPerGoroutine
	for i := 0; i < total; i++ {
		val, err := tree.Get(concKey(i), concTxnID)
		if err != nil {
			t.Fatalf("Get(%d) after concurrent Put: %v", i, err)
		}
		if !bytes.Equal(val, concValue(i)) {
			t.Fatalf("Get(%d): got %q, want %q", i, val, concValue(i))
		}
	}
}

// ─── 3. TestConcurrentPutGet ────────────────────────────────────────

func TestConcurrentPutGet(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Pre-populate some keys
	nPre := 100
	for i := 0; i < nPre; i++ {
		if err := tree.Put(concKey(i), concValue(i), 1); err != nil {
			t.Fatalf("Pre-Put %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// 5 readers reading pre-populated keys
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < nPre; i++ {
				val, err := tree.Get(concKey(i), concTxnID)
				if err != nil {
					errs <- fmt.Errorf("reader Get(%d): %v", i, err)
					return
				}
				if !bytes.Equal(val, concValue(i)) {
					errs <- fmt.Errorf("reader Get(%d): got %q, want %q", i, val, concValue(i))
					return
				}
			}
		}()
	}

	// 5 writers writing new keys (non-overlapping with pre-populated)
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := nPre + gid*50
			for i := 0; i < 50; i++ {
				k := concKey(base + i)
				v := concValue(base + i)
				if err := tree.Put(k, v, uint64(gid+10)); err != nil {
					errs <- fmt.Errorf("writer %d Put(%d): %v", gid, base+i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Verify all keys (pre-populated + newly written)
	total := nPre + 5*50
	for i := 0; i < total; i++ {
		_, err := tree.Get(concKey(i), concTxnID)
		if err != nil {
			t.Fatalf("Final Get(%d): %v", i, err)
		}
	}
}

// ─── 4. TestConcurrentScan ──────────────────────────────────────────

func TestConcurrentScan(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Pre-populate 500 keys
	n := 500
	for i := 0; i < n; i++ {
		if err := tree.Put(concKey(i), concValue(i), 1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// 5 scanners reading the full range
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			iter := tree.Scan(concKey(0), concKey(n), concTxnID)
			defer iter.Close()
			count := 0
			for iter.Next() {
				count++
			}
			if err := iter.Err(); err != nil {
				errs <- fmt.Errorf("scanner %d: %v", gid, err)
				return
			}
			if count != n {
				errs <- fmt.Errorf("scanner %d: expected %d, got %d", gid, n, count)
			}
		}(g)
	}

	// 3 writers adding new keys concurrently with scans
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := n + gid*100
			for i := 0; i < 100; i++ {
				k := concKey(base + i)
				v := concValue(base + i)
				if err := tree.Put(k, v, uint64(gid+100)); err != nil {
					errs <- fmt.Errorf("writer %d Put(%d): %v", gid, base+i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// ─── 5. TestConcurrentSplit ─────────────────────────────────────────

func TestConcurrentSplit(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Many goroutines writing enough keys to trigger many concurrent splits
	numGoroutines := 8
	keysPerGoroutine := 200
	var wg sync.WaitGroup
	var errCount atomic.Int32

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := gid * keysPerGoroutine
			for i := 0; i < keysPerGoroutine; i++ {
				k := concKey(base + i)
				v := []byte(fmt.Sprintf("value-%06d-padding-to-fill-page-faster!!", base+i))
				if err := tree.Put(k, v, uint64(gid+1)); err != nil {
					t.Logf("goroutine %d Put(%d): %v", gid, base+i, err)
					errCount.Add(1)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	if errCount.Load() > 0 {
		t.Fatalf("%d goroutines encountered errors", errCount.Load())
	}

	// Verify all keys
	total := numGoroutines * keysPerGoroutine
	for i := 0; i < total; i++ {
		k := concKey(i)
		v := []byte(fmt.Sprintf("value-%06d-padding-to-fill-page-faster!!", i))
		got, err := tree.Get(k, concTxnID)
		if err != nil {
			t.Fatalf("Get(%d) after concurrent splits: %v", i, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("Get(%d): got %q, want %q", i, got, v)
		}
	}

	// Should have many pages (splits happened)
	if pages.PageCount() < 10 {
		t.Logf("Warning: only %d pages after %d inserts — expected more splits", pages.PageCount(), total)
	}
}

// ─── 6. TestConcurrentDelete ────────────────────────────────────────

func TestConcurrentDelete(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Pre-populate 500 keys
	n := 500
	for i := 0; i < n; i++ {
		if err := tree.Put(concKey(i), concValue(i), 1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// 5 goroutines each deleting a disjoint range of 100 keys
	var wg sync.WaitGroup
	errs := make(chan error, 5*100)

	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := gid * 100
			for i := 0; i < 100; i++ {
				k := concKey(base + i)
				if err := tree.Delete(k, uint64(gid+10)); err != nil {
					errs <- fmt.Errorf("goroutine %d Delete(%d): %v", gid, base+i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Verify: deleted keys should not be visible at txnID >= 10
	for i := 0; i < n; i++ {
		_, err := tree.Get(concKey(i), concTxnID)
		if err != btreeapi.ErrKeyNotFound {
			t.Fatalf("Get(%d) after delete: expected ErrKeyNotFound, got %v", i, err)
		}
	}
}

// ─── 7. TestConcurrentPutSameKey ────────────────────────────────────

func TestConcurrentPutSameKey(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Multiple goroutines writing the same key with different txnIDs
	numGoroutines := 10
	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			txnID := uint64(gid + 1)
			val := []byte(fmt.Sprintf("version-%d", gid))
			if err := tree.Put([]byte("shared-key"), val, txnID); err != nil {
				errs <- fmt.Errorf("goroutine %d Put: %v", gid, err)
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// The key should be readable at a high txnID
	val, err := tree.Get([]byte("shared-key"), concTxnID)
	if err != nil {
		t.Fatalf("Get shared-key: %v", err)
	}
	if len(val) == 0 {
		t.Fatal("Get shared-key: empty value")
	}
}

// ─── 8. TestConcurrentBootstrap ─────────────────────────────────────

func TestConcurrentBootstrap(t *testing.T) {
	// Multiple goroutines try to Put when root doesn't exist yet
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	numGoroutines := 10
	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			k := concKey(gid)
			v := concValue(gid)
			if err := tree.Put(k, v, uint64(gid+1)); err != nil {
				errs <- fmt.Errorf("goroutine %d Put: %v", gid, err)
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// All keys should be readable
	for i := 0; i < numGoroutines; i++ {
		val, err := tree.Get(concKey(i), concTxnID)
		if err != nil {
			t.Fatalf("Get(%d) after bootstrap: %v", i, err)
		}
		if !bytes.Equal(val, concValue(i)) {
			t.Fatalf("Get(%d): got %q, want %q", i, val, concValue(i))
		}
	}
}
