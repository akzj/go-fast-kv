package kvstore

import (
	"fmt"
	"sync"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── 14. TestConcurrentGet ──────────────────────────────────────────

func TestConcurrentGet(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Pre-populate 100 keys
	for i := 0; i < 100; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// 10 goroutines reading all 100 keys concurrently
	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				val, err := s.Get(testKey(i))
				if err != nil {
					errs <- fmt.Errorf("Get(%d): %v", i, err)
					return
				}
				if string(val) != string(testValue(i)) {
					errs <- fmt.Errorf("Get(%d): got %q, want %q", i, val, testValue(i))
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}
}

// ─── 15. TestConcurrentPutGet ───────────────────────────────────────

func TestConcurrentPutGet(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Pre-populate some keys so readers have something to read
	for i := 0; i < 50; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// 5 writers: write keys 50-99
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 50 + gid*10; i < 50+(gid+1)*10; i++ {
				if err := s.Put(testKey(i), testValue(i)); err != nil {
					errs <- fmt.Errorf("writer %d Put(%d): %v", gid, i, err)
					return
				}
			}
		}(g)
	}

	// 5 readers: read keys 0-49 (guaranteed to exist)
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				val, err := s.Get(testKey(i))
				if err != nil {
					errs <- fmt.Errorf("reader %d Get(%d): %v", gid, i, err)
					return
				}
				if string(val) != string(testValue(i)) {
					errs <- fmt.Errorf("reader %d Get(%d): got %q, want %q", gid, i, val, testValue(i))
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}

	// Verify all 100 keys exist after concurrent operations
	for i := 0; i < 100; i++ {
		val, err := s.Get(testKey(i))
		if err != nil {
			t.Fatalf("post-check Get(%d): %v", i, err)
		}
		if string(val) != string(testValue(i)) {
			t.Fatalf("post-check Get(%d): got %q, want %q", i, val, testValue(i))
		}
	}
}

// ─── 16. TestConcurrentScan ─────────────────────────────────────────

func TestConcurrentScan(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Pre-populate 50 keys
	for i := 0; i < 50; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	// 5 scanners
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			iter := s.Scan(testKey(0), testKey(50))
			defer iter.Close()

			count := 0
			for iter.Next() {
				count++
			}
			if err := iter.Err(); err != nil {
				errs <- fmt.Errorf("scanner %d: %v", gid, err)
				return
			}
			// Should see at least 50 keys (writers may add more concurrently)
			if count < 50 {
				errs <- fmt.Errorf("scanner %d: got %d keys, want >= 50", gid, count)
			}
		}(g)
	}

	// 3 writers: write keys 50-79
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 50 + gid*10; i < 50+(gid+1)*10; i++ {
				if err := s.Put(testKey(i), testValue(i)); err != nil {
					errs <- fmt.Errorf("writer %d Put(%d): %v", gid, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}
}

// ─── 17. TestConcurrentPut ──────────────────────────────────────────

func TestConcurrentPut(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	// 5 goroutines writing disjoint key ranges
	keysPerGoroutine := 20
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := gid * keysPerGoroutine
			for i := base; i < base+keysPerGoroutine; i++ {
				if err := s.Put(testKey(i), testValue(i)); err != nil {
					errs <- fmt.Errorf("goroutine %d Put(%d): %v", gid, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}

	// Verify all 100 keys
	for i := 0; i < 5*keysPerGoroutine; i++ {
		val, err := s.Get(testKey(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if string(val) != string(testValue(i)) {
			t.Fatalf("Get(%d): got %q, want %q", i, val, testValue(i))
		}
	}
}

// ─── 18. TestConcurrentDelete ───────────────────────────────────────

func TestConcurrentDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Pre-populate 50 keys
	for i := 0; i < 50; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	// 5 goroutines deleting disjoint ranges
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := gid * 10; i < (gid+1)*10; i++ {
				if err := s.Delete(testKey(i)); err != nil {
					errs <- fmt.Errorf("goroutine %d Delete(%d): %v", gid, i, err)
					return
				}
			}
		}(g)
	}

	// Concurrent readers on the same keys (may get ErrKeyNotFound)
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, err := s.Get(testKey(i))
				if err != nil && err != kvstoreapi.ErrKeyNotFound {
					errs <- fmt.Errorf("reader %d Get(%d): unexpected error %v", gid, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}

	// All 50 keys should be deleted
	for i := 0; i < 50; i++ {
		_, err := s.Get(testKey(i))
		if err != kvstoreapi.ErrKeyNotFound {
			t.Fatalf("Get(%d) after delete: expected ErrKeyNotFound, got %v", i, err)
		}
	}
}
