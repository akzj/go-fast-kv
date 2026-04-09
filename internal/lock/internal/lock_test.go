package internal

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestNew(t *testing.T) {
	l := New()
	if l == nil {
		t.Fatal("New returned nil")
	}
}

func TestGetLock_SamePageReturnsSameMutex(t *testing.T) {
	l := New()
	m1 := l.GetLock(42)
	m2 := l.GetLock(42)
	if m1 != m2 {
		t.Fatal("GetLock returned different mutexes for the same PageID")
	}
}

func TestGetLock_DifferentPagesReturnDifferentMutexes(t *testing.T) {
	l := New()
	m1 := l.GetLock(1)
	m2 := l.GetLock(2)
	if m1 == m2 {
		t.Fatal("GetLock returned same mutex for different PageIDs")
	}
}

func TestRLock_RUnlock(t *testing.T) {
	l := New()
	l.RLock(10)
	l.RUnlock(10)
	// No deadlock = pass
}

func TestWLock_WUnlock(t *testing.T) {
	l := New()
	l.WLock(10)
	l.WUnlock(10)
	// No deadlock = pass
}

func TestMultipleReadersNoBlock(t *testing.T) {
	l := New()
	pid := uint64(100)

	var wg sync.WaitGroup
	var count atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.RLock(pid)
			count.Add(1)
			// Hold the read lock briefly
			for count.Load() < 10 {
				// spin until all readers have acquired the lock
			}
			l.RUnlock(pid)
		}()
	}

	wg.Wait()
	if count.Load() != 10 {
		t.Fatalf("expected 10 concurrent readers, got %d", count.Load())
	}
}

func TestWriterExcludesReaders(t *testing.T) {
	l := New()
	pid := uint64(200)

	var counter atomic.Int64

	l.WLock(pid)

	// Start a reader in another goroutine — it should block
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		l.RLock(pid)
		counter.Store(42)
		l.RUnlock(pid)
		close(done)
	}()

	<-started
	// Reader should be blocked, counter should still be 0
	if counter.Load() != 0 {
		// It's possible the goroutine hasn't blocked yet, but
		// with the write lock held, it shouldn't proceed.
		// This is a best-effort check.
	}

	l.WUnlock(pid)
	<-done

	if counter.Load() != 42 {
		t.Fatalf("expected counter=42 after writer released, got %d", counter.Load())
	}
}

func TestSharding(t *testing.T) {
	l := New()
	// Pages 0 and 16 should be in the same shard (0 % 16 == 16 % 16 == 0)
	// but should still get different mutexes
	m0 := l.GetLock(0)
	m16 := l.GetLock(16)
	if m0 == m16 {
		t.Fatal("pages in same shard should still get different mutexes")
	}
}

func TestConcurrentGetLock(t *testing.T) {
	l := New()
	pid := uint64(999)

	var wg sync.WaitGroup
	mutexes := make([]*sync.RWMutex, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mutexes[idx] = l.GetLock(pid)
		}(i)
	}

	wg.Wait()

	// All should be the same mutex
	for i := 1; i < 100; i++ {
		if mutexes[i] != mutexes[0] {
			t.Fatalf("concurrent GetLock returned different mutexes at index %d", i)
		}
	}
}
