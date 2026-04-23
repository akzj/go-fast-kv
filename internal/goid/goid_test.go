package goid

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

// goroutineIDSlow is the reference implementation using runtime.Stack.
func goroutineIDSlow() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := buf[:n]
	s = s[len("goroutine "):]
	s = s[:bytes.IndexByte(s, ' ')]
	id, _ := strconv.ParseInt(string(s), 10, 64)
	return id
}

func TestGet_MatchesRuntimeStack(t *testing.T) {
	// Verify the fast path returns the same value as the slow path.
	fast := Get()
	slow := goroutineIDSlow()
	if fast != slow {
		t.Fatalf("goid.Get()=%d, runtime.Stack=%d — mismatch", fast, slow)
	}
}

func TestGet_DifferentGoroutines(t *testing.T) {
	// Verify different goroutines get different IDs.
	const N = 100
	ids := make([]int64, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			ids[i] = Get()
		}()
	}
	wg.Wait()

	seen := make(map[int64]bool, N)
	for _, id := range ids {
		if id <= 0 {
			t.Fatalf("got non-positive goroutine ID: %d", id)
		}
		if seen[id] {
			t.Fatalf("duplicate goroutine ID: %d", id)
		}
		seen[id] = true
	}
}

func TestGet_ConsistentWithinGoroutine(t *testing.T) {
	// Calling Get() multiple times in the same goroutine returns the same value.
	id1 := Get()
	id2 := Get()
	id3 := Get()
	if id1 != id2 || id2 != id3 {
		t.Fatalf("inconsistent goid: %d, %d, %d", id1, id2, id3)
	}
}

func TestGet_ConcurrentCorrectness(t *testing.T) {
	// Verify correctness under concurrent access by comparing with runtime.Stack.
	const N = 1000
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			fast := Get()
			slow := goroutineIDSlow()
			if fast != slow {
				t.Errorf("mismatch in goroutine: fast=%d slow=%d", fast, slow)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Get()
	}
}

func BenchmarkGoroutineIDSlow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		goroutineIDSlow()
	}
}
