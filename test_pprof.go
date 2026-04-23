package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	tkvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

const N = 1000000

func main() {
	fmt.Printf("=== Performance Test: %d operations, %d CPUs ===\n", N, runtime.NumCPU())

	// Run sequential test first
	fmt.Println("\n========== SEQUENTIAL TEST ==========")
	runTest(1)

	// Run concurrent tests with different goroutine counts
	for _, workers := range []int{2, 4, 8, runtime.NumCPU()} {
		fmt.Printf("\n========== CONCURRENT TEST (%d goroutines) ==========\n", workers)
		runTest(workers)
	}
}

func runTest(workers int) {
	dir := fmt.Sprintf("/tmp/kv_pprof_test_%d", workers)
	os.RemoveAll(dir)

	s, err := tkvstore.Open(kvstoreapi.Config{
		Dir:      dir,
		SyncMode: kvstoreapi.SyncNone,
	})
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Generate random keys
	keys := make([][]byte, N)
	values := make([][]byte, N)
	for i := 0; i < N; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%08d_%08d", i, rand.Int()))
		values[i] = []byte(fmt.Sprintf("value_%08d", i))
	}
	rand.Seed(42)
	rand.Shuffle(N, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
		values[i], values[j] = values[j], values[i]
	})

	// CPU profile for write phase
	profName := fmt.Sprintf("/tmp/cpu_w%d.prof", workers)
	f, _ := os.Create(profName)
	pprof.StartCPUProfile(f)

	// === WRITE TEST ===
	fmt.Printf("Write Test: %d ops, %d goroutines\n", N, workers)
	start := time.Now()

	if workers == 1 {
		for i := 0; i < N; i++ {
			if err := s.Put(keys[i], values[i]); err != nil {
				panic(err)
			}
		}
	} else {
		var wg sync.WaitGroup
		var errCount atomic.Int64
		perWorker := N / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					if err := s.Put(keys[i], values[i]); err != nil {
						errCount.Add(1)
					}
				}
			}(w*perWorker, (w+1)*perWorker)
		}
		wg.Wait()
		if ec := errCount.Load(); ec > 0 {
			fmt.Printf("  Errors: %d\n", ec)
		}
	}

	writeDur := time.Since(start)
	fmt.Printf("Write: %d ops in %v (%.0f ops/s)\n", N, writeDur, float64(N)/writeDur.Seconds())

	pprof.StopCPUProfile()
	f.Close()

	// === READ TEST ===
	fmt.Printf("Read Test: %d ops, %d goroutines\n", N, workers)
	start = time.Now()
	var hits atomic.Int64

	if workers == 1 {
		for i := 0; i < N; i++ {
			v, err := s.Get(keys[i])
			if err == nil && v != nil {
				hits.Add(1)
			}
		}
	} else {
		var wg sync.WaitGroup
		perWorker := N / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					v, err := s.Get(keys[i])
					if err == nil && v != nil {
						hits.Add(1)
					}
				}
			}(w*perWorker, (w+1)*perWorker)
		}
		wg.Wait()
	}

	readDur := time.Since(start)
	fmt.Printf("Read: %d ops in %v (%.0f ops/s), Hit rate: %.2f%%\n",
		N, readDur, float64(N)/readDur.Seconds(), float64(hits.Load())/float64(N)*100)

	// Metrics
	m := s.GetMetrics()
	fmt.Printf("Metrics: PageWrites=%d, PageSplits=%d, CacheHitRate=%.1f%%\n",
		m.PageWrites, m.PageSplits,
		safePct(m.PageCacheHits, m.PageReads))

	fmt.Printf("Profile: %s\n", profName)
}

// safePct avoids division by zero.
func safePct(num, denom uint64) float64 {
	if denom == 0 {
		return 0
	}
	return float64(num) / float64(denom) * 100
}
