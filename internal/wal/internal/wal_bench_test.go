package internal

import (
	"fmt"
	"sync"
	"testing"

	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

func newBenchWAL(b *testing.B) walapi.WAL {
	b.Helper()
	dir := b.TempDir()
	w, err := New(walapi.Config{Dir: dir})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	return w
}

func makeBatch(id uint64) *walapi.Batch {
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordPageMap, id, 0x0001_00000010, 0)
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, id, 0, 0)
	return batch
}

// BenchmarkWriteBatchSingle — single goroutine, sequential writes.
// Measures per-write latency (dominated by fsync).
func BenchmarkWriteBatchSingle(b *testing.B) {
	w := newBenchWAL(b)
	defer w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := makeBatch(uint64(i))
		if _, err := w.WriteBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteBatchConcurrent runs N goroutines each writing batches.
func benchmarkWriteBatchConcurrent(b *testing.B, goroutines int) {
	w := newBenchWAL(b)
	defer w.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	perGoroutine := b.N / goroutines
	if perGoroutine == 0 {
		perGoroutine = 1
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := uint64(gid*perGoroutine + i)
				batch := makeBatch(id)
				if _, err := w.WriteBatch(batch); err != nil {
					b.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkWriteBatchConcurrent4(b *testing.B) {
	benchmarkWriteBatchConcurrent(b, 4)
}

func BenchmarkWriteBatchConcurrent10(b *testing.B) {
	benchmarkWriteBatchConcurrent(b, 10)
}

func BenchmarkWriteBatchConcurrent50(b *testing.B) {
	benchmarkWriteBatchConcurrent(b, 50)
}

// BenchmarkWriteBatchThroughput measures total ops/sec with concurrent writers.
func BenchmarkWriteBatchThroughput(b *testing.B) {
	for _, n := range []int{1, 4, 10, 50} {
		b.Run(fmt.Sprintf("goroutines=%d", n), func(b *testing.B) {
			benchmarkWriteBatchConcurrent(b, n)
		})
	}
}
