package internal

import (
	"fmt"
	"math/rand"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── Benchmark Helpers ───────────────────────────────────────────────

// openBenchmarkStore creates a fresh store with SyncNone for faster benchmarks.
// The store is automatically closed after the benchmark function returns.
func openBenchmarkStore(b *testing.B) kvstoreapi.Store {
	dir := b.TempDir()
	s, err := Open(kvstoreapi.Config{
		Dir:      dir,
		SyncMode: kvstoreapi.SyncNone,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// keyFor creates a formatted key for benchmarking.
func keyFor(i int) []byte {
	return []byte(fmt.Sprintf("k%08d", i))
}

// valueFor creates a formatted value for benchmarking.
func valueFor(i int) []byte {
	return []byte(fmt.Sprintf("v%08d", i))
}

// ─── 1. Sequential Write Benchmarks ─────────────────────────────────

func BenchmarkKVStore_Put_SeqWrite_100(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 100; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
	}
}

func BenchmarkKVStore_Put_SeqWrite_1k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 1000; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
	}
}

func BenchmarkKVStore_Put_SeqWrite_10k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 10000; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
	}
}

// ─── 2. Random Write Benchmarks ──────────────────────────────────────

func BenchmarkKVStore_Put_RandWrite_100(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		keys := make([][]byte, 100)
		for j := 0; j < 100; j++ {
			keys[j] = keyFor(j)
		}
		rand.New(rand.NewSource(int64(i))).Shuffle(100, func(a, b int) {
			keys[a], keys[b] = keys[b], keys[a]
		})
		for j := 0; j < 100; j++ {
			s.Put(keys[j], valueFor(j))
		}
	}
}

func BenchmarkKVStore_Put_RandWrite_1k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		keys := make([][]byte, 1000)
		for j := 0; j < 1000; j++ {
			keys[j] = keyFor(j)
		}
		rand.New(rand.NewSource(int64(i))).Shuffle(1000, func(a, b int) {
			keys[a], keys[b] = keys[b], keys[a]
		})
		for j := 0; j < 1000; j++ {
			s.Put(keys[j], valueFor(j))
		}
	}
}

// ─── 3. Sequential Read Benchmarks ────────────────────────────────────

func BenchmarkKVStore_Get_SeqRead_100(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 100; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			s.Get(keyFor(j))
		}
	}
}

func BenchmarkKVStore_Get_SeqRead_1k(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 1000; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			s.Get(keyFor(j))
		}
	}
}

// ─── 4. Random Read Benchmarks ───────────────────────────────────────

func BenchmarkKVStore_Get_RandRead_100(b *testing.B) {
	s := openBenchmarkStore(b)
	keys := make([][]byte, 100)
	for j := 0; j < 100; j++ {
		keys[j] = keyFor(j)
		s.Put(keys[j], valueFor(j))
	}
	indices := make([]int, 100)
	for i := range indices {
		indices[i] = i
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		rng.Shuffle(100, func(a, b int) {
			indices[a], indices[b] = indices[b], indices[a]
		})
		for j := 0; j < 100; j++ {
			s.Get(keys[indices[j]])
		}
	}
}

func BenchmarkKVStore_Get_RandRead_1k(b *testing.B) {
	s := openBenchmarkStore(b)
	keys := make([][]byte, 1000)
	for j := 0; j < 1000; j++ {
		keys[j] = keyFor(j)
		s.Put(keys[j], valueFor(j))
	}
	indices := make([]int, 1000)
	for i := range indices {
		indices[i] = i
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		rng.Shuffle(1000, func(a, b int) {
			indices[a], indices[b] = indices[b], indices[a]
		})
		for j := 0; j < 1000; j++ {
			s.Get(keys[indices[j]])
		}
	}
}

// ─── 5. Batch Write Benchmarks ───────────────────────────────────────

func BenchmarkKVStore_WriteBatch_10(b *testing.B) {
	s := openBenchmarkStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := s.NewWriteBatch()
		for j := 0; j < 10; j++ {
			wb.Put(keyFor(i*10+j), valueFor(i*10+j))
		}
		wb.Commit()
	}
}

func BenchmarkKVStore_WriteBatch_100(b *testing.B) {
	s := openBenchmarkStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := s.NewWriteBatch()
		for j := 0; j < 100; j++ {
			wb.Put(keyFor(i*100+j), valueFor(i*100+j))
		}
		wb.Commit()
	}
}

func BenchmarkKVStore_WriteBatch_1k(b *testing.B) {
	s := openBenchmarkStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := s.NewWriteBatch()
		for j := 0; j < 1000; j++ {
			wb.Put(keyFor(i*1000+j), valueFor(i*1000+j))
		}
		wb.Commit()
	}
}

// ─── 6. Scan Benchmarks ──────────────────────────────────────────────

func BenchmarkKVStore_Scan_100(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 100; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := s.Scan(keyFor(0), keyFor(100))
		for iter.Next() {
		}
		iter.Close()
	}
}

func BenchmarkKVStore_Scan_1k(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 1000; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := s.Scan(keyFor(0), keyFor(1000))
		for iter.Next() {
		}
		iter.Close()
	}
}

// ─── 7. Concurrent Write Benchmarks ─────────────────────────────────

func BenchmarkKVStore_ConcurrentWrite_2(b *testing.B) {
	s := openBenchmarkStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			s.Put(keyFor(counter), valueFor(counter))
			counter++
		}
	})
}

func BenchmarkKVStore_ConcurrentWrite_4(b *testing.B) {
	s := openBenchmarkStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			s.Put(keyFor(counter), valueFor(counter))
			counter++
		}
	})
}

func BenchmarkKVStore_ConcurrentWrite_8(b *testing.B) {
	s := openBenchmarkStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			s.Put(keyFor(counter), valueFor(counter))
			counter++
		}
	})
}

// ─── 8. Overwrite Benchmarks ─────────────────────────────────────────

func BenchmarkKVStore_Overwrite_100(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 100; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
	}
}

func BenchmarkKVStore_Overwrite_1k(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 1000; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
	}
}

// ─── 9. Delete Benchmarks ───────────────────────────────────────────

func BenchmarkKVStore_Delete_100(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 100; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
		for j := 0; j < 100; j++ {
			s.Delete(keyFor(j))
		}
	}
}

func BenchmarkKVStore_Delete_1k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 1000; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
		for j := 0; j < 1000; j++ {
			s.Delete(keyFor(j))
		}
	}
}

// ─── 10. Delete Batch Benchmarks ─────────────────────────────────────

func BenchmarkKVStore_DeleteBatch_100(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 100; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
		wb := s.NewWriteBatch()
		for j := 0; j < 100; j++ {
			wb.Delete(keyFor(j))
		}
		wb.Commit()
	}
}

func BenchmarkKVStore_DeleteBatch_1k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := openBenchmarkStore(b)
		for j := 0; j < 1000; j++ {
			s.Put(keyFor(j), valueFor(j))
		}
		wb := s.NewWriteBatch()
		for j := 0; j < 1000; j++ {
			wb.Delete(keyFor(j))
		}
		wb.Commit()
	}
}

// ─── 11. Mixed Read/Write Benchmarks ─────────────────────────────────

func BenchmarkKVStore_Mixed_50_50_100(b *testing.B) {
	s := openBenchmarkStore(b)
	for j := 0; j < 100; j++ {
		s.Put(keyFor(j), valueFor(j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := s.NewWriteBatch()
		for j := 0; j < 50; j++ {
			wb.Put(keyFor(j), valueFor(i*50+j))
		}
		wb.Commit()
		for j := 0; j < 50; j++ {
			s.Get(keyFor(j))
		}
	}
}