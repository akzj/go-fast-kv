package internal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"os"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

// ─── Helpers ────────────────────────────────────────────────────────

func benchKey(i int) []byte   { return []byte(fmt.Sprintf("bench-key-%08d", i)) }
func benchValue(i int) []byte { return []byte(fmt.Sprintf("bench-value-%08d-padding-to-make-it-about-100-bytes-long-for-realistic-workloads-xxxxxxxxxxxxxx", i)) }

func openBenchStore(b *testing.B) kvstoreapi.Store {
	b.Helper()
	dir := b.TempDir()
	s, err := Open(kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 256 * 1024 * 1024, // 256 MB — larger to avoid segment-full during benchmarks
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// openBenchStoreSSI opens a KVStore with SSI (Serializable Snapshot Isolation) mode.
func openBenchStoreSSI(b *testing.B) kvstoreapi.Store {
	b.Helper()
	dir := b.TempDir()
	s, err := Open(kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 256 * 1024 * 1024,
		IsolationLevel: kvstoreapi.IsolationSerializable,
	})
	if err != nil {
		b.Fatalf("Open SSI: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// ─── BenchmarkKVStorePutSingle ──────────────────────────────────────

func BenchmarkKVStorePutSingle(b *testing.B) {
	s := openBenchStore(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
}

// ─── BenchmarkKVStorePutSequential_SI ───────────────────────────────

func BenchmarkPutSequential_SI(b *testing.B) {
	s := openBenchStore(b)

	b.ResetTimer()
	b.ReportMetric(0, "ssi_conflicts")
	for i := 0; i < b.N; i++ {
		if err := s.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
}

// ─── BenchmarkPutSequential_SSI ────────────────────────────────────

func BenchmarkPutSequential_SSI(b *testing.B) {
	s := openBenchStoreSSI(b)

	var conflicts atomic.Int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s.Put(benchKey(i), benchValue(i))
		if err != nil {
			if err == txnapi.ErrSerializationFailure {
				conflicts.Add(1)
				i-- // retry: key is unique, no deadlock risk
				continue
			}
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
	b.ReportMetric(float64(conflicts.Load()), "ssi_conflicts")
}

// ─── BenchmarkKVStorePutConcurrent4 ─────────────────────────────────

func BenchmarkKVStorePutConcurrent4(b *testing.B) {
	benchPutConcurrent(b, 4)
}

// ─── BenchmarkKVStorePutConcurrent10 ────────────────────────────────

func BenchmarkKVStorePutConcurrent10(b *testing.B) {
	benchPutConcurrent(b, 10)
}

// benchPutConcurrent is the original function — kept for backward compat and non-SSI benchmarks.
func benchPutConcurrent(b *testing.B, goroutines int) {
	s := openBenchStore(b)
	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(counter.Add(1))
			if err := s.Put(benchKey(i), benchValue(i)); err != nil {
				b.Errorf("Put(%d): %v", i, err)
				return
			}
		}
	})
}

// ─── BenchmarkPutConcurrent4_SI ────────────────────────────────────

func BenchmarkPutConcurrent4_SI(b *testing.B) {
	benchPutConcurrentSI(b, 4, false)
}

// ─── BenchmarkPutConcurrent4_SSI ────────────────────────────────────

func BenchmarkPutConcurrent4_SSI(b *testing.B) {
	benchPutConcurrentSI(b, 4, true)
}

// ─── BenchmarkPutConcurrent10_SI ──────────────────────────────────

func BenchmarkPutConcurrent10_SI(b *testing.B) {
	benchPutConcurrentSI(b, 10, false)
}

// ─── BenchmarkPutConcurrent10_SSI ──────────────────────────────────

func BenchmarkPutConcurrent10_SSI(b *testing.B) {
	benchPutConcurrentSI(b, 10, true)
}

// benchPutConcurrentSI is the shared implementation used by both SI and SSI concurrent benchmarks.
func benchPutConcurrentSI(b *testing.B, goroutines int, ssiMode bool) {
	var s kvstoreapi.Store
	if ssiMode {
		s = openBenchStoreSSI(b)
	} else {
		s = openBenchStore(b)
	}

	var counter atomic.Int64
	var conflicts atomic.Int64
	maxRetries := 10

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(counter.Add(1))
			var err error
			retries := 0
			for {
				err = s.Put(benchKey(i), benchValue(i))
				if err == nil {
					break
				}
				if err == txnapi.ErrSerializationFailure && retries < maxRetries {
					conflicts.Add(1)
					retries++
					// Small backoff to reduce contention
					continue
				}
				b.Errorf("Put(%d) after %d retries: %v", i, retries, err)
				return
			}
		}
	})
	b.ReportMetric(float64(conflicts.Load()), "ssi_conflicts")
}

// ─── BenchmarkKVStoreGetConcurrent10 ────────────────────────────────

func BenchmarkKVStoreGetConcurrent10(b *testing.B) {
	benchGetConcurrent(b, 10, false)
}

func benchGetConcurrent(b *testing.B, goroutines int, ssiMode bool) {
	var s kvstoreapi.Store
	if ssiMode {
		s = openBenchStoreSSI(b)
	} else {
		s = openBenchStore(b)
	}

	// Pre-populate 1000 keys
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		if err := s.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Pre-populate Put(%d): %v", i, err)
		}
	}

	var counter atomic.Int64
	b.ResetTimer()
	b.SetParallelism(goroutines)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(counter.Add(1)) % numKeys
			val, err := s.Get(benchKey(i))
			if err != nil {
				b.Errorf("Get(%d): %v", i, err)
				return
			}
			if len(val) == 0 {
				b.Errorf("Get(%d): empty value", i)
				return
			}
		}
	})
}

// ─── BenchmarkGetConcurrent10_SI ───────────────────────────────────

func BenchmarkGetConcurrent10_SI(b *testing.B) {
	benchGetConcurrent(b, 10, false)
}

// ─── BenchmarkGetConcurrent10_SSI ──────────────────────────────────

func BenchmarkGetConcurrent10_SSI(b *testing.B) {
	benchGetConcurrent(b, 10, true)
}

// ─── BenchmarkKVStoreMixed ──────────────────────────────────────────

func BenchmarkKVStoreMixed(b *testing.B) {
	benchMixed(b, false)
}

func benchMixed(b *testing.B, ssiMode bool) {
	var s kvstoreapi.Store
	if ssiMode {
		s = openBenchStoreSSI(b)
	} else {
		s = openBenchStore(b)
	}

	// Pre-populate 500 keys for readers
	const numKeys = 500
	for i := 0; i < numKeys; i++ {
		if err := s.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Pre-populate Put(%d): %v", i, err)
		}
	}

	var writeCounter atomic.Int64
	var readCounter atomic.Int64
	var wg sync.WaitGroup

	b.ResetTimer()

	// 5 writers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				idx := int(writeCounter.Add(1)) + numKeys
				err := s.Put(benchKey(idx), benchValue(idx))
				if err != nil {
					if err == txnapi.ErrSerializationFailure {
						// Retry on conflict
						continue
					}
					b.Errorf("writer Put(%d): %v", idx, err)
					return
				}
			}
		}()
	}

	// 5 readers
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				idx := int(readCounter.Add(1)) % numKeys
				_, err := s.Get(benchKey(idx))
				if err != nil {
					b.Errorf("reader Get(%d): %v", idx, err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// ─── BenchmarkMixed_SI ───────────────────────────────────────────────

func BenchmarkMixed_SI(b *testing.B) {
	benchMixed(b, false)
}

// ─── BenchmarkMixed_SSI ─────────────────────────────────────────────

func BenchmarkMixed_SSI(b *testing.B) {
	benchMixed(b, true)
}

// ─── Segment Benchmarks ────────────────────────────────────────────

func BenchmarkSegmentMmapRead(b *testing.B) {
	dir := b.TempDir()
	sm, err := segment.New(segmentapi.Config{Dir: dir, MaxSize: 64 * 1024 * 1024})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer sm.Close()

	// Write pages to fill a segment (each page = 4108 bytes record)
	const pageSize = 4108
	pageData := make([]byte, pageSize)
	for i := range pageData {
		pageData[i] = byte(i & 0xff)
	}

	// Write enough pages to fill the segment (64MB / 4108 ≈ 15728 pages)
	var addrs []segmentapi.VAddr
	for i := 0; i < 10000; i++ {
		addr, err := sm.Append(pageData)
		if err != nil {
			b.Fatalf("Append: %v", err)
		}
		addrs = append(addrs, addr)
	}

	// Seal the segment — now reads use mmap path.
	if err := sm.Rotate(); err != nil {
		b.Fatalf("Rotate: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(pageData)))
	for i := 0; i < b.N; i++ {
		addr := addrs[i%len(addrs)]
		_, err := sm.ReadAt(addr, pageSize)
		if err != nil {
			b.Fatalf("ReadAt: %v", err)
		}
	}
}

// ─── BenchmarkSegmentReadAt — measures ReadAt (active/unsealed segment) ──
// Same as BenchmarkSegmentMmapRead but reads from the ACTIVE (writable)
// segment, which uses file.ReadAt syscalls instead of mmap.
// This is the comparison baseline.

func BenchmarkSegmentReadAt(b *testing.B) {
	dir := b.TempDir()
	sm, err := segment.New(segmentapi.Config{Dir: dir, MaxSize: 64 * 1024 * 1024})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer sm.Close()

	const pageSize = 4108
	pageData := make([]byte, pageSize)
	for i := range pageData {
		pageData[i] = byte(i & 0xff)
	}

	// Write pages — STAYS in active segment (no Rotate).
	var addrs []segmentapi.VAddr
	for i := 0; i < 10000; i++ {
		addr, err := sm.Append(pageData)
		if err != nil {
			b.Fatalf("Append: %v", err)
		}
		addrs = append(addrs, addr)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(pageData)))
	for i := 0; i < b.N; i++ {
		addr := addrs[i%len(addrs)]
		_, err := sm.ReadAt(addr, pageSize)
		if err != nil {
			b.Fatalf("ReadAt: %v", err)
		}
	}
}

func BenchmarkBulkLoad1K(b *testing.B) {
	dir := b.TempDir()
	defer os.RemoveAll(dir)
	store, _ := Open(kvstoreapi.Config{Dir: dir})
	defer store.Close()

	pairs := make([]btreeapi.KVPair, 1000)
	for i := 0; i < 1000; i++ {
		pairs[i] = btreeapi.KVPair{Key: []byte(fmt.Sprintf("k%08d", i)), Value: []byte(fmt.Sprintf("v%08d", i))}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.BulkLoad(pairs)
	}
}
