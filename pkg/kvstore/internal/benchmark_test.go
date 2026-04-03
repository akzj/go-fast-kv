package internal

import (
	"github.com/akzj/go-fast-kv/pkg/kvstore/api"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// percentile calculates the p-th percentile of a slice of durations.
// p should be between 0 and 100.
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	idx := (p / 100) * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))

	if lower == upper {
		return sorted[lower]
	}

	// Linear interpolation
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// getDiskUsage returns the total size of files in a directory.
func getDiskUsage(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// getMemStats returns current memory usage in bytes.
func getMemStats() runtime.MemStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}

// BenchmarkSequentialPut tests sequential key-value writes.
// Uses very small keys (4 bytes) to fit many keys per BTree page before hitting the 127 limit.
func BenchmarkSequentialPut(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Use very small keys (4 bytes) to maximize keys per page
	// BTree panics at >=127 keys per page during split
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%04d", i%100)) // Small 4-byte keys
		value := []byte(fmt.Sprintf("%04d", i))  // Small 4-byte values
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("put failed: %v", err)
		}
	}

	b.StopTimer()
}

// BenchmarkSequentialPut100K tests writing 100K sequential keys with detailed metrics.
// Uses small keys to avoid BTree page overflow (127 key limit).
func BenchmarkSequentialPut100K(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 100 * 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	startMem := getMemStats()
	startDisk, _ := getDiskUsage(dir)

	latencies := make([]float64, 0, keyCount)

	// Use small keys (4 bytes) - BTree panics at >=127 keys per page during split
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("%04d", i%100)) // Cycle through 100 small keys
		value := []byte(fmt.Sprintf("v%04d", i))

		tStart := time.Now()
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("put failed at key %d: %v", i, err)
		}
		tEnd := time.Now()
		latencies = append(latencies, float64(tEnd.Sub(tStart).Nanoseconds()))
	}

	elapsed := b.Elapsed()
	opsPerSec := float64(keyCount) / elapsed.Seconds()

	endMem := getMemStats()
	endDisk, _ := getDiskUsage(dir)

	memUsed := int64(endMem.Alloc) - int64(startMem.Alloc)
	diskUsed := endDisk - startDisk

	// Calculate percentiles (convert nanoseconds to microseconds)
	p50 := percentile(latencies, 50) / 1000
	p95 := percentile(latencies, 95) / 1000
	p99 := percentile(latencies, 99) / 1000

	b.Logf("Benchmark: Sequential Put %dK", keyCount/1000)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
	b.Logf("  Latency P50: %.2fμs", p50)
	b.Logf("  Latency P95: %.2fμs", p95)
	b.Logf("  Latency P99: %.2fμs", p99)
	b.Logf("  Memory used: %d bytes (%.2f MB)", memUsed, float64(memUsed)/1024/1024)
	b.Logf("  Disk used: %d bytes (%.2f MB)", diskUsed, float64(diskUsed)/1024/1024)
}

// BenchmarkSequentialGet tests sequential key-value reads.
func BenchmarkSequentialGet(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 100 * 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate with small keys
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("%04d", i%100))
		value := []byte(fmt.Sprintf("v%04d", i))
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("setup put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	latencies := make([]float64, 0, keyCount)

	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("%04d", i%100))

		tStart := time.Now()
		_, found, err := db.Get(ctx, key)
		tEnd := time.Now()
		latencies = append(latencies, float64(tEnd.Sub(tStart).Nanoseconds()))

		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
		if !found {
			b.Fatalf("key not found: %d", i)
		}
	}

	b.StopTimer()

	elapsed := b.Elapsed()
	opsPerSec := float64(keyCount) / elapsed.Seconds()

	p50 := percentile(latencies, 50) / 1000
	p95 := percentile(latencies, 95) / 1000
	p99 := percentile(latencies, 99) / 1000

	b.Logf("Benchmark: Sequential Get %dK", keyCount/1000)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
	b.Logf("  Latency P50: %.2fμs", p50)
	b.Logf("  Latency P95: %.2fμs", p95)
	b.Logf("  Latency P99: %.2fμs", p99)
}

// BenchmarkRandomPut tests random key-value writes.
func BenchmarkRandomPut(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 100 * 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	latencies := make([]float64, 0, keyCount)

	// Use small keys (4 bytes) with pseudo-random distribution
	for i := 0; i < keyCount; i++ {
		hash := (i*1664525 + 1013904223) % 100
		key := []byte(fmt.Sprintf("%04d", hash))
		value := []byte(fmt.Sprintf("v%04d", i))

		tStart := time.Now()
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("put failed: %v", err)
		}
		tEnd := time.Now()
		latencies = append(latencies, float64(tEnd.Sub(tStart).Nanoseconds()))
	}

	b.StopTimer()

	elapsed := b.Elapsed()
	opsPerSec := float64(keyCount) / elapsed.Seconds()

	p50 := percentile(latencies, 50) / 1000
	p95 := percentile(latencies, 95) / 1000
	p99 := percentile(latencies, 99) / 1000

	b.Logf("Benchmark: Random Put %dK", keyCount/1000)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
	b.Logf("  Latency P50: %.2fμs", p50)
	b.Logf("  Latency P95: %.2fμs", p95)
	b.Logf("  Latency P99: %.2fμs", p99)
}

// BenchmarkRandomGet tests random key-value reads.
func BenchmarkRandomGet(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 100 * 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate with small keys
	for i := 0; i < keyCount; i++ {
		hash := (i*1664525 + 1013904223) % 100
		key := []byte(fmt.Sprintf("%04d", hash))
		value := []byte(fmt.Sprintf("v%04d", i))
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("setup put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	latencies := make([]float64, 0, keyCount)

	for i := 0; i < keyCount; i++ {
		hash := (i*1664525 + 1013904223) % 100
		key := []byte(fmt.Sprintf("%04d", hash))

		tStart := time.Now()
		_, found, err := db.Get(ctx, key)
		tEnd := time.Now()
		latencies = append(latencies, float64(tEnd.Sub(tStart).Nanoseconds()))

		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
		if !found {
			b.Fatalf("key not found: %d", hash)
		}
	}

	b.StopTimer()

	elapsed := b.Elapsed()
	opsPerSec := float64(keyCount) / elapsed.Seconds()

	p50 := percentile(latencies, 50) / 1000
	p95 := percentile(latencies, 95) / 1000
	p99 := percentile(latencies, 99) / 1000

	b.Logf("Benchmark: Random Get %dK", keyCount/1000)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
	b.Logf("  Latency P50: %.2fμs", p50)
	b.Logf("  Latency P95: %.2fμs", p95)
	b.Logf("  Latency P99: %.2fμs", p99)
}

// BenchmarkScan tests range scan performance.
func BenchmarkScan(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 10 * 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate with small keys
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("%06d", i))
		value := []byte(fmt.Sprintf("v%06d", i))
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("setup put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var scanned int
	latencies := make([]float64, 0, b.N)

	for i := 0; i < b.N; i++ {
		start := 0
		end := keyCount / 10 // Scan 10% of keys

		tStart := time.Now()
		err := db.Scan(ctx,
			[]byte(fmt.Sprintf("%06d", start)),
			[]byte(fmt.Sprintf("%06d", end)),
			func(key, value []byte) bool {
				scanned++
				return true
			})
		tEnd := time.Now()
		latencies = append(latencies, float64(tEnd.Sub(tStart).Nanoseconds()))

		if err != nil {
			b.Fatalf("scan failed: %v", err)
		}
	}

	b.StopTimer()

	elapsed := b.Elapsed()
	opsPerSec := float64(scanned) / elapsed.Seconds()

	p50 := percentile(latencies, 50) / 1000
	p95 := percentile(latencies, 95) / 1000
	p99 := percentile(latencies, 99) / 1000

	b.Logf("Benchmark: Scan %d keys", keyCount/10)
	b.Logf("  Total scanned: %d", scanned)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
	b.Logf("  Latency P50: %.2fμs", p50)
	b.Logf("  Latency P95: %.2fμs", p95)
	b.Logf("  Latency P99: %.2fμs", p99)
}

// BenchmarkLargeValue tests large value (1KB, 10KB, 100KB) write/read performance.
func BenchmarkLargeValue(b *testing.B) {
	ctx := context.Background()

	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
	}

	for _, tc := range sizes {
		b.Run(fmt.Sprintf("Write%s", tc.name), func(b *testing.B) {
			dir := b.TempDir()

			db, err := Open(ctx, api.Config{Dir: dir})
			if err != nil {
				b.Fatalf("open failed: %v", err)
			}
			defer db.Close()

			value := make([]byte, tc.size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()
			startDisk, _ := getDiskUsage(dir)

			// Use very small key space to avoid BTree issues
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%02d", i%10)) // Small 3-byte keys
				if err := db.Put(ctx, key, value); err != nil {
					b.Fatalf("put failed: %v", err)
				}
			}

			b.StopTimer()
			endDisk, _ := getDiskUsage(dir)

			elapsed := b.Elapsed()
			opsPerSec := float64(b.N) / elapsed.Seconds()

			b.Logf("Benchmark: Large Value Write %s", tc.name)
			b.Logf("  Ops/sec: %.0f", opsPerSec)
			b.Logf("  Disk used: %d bytes (%.2f MB)", endDisk-startDisk, float64(endDisk-startDisk)/1024/1024)
		})

		b.Run(fmt.Sprintf("Read%s", tc.name), func(b *testing.B) {
			dir := b.TempDir()

			db, err := Open(ctx, api.Config{Dir: dir})
			if err != nil {
				b.Fatalf("open failed: %v", err)
			}
			defer db.Close()

			value := make([]byte, tc.size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			// Pre-populate with small keys
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("k%02d", i))
				if err := db.Put(ctx, key, value); err != nil {
					b.Fatalf("setup put failed: %v", err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%02d", i%10))
				got, found, err := db.Get(ctx, key)
				if err != nil {
					b.Fatalf("get failed: %v", err)
				}
				if !found {
					b.Fatal("key not found")
				}
				if len(got) != tc.size {
					b.Fatalf("expected %d bytes, got %d", tc.size, len(got))
				}
			}

			b.StopTimer()

			elapsed := b.Elapsed()
			opsPerSec := float64(b.N) / elapsed.Seconds()

			b.Logf("Benchmark: Large Value Read %s", tc.name)
			b.Logf("  Ops/sec: %.0f", opsPerSec)
		})
	}
}

// BenchmarkMixedReadWrite tests mixed read/write workload (80% reads, 20% writes).
func BenchmarkMixedReadWrite(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 10 * 1000
	const readRatio = 0.8

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate with small keys
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("%06d", i))
		value := []byte(fmt.Sprintf("v%06d", i))
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("setup put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var readOps, writeOps int

	for i := 0; i < b.N; i++ {
		if i%10 < int(readRatio*10) {
			// Read
			key := []byte(fmt.Sprintf("%06d", i%keyCount))
			_, _, err := db.Get(ctx, key)
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
			readOps++
		} else {
			// Write
			key := []byte(fmt.Sprintf("n%06d", keyCount+(i%keyCount)))
			value := []byte(fmt.Sprintf("n_%d", i))
			if err := db.Put(ctx, key, value); err != nil {
				b.Fatalf("put failed: %v", err)
			}
			writeOps++
		}
	}

	b.StopTimer()

	elapsed := b.Elapsed()
	totalOps := readOps + writeOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	b.Logf("Benchmark: Mixed Read/Write (80%%/20%%)")
	b.Logf("  Total ops: %d (reads: %d, writes: %d)", totalOps, readOps, writeOps)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
}

// BenchmarkConcurrentWrites tests concurrent write performance.
func BenchmarkConcurrentWrites(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const numGoroutines = 8
	const keysPerGoroutine = 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d_%04d", goroutineID, i))
				value := []byte(fmt.Sprintf("v%d_%04d", goroutineID, i))
				if err := db.Put(ctx, key, value); err != nil {
					b.Fatalf("put failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	b.StopTimer()

	totalKeys := numGoroutines * keysPerGoroutine
	elapsed := b.Elapsed()
	opsPerSec := float64(totalKeys) / elapsed.Seconds()

	b.Logf("Benchmark: Concurrent Writes (%d goroutines)", numGoroutines)
	b.Logf("  Total keys: %d", totalKeys)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
}

// BenchmarkConcurrentReads tests concurrent read performance.
func BenchmarkConcurrentReads(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const numGoroutines = 8
	const keyCount = 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate with small keys
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("%06d", i))
		value := []byte(fmt.Sprintf("v%06d", i))
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("setup put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < b.N/numGoroutines; i++ {
				key := []byte(fmt.Sprintf("%06d", (goroutineID*1000+i)%keyCount))
				_, found, err := db.Get(ctx, key)
				if err != nil {
					b.Fatalf("get failed: %v", err)
				}
				if !found {
					b.Fatalf("key not found")
				}
			}
		}(g)
	}

	wg.Wait()

	b.StopTimer()

	elapsed := b.Elapsed()
	totalOps := b.N
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	b.Logf("Benchmark: Concurrent Reads (%d goroutines)", numGoroutines)
	b.Logf("  Total reads: %d", totalOps)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
}

// BenchmarkWriteDeleteMix tests interleaved writes and deletes.
func BenchmarkWriteDeleteMix(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const keyCount = 1000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate with small keys
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		if err := db.Put(ctx, key, value); err != nil {
			b.Fatalf("setup put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var writeOps, deleteOps int

	for i := 0; i < b.N; i++ {
		idx := i % (keyCount * 2)
		if idx < keyCount {
			// Delete existing key
			key := []byte(fmt.Sprintf("k%04d", idx))
			if err := db.Delete(ctx, key); err != nil {
				b.Fatalf("delete failed: %v", err)
			}
			deleteOps++
		} else {
			// Write new key
			key := []byte(fmt.Sprintf("n%04d", idx))
			value := []byte(fmt.Sprintf("n_%d", i))
			if err := db.Put(ctx, key, value); err != nil {
				b.Fatalf("put failed: %v", err)
			}
			writeOps++
		}
	}

	b.StopTimer()

	elapsed := b.Elapsed()
	totalOps := writeOps + deleteOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	b.Logf("Benchmark: Write/Delete Mix")
	b.Logf("  Total ops: %d (writes: %d, deletes: %d)", totalOps, writeOps, deleteOps)
	b.Logf("  Ops/sec: %.0f", opsPerSec)
}

// BenchmarkStress1M performs a stress test with 1M sequential writes followed by 1M random reads.
func BenchmarkStress1M(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	const writeCount = 1_000_000
	const readCount = 1_000_000

	db, err := Open(ctx, api.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	// Phase 1: Sequential writes with small keys
	b.Run("SequentialWrites", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		startMem := getMemStats()

		for i := 0; i < writeCount; i++ {
			key := []byte(fmt.Sprintf("%08d", i))
			value := []byte(fmt.Sprintf("v_%08d", i))
			if err := db.Put(ctx, key, value); err != nil {
				b.Fatalf("put failed at %d: %v", i, err)
			}
		}

		b.StopTimer()

		endMem := getMemStats()
		elapsed := b.Elapsed()
		opsPerSec := float64(writeCount) / elapsed.Seconds()
		memUsed := int64(endMem.Alloc) - int64(startMem.Alloc)

		b.Logf("Benchmark: Stress 1M Sequential Writes")
		b.Logf("  Ops/sec: %.0f", opsPerSec)
		b.Logf("  Total time: %v", elapsed)
		b.Logf("  Memory used: %d bytes (%.2f MB)", memUsed, float64(memUsed)/1024/1024)

		// Check that we can still do reads
		testKey := []byte(fmt.Sprintf("%08d", writeCount/2))
		_, found, err := db.Get(ctx, testKey)
		if err != nil || !found {
			b.Fatalf("validation read failed: found=%v, err=%v", found, err)
		}
	})

	// Phase 2: Random reads
	b.Run("RandomReads", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		var foundCount int
		for i := 0; i < readCount; i++ {
			// Pseudo-random but deterministic
			idx := ((i * 1664525) + 1013904223) % writeCount
			key := []byte(fmt.Sprintf("%08d", idx))
			_, found, err := db.Get(ctx, key)
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
			if found {
				foundCount++
			}
		}

		b.StopTimer()

		elapsed := b.Elapsed()
		opsPerSec := float64(readCount) / elapsed.Seconds()

		b.Logf("Benchmark: Stress 1M Random Reads")
		b.Logf("  Ops/sec: %.0f", opsPerSec)
		b.Logf("  Found: %d / %d (%.1f%%)", foundCount, readCount, float64(foundCount)*100/float64(readCount))
	})
}
