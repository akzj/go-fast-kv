// Package internal hosts the core KVStore implementation.
// See docs/DESIGN.md §1, §3.6.
package internal

import (
	"sync/atomic"
	"time"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── latencyRing ────────────────────────────────────────────────────

// latencyRing is a fixed-size ring buffer for sampling operation latencies.
// Thread-safe writer via atomic index increment; reader (GetMetrics) is
// lock-free. Memory layout is contiguous for cache efficiency.
//
// Sampling strategy: record every Nth operation to limit overhead.
// N=16 means ~6% sampling overhead at full throughput, ~10ns per sampled op.
const latencyRingSize = 2048
const latencySampleMod = 16 // sample 1 in every 16 operations

type latencyRing struct {
	// timestamps stored as int64 (Unix nanoseconds)
	items [latencyRingSize]int64
	// writerIndex is the slot the next sample will overwrite.
	// uint64 so we can atomically increment without wrapping concerns.
	writerIndex uint64
	// count tracks total samples ever written (for percentile calculation).
	count uint64
}

// record stores a latency sample in the ring buffer.
// This is called from Put/Get paths — must be ~10ns overhead.
// Returns true if this sample was recorded (for sampling rate control).
func (r *latencyRing) record(startNs int64) bool {
	idx := atomic.AddUint64(&r.writerIndex, 1) - 1
	r.items[idx%latencyRingSize] = startNs
	atomic.AddUint64(&r.count, 1)
	return true
}

// percentile returns the p-th percentile latency in microseconds from
// the samples in the ring buffer. Calculated on-demand (no locks held).
// Returns 0 if fewer than 2 samples are available.
func (r *latencyRing) percentile(p float64) float64 {
	n := atomic.LoadUint64(&r.count)
	if n < 2 {
		return 0
	}

	// Copy samples into a temporary slice for sorting.
	// Ring buffer is 2048 entries — this is fast.
	startIdx := r.writerIndex - n
	if n > latencyRingSize {
		startIdx = r.writerIndex - latencyRingSize
		n = latencyRingSize
	}

	// Collect samples into a temporary slice.
	// We read atomically from writerIndex to get a consistent snapshot.
	samples := make([]float64, 0, n)
	now := time.Now().UnixNano()
	limit := startIdx + n
	for i := startIdx; i < limit; i++ {
		ts := atomic.LoadInt64(&r.items[i%latencyRingSize])
		if ts == 0 {
			continue // unwritten slot
		}
		latencyUs := float64(now-ts) / 1000.0
		if latencyUs < 0 {
			latencyUs = 0 // clock skew guard
		}
		samples = append(samples, latencyUs)
	}
	if len(samples) < 2 {
		return 0
	}

	// Sort and extract percentile.
	// Simple insertion sort — fine for small N (≤2048).
	for i := 1; i < len(samples); i++ {
		for j := i; j > 0 && samples[j-1] > samples[j]; j-- {
			samples[j], samples[j-1] = samples[j-1], samples[j]
		}
	}
	idx := int(float64(len(samples)-1) * p)
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return samples[idx]
}

// ─── throughputWindow ──────────────────────────────────────────────

// throughputWindow tracks operations in a rolling time window.
// Uses atomic counters — zero blocking on read.
type throughputWindow struct {
	// Counters per bucket.
	buckets [10]atomic.Int64
	// Bucket timestamps (unix nano).
	bucketTimes [10]int64
	// Index of the current (most recent) bucket.
	curBucket uint64
}

// nextBucket advances to the next bucket, resetting its counter.
// Called by the background tick goroutine (started lazily).
func (tw *throughputWindow) nextBucket() {
	idx := (atomic.AddUint64(&tw.curBucket, 1)) % 10
	tw.buckets[idx].Store(0)
	tw.bucketTimes[idx] = time.Now().UnixNano()
}

// inc increments the current bucket's counter.
func (tw *throughputWindow) inc() {
	idx := atomic.LoadUint64(&tw.curBucket) % 10
	tw.buckets[idx].Add(1)
}

// rate returns the total operations across all buckets divided by the
// elapsed time in seconds, giving operations/sec. Returns 0 on first call
// or if fewer than 2 buckets have been touched.
func (tw *throughputWindow) rate() uint64 {
	nowNanos := time.Now().UnixNano()
	var total int64
	var minTime int64

	// Collect bucket counts and find earliest touched bucket time.
	for i := 0; i < 10; i++ {
		c := tw.buckets[i].Load()
		if c > 0 {
			total += c
			bt := tw.bucketTimes[i]
			if minTime == 0 || bt < minTime {
				minTime = bt
			}
		}
	}

	if total <= 0 || minTime == 0 {
		return 0
	}

	elapsed := nowNanos - minTime
	if elapsed <= 0 {
		return 0
	}

	// Convert to ops/sec.
	opsPerSec := float64(total) / (float64(elapsed) / 1e9)
	if opsPerSec < 0 {
		return 0
	}
	return uint64(opsPerSec)
}

// ─── metricsCollector ───────────────────────────────────────────────

// metricsCollector holds all metrics state for a store.
// All fields are accessed via atomics from operation paths (Put/Get/Scan).
// GetMetrics() reads atomically — zero blocking.
type metricsCollector struct {
	// Latency rings
	getLatency latencyRing
	putLatency latencyRing
	scanLatency latencyRing

	// Throughput windows
	readWindow  throughputWindow
	writeWindow throughputWindow
	scanWindow  throughputWindow

	// Error counters
	totalErrors atomic.Int64

	// Sampling counters for latency rings
	getSampleCounter atomic.Uint64
	putSampleCounter atomic.Uint64
	scanSampleCounter atomic.Uint64

	// Background status (set by GC/compaction goroutines)
	gcRunning           atomic.Bool
	compactionRunning    atomic.Bool
	compactionProgress   atomic.Uint64 // packed: pct*1000 as uint64

	// Resource estimates (updated periodically by background tick)
	walSizeBytes atomic.Uint64

	// Page-level operation stats (wired from RealPageProvider).
	pageReads      atomic.Uint64
	pageWrites     atomic.Uint64
	pageCacheHits  atomic.Uint64
	pageAlloc      atomic.Uint64

	// Page I/O latency tracking (microseconds, P99).
	pageReadLatencyNanos  atomic.Uint64 // Total read latency (ns)
	pageReadCount         atomic.Uint64 // Number of reads
	pageWriteLatencyNanos atomic.Uint64 // Total write latency (ns)
	pageWriteLatencyCount atomic.Uint64 // Number of writes

	// B-tree traversal stats (wired from bTree).
	pageSplits         atomic.Uint64 // Leaf node split count
	searchDepthSum     atomic.Uint64 // Total search depth (for avg calculation)
	searchCount        atomic.Uint64 // Number of search operations
	btreeSearchDepth   atomic.Uint64 // Average search depth
	rightSiblingNavs   atomic.Uint64 // B-link correction traversals
}

// incRead records a read operation. Called from Get/Scan paths.
func (mc *metricsCollector) incRead(startNs int64) {
	mc.readWindow.inc()
	// Sample latency: record 1 in every sampleMod reads.
	idx := mc.getSampleCounter.Add(1)
	if idx%latencySampleMod == 0 {
		mc.getLatency.record(startNs)
	}
}

// incWrite records a write operation. Called from Put/Delete paths.
func (mc *metricsCollector) incWrite(startNs int64) {
	mc.writeWindow.inc()
	// Sample latency: record 1 in every sampleMod writes.
	idx := mc.putSampleCounter.Add(1)
	if idx%latencySampleMod == 0 {
		mc.putLatency.record(startNs)
	}
}

// incScan records a scan operation. Called from Scan/ScanWithParams paths.
func (mc *metricsCollector) incScan(startNs int64) {
	mc.scanWindow.inc()
	// Sample latency: record 1 in every sampleMod scans.
	idx := mc.scanSampleCounter.Add(1)
	if idx%latencySampleMod == 0 {
		mc.scanLatency.record(startNs)
	}
}

// incError records an error from Put/Get/Delete.
func (mc *metricsCollector) incError() {
	mc.totalErrors.Add(1)
}

// setGCRunning updates the GC running status.
func (mc *metricsCollector) setGCRunning(running bool) {
	mc.gcRunning.Store(running)
}

// setCompaction updates compaction status and progress.
// Progress is packed as uint64(pct*1000) to avoid atomic.Float64.
func (mc *metricsCollector) setCompaction(running bool, progressPct float64) {
	mc.compactionRunning.Store(running)
	mc.compactionProgress.Store(uint64(progressPct * 1000))
}

// setWALSize updates the WAL size estimate.
func (mc *metricsCollector) setWALSize(bytes uint64) {
	mc.walSizeBytes.Store(bytes)
}

// UpdatePageStats updates page operation stats from RealPageProvider.
func (mc *metricsCollector) UpdatePageStats(stats struct {
	PageReads      uint64
	PageWrites     uint64
	PageCacheHits  uint64
	PageAlloc      uint64
	ReadLatNs      uint64
	ReadCount      uint64
	WriteLatNs     uint64
	WriteCount     uint64
}) {
	mc.pageReads.Store(stats.PageReads)
	mc.pageWrites.Store(stats.PageWrites)
	mc.pageCacheHits.Store(stats.PageCacheHits)
	mc.pageAlloc.Store(stats.PageAlloc)
	mc.pageReadLatencyNanos.Store(stats.ReadLatNs)
	mc.pageReadCount.Store(stats.ReadCount)
	mc.pageWriteLatencyNanos.Store(stats.WriteLatNs)
	mc.pageWriteLatencyCount.Store(stats.WriteCount)
}

// UpdateBTreeStats updates B-tree traversal stats.
func (mc *metricsCollector) UpdateBTreeStats(stats struct {
	SplitCount      uint64
	SearchDepthSum  uint64
	SearchCount     uint64
	RightSiblingNav uint64
}) {
	mc.pageSplits.Store(stats.SplitCount)
	mc.searchDepthSum.Store(stats.SearchDepthSum)
	mc.searchCount.Store(stats.SearchCount)
	// Average search depth
	if stats.SearchCount > 0 {
		mc.btreeSearchDepth.Store(stats.SearchDepthSum / stats.SearchCount)
	}
	mc.rightSiblingNavs.Store(stats.RightSiblingNav)
}

// collect returns a snapshot of all metrics.
func (mc *metricsCollector) collect() *kvstoreapi.Metrics {
	// Page-level latency P99 in microseconds.
	readLatencyP99Us := uint64(0)
	readCount := mc.pageReadCount.Load()
	if readCount > 0 {
		// Approximate P99 from accumulated latency (total / count * 1.2 for P99 skew)
		readLatencyP99Us = (mc.pageReadLatencyNanos.Load() / readCount) * 12 / 10 / 1000
	}
	writeLatencyP99Us := uint64(0)
	writeCount := mc.pageWriteLatencyCount.Load()
	if writeCount > 0 {
		writeLatencyP99Us = (mc.pageWriteLatencyNanos.Load() / writeCount) * 12 / 10 / 1000
	}

	// Average B-tree search depth.
	btreeSearchDepth := uint64(0)
	searchCount := mc.searchCount.Load()
	searchDepthSum := mc.searchDepthSum.Load()
	if searchCount > 0 {
		btreeSearchDepth = searchDepthSum / searchCount
	}

	return &kvstoreapi.Metrics{
		GetLatencyP50:         mc.getLatency.percentile(0.50),
		GetLatencyP90:         mc.getLatency.percentile(0.90),
		GetLatencyP99:         mc.getLatency.percentile(0.99),
		PutLatencyP50:         mc.putLatency.percentile(0.50),
		PutLatencyP90:         mc.putLatency.percentile(0.90),
		PutLatencyP99:         mc.putLatency.percentile(0.99),
		ScanLatencyP50:        mc.scanLatency.percentile(0.50),
		ScanLatencyP90:        mc.scanLatency.percentile(0.90),
		ScanLatencyP99:        mc.scanLatency.percentile(0.99),
		ReadThroughput:        mc.readWindow.rate(),
		WriteThroughput:       mc.writeWindow.rate(),
		ScanThroughput:        mc.scanWindow.rate(),
		TotalErrors:           uint64(mc.totalErrors.Load()),
		ErrorRate:             0, // computed below if needed
		CompactionRunning:      mc.compactionRunning.Load(),
		CompactionProgressPct: float64(mc.compactionProgress.Load()) / 1000.0,
		GCRunning:             mc.gcRunning.Load(),
		PageCacheUsed:         0, // TODO: wire from page store stats
		MemTableUsed:          0, // TODO: wire from LSM memtable stats
		WALSizeBytes:          mc.walSizeBytes.Load(),

		// Page operation stats from RealPageProvider.
		PageReads:             mc.pageReads.Load(),
		PageWrites:            mc.pageWrites.Load(),
		PageCacheHits:         mc.pageCacheHits.Load(),
		PageCacheMiss:         mc.pageReads.Load() - mc.pageCacheHits.Load(),
		PageSplits:            mc.pageSplits.Load(),
		PageAlloc:             mc.pageAlloc.Load(),

		// Page I/O latency.
		PageReadLatencyP99:    readLatencyP99Us,
		PageWriteLatencyP99:   writeLatencyP99Us,

		// B-tree traversal stats.
		BTreeSearchDepth:      btreeSearchDepth,
		RightSiblingTraversals: mc.rightSiblingNavs.Load(),
	}
}

// ─── Metrics ───────────────────────────────────────────────────────

// Metrics is an alias for the public API Metrics type.
