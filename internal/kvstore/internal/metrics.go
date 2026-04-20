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
// All fields are accessed via atomics from operation paths (Put/Get).
// GetMetrics() reads atomically — zero blocking.
type metricsCollector struct {
	// Latency rings
	getLatency latencyRing
	putLatency latencyRing

	// Throughput windows
	readWindow  throughputWindow
	writeWindow throughputWindow

	// Error counters
	totalErrors atomic.Int64

	// Sampling counters for latency rings
	getSampleCounter atomic.Uint64
	putSampleCounter atomic.Uint64

	// Background status (set by GC/compaction goroutines)
	gcRunning           atomic.Bool
	compactionRunning    atomic.Bool
	compactionProgress   atomic.Uint64 // packed: pct*1000 as uint64

	// Resource estimates (updated periodically by background tick)
	walSizeBytes atomic.Uint64
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

// collect returns a snapshot of all metrics.
func (mc *metricsCollector) collect() *kvstoreapi.Metrics {
	return &kvstoreapi.Metrics{
		GetLatencyP50:         mc.getLatency.percentile(0.50),
		GetLatencyP90:         mc.getLatency.percentile(0.90),
		GetLatencyP99:         mc.getLatency.percentile(0.99),
		PutLatencyP50:         mc.putLatency.percentile(0.50),
		PutLatencyP90:         mc.putLatency.percentile(0.90),
		PutLatencyP99:         mc.putLatency.percentile(0.99),
		ReadThroughput:        mc.readWindow.rate(),
		WriteThroughput:       mc.writeWindow.rate(),
		TotalErrors:           uint64(mc.totalErrors.Load()),
		ErrorRate:             0, // computed below if needed
		CompactionRunning:      mc.compactionRunning.Load(),
		CompactionProgressPct: float64(mc.compactionProgress.Load()) / 1000.0,
		GCRunning:             mc.gcRunning.Load(),
		PageCacheUsed:         0, // TODO: wire from page store stats
		MemTableUsed:          0, // TODO: wire from LSM memtable stats
		WALSizeBytes:          mc.walSizeBytes.Load(),
	}
}

// ─── Metrics ───────────────────────────────────────────────────────

// Metrics is an alias for the public API Metrics type.
