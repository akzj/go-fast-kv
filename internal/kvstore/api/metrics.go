// Package kvstoreapi defines the public interface for go-fast-kv.
package kvstoreapi

// Metrics holds operational statistics for monitoring.
//
// All fields are populated atomically — GetMetrics() returns instantly
// with no blocking. Latency percentiles are calculated from a fixed-size
// ring buffer sampled at ~10ns overhead per Put/Get operation.
type Metrics struct {
	// Latency percentiles in microseconds.
	GetLatencyP50  float64
	GetLatencyP90  float64
	GetLatencyP99  float64
	PutLatencyP50  float64
	PutLatencyP90  float64
	PutLatencyP99  float64
	ScanLatencyP50 float64
	ScanLatencyP90 float64
	ScanLatencyP99 float64

	// Throughput in operations/sec, measured over a rolling 10-second window.
	ReadThroughput  uint64
	WriteThroughput uint64
	ScanThroughput  uint64

	// Error tracking.
	TotalErrors uint64
	ErrorRate   float64 // errors per second over the rolling window

	// Background operations status.
	CompactionRunning     bool
	CompactionProgressPct float64 // 0.0–100.0
	GCRunning             bool

	// Resource usage (approximate bytes).
	PageCacheUsed uint64
	MemTableUsed  uint64
	WALSizeBytes  uint64

	// Page-level operation statistics for B-tree analysis.
	// These help identify query path issues (e.g., sequential scan vs hierarchical lookup).
	PageReads     uint64 // Total page read operations (from disk or cache)
	PageWrites    uint64 // Total page write operations
	PageCacheHits uint64 // Page cache hits (LRU cache)
	PageCacheMiss uint64 // Page cache misses (required disk I/O)
	PageSplits    uint64 // Leaf node split count (high value → frequent splits → bottleneck)
	PageAlloc     uint64 // Page allocation count

	// Page I/O latency in microseconds (99th percentile).
	// High values indicate disk I/O bottleneck.
	PageReadLatencyP99  uint64
	PageWriteLatencyP99 uint64

	// B-tree traversal statistics.
	BTreeSearchDepth       uint64 // Average search depth (higher = deeper tree = more I/O per operation)
	RightSiblingTraversals uint64 // B-link correction traversals (high value = many splits during insert)
}
