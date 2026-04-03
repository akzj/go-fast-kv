// Package api provides the public interfaces for gc module.
// This package contains ONLY interfaces, types, and constants - NO implementation.
// Invariant: Any concrete implementation MUST live in the internal/ package.
package api

// =============================================================================
// Types
// =============================================================================

// GCPolicy contains GC policy configuration.
type GCPolicy struct {
	// PageSegmentThreshold is the garbage ratio threshold for Page Segments (40%).
	// Garbage ratio = deleted object size / segment total size.
	PageSegmentThreshold float64

	// BlobSegmentThreshold is the garbage ratio threshold for Blob Segments (50%).
	BlobSegmentThreshold float64

	// BlobSegmentModRateThreshold is the modification rate threshold for Blob Segments.
	// Used to determine if Blob Segment modification rate is stable.
	BlobSegmentModRateThreshold float64

	// CheckIntervalMs is the GC check interval in milliseconds.
	CheckIntervalMs uint64
}

// DefaultGCPolicy returns the default GC policy.
func DefaultGCPolicy() GCPolicy {
	return GCPolicy{
		PageSegmentThreshold:       0.40,  // 40%
		BlobSegmentThreshold:       0.50,  // 50%
		BlobSegmentModRateThreshold: 0.10,  // 10%
		CheckIntervalMs:            1000,  // 1 second
	}
}

// GCStats contains GC statistics.
type GCStats struct {
	TotalCompactions    uint64
	TotalBytesFreed     uint64
	TotalBytesMoved     uint64
	LastCompactionTime  int64
	SegmentsUnderGC     uint64
}

// SegmentGCState contains the GC state of a segment.
type SegmentGCState struct {
	SegmentID        uint64
	SegmentType      uint8 // 0=Page, 1=Blob, 2=Large
	GarbageRatio     float64
	ModificationRate float64 // Modification rate per unit time
	LastCheckTime    int64
	ShouldCompact    bool
}

// ModRateStabilityChecker monitors modification rate stability.
// Uses a sliding window to track historical modification rates.
type ModRateStabilityChecker struct {
	// WindowSize is the sliding window size (sample count).
	WindowSize int
	// Samples stores modification rate samples.
	Samples []float64
	// Threshold is the stability threshold (variance).
	Threshold float64
}

// =============================================================================
// Interfaces
// =============================================================================

// GCController is the GC controller interface.
type GCController interface {
	// ShouldCompactPageSegment determines if a Page Segment should be compacted.
	// Condition: garbage ratio > threshold.
	ShouldCompactPageSegment(segmentID uint64, garbageRatio float64) bool

	// ShouldCompactBlobSegment determines if a Blob Segment should be compacted.
	// Condition: garbage ratio > threshold AND modification rate < threshold.
	// Why both? Blob Segments are large; frequent compaction is expensive, needs stability check.
	ShouldCompactBlobSegment(segmentID uint64, garbageRatio float64, modRate float64) bool

	// OnDeleteLargeBlob callback when a Large Blob is deleted.
	// Directly deletes the file, no GC needed.
	OnDeleteLargeBlob(segmentID uint64) error

	// GetStats returns GC statistics.
	GetStats() GCStats
}
