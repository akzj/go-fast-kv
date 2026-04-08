// Package gcapi defines the interface for the garbage collector (GC).
//
// GC reclaims space in sealed segment files by copying live data to the
// active segment and removing the old segment file. There are two
// independent GC processes: one for page segments and one for blob segments.
//
// GC operates at the physical storage layer — it does NOT touch B-tree
// entries (that's Vacuum's job). GC only cares about whether a VAddr
// in a segment is still referenced by the mapping table.
//
// Design reference: docs/DESIGN.md §3.7
package gcapi

import "errors"

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrNoSegmentsToGC is returned when there are no sealed segments
	// eligible for garbage collection.
	ErrNoSegmentsToGC = errors.New("gc: no sealed segments to collect")
)

// ─── Stats ──────────────────────────────────────────────────────────

// GCStats reports the results of a GC run.
type GCStats struct {
	SegmentID    uint32 // which segment was collected
	TotalRecords int    // total records scanned in the segment
	LiveRecords  int    // records still alive (copied to new segment)
	DeadRecords  int    // records no longer referenced (skipped)
	BytesFreed   int64  // approximate bytes freed
}

// ─── Interface ──────────────────────────────────────────────────────

// PageGC collects garbage from page segments.
//
// It scans a sealed page segment, checks each page's liveness against
// the PageStore mapping table, copies live pages to the active segment,
// updates the mapping, and removes the old segment.
//
// Thread safety: PageGC must be safe for concurrent use with PageStore
// reads/writes. The mapping updates must go through WAL for crash safety.
//
// Design reference: docs/DESIGN.md §3.7 (Page GC)
type PageGC interface {
	// CollectOne selects the best candidate sealed page segment and
	// collects it. Returns stats about what was collected.
	//
	// "Best candidate" = the sealed segment with the highest estimated
	// dead record ratio. A simple heuristic: pick any sealed segment.
	//
	// Returns ErrNoSegmentsToGC if no sealed segments exist.
	CollectOne() (*GCStats, error)
}

// BlobGC collects garbage from blob segments.
//
// Similar to PageGC but handles variable-length blob records.
//
// Design reference: docs/DESIGN.md §3.7 (Blob GC)
type BlobGC interface {
	// CollectOne selects the best candidate sealed blob segment and
	// collects it. Returns stats about what was collected.
	//
	// Returns ErrNoSegmentsToGC if no sealed segments exist.
	CollectOne() (*GCStats, error)
}
