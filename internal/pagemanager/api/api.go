// Package pagemanager provides PageID → VAddr mapping and page allocation.
//
// This is the API layer contract. Only interfaces and types defined here
// are public. All implementation is in internal/.
package pagemanager

import "errors"

// =============================================================================
// Core Types
// =============================================================================

// PageID is a logical identifier for a page.
// Invariant: PageID > 0 (0 is reserved for invalid/null).
// Why uint64? Allows up to 2^64-1 pages; fits in fixed-size index entry.
type PageID uint64

const (
	// PageIDInvalid is the reserved value for no page.
	// Invariant: No valid page has PageID 0.
	PageIDInvalid PageID = 0
)

// PageManagerIndexEntry is a fixed-size key-value pair for the mapping index.
// Invariant: Entry is always 24 bytes (8 + 16), enabling dense array storage.
// Why not a struct with methods? Entry is raw memory-mapped; methods inappropriate.
type PageManagerIndexEntry struct {
	PageID PageID  // 8 bytes
	VAddr  [16]byte // 16 bytes: SegmentID[8] + Offset[8], big-endian
}

// IndexType identifies the underlying index structure.
type IndexType uint8

const (
	// IndexTypeDenseArray uses O(1) array indexing.
	// Best when PageIDs are densely allocated (sequential allocation).
	// Why DenseArray default? O(1) lookup is critical for VAddr resolution
	// in the OS Page Cache path. Simpler than Radix Tree with less overhead.
	IndexTypeDenseArray IndexType = iota

	// IndexTypeRadixTree uses O(k=4) radix tree with 16-bit splits.
	// Best when PageIDs are sparse or non-sequential.
	IndexTypeRadixTree
)

// PageManagerConfig holds initialization parameters.
type PageManagerConfig struct {
	InitialPageCount uint64  // Hints at initial allocation (default: 1024)
	GrowFactor       float64 // Exponential growth factor (default: 1.5)
	IndexType        IndexType // DenseArray or RadixTree (default: DenseArray)
}

// =============================================================================
// Interfaces
// =============================================================================

// FixedSizeKVIndex is the underlying index for page_id → vaddr mapping.
// Invariant: All entries are fixed-size (24 bytes), enabling dense array storage.
// Why not a generic map? Generic maps have ~32 bytes overhead per entry;
// fixed-size entries allow memory-mapped dense arrays with O(1) lookup.
type FixedSizeKVIndex interface {
	// Get returns the VAddr for key, or zero VAddr if not present.
	// Why [16]byte return? VAddr is 16 bytes; avoid importing vaddr package
	// to keep this API self-contained and mockable.
	Get(key PageID) [16]byte

	// Put stores the mapping key → value.
	// Invariant: Put is idempotent for the same key.
	Put(key PageID, value [16]byte)

	// Len returns the number of entries in the index.
	Len() uint64

	// RangeQuery returns all entries where start <= pageID < end.
	// Why inclusive start, exclusive end? Standard Go slice convention.
	RangeQuery(start, end PageID) []PageManagerIndexEntry

	// ByteSize returns the memory footprint of the index.
	ByteSize() uint64
}

// FreeList manages reclaimable pages for reuse.
// Invariant: Freed pages are reused before allocating new PageIDs.
// Design: Lock-free stack using atomic CAS operations.
type FreeList interface {
	// Pop returns a freed PageID or false if the list is empty.
	// Invariant: Pop returns PageID > 0.
	Pop() (PageID, bool)

	// Push adds a PageID to the free list.
	// Invariant: Push is idempotent (caller ensures no double-free).
	Push(pageID PageID)

	// Len returns the number of pages on the free list.
	Len() uint64

	// Clear removes all entries from the free list.
	// Why Clear? Used during recovery to rebuild clean state.
	Clear()
}

// PageManager maps PageID → VAddr and manages page allocation.
// Invariant: All mutations are append-only; old entries are tombstoned.
// Why append-only? Enables consistent snapshots and crash recovery.
type PageManager interface {
	// GetVAddr returns the VAddr for pageID, or zero VAddr if not allocated.
	// Invariant: Returns consistent result until page is freed.
	GetVAddr(pageID PageID) [16]byte

	// AllocatePage allocates a new page and returns its PageID and VAddr.
	// Invariant: Returned PageID is monotonically increasing (when not reusing).
	// Invariant: Returned VAddr is unique and never reused.
	AllocatePage() (PageID, [16]byte)

	// FreePage marks a page as reclaimable.
	// Invariant: FreePage is idempotent (safe to call multiple times).
	// Post-condition: GetVAddr(pageID) returns zero VAddr.
	FreePage(pageID PageID)

	// UpdateMapping records that pageID now lives at vaddr.
	// Why needed? B-link tree splits may relocate pages.
	// Invariant: vaddr must be valid (non-zero).
	UpdateMapping(pageID PageID, vaddr [16]byte)

	// PageCount returns the total number of allocated pages.
	// Does not subtract freed pages.
	PageCount() uint64

	// LivePageCount returns pages that have valid (non-zero) VAddrs.
	LivePageCount() uint64

	// Iter calls fn for each page_id → vaddr mapping.
	// Iteration order is implementation-defined.
	// Why callback? Avoids allocating iterator object.
	Iter(fn func(pageID PageID, vaddr [16]byte))

	// Flush ensures durable storage of the index.
	// Invariant: After Flush returns, all prior operations are durable.
	Flush() error

	// Close releases resources held by the PageManager.
	Close() error
}

// =============================================================================
// Errors
// =============================================================================

// Errors returned by PageManager operations.
var (
	// ErrPageNotFound is returned when a pageID has no mapping.
	ErrPageNotFound = errors.New("page not found")

	// ErrInvalidVAddr is returned when a VAddr is invalid (zero).
	ErrInvalidVAddr = errors.New("invalid VAddr")

	// ErrIndexFull is returned when the index cannot accommodate more entries.
	// Only applies to fixed-capacity indexes like DenseArray.
	ErrIndexFull = errors.New("index is full")

	// ErrClosed is returned when the PageManager has been closed.
	ErrClosed = errors.New("page manager is closed")
)

// =============================================================================
// Internal Types (for serialization only)
// =============================================================================

// These types exist for wire/file format compatibility. They are not part of
// the public interface and should not be used directly by callers.

// DenseArrayHeader is the on-disk header for DenseArray index.
// Layout must match file format documented in fixed-size-kvindex-persist.md.
type DenseArrayHeader struct {
	Magic          [8]byte // "DAIDX\0\0\0"
	Version        uint16
	IndexType      uint8 // Always IndexTypeDenseArray
	CheckpointLSN  uint64
	PageIDBase     uint64 // First PageID in this array
	EntryCount     uint64
	LiveEntryCount uint64
	ArrayCapacity  uint64
	_              [14]byte // Reserved for alignment
}

// Why not embed in struct? Header has specific alignment requirements
// that may differ from Go's memory layout rules.

// RadixTreeManifestEntry is stored in the index manifest.
type RadixTreeManifestEntry struct {
	IndexType     IndexType // Must be IndexTypeRadixTree
	RootVAddr     [16]byte  // VAddr of root RadixNode
	NodeCount     uint64    // Total nodes in tree
	CheckpointLSN uint64   // LSN of checkpoint
	Height        uint8     // Tree height (typically 4)
}
