// Package pagestoreapi defines the interface for the PageStore,
// which provides fixed-size page storage with stable PageID addressing.
//
// PageStore maps PageID → VAddr (via a dense in-memory array) and
// delegates physical storage to the SegmentManager. All mapping
// changes are recorded in the shared WAL for crash recovery.
//
// Design reference: docs/DESIGN.md §3.2
package pagestoreapi

import (
	"errors"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrPageNotFound is returned when reading a PageID that has not
	// been allocated or has been freed.
	ErrPageNotFound = errors.New("pagestore: page not found")

	// ErrInvalidPageSize is returned when Write receives data whose
	// length is not exactly PageSize.
	ErrInvalidPageSize = errors.New("pagestore: data must be exactly PageSize bytes")

	// ErrClosed is returned when operating on a closed PageStore.
	ErrClosed = errors.New("pagestore: closed")

	// ErrChecksumMismatch is returned when a page record's CRC32 checksum
	// does not match the stored value. This indicates data corruption,
	// typically from a torn write (partial page flush to disk).
	ErrChecksumMismatch = errors.New("pagestore: checksum mismatch — data corruption detected")
)

// ─── Constants ──────────────────────────────────────────────────────

const (
	// PageSize is the fixed size of a page in bytes (4KB).
	PageSize = 4096

	// PageRecordSize is the size of a page record in a segment file:
	// 8 bytes pageID header + 4096 bytes page data + 4 bytes CRC32 = 4108 bytes.
	//
	// The CRC32 (IEEE polynomial) covers [pageID:8][data:4096] and is stored
	// as the last 4 bytes of the record in big-endian order.
	//
	// Design reference: docs/DESIGN.md §3.2, §3.7, §7.7
	PageRecordSize = 8 + PageSize + 4 // 4108
)

// ─── Types ──────────────────────────────────────────────────────────

// PageID uniquely identifies a page. PageIDs are monotonically
// increasing and never reused. PageID 0 is reserved (invalid).
type PageID = uint64

// ─── Interface ──────────────────────────────────────────────────────

// PageStore provides fixed-size page storage with stable PageID addressing.
//
// Mapping table: a dense []VAddr array indexed by PageID, providing O(1) lookup.
// All mapping changes go through the shared WAL for crash recovery.
//
// Thread safety: PageStore must be safe for concurrent use.
// The B-tree's per-page RwLock ensures that the same PageID is not
// written concurrently, so PageStore only needs a simple mutex to
// protect mapping table updates.
//
// Design reference: docs/DESIGN.md §3.2, §4 (why dense array, not LSM)
type PageStore interface {
	// Alloc allocates a new PageID. The page has no data until Write is called.
	// PageIDs are monotonically increasing and never reused.
	Alloc() PageID

	// Write writes page data for the given PageID.
	//
	// Internally:
	//   1. Prepends pageID (8 bytes big-endian) to data, appends CRC32 → 4108-byte record
	//   2. Appends record to page segment via SegmentManager
	//   3. Returns a WAL record (RecordPageMap) that the caller must
	//      include in their WAL batch for crash recovery
	//   4. Updates the in-memory mapping: mapping[pageID] = newVAddr
	//
	// The caller is responsible for:
	//   - Calling segment.Sync() before writing the WAL batch
	//   - Writing the WAL batch (which includes the returned WAL record)
	//   - The fsync ordering: segment.Sync → wal.WriteBatch → mapping update
	//
	// Note: In the current implementation, Write updates the mapping
	// immediately and returns the WAL record for the caller to batch.
	// This is safe because the B-tree's per-page WLock prevents
	// concurrent reads of the same page during a write.
	//
	// Returns ErrInvalidPageSize if len(data) != PageSize.
	Write(pageID PageID, data []byte) (WALEntry, error)

	// Read reads the page data for the given PageID.
	//
	// Returns exactly PageSize (4096) bytes — the pageID header is stripped.
	// Returns ErrPageNotFound if the page has not been allocated or was freed.
	Read(pageID PageID) ([]byte, error)

	// Free marks a PageID as freed. The mapping is cleared.
	//
	// Returns a WAL record (RecordPageFree) that the caller must
	// include in their WAL batch.
	//
	// The actual segment space is reclaimed by GC later.
	Free(pageID PageID) WALEntry

	// NextPageID returns the next PageID that will be allocated.
	// Useful for checkpoint serialization.
	NextPageID() PageID

	// Close closes the PageStore. After Close, all operations return ErrClosed.
	// Note: PageStore does NOT close the underlying SegmentManager or WAL —
	// those are owned by the caller.
	Close() error
}

// WALEntry represents a WAL record to be included in a WAL batch.
// The caller collects WALEntries from PageStore/BlobStore operations
// and writes them as a single atomic WAL batch.
type WALEntry struct {
	Type  uint8  // walapi.RecordType (1=PageMap, 4=PageFree)
	ID    uint64 // PageID
	VAddr uint64 // packed VAddr (for PageMap), 0 for PageFree
	Size  uint32 // always 0 for PageStore
}

// ─── Recovery ───────────────────────────────────────────────────────

// MappingEntry represents a single entry in the checkpoint's page mapping table.
type MappingEntry struct {
	PageID PageID
	VAddr  uint64 // packed VAddr
}

// Checkpoint data for PageStore recovery:
//   - Load checkpoint: restore mapping from []MappingEntry
//   - Replay WAL: apply RecordPageMap and RecordPageFree records
//
// These are used by the top-level recovery orchestrator (KVStore),
// not by PageStore itself. PageStore provides methods to apply them:

// PageStoreRecovery provides methods for crash recovery and checkpoint.
// Implemented by the same struct that implements PageStore.
type PageStoreRecovery interface {
	// LoadMapping bulk-loads the mapping table from checkpoint data.
	// Called once during recovery before WAL replay.
	LoadMapping(entries []MappingEntry)

	// ExportMapping returns all non-zero page mappings for checkpoint serialization.
	ExportMapping() []MappingEntry

	// ApplyPageMap applies a WAL RecordPageMap record during replay.
	ApplyPageMap(pageID PageID, vaddr uint64)

	// ApplyPageFree applies a WAL RecordPageFree record during replay.
	ApplyPageFree(pageID PageID)

	// SetNextPageID sets the next allocatable PageID.
	// Called during recovery after loading checkpoint.
	SetNextPageID(nextID PageID)

	// LSMLifecycle returns the LSM store for WAL replay routing.
	// Returns the underlying LSM so recovery.go can delegate ModuleLSM records.
	LSMLifecycle() LSMLifecycle
}

// LSMLifecycle represents the LSM store's recovery surface.
// Used by recovery.go to route ModuleLSM WAL records.
type LSMLifecycle interface {
	ApplyPageMapping(pageID uint64, vaddr uint64)
	ApplyPageDelete(pageID uint64)
	ApplyBlobMapping(blobID uint64, vaddr uint64, size uint32)
	ApplyBlobDelete(blobID uint64)
	SetCheckpointLSN(lsn uint64)
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the PageStore.
// PageStore requires an external SegmentManager instance (for page segments).
type Config struct {
	// InitialCapacity is the initial size of the mapping table (number of slots).
	// Defaults to 1024 if zero.
	// Design reference: docs/DESIGN.md §7.6
	InitialCapacity int

	// PageCacheSize is the number of pages to cache (LRU).
	// 0 or negative means no cache. Default is 0 (cache disabled).
	PageCacheSize int
}
