// Package blobstoreapi defines the interface for the BlobStore,
// which provides variable-length blob storage with stable BlobID addressing.
//
// BlobStore maps BlobID → (VAddr, Size) via a dense in-memory array and
// delegates physical storage to the SegmentManager. All mapping changes
// are recorded in the shared WAL for crash recovery.
//
// Design reference: docs/DESIGN.md §3.3
package blobstoreapi

import (
	"errors"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrBlobNotFound is returned when reading a BlobID that has not
	// been allocated or has been deleted.
	ErrBlobNotFound = errors.New("blobstore: blob not found")

	// ErrClosed is returned when operating on a closed BlobStore.
	ErrClosed = errors.New("blobstore: closed")

	// ErrChecksumMismatch is returned when a blob's CRC32 checksum
	// does not match the stored value, indicating data corruption.
	ErrChecksumMismatch = errors.New("blobstore: checksum mismatch")
)

// ─── Types ──────────────────────────────────────────────────────────

// BlobID uniquely identifies a blob. BlobIDs are monotonically
// increasing and never reused. BlobID 0 is reserved (invalid).
type BlobID = uint64

// BlobMeta stores the location and size of a blob in the segment file.
type BlobMeta struct {
	VAddr uint64 // packed VAddr (segmentID<<32 | offset)
	Size  uint32 // blob data size (excluding headers)
}

// IsZero returns true if this BlobMeta is the zero value (invalid/deleted).
func (m BlobMeta) IsZero() bool {
	return m.VAddr == 0 && m.Size == 0
}

// ─── Interface ──────────────────────────────────────────────────────

// BlobStore provides variable-length blob storage with stable BlobID addressing.
//
// Mapping table: a dense []BlobMeta array indexed by BlobID, providing O(1) lookup.
// All mapping changes go through the shared WAL for crash recovery.
//
// Segment record format (per blob):
//
//	[0:8]    uint64  blobID    (big-endian)
//	[8:12]   uint32  size      (big-endian)
//	[12:12+size]     blob data
//
// Total record size: 12 + size bytes.
//
// Thread safety: BlobStore must be safe for concurrent use.
//
// Design reference: docs/DESIGN.md §3.3, §3.7 (GC format), §7.7
type BlobStore interface {
	// Write allocates a new BlobID and writes the blob data.
	//
	// Internally:
	//   1. Allocates a new BlobID
	//   2. Prepends blobID (8 bytes) + size (4 bytes) to data → record
	//   3. Appends record to blob segment via SegmentManager
	//   4. Updates the in-memory mapping: mapping[blobID] = {vaddr, size}
	//   5. Returns the BlobID and a WAL record (RecordBlobMap)
	//
	// The caller is responsible for including the WAL record in their
	// WAL batch and following the fsync ordering.
	Write(data []byte) (BlobID, WALEntry, error)

	// Read reads the blob data for the given BlobID.
	//
	// Returns the raw blob data (without blobID/size headers).
	// Returns ErrBlobNotFound if the blob has not been allocated or was deleted.
	Read(blobID BlobID) ([]byte, error)

	// Delete marks a BlobID as deleted. The mapping is cleared.
	//
	// Returns a WAL record (RecordBlobFree) that the caller must
	// include in their WAL batch.
	//
	// The actual segment space is reclaimed by GC later.
	Delete(blobID BlobID) WALEntry

	// NextBlobID returns the next BlobID that will be allocated.
	// Useful for checkpoint serialization.
	NextBlobID() BlobID

	// GetMeta returns the metadata for a blobID if it exists, or zero BlobMeta otherwise.
	// Used by GC for CAS checks during mapping updates.
	GetMeta(blobID BlobID) BlobMeta

	// CompareAndSetBlobMapping atomically sets a blob mapping only if the current
	// value equals expectedVAddr and expectedSize. Returns true if the update was applied.
	// Used by GC for race-free mapping updates.
	CompareAndSetBlobMapping(blobID uint64, expectedVAddr uint64, expectedSize uint32, newVAddr uint64, newSize uint32) bool

	// ExportMapping returns all non-zero blob mappings for checkpoint serialization.
	ExportMapping() []MappingEntry

	// GetSnapshotMappings returns a COW copy of all non-zero blob mappings.
	// Used by the checkpoint goroutine to capture state without blocking user operations.
	// Takes a short write lock (~10ns) to swap the pointer. No user operation is blocked.
	GetSnapshotMappings() []MappingEntry

	// Close closes the BlobStore. After Close, all operations return ErrClosed.
	// Note: BlobStore does NOT close the underlying SegmentManager —
	// that is owned by the caller.
	Close() error
}

// WALEntry represents a WAL record to be included in a WAL batch.
type WALEntry struct {
	Type  uint8  // walapi.RecordType (2=BlobMap, 3=BlobFree)
	ID    uint64 // BlobID
	VAddr uint64 // packed VAddr (for BlobMap), 0 for BlobFree
	Size  uint32 // blob data size (for BlobMap), 0 for BlobFree
}

// ─── Recovery ───────────────────────────────────────────────────────

// MappingEntry represents a single entry in the checkpoint's blob mapping table.
type MappingEntry struct {
	BlobID BlobID
	VAddr  uint64 // packed VAddr
	Size   uint32
}

// BlobStoreRecovery provides methods for crash recovery and checkpoint.
// Implemented by the same struct that implements BlobStore.
type BlobStoreRecovery interface {
	// LoadMapping bulk-loads the mapping table from checkpoint data.
	LoadMapping(entries []MappingEntry)

	// ApplyBlobMap applies a WAL RecordBlobMap record during replay.
	ApplyBlobMap(blobID BlobID, vaddr uint64, size uint32)

	// ApplyBlobFree applies a WAL RecordBlobFree record during replay.
	ApplyBlobFree(blobID BlobID)

	// SetNextBlobID sets the next allocatable BlobID.
	SetNextBlobID(nextID BlobID)
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the BlobStore.
type Config struct {
	// InitialCapacity is the initial size of the mapping table (number of slots).
	// Defaults to 1024 if zero.
	InitialCapacity int

	// StatsManager is an optional stats tracker for GC segment dead-byte analysis.
	// If non-nil, BlobStore will call Increment/Decrement on writes and deletes.
	StatsManager interface {
		Increment(segID uint32, count, bytes int64)
		Decrement(segID uint32, count, bytes int64)
	}
}

// StatsManager is the interface for segment-level live-record statistics.
// Implemented by kvstore's segmentStatsManager; passed to BlobStore.New.
type StatsManager interface {
	Increment(segID uint32, count, bytes int64)
	Decrement(segID uint32, count, bytes int64)
}
