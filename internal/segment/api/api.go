// Package segmentapi defines the interface for the Segment Manager,
// the lowest storage layer in go-fast-kv.
//
// The Segment Manager provides append-only writes and random reads
// over a set of segment files. It does not know about pages or blobs —
// it only sees raw []byte data.
//
// Design reference: docs/DESIGN.md §3.1
package segmentapi

import (
	"errors"
	"fmt"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrSegmentFull is returned by Append when the active segment has
	// reached MaxSegmentSize. The caller should call Rotate() first.
	ErrSegmentFull = errors.New("segment: active segment is full")

	// ErrInvalidVAddr is returned by ReadAt when the VAddr points to
	// a non-existent segment or an out-of-bounds offset.
	ErrInvalidVAddr = errors.New("segment: invalid vaddr")

	// ErrSegmentSealed is returned when attempting to write to a sealed segment.
	ErrSegmentSealed = errors.New("segment: segment is sealed")

	// ErrClosed is returned when operating on a closed SegmentManager.
	ErrClosed            = errors.New("segment: manager is closed")
	ErrMaxSizeOverflow   = errors.New("segment: MaxSize exceeds uint32 limit (4GB per segment)")
)

// ─── Types ──────────────────────────────────────────────────────────

// VAddr (Virtual Address) uniquely identifies a position in the segment files.
// It consists of a SegmentID (which file) and an Offset (position within the file).
//
// VAddr points to the first byte of the stored record, including any ID headers
// prepended by the caller (PageStore prepends pageID, BlobStore prepends blobID).
// The Segment Manager itself does not interpret the data — it stores raw bytes.
//
// Encoding: 8 bytes total — SegmentID in upper 32 bits, Offset in lower 32 bits.
type VAddr struct {
	SegmentID uint32
	Offset    uint32
}

// Pack encodes VAddr into a uint64 for storage in mapping tables.
//
//	packed = (uint64(SegmentID) << 32) | uint64(Offset)
func (v VAddr) Pack() uint64 {
	return (uint64(v.SegmentID) << 32) | uint64(v.Offset)
}

// UnpackVAddr decodes a uint64 back into a VAddr.
func UnpackVAddr(packed uint64) VAddr {
	return VAddr{
		SegmentID: uint32(packed >> 32),
		Offset:    uint32(packed),
	}
}

// IsZero returns true if this VAddr is the zero value (invalid).
func (v VAddr) IsZero() bool {
	return v.SegmentID == 0 && v.Offset == 0
}

// String returns a human-readable representation: "seg:0003/off:00001024".
func (v VAddr) String() string {
	return fmt.Sprintf("seg:%04d/off:%08d", v.SegmentID, v.Offset)
}

// ─── Constants ──────────────────────────────────────────────────────

const (
	// MaxSegmentSize is the maximum size of a single segment file (64MB).
	// When the active segment reaches this size, it is sealed and a new
	// segment is opened via Rotate().
	MaxSegmentSize = 64 * 1024 * 1024 // 64 MB
)

// ─── Interface ──────────────────────────────────────────────────────

// SegmentManager manages append-only segment files on disk.
//
// Each segment file is named "data/{segmentID}.seg" and grows up to
// MaxSegmentSize bytes. Once sealed, a segment is immutable (read-only)
// until explicitly removed by GC.
//
// Thread safety: SegmentManager must be safe for concurrent use.
// Multiple goroutines may call ReadAt concurrently. Append and Rotate
// must be serialized by the caller (typically via WAL batch locking).
//
// Design reference: docs/DESIGN.md §3.1, §7.5 (segment size = 64MB)
type SegmentManager interface {
	// Append writes data to the active segment and returns its VAddr.
	//
	// The data is written as-is — the Segment Manager does not add any
	// headers or framing. The caller (PageStore/BlobStore) is responsible
	// for prepending ID headers before calling Append.
	//
	// Returns ErrSegmentFull if len(data) would exceed MaxSegmentSize
	// for the active segment. The caller should call Rotate() and retry.
	//
	// The data is written to the OS buffer. Call Sync() to ensure
	// durability before acknowledging to the WAL.
	Append(data []byte) (VAddr, error)

	// ReadAt reads exactly `size` bytes starting at the given VAddr.
	//
	// Returns ErrInvalidVAddr if:
	//   - The segment file for addr.SegmentID does not exist
	//   - addr.Offset + size exceeds the segment's written length
	//
	// ReadAt is safe for concurrent use by multiple goroutines.
	ReadAt(addr VAddr, size uint32) ([]byte, error)

	// Sync flushes the active segment's data to durable storage (fsync).
	//
	// Must be called after Append and before the corresponding WAL
	// record is written, to ensure the fsync ordering:
	//   segment.Append → segment.Sync → wal.Append → wal.Sync
	//
	// Design reference: docs/DESIGN.md §3.6 (fsync ordering)
	Sync() error

	// Rotate seals the current active segment and opens a new one.
	//
	// After Rotate, the sealed segment becomes read-only. New Append
	// calls write to the newly created segment.
	//
	// The new segment's ID is the previous segment's ID + 1.
	Rotate() error

	// RemoveSegment deletes a sealed segment file from disk.
	//
	// Used by GC after all live data has been copied out of the segment.
	// Attempting to remove the active (unsealed) segment is an error.
	//
	// After removal, any ReadAt to this segment returns ErrInvalidVAddr.
	RemoveSegment(segID uint32) error

	// ActiveSegmentID returns the ID of the current active (writable) segment.
	ActiveSegmentID() uint32

	// SegmentSize returns the total size in bytes of the segment with the
	// given ID. Works for both sealed and active segments.
	// Returns ErrInvalidVAddr if the segment does not exist.
	//
	// Used by GC to iterate through all records in a sealed segment.
	SegmentSize(segID uint32) (int64, error)

	// SealedSegments returns the IDs of all sealed (read-only) segments,
	// sorted in ascending order. Used by GC to select candidates.
	SealedSegments() []uint32

	// Close flushes and closes all segment files.
	// After Close, all operations return ErrClosed.
	Close() error
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the SegmentManager.
type Config struct {
	// Dir is the directory where segment files are stored.
	// Segment files are named "{segmentID}.seg" within this directory.
	// The directory is created if it does not exist.
	Dir string

	// MaxSize is the maximum size of a single segment file in bytes.
	// Defaults to MaxSegmentSize (64MB) if zero.
	MaxSize int64
}
