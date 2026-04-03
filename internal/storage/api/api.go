// Package storage provides segment lifecycle management for append-only storage.
// This module manages segment files (Active → Sealed → Archived) and provides
// low-level file I/O operations.
//
// Architecture:
//   - storage is the foundation: all other modules depend on it
//   - SegmentManager coordinates segment lifecycle
//   - FileOperations abstracts OS-level file I/O (testable, mockable)
//
// Design invariants:
//   - Exactly one segment is Active at any time
//   - Segment state transitions: Active → Sealed → Archived (never backwards)
//   - Segment IDs are monotonically increasing
//   - All writes are append-only (no in-place modification)
//
// Module boundaries:
//   - storage has NO dependencies on other internal packages
//   - Other modules depend on storage via interfaces defined here
package storage

import (
    "errors"

    vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// FileType - File Classification
// =============================================================================

// FileType categorizes files for cache strategy assignment.
// Different file types may use different access patterns (buffered, direct I/O, mmap).
type FileType uint8

const (
    // FileTypeWAL is the Write-Ahead Log file.
    FileTypeWAL FileType = iota

    // FileTypeSegment is a data segment file.
    FileTypeSegment

    // FileTypeExternalValue is an external value store file.
    FileTypeExternalValue

    // FileTypeIndex is a page manager index file.
    FileTypeIndex

    // FileTypeCheckpoint is a checkpoint file.
    FileTypeCheckpoint
)

// =============================================================================
// FileOperations - Low-Level I/O
// =============================================================================

// FileOperations provides low-level file I/O.
// Why separate interface?
//   - Allows mock implementations for unit testing
//   - Enables different I/O strategies (buffered, direct I/O, mmap)
//   - Decouples from OS-specific file operations
type FileOperations interface {
    // Open opens or creates a file.
    // Returns error if file cannot be opened.
    Open(path string) error

    // Close closes the file.
    // Invariant: Close is idempotent.
    Close() error

    // ReadAt reads len(p) bytes into p starting at offset.
    // Returns (n, nil) on success, (0, error) on failure.
    // Invariant: offset >= 0.
    ReadAt(p []byte, offset int64) (int, error)

    // WriteAt writes len(p) bytes from p starting at offset.
    // Returns (n, nil) on success, (0, error) on failure.
    // Invariant: offset >= 0.
    WriteAt(p []byte, offset int64) (int, error)

    // Sync flushes writes to durable storage.
    // Invariant: After Sync returns, all prior writes are durable.
    Sync() error

    // Size returns the current file size in bytes.
    Size() (int64, error)

    // Truncate changes the file size to exactly size bytes.
    // If size < current size, data is truncated.
    // If size > current size, data is extended with zeros.
    Truncate(size int64) error

    // Path returns the file path.
    Path() string
}

// =============================================================================
// Segment - Single Segment File
// =============================================================================

// Segment represents a single segment file.
// Invariant: Segment state never changes after Sealed or Archived.
type Segment interface {
    // ID returns the segment's unique identifier.
    // Invariant: ID > 0.
    ID() vaddr.SegmentID

    // State returns the current segment state.
    State() vaddr.SegmentState

    // File returns the underlying file operations.
    File() FileOperations

    // Append appends data to the segment.
    // Returns the VAddr where data was written.
    //
    // Invariant: Segment must be Active.
    // Invariant: Returned Offset is monotonically increasing within segment.
    // Invariant: Data is aligned to PageSize boundary.
    Append(data []byte) (vaddr.VAddr, error)

    // ReadAt reads data at the given offset.
    // Returns the data read.
    //
    // Invariant: offset + length must be within segment bounds.
    // Invariant: Segment can be read in any state (Active, Sealed, Archived).
    ReadAt(offset int64, length int) ([]byte, error)

    // Size returns current data size (excluding header/trailer).
    Size() int64

    // PageCount returns the number of pages in this segment.
    PageCount() uint64

    // Sync ensures all writes to this segment are durable.
    Sync() error

    // Close closes the segment file.
    // Invariant: Close is idempotent.
    Close() error
}

// =============================================================================
// SegmentManager - Segment Lifecycle Coordination
// =============================================================================

// SegmentManager manages segment lifecycle (Active → Sealed → Archived).
// Invariant: Exactly one segment is Active at any time.
// Invariant: Segment IDs are monotonically increasing.
type SegmentManager interface {
    // Directory returns the storage directory path.
    Directory() string
    // ActiveSegment returns the current active segment for writing.
    // Returns nil if no active segment (store closed or during recovery).
    // Invariant: Returned segment State() == SegmentStateActive.
    ActiveSegment() Segment

    // GetSegment returns segment by ID.
    // Returns nil if segment doesn't exist.
    GetSegment(id vaddr.SegmentID) Segment

    // CreateSegment creates a new active segment.
    // The old active segment is sealed first (if any).
    //
    // Invariant: New segment ID > all existing segment IDs.
    // Invariant: Only one active segment at a time.
    // Invariant: Returns error if directory cannot be created or file cannot be opened.
    CreateSegment() (Segment, error)

    // SealSegment marks a segment as sealed (no new writes).
    // Invariant: Segment must be Active.
    // Invariant: After Seal, segment transitions to Sealed state.
    // Invariant: Returns error if segment doesn't exist or is not Active.
    SealSegment(id vaddr.SegmentID) error

    // ArchiveSegment marks a segment as archived (read-only, may be compacted).
    // Invariant: Segment must be Sealed.
    // Invariant: After Archive, segment transitions to Archived state.
    ArchiveSegment(id vaddr.SegmentID) error

    // ListSegments returns all segments in ID order.
    // Used for compaction, recovery, and debugging.
    ListSegments() []Segment

    // ListSegmentsByState returns segments filtered by state.
    ListSegmentsByState(state vaddr.SegmentState) []Segment

    // SegmentCount returns the total number of segments.
    SegmentCount() int

    // ActiveSegmentCount returns the number of active segments.
    ActiveSegmentCount() int

    // TotalSize returns the total bytes across all segments.
    TotalSize() int64

    // Close releases all resources held by the segment manager.
    // All segments are closed.
    // Invariant: Close is idempotent.
    Close() error
}

// =============================================================================
// StorageConfig
// =============================================================================

// StorageConfig holds initialization parameters.
type StorageConfig struct {
    // Directory is the root directory for storage files.
    Directory string

    // SegmentSize is the target size for segment rotation.
    // When active segment exceeds this, a new segment is created.
    // Default: 1 GB (1 << 30)
    SegmentSize uint64

    // MaxSegmentCount limits the number of segments.
    // Prevents unbounded disk usage.
    // Default: 0 (unlimited)
    MaxSegmentCount int

    // FileType is the type of files this storage manages.
    // Used for cache strategy assignment.
    FileType FileType
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() StorageConfig {
    return StorageConfig{
        SegmentSize: 1 << 30, // 1 GB
        MaxSegmentCount: 0,  // Unlimited
        FileType: FileTypeSegment,
    }
}

// =============================================================================
// Errors
// =============================================================================

var (
    // ErrSegmentNotFound is returned when segment ID doesn't exist.
    ErrSegmentNotFound = errors.New("storage: segment not found")

    // ErrSegmentNotActive is returned when operation requires Active segment.
    ErrSegmentNotActive = errors.New("storage: segment is not active")

    // ErrSegmentNotSealed is returned when operation requires Sealed segment.
    ErrSegmentNotSealed = errors.New("storage: segment is not sealed")

    // ErrSegmentFull is returned when segment reaches max size.
    ErrSegmentFull = errors.New("storage: segment is full")

    // ErrMaxSegments is returned when MaxSegmentCount limit is reached.
    ErrMaxSegments = errors.New("storage: maximum segment count reached")

    // ErrStorageClosed is returned when storage is closed.
    ErrStorageClosed = errors.New("storage: storage is closed")

    // ErrInvalidOffset is returned when offset is invalid.
    ErrInvalidOffset = errors.New("storage: invalid offset")

    // ErrInvalidSegmentID is returned when segment ID is invalid.
    ErrInvalidSegmentID = errors.New("storage: invalid segment ID")
)

// =============================================================================
// Factory Functions
// =============================================================================

// OpenSegmentManager opens or creates a segment manager.
func OpenSegmentManager(config StorageConfig) (SegmentManager, error) {
    panic("TODO: implementation provided by branch")
}

// NewSegment opens an existing segment file (for testing).
func NewSegment(id vaddr.SegmentID, file FileOperations) Segment {
    panic("TODO: implementation provided by branch")
}
