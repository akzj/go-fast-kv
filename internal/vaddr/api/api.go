// Package vaddr provides foundation types for the append-only storage system.
// This package has NO dependencies on other internal packages.
//
// Design invariants:
//   - VAddr is 16 bytes, never zero (SegmentID 0 is reserved)
//   - SegmentID is monotonically increasing (new > old)
//   - Offset is always aligned to PageSize boundaries
//
// Why this package exists:
//   Foundation types must be shared by all modules without circular dependencies.
//   Placing them in a dedicated package avoids import cycles.
package vaddr

// =============================================================================
// VAddr - Virtual Address
// =============================================================================

// VAddr encodes a physical address in the append-only address space.
// Invariant: VAddr is 16 bytes, never zero (SegmentID 0 is reserved).
//
// Why 16 bytes?
//   - 8 bytes SegmentID: allows up to 2^64-1 segments
//   - 8 bytes Offset: allows up to 2^64-1 bytes per segment
//
// Why big-endian for serialization?
//   Natural byte ordering for segment comparisons; simplifies debug output.
type VAddr struct {
    SegmentID uint64  // Identifies the segment file (1..N, 0 is invalid)
    Offset    uint64  // Byte offset within the segment
}

// IsValid returns true if this VAddr represents a valid address.
func (v VAddr) IsValid() bool {
    return v.SegmentID != 0
}

// IsZero returns true if both fields are zero.
func (v VAddr) IsZero() bool {
    return v.SegmentID == 0 && v.Offset == 0
}

// ToBytes returns the 16-byte big-endian representation.
// Why big-endian? SegmentID comparison is more natural (higher segments = later bytes).
func (v VAddr) ToBytes() [16]byte {
    var b [16]byte
    for i := 0; i < 8; i++ {
        b[i] = byte(v.SegmentID >> (56 - 8*i))
        b[8+i] = byte(v.Offset >> (56 - 8*i))
    }
    return b
}

// VAddrFromBytes decodes a 16-byte big-endian VAddr.
// Panics if b is not exactly 16 bytes.
func VAddrFromBytes(b [16]byte) VAddr {
    var segID, offset uint64
    for i := 0; i < 8; i++ {
        segID = segID<<8 | uint64(b[i])
        offset = offset<<8 | uint64(b[8+i])
    }
    return VAddr{SegmentID: segID, Offset: offset}
}

// =============================================================================
// SegmentID
// =============================================================================

// SegmentID identifies a segment file.
// Invariant: SegmentID is monotonically increasing (new segments have higher IDs).
type SegmentID uint64

const (
    // SegmentIDInvalid is the reserved value for no segment.
    // Invariant: No valid segment has ID 0.
    SegmentIDInvalid SegmentID = 0

    // SegmentIDMin is the minimum valid segment ID.
    SegmentIDMin SegmentID = 1
)

// IsValid returns true if this is a valid segment ID.
func (s SegmentID) IsValid() bool {
    return s != SegmentIDInvalid
}

// =============================================================================
// PageID
// =============================================================================

// PageID is the logical identifier for a page in the page manager.
// Invariant: PageID > 0 (0 is reserved for invalid/null).
//
// Why uint64?
//   - Allows up to 2^64-1 pages
//   - Fits in fixed-size index entry (8 bytes)
//   - Sequential allocation produces dense IDs (good for dense array index)
type PageID uint64

const (
    // PageIDInvalid is the reserved value for no page.
    // Invariant: No valid page has PageID 0.
    PageIDInvalid PageID = 0
)

// IsValid returns true if this is a valid page ID.
func (p PageID) IsValid() bool {
    return p != PageIDInvalid
}

// =============================================================================
// SegmentState
// =============================================================================

// SegmentState represents the lifecycle state of a segment.
//
// Lifecycle: Active → Sealed → Archived
//   Active:   Accepting new writes
//   Sealed:   No new writes; may still be read
//   Archived: Read-only; may be compacted or moved to cold storage
//
// Invariant: Segment state never transitions backwards (Sealed never goes back to Active).
type SegmentState uint8

const (
    // SegmentStateActive means the segment is accepting new writes.
    // Exactly one segment is Active at any time.
    SegmentStateActive SegmentState = 0x01

    // SegmentStateSealed means the segment is closed to new writes.
    // All data is durable; segment can be read but not modified.
    SegmentStateSealed SegmentState = 0x02

    // SegmentStateArchived means the segment is read-only and may be compacted.
    // Archived segments may be moved to cold storage or deleted after compaction.
    SegmentStateArchived SegmentState = 0x04
)

// String returns a human-readable name for the state.
func (s SegmentState) String() string {
    switch s {
    case SegmentStateActive:
        return "Active"
    case SegmentStateSealed:
        return "Sealed"
    case SegmentStateArchived:
        return "Archived"
    default:
        return "Unknown"
    }
}

// =============================================================================
// EpochID
// =============================================================================

// EpochID identifies a compaction epoch for MVCC.
// Epochs provide visibility guarantees for readers during compaction.
// Invariant: EpochID is monotonically increasing.
type EpochID uint64

// =============================================================================
// Constants
// =============================================================================

const (
    // PageSize is the fixed page size, aligned with OS page size.
    // Why 4096? Standard OS page size; good balance for I/O efficiency.
    PageSize = 4096

    // ExternalThreshold is the maximum size for inline values.
    // Values > 48 bytes are stored externally with VAddr reference.
    //
    // Why 48 bytes?
    //   - InlineValue.Data has 56 bytes available
    //   - When external: 1 byte length prefix + 16 bytes VAddr = 39 bytes minimum overhead
    //   - 48 bytes balances inline storage efficiency vs external store overhead
    ExternalThreshold = 48

    // MaxSegmentSize is the target size for segment rotation.
    // When active segment exceeds this, a new segment is created.
    // Default: 1 GB
    MaxSegmentSize = 1 << 30

    // SegmentHeaderSize is the size of the segment header.
    // Magic(8) + Version(2) + SegmentID(8) + CreatedAt(8) + Flags(2) + Reserved(6) = 32 bytes
    SegmentHeaderSize = 32

    // SegmentTrailerSize is the size of the segment trailer.
    // PageCount(8) + DataSize(8) + Checksum(8) + Reserved(8) = 32 bytes
    SegmentTrailerSize = 32

    // EpochGracePeriod is the number of epochs before VAddrs can be reclaimed.
    // Readers holding references must complete within this period.
    EpochGracePeriod = 3
)
