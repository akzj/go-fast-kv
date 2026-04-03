# VAddr Format and Address Space Layout

## 1. Overview

This document specifies the VAddr (Virtual Address) format and the append-only address space layout for the KV storage system.

**Invariants:**
- VAddr is immutable once assigned — append-only semantics guarantee no address reuse
- VAddr uniquely identifies a physical location across crash/recovery
- VAddr encoding supports efficient segment-based reads

## 2. VAddr Format

### 2.1 Binary Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  VAddr (16 bytes)                                                │
├─────────────────────────────────────────────────────────────────┤
│  SegmentID (8 bytes, big-endian)  │  Offset (8 bytes, big-endian)│
└─────────────────────────────────────────────────────────────────┘
```

**Type Definition:**
```go
// VAddr encodes a physical address in the append-only address space.
// Invariant: VAddr is 16 bytes, never zero (SegmentID 0 is reserved).
// Why 16 bytes? Allows segment IDs up to uint64_max with 8-byte offsets.
// Why big-endian? Natural byte ordering for segment comparisons.
type VAddr struct {
    SegmentID uint64  // Identifies the segment file
    Offset    uint64  // Byte offset within the segment
}

// IsValid returns true if this VAddr represents a valid address.
// Invariant: SegmentID != 0 (segment 0 is reserved for invalid/null)
func (v VAddr) IsValid() bool {
    return v.SegmentID != 0
}

// Compare returns -1, 0, 1 for v < o, v == o, v > o
func (v VAddr) Compare(o VAddr) int {
    if v.SegmentID != o.SegmentID {
        return compareUint64(v.SegmentID, o.SegmentID)
    }
    return compareUint64(v.Offset, o.Offset)
}
```

### 2.2 Why Not Alternative Formats

| Alternative | Why Rejected |
|-------------|--------------|
| `[file_id:offset]` (variable length) | File IDs require name resolution; segments are simpler |
| Single uint64 with embedded segment | Limits segment size to 2^32 bytes; fixed-size struct is clearer |
| Relative offset only | Cannot cross segment boundaries without resolution |
| Pointer-sized (8 bytes) | Insufficient: segment + offset cannot fit in 8 bytes |

### 2.3 Reserved Values

```go
const (
    // VAddrInvalid represents a null/invalid address.
    // Used for deleted entries, tombstones, uninitialized fields.
    VAddrInvalid = VAddr{SegmentID: 0, Offset: 0}
    
    // VAddrMinValid is the smallest valid address.
    VAddrMinValid = VAddr{SegmentID: 1, Offset: 0}
)
```

## 3. Address Space Layout

### 3.1 Segment Organization

```
┌─────────────────────────────────────────────────────────────────┐
│                    Address Space                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │  Segment 1  │  │  Segment 2  │  │  Segment N  │  ...        │
│  │  (active)   │  │  (active)   │  │  (sealed)   │             │
│  │  0 - 2^63   │  │  0 - 2^63   │  │  0 - size   │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

**Segment Types:**
1. **Active Segment**: Currently being written to. Only one active segment at a time.
2. **Sealed Segment**: Closed for writes, read-only. Can be compacted or archived.
3. **Archived Segment**: Moved to cold storage. Accessed via segment manifest.

### 3.2 Segment File Format

```
┌─────────────────────────────────────────────────────────────────┐
│  Segment Header (32 bytes)                                      │
├─────────────────────────────────────────────────────────────────┤
│  Magic: "FASTSEG" (8 bytes)                                     │
│  Version: uint16                                                │
│  SegmentID: uint64 (big-endian)                                 │
│  CreatedAt: uint64 (unix timestamp nanos)                       │
│  Flags: uint16 (0x01 = sealed, 0x02 = archived)                  │
│  Reserved: 6 bytes                                              │
├─────────────────────────────────────────────────────────────────┤
│  Data Pages (variable, aligned to 4KB)                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                        │
│  │  Page 0  │ │  Page 1  │ │  Page N  │  ...                  │
│  └──────────┘ └──────────┘ └──────────┘                        │
├─────────────────────────────────────────────────────────────────┤
│  Segment Trailer (32 bytes)                                     │
├─────────────────────────────────────────────────────────────────┤
│  PageCount: uint64                                              │
│  DataSize: uint64 (total bytes of data)                         │
│  Checksum: uint64 (CRC64 of header + data)                     │
│  Reserved: 8 bytes                                             │
└─────────────────────────────────────────────────────────────────┘
```

**Type Definition:**
```go
// SegmentID uniquely identifies a segment in the address space.
// Invariant: SegmentID > 0, monotonically increasing.
// Why monotonically increasing? Enables segment age estimation via ID comparison.
type SegmentID uint64

// SegmentState represents the lifecycle state of a segment.
type SegmentState uint8

const (
    SegmentStateActive   SegmentState = 0x01  // Open for writes
    SegmentStateSealed   SegmentState = 0x02  // Closed, read-only
    SegmentStateArchived SegmentState = 0x04  // Moved to cold storage
)

// SegmentHeader is the fixed-size header at the start of each segment file.
// Invariant: Header is always 32 bytes, ensuring stable offsets.
type SegmentHeader struct {
    Magic     [8]byte   // "FASTSEG\0" for identification
    Version   uint16    // Format version (currently 1)
    ID        SegmentID // This segment's unique identifier
    CreatedAt uint64    // Unix timestamp in nanoseconds
    Flags     uint16    // SegmentState flags
    Reserved  [6]byte   // Padding for alignment
}

// SegmentTrailer is written after all data pages.
// Invariant: Trailer is always 32 bytes, allowing footer detection.
type SegmentTrailer struct {
    PageCount uint64  // Number of data pages
    DataSize  uint64  // Total bytes of data (excluding header/trailer)
    Checksum  uint64  // CRC64 of header + data
    Reserved  [8]byte // Future use
}
```

### 3.3 Segment Manifest

The segment manifest tracks all segments and their states:

```go
// SegmentManifestEntry describes a single segment.
// Stored in: manifest file + optionally in-memory cache.
type SegmentManifestEntry struct {
    ID         SegmentID
    State      SegmentState
    FilePath   string      // Relative to data directory
    SizeBytes  uint64      // Current file size
    PageCount  uint64      // Number of pages written
    CreatedAt  uint64      // Unix timestamp nanos
    SealedAt   uint64      // Unix timestamp nanos (0 if active)
}

// SegmentManifest manages segment metadata.
// Invariant: Manifest is append-only; old entries never deleted, only superseded.
type SegmentManifest struct {
    mu      sync.RWMutex
    entries []SegmentManifestEntry // Sorted by SegmentID ascending
    path    string
}
```

## 4. VAddr Allocation

### 4.1 Allocation Algorithm

```
AllocateVAddr():
    1. Get current active segment (create if none exists)
    2. If active segment has room:
         offset = current_write_offset
         write_offset += page_size
         return VAddr{segment.ID, offset}
    3. Else:
         Seal current segment
         Create new active segment
         offset = 0
         write_offset = page_size
         return VAddr{new_segment.ID, offset}
```

### 4.2 Why Not Alternative Allocation Strategies

| Alternative | Why Rejected |
|-------------|--------------|
| Round-robin across segments | Reduces sequential write benefits |
| Dynamic segment sizing | Complexity for marginal benefit |
| Delayed allocation | Adds indirection, complicates crash recovery |

## 5. VAddr Resolution (Read Path)

```
ResolveVAddr(vaddr):
    1. Look up segment by vaddr.SegmentID in manifest
    2. Open segment file
    3. Seek to vaddr.Offset
    4. Read page_size bytes
    5. Return data
```

**Type Definition:**
```go
// Resolver translates VAddrs to physical data.
// Thread-safe: multiple goroutines can resolve concurrently.
type VAddrResolver interface {
    // Resolve returns the data at the given VAddr.
    // Returns error if segment is archived and not accessible.
    Resolve(vaddr VAddr) ([]byte, error)
    
    // ResolvePage reads a full page at vaddr.
    // Panics if less than PageSize bytes are available.
    ResolvePage(vaddr VAddr) (Page, error)
    
    // GetSegmentInfo returns metadata for the segment containing vaddr.
    GetSegmentInfo(vaddr VAddr) (SegmentManifestEntry, error)
}
```

## 6. Page Structure

### 6.1 Page Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  Page (4096 bytes, aligned)                                    │
├─────────────────────────────────────────────────────────────────┤
│  PageHeader (16 bytes)                                         │
│  ├── PageID: uint64                                            │
│  ├── Checksum: uint32 (CRC32)                                  │
│  └── Flags: uint16                                             │
├─────────────────────────────────────────────────────────────────┤
│  PageData (4080 bytes)                                         │
│  └── User data or B-link-tree node content                     │
└─────────────────────────────────────────────────────────────────┘
```

**Why no padding after PageData?** Header (16) + Data (4080) = 4096, exactly one page.

```go
// PageSize is the standard size for all data pages.
// Why 4KB? Aligned with OS page size, good for IO performance.
const PageSize = 4096

// Page represents a fixed-size data page.
// Invariant: Page is always PageSize bytes.
type Page struct {
    ID       PageID   // Logical identifier
    Checksum uint32   // CRC32 of page data
    Flags    uint16   // Page flags
    Data     [PageDataSize]byte
}

const PageDataSize = PageSize - 16  // 4080 bytes

// PageID is the logical identifier for a page.
// Maps to VAddr via the Page Manager's index.
type PageID uint64
```

## 7. Address Space Invariants

```go
// Invariant documentation for the append-only address space:

// 1. VAddr uniqueness: No two allocations produce the same VAddr.
//    Proved by: AllocateVAddr is single-threaded for active segment.

// 2. VAddr stability: A VAddr never changes after allocation.
//    Proved by: Append-only semantics; segments are immutable once sealed.

// 3. Segment ID monotonicity: New segments have higher IDs than old segments.
//    Proved by: SegmentID counter is atomic/increment-only.

// 4. Offset bounds: Offset < MaxSegmentSize for active segments.
//    Enforced by: Segment rotation when offset would exceed limit.

// 5. Page alignment: All offsets are multiples of PageSize.
//    Enforced by: AllocateVAddr rounds up to PageSize boundary.
```

## 8. Related Specifications

- **Page Manager**: Uses VAddr format for page_id → vaddr mapping
- **B-link-tree**: Stores VAddrs as pointers to child nodes
- **Compaction**: May archive segments but never reclaims VAddr space

---

*Document Status: Contract Spec*
*Last Updated: 2024*
