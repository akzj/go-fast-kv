# External Value Store

## 1. Overview

Stores values exceeding the B-link-tree's inline threshold (>48 bytes). Provides crash-consistent storage and retrieval using VAddr references.

**Relates to:**
- `vaddr-format.md` — VAddr format (SegmentID + Offset, 16 bytes)
- `blinktree-node-format.md` — InlineValue encoding and ExternalThreshold constant

## 2. Threshold and Encoding

### 2.1 Value Classification

```go
// ExternalThreshold is defined in blinktree-node-format.md.
// Values ≤ 48 bytes: stored inline in InlineValue.Data
// Values > 48 bytes: stored externally, VAddr stored in InlineValue.Data
//
// Why 48 bytes?
// InlineValue.Data has 56 bytes. When external:
// - 1 byte for length (stored in InlineValue.Length)
// - 16 bytes for VAddr = 17 bytes used
// - 56 - 16 = 40 bytes overhead if we tried to store VAddr inline
// - 48 leaves room for length prefix + small alignment
//
// Value size 1-48 bytes: InlineValue { length: size, data: [size]bytes }
// Value size 49+ bytes:  InlineValue { length: 16 | 0x8000, data: VAddr }
```

### 2.2 InlineValue Encoding for External References

InlineValue encoding for external references is defined in `blinktree-node-format.md`:
- `IsExternal()` — returns true if InlineValue contains a VAddr
- `GetExternalVAddr()` — extracts VAddr from InlineValue (panics if inline)
- `SetExternalVAddr(addr VAddr)` — encodes VAddr as external reference

## 3. External Value Storage Format

### 3.1 Page Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  External Value Page(s)                                         │
├─────────────────────────────────────────────────────────────────┤
│  Page 0 (4096 bytes)                                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  ExternalValueHeader (32 bytes)                           │  │
│  │  ├── Magic: "EXTVAL\0\0" (8 bytes)                        │  │
│  │  ├── Version: uint16 (currently 1)                       │  │
│  │  ├── ValueSize: uint64 (total bytes of value data)       │  │
│  │  ├── Flags: uint16                                        │  │
│  │  └── Reserved: [4]byte                                    │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │  ValueData (4064 bytes on page 0)                         │  │
│  │  └── First portion of value data                         │  │
│  └───────────────────────────────────────────────────────────┘  │
│  Page 1-N (4096 bytes each, if value spans multiple pages)      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Continuation data (4096 bytes per page)                   │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Type Definitions

```go
// ExternalValueHeader is the fixed-size header for external values.
// Invariant: Header is always 32 bytes, page-aligned for O(1) offset calculation.
// Why page-aligned? VAddr points to page start; header is always at Offset+0.
type ExternalValueHeader struct {
    Magic     [8]byte  // "EXTVAL\0\0" for identification
    Version   uint16   // Format version
    ValueSize uint64   // Total size in bytes (not including header)
    Flags     uint16   // Reserved for future use
    Reserved  [4]byte  // Padding to 32 bytes
}

// Constants
const (
    ExternalValueHeaderSize = 32
    ExternalValueDataPerPage = PageSize - ExternalValueHeaderSize  // 4064 bytes
    
    // Magic bytes for external value identification
    ExternalValueMagic = "EXTVAL\0\0"
    
    // Current version
    ExternalValueVersion = 1
)

// PageCount returns the number of pages needed to store a value of given size.
func PageCount(valueSize uint64) int {
    if valueSize == 0 {
        return 1  // Always at least one page
    }
    pages := int(valueSize) / ExternalValueDataPerPage
    if int(valueSize)%ExternalValueDataPerPage != 0 {
        pages++
    }
    return pages
}

// Why not variable header size?
// Fixed header enables reading size in O(1) without parsing.
// Single-page values: read header + data in one IO.
// Multi-page values: read header, then sequential page reads.
```

## 4. External Value Store Interface

### 4.1 Core Interface

```go
// ExternalValueStore manages large value storage and retrieval.
// Thread-safe: multiple goroutines can store/retrieve concurrently.
// Crash-consistent: append-only semantics guarantee recovery.
type ExternalValueStore interface {
    // Store persists a value and returns its VAddr.
    // The value can be retrieved by passing this VAddr to Retrieve.
    //
    // Invariant: VAddr returned is unique and stable (never reused).
    // Invariant: Store is atomic; either fully written or not at all.
    //
    // Why not return error for large values?
    // The store handles arbitrary sizes by allocating pages as needed.
    // Caller should check value size if limit is required.
    Store(value []byte) (VAddr, error)

    // Retrieve reads the complete value at the given VAddr.
    // Returns the raw value bytes.
    //
    // Invariant: Retrieve(vaddr) succeeds iff Store was called with same vaddr.
    // Invariant: Retrieve never modifies stored data.
    //
    // Why return []byte, not reader?
    // Simpler API for typical use case. Large values are still read efficiently.
    // For streaming, use RetrieveAt with explicit page boundaries.
    Retrieve(vaddr VAddr) ([]byte, error)

    // RetrieveAt reads a slice of the value without loading entire value.
    // offset and length are in bytes within the value.
    //
    // Invariant: offset + length ≤ value size (stored in header).
    // Returns error if out of bounds.
    //
    // Why needed?
    // Large value updates may need partial reads (e.g., append-only modification).
    // Also useful for large value inspection without full load.
    RetrieveAt(vaddr VAddr, offset, length uint64) ([]byte, error)

    // Delete marks a value for future reclamation.
    // Values are not immediately freed (append-only constraint).
    // Reclamation happens during compaction.
    //
    // Invariant: Delete is idempotent.
    // Invariant: After Delete, Retrieve may return stale data until compaction.
    //
    // Why soft delete?
    // Append-only storage cannot immediately reclaim space.
    // Tombstones track deletable values for compaction pass.
    Delete(vaddr VAddr) error

    // GetValueSize returns the size of the value at vaddr without loading data.
    // Reads only the header (32 bytes).
    GetValueSize(vaddr VAddr) (uint64, error)

    // GetValueSizeAt returns size at offset without loading full header.
    // Optimized for batch operations.
    GetValueSizeAt(addrs []VAddr) ([]uint64, error)
}

// Why not Read/Write interface?
// Store/Retrieve is simpler for atomic value semantics.
// ReadAt/WriteAt is provided via RetrieveAt for partial access.
// Why not streaming interface?
// Most values are not huge. Simpler API for common case.
```

### 4.2 B-link-tree Integration Interface

```go
// ExternalValueStoreForTree is the interface seen by B-link-tree nodes.
// Exists to keep B-link-tree unaware of ExternalValueStore internals.
type ExternalValueStoreForTree interface {
    // StoreExternal stores value > 48 bytes, returns InlineValue with VAddr.
    // Convenience method combining Store + InlineValue encoding.
    //
    // Why not just Store?
    // B-link-tree nodes work with InlineValue, not raw bytes.
    // This method encapsulates the encoding.
    StoreExternal(value []byte) (InlineValue, error)

    // ResolveInlineValue extracts the value if external, returns nil if inline.
    // If external, calls Retrieve to fetch actual bytes.
    ResolveInlineValue(iv InlineValue) ([]byte, bool)  // (value, was_external)
}

// Why not merge into ExternalValueStore?
// Separation of concerns:
// - ExternalValueStore: raw storage operations
// - ExternalValueStoreForTree: encoding/decoding for tree nodes
```

### 4.3 Configuration

```go
// ExternalValueStoreConfig holds initialization parameters.
type ExternalValueStoreConfig struct {
    // VAddrAllocator for allocating pages.
    // Required; ExternalValueStore does not implement allocation itself.
    VAddrAllocator VAddrAllocator

    // SegmentManager for persisting pages.
    // Required; writes go through segment infrastructure.
    SegmentManager SegmentManager

    // ValueSizeLimit is the maximum value size allowed.
    // If 0, uses default (MaxSegmentSize).
    // Values exceeding this are rejected with error.
    ValueSizeLimit uint64

    // EnableDeduplication causes identical values to share storage.
    // If true, Store checks content hash before writing.
    // Default: false (simpler, better isolation).
    EnableDeduplication bool

    // DedupIndex is the content-addressable index for deduplication.
    // Required if EnableDeduplication is true.
    DedupIndex DeduplicationIndex
}

// Default limits
const (
    DefaultMaxValueSize = 64 * 1024 * 1024  // 64 MB per value
    MaxSafeValueSize    = 1024 * 1024 * 1024 // 1 GB (soft limit)
)

// VAddrAllocator interface (from vaddr-format.md context)
type VAddrAllocator interface {
    Allocate(pages int) (VAddr, error)  // Allocates N contiguous pages
    // Returns VAddr of first page; subsequent pages at Offset + N*PageSize
}

// SegmentManager interface (from vaddr-format.md context)
type SegmentManager interface {
    Write(vaddr VAddr, data []byte) error
    Read(vaddr VAddr, length int) ([]byte, error)
    Flush() error
}

// DeduplicationIndex interface
type DeduplicationIndex interface {
    // Get returns VAddr if content hash exists, VAddrInvalid otherwise.
    Get(contentHash [32]byte) VAddr
    
    // Put records that content hash maps to vaddr.
    Put(contentHash [32]byte, vaddr VAddr)
}
```

## 5. Operations Detail

### 5.1 Store Operation

```
Store(value []byte):
    1. If EnableDeduplication:
           hash = SHA256(value)
           existing = DedupIndex.Get(hash)
           if existing != VAddrInvalid:
               return existing, nil
    2. pages_needed = PageCount(len(value))
    3. vaddr = VAddrAllocator.Allocate(pages_needed)
    4. header = ExternalValueHeader{
           Magic: "EXTVAL\0\0",
           Version: 1,
           ValueSize: uint64(len(value)),
           Flags: 0,
       }
    5. Write header at vaddr
    6. Write value data starting at vaddr + HeaderSize
    7. If EnableDeduplication:
           DedupIndex.Put(hash, vaddr)
    8. return vaddr, nil
```

### 5.2 Retrieve Operation

```
Retrieve(vaddr VAddr):
    1. Read 32 bytes at vaddr → header
    2. Validate Magic == "EXTVAL\0\0"
    3. pages_needed = PageCount(header.ValueSize)
    4. Read pages_needed * PageSize starting at vaddr
    5. Extract value data (skip header on page 0)
    6. Return value bytes
```

### 5.3 RetrieveAt Operation

```
RetrieveAt(vaddr VAddr, offset, length uint64):
    1. Read 32 bytes at vaddr → header
    2. Validate offset + length ≤ header.ValueSize
    3. first_page_data_offset = ExternalValueHeaderSize + offset
    4. first_page = (vaddr.Offset + first_page_data_offset) / PageSize
    5. Read requested bytes from appropriate pages
    6. Return partial data
```

## 6. Multi-Page Value Handling

### 6.1 Page Layout for Large Values

```
Value size: 10,000 bytes
PageCount = ceil(10000 / 4064) = 3 pages

Page 0: Header (32) + Data (4064) = 4096 bytes
Page 1: Data (4064) = 4096 bytes  
Page 2: Data (1872) = 4096 bytes (4096 - 224 padding)

VAddr{SegmentID: 1, Offset: 8192} → First page of value

Value layout in storage:
[Page 0: Header(32) | Data(0..4063)]
[Page 1: Data(4064..8127)]
[Page 2: Data(8128..9999) | Padding(224)]
```

### 6.2 Why Contiguous Allocation?

| Alternative | Why Rejected |
|-------------|--------------|
| Chained pages | Extra I/O for non-sequential access; more complex recovery |
| B-tree for data | Overkill; external values are typically accessed atomically |
| Indirect blocks | Extra indirection; header already serves this purpose |

Contiguous allocation:
- Single seek for first page
- Sequential read for remaining pages
- Simple recovery: read header, read N pages
- Effective for both small and large values

## 7. Crash Recovery

### 7.1 Recovery Process

```
RecoverExternalValueStore():
    1. Scan all segments for pages with Magic "EXTVAL\0\0"
    2. For each found:
           Read header to get ValueSize
           Validate page chain is intact
           Add to live values index
    3. Build tombstone list from deleted values
    4. Return ExternalValueStore with recovered state
```

### 7.2 Consistency Guarantees

- **Write atomicity**: Either all pages of a value are written, or none
- **Value integrity**: Header checksum validates size before reading
- **No partial values**: Recovery skips values without complete page chain

## 8. Invariants Summary

```go
// ExternalValueHeader invariants:
// - Magic is always "EXTVAL\0\0" (8 bytes)
// - Version is currently 1
// - ValueSize > 48 (enforced by store threshold)
// - ValueSize ≤ Config.ValueSizeLimit (enforced at store time)

// VAddr invariants:
// - VAddr points to page-aligned address
// - First page contains header at Offset 0
// - Subsequent pages immediately follow (contiguous)

// Page invariants:
// - All pages are PageSize bytes (except possibly last)
// - Last page may have padding
// - Total stored bytes = HeaderSize + ValueSize

// Concurrency invariants:
// - Store is thread-safe (atomic allocation)
// - Retrieve is thread-safe (read-only)
// - Delete is thread-safe (tombstone)
```

## 9. Why Not Alternatives

| Alternative | Why Rejected |
|-------------|--------------|
| Store VAddr in separate index | VAddr already IS the index; extra indirection wasteful |
| Variable-size header | Fixed header enables O(1) size lookup |
| In-place updates | Append-only storage cannot modify; creates new versions |
| Separate segment for external values | Flexibility; values can be in any segment |

## 10. Related Specifications

- **B-link-tree**: Uses ExternalValueStore for values >48 bytes
- **VAddr allocator**: Provides page allocation for external values
- **Segment manager**: Persists pages to storage
- **Compaction**: Reclaims space from deleted external values

---

*Document Status: Contract Spec*
*Last Updated: 2024*
