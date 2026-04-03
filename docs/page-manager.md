# Page Manager and Fixed-Size KV Index

## 1. Overview

**Purpose**: Map logical `PageID` (uint64) to physical `VAddr` (16 bytes) with optimized fixed-size operations.

**Scope**: Page allocation/deallocation, page_id → vaddr mapping index, free list management.

**Relates to**:
- `vaddr-format.md` — VAddr format (SegmentID + Offset, 16 bytes)
- `kv-store-design.md` — Page Manager's position in the architecture stack

## 2. Type Definitions

```go
// PageID is a logical identifier for a page.
// Invariant: PageID > 0 (0 is reserved for invalid/null).
type PageID uint64

const PageIDInvalid PageID = 0

// VAddr represents a physical address in append-only storage.
// Defined in vaddr-format.md; included here for reference.
// Layout: [SegmentID: uint64, Offset: uint64], big-endian, 16 bytes.
type VAddr struct {
    SegmentID uint64
    Offset    uint64
}

// PageManagerIndexEntry is a fixed-size key-value pair.
// Invariant: Entry is always 24 bytes (8 + 16).
type PageManagerIndexEntry struct {
    PageID PageID  // 8 bytes
    VAddr  VAddr   // 16 bytes
}

// PageManagerConfig holds initialization parameters.
type PageManagerConfig struct {
    InitialPageCount uint64  // Hints at initial allocation (default: 1024)
    GrowFactor       float64 // Exponential growth factor (default: 1.5)
    RadixTreeEnabled bool    // Use radix tree vs dense array
}
```

## 3. Interface Specification

```go
// PageManager maps PageID → VAddr and manages page allocation.
// Invariant: All mutations are append-only; old entries are tombstoned.
type PageManager interface {
    // GetVAddr returns the VAddr for a page_id, or VAddrInvalid if not allocated.
    GetVAddr(pageID PageID) VAddr

    // AllocatePage allocates a new page and returns its PageID and VAddr.
    // Invariant: Returned PageID is monotonically increasing.
    // Invariant: Returned VAddr is unique and never reused.
    AllocatePage() (PageID, VAddr)

    // FreePage marks a page as reclaimable.
    // Invariant: FreePage is idempotent.
    FreePage(pageID PageID)

    // UpdateMapping records that page_id now lives at vaddr.
    UpdateMapping(pageID PageID, vaddr VAddr)

    // PageCount returns the total number of allocated pages.
    PageCount() uint64

    // Iter calls fn for each page_id → vaddr mapping.
    Iter(fn func(pageID PageID, vaddr VAddr))

    // Flush ensures durable storage of the index.
    Flush() error
}

// FixedSizeKVIndex is the underlying index for page_id → vaddr mapping.
// Invariant: All entries are fixed-size (24 bytes).
type FixedSizeKVIndex interface {
    Get(key PageID) VAddr
    Put(key PageID, value VAddr)
    Len() uint64
    RangeQuery(start, end PageID) []PageManagerIndexEntry
    ByteSize() uint64
}
```

## 4. Index Structure Options

### 4.1 Dense Array (Default for Dense Page IDs)

**Structure**: `[]VAddr` indexed directly by `pageID - 1`.

```
Index:
  page_id=1 → entries[0] = VAddr{SegmentID:1, Offset:4096}
  page_id=2 → entries[1] = VAddr{SegmentID:1, Offset:8192}
  page_id=N → entries[N-1] = VAddr{...}
```

**Pros**:
- O(1) lookup and insert
- Zero overhead per entry (just the VAddr)
- Cache-friendly sequential access

**Cons**:
- Wastes space if page IDs are sparse

**When to use**: Page IDs are densely allocated.

### 4.2 Radix Tree (For Sparse Page IDs)

**Structure**: 4-level radix tree with 16-bit splits per level.

```
       [root]
      /  |  \
    [L1] [L1] [L1]  ← 16 branches per level
```

**Pros**:
- Handles sparse page IDs efficiently
- ~24 bytes overhead per entry
- O(k) where k = 4 levels

**Cons**:
- More complex than array

**Why 4 levels, 16-bit splits**: 64 bits / 4 levels = 16 bits per level

### 4.3 Why Not Alternatives

| Alternative | Why Rejected |
|-------------|--------------|
| B-tree (generic) | Overkill for fixed-size keys; higher overhead |
| Hash map | No range query support; higher overhead than array |
| Skip list | Higher overhead; no benefit over radix for integer keys |
| LSM tree | Page manager is already append-only; adds complexity |

## 5. Free List Management

```go
// FreeList manages reclaimable pages for reuse.
// Design: Lock-free stack of PageIDs.
// Invariant: Freed pages are reused before allocating new PageIDs.
type FreeList interface {
    Pop() (PageID, bool)   // Returns freed PageID or false if empty
    Push(pageID PageID)    // Adds to free list
    Len() uint64           // Number of pages on free list
    Clear()                // Remove all entries
}
```

**Concurrency**: FreeList uses atomic CAS for lock-free push/pop.

## 6. Allocation Strategy

```
AllocatePage():
    1. If FreeList.Len() > 0:
           pageID = FreeList.Pop()
    2. Else:
           pageID = nextPageID++
    3. vaddr = AllocateVAddr()
    4. Index.Put(pageID, vaddr)
    5. Return (pageID, vaddr)

FreePage(pageID):
    1. Index.Put(pageID, VAddrInvalid)  // tombstone
    2. FreeList.Push(pageID)
```

## 7. Persistence Strategy

**In-Memory Index (Default)**:
- Index kept in memory
- On crash: rebuild index from segment files
- Rebuild cost: O(n) scan of all pages

**Persistent Index (Optional)**: Separate index file with header + entries + footer.

## 8. Invariants Summary

- **PageID uniqueness**: No two allocations return the same PageID (except safe reuse from free list)
- **VAddr uniqueness**: No two allocations return the same VAddr
- **Page alignment**: All VAddr.Offset values are multiples of PageSize
- **PageID > 0**: PageID 0 is reserved for invalid/null

## 9. Memory Budget (1 Million Pages)

| Component | Dense Array | Radix Tree |
|-----------|-------------|------------|
| Index entries | 16 MB | 16 MB |
| Array overhead | ~1 MB | N/A |
| Radix nodes | N/A | ~8 MB |
| Free list | ~8 MB | ~8 MB |
| **Total** | ~25 MB | ~32 MB |

vs generic B-tree: likely 64+ MB for same scale.

---

*Document Status: Contract Spec*
*Last Updated: 2024*
