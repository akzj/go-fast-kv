# Solution-C: Integrated Architecture Document

## Overview

This document integrates all 11 design specifications into a complete, coherent architecture for a high-performance key-value storage system.

**Key Integration Decisions:**
- VAddr = 16 bytes (SegmentID[8] + Offset[8])
- Inline value threshold = 48 bytes
- Cache: OS Page Cache as default (Buffered for WAL/segments, Mmap for index)
- Concurrency: Single-writer, multi-reader with latch crabbing
- Recovery: WAL + Checkpoint, redo-only algorithm
- Compaction: Generational segmented, 3-epoch grace period, non-blocking

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [VAddr Format and Address Space](#2-vaddr-format-and-address-space)
3. [B-link-tree Index](#3-b-link-tree-index)
4. [Page Manager](#4-page-manager)
5. [External Value Store](#5-external-value-store)
6. [API Layer](#6-api-layer)
7. [Caching Strategy](#7-caching-strategy)
8. [Concurrency Model](#8-concurrency-model)
9. [Crash Recovery](#9-crash-recovery)
10. [Compaction Strategy](#10-compaction-strategy)
11. [Index Persistence](#11-index-persistence)
12. [Invariant Summary](#12-invariant-summary)

---

## 1. Architecture Overview

### 1.1 System Layers

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Layer                                       │
│                   (Get / Put / Delete / Scan / Batch / Tx)                  │
│  File: api-layer.md                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           B-link-tree Index                                 │
│    - Leaf nodes store (key, value) for inline values ≤48 bytes             │
│    - Leaf nodes store (key, VAddr) for external values >48 bytes           │
│    - Sibling chain traversal for lock-free reads                            │
│    - All node operations append to storage (never in-place)                 │
│  File: blinktree-node-format.md                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Append-only Storage                                  │
│    - All writes are append operations                                        │
│    - VAddr = SegmentID(8) + Offset(8) = 16 bytes                            │
│    - Segments: Active → Sealed → Compact → Archived                        │
│  File: vaddr-format.md                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Page Manager                                     │
│    - Maps PageID → VAddr for fixed-size page allocation                     │
│    - Dense Array (default) or Radix Tree for index                          │
│    - Free list management for page reuse                                    │
│  File: page-manager.md, fixed-size-kvindex-persist.md                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       External Value Store                                   │
│    - Values >48 bytes stored externally                                     │
│    - Contiguous page allocation with header-based retrieval                 │
│    - Referenced by VAddr stored in B-link-tree nodes                        │
│  File: external-value-store.md                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         OS Page Cache (Default)                              │
│    - Buffered I/O for WAL, segments, external values                        │
│    - Memory-mapped I/O for index files (rarely accessed, random access)     │
│    - fsync ordering: WAL synced before data                                 │
│  File: os-page-cache.md                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage Model | Append-only | Sequential writes, no random I/O, crash-consistent |
| Primary Index | B-link-tree | Range queries, lock-free reads, single-writer friendly |
| Address Format | VAddr = SegmentID(8) + Offset(8) | 16 bytes, extensible, segment comparisons |
| Inline Value | ≤48 bytes | Fits in 56-byte InlineValue.Data slot with VAddr encoding |
| Page Mapping | Dense Array / Radix Tree | Fixed-size keys, O(1) or O(k) lookup |
| Concurrency | Single-writer, multi-reader | Simpler than multi-writer; append-only serializes anyway |
| Recovery | WAL + Checkpoint, redo-only | Append-only needs no undo; WAL provides atomicity |
| Compaction | Generational segmented | Non-blocking, epoch-based MVCC safety |
| Cache | OS Page Cache (default) | Kernel optimized, less code; Integrated Cache as alternative |

### 1.3 Module Summary

| Module | Document | Key Interface |
|--------|----------|---------------|
| VAddr / Storage | vaddr-format.md | VAddr, Segment, Page |
| B-link-tree | blinktree-node-format.md | NodeOperations, NodeManager |
| Page Manager | page-manager.md | PageManager, FixedSizeKVIndex |
| Index Persistence | fixed-size-kvindex-persist.md | DenseArray, RadixTree |
| External Value | external-value-store.md | ExternalValueStore |
| API | api-layer.md | KVStore, Iterator, Transaction |
| Concurrency | concurrency-recovery.md | LatchManager, WAL, Checkpoint |
| Compaction | compaction-strategy.md | GenerationalCompaction, EpochManager |
| Cache | os-page-cache.md | BufferedFile, MmapFile |

---

## 2. VAddr Format and Address Space

### 2.1 VAddr Binary Layout

```go
// VAddr encodes a physical address in the append-only address space.
// Layout: [SegmentID: uint64, Offset: uint64], big-endian, 16 bytes total
// Invariant: VAddr is 16 bytes, never zero (SegmentID 0 is reserved).
type VAddr struct {
    SegmentID uint64  // Identifies the segment file
    Offset    uint64  // Byte offset within the segment
}

// Reserved values
const (
    VAddrInvalid   = VAddr{SegmentID: 0, Offset: 0}
    VAddrMinValid  = VAddr{SegmentID: 1, Offset: 0}
)
```

### 2.2 Why 16-Byte VAddr

| Alternative | Why Rejected |
|-------------|--------------|
| `[file_id:offset]` (variable length) | File IDs require name resolution; segments are simpler |
| Single uint64 with embedded segment | Limits segment size to 2^32 bytes; fixed-size struct is clearer |
| Relative offset only | Cannot cross segment boundaries without resolution |
| Pointer-sized (8 bytes) | Insufficient: segment + offset cannot fit in 8 bytes |

### 2.3 Segment File Format

```
┌─────────────────────────────────────────────────────────────────┐
│  Segment Header (32 bytes)                                      │
├─────────────────────────────────────────────────────────────────┤
│  Magic: "FASTSEG" (8 bytes)                                     │
│  Version: uint16                                                │
│  SegmentID: uint64 (big-endian)                                 │
│  CreatedAt: uint64 (unix timestamp nanos)                       │
│  Flags: uint16 (0x01 = sealed, 0x02 = archived)                 │
│  Reserved: 6 bytes                                              │
├─────────────────────────────────────────────────────────────────┤
│  Data Pages (variable, aligned to 4KB)                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                        │
│  │  Page 0  │ │  Page 1  │ │  Page N  │                        │
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

### 2.4 Segment Lifecycle

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Active   │────▶│ Sealed   │────▶│ Compact  │────▶│ Archived │
│ (write)  │     │ (read)   │     │ (target) │     │ (cold)   │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
     │                │                │                │
 Only one          Full, no        Segments         Read-only,
 at a time         new writes      being rewritten  maybe moved
```

---

## 3. B-link-tree Index

### 3.1 Node Binary Layout

```go
// NodeFormat is the binary layout for both internal and leaf nodes.
type NodeFormat struct {
    // Common header (32 bytes)
    NodeType     uint8   // 0=Leaf, 1=Internal
    IsDeleted    uint8   // Soft delete flag
    Level        uint8   // 0=leaf, 1+=internal levels
    Count        uint8   // Number of entries in use
    Capacity     uint16  // Total entry slots
    Reserved     uint16
    
    HighSibling  VAddr   // Pointer to next node at same level
    LowSibling   VAddr   // Pointer to previous node at same level
    HighKey      PageID  // Maximum key in this subtree (internal only)
    Checksum     uint32  // CRC32c
}

// LeafNode body: Count entries of LeafEntry format
type LeafEntry struct {
    Key     PageID        // Fixed-size key (8 bytes)
    Value   InlineValue   // Inline value or pointer to external
}

// InlineValue encodes both inline values and external references.
type InlineValue struct {
    Length [8]byte        // Big-endian length (top bit = is_external flag)
    Data   [56]byte       // Inline data or VAddr of external value
}

// Constants
const (
    MaxInlineValueSize = 48    // Values ≤48 bytes stored inline
    ExternalThreshold  = 48    // Values >48 bytes get externalized
)
```

### 3.2 Value Classification

```
Value size 1-48 bytes:
    InlineValue { length: size, data: [size]bytes }
    
Value size 49+ bytes:
    InlineValue { length: 16 | 0x8000, data: VAddr }
    VAddr points to first page in ExternalValueStore
```

### 3.3 Node Invariants

1. **NodeImmutability**: Once written, a node is never modified (append-only)
2. **SiblingChain**: Leaf nodes form a doubly-linked list via HighSibling/LowSibling
3. **KeyOrdering**: Keys within a node are strictly increasing (no duplicates)
4. **SplitBoundary**: For a split at key K, left contains ≤K, right contains >K
5. **HighKeyInvariant**: Internal node's HighKey equals max key in rightmost child subtree

---

## 4. Page Manager

### 4.1 Interface

```go
// PageManager maps PageID → VAddr and manages page allocation.
type PageManager interface {
    GetVAddr(pageID PageID) VAddr
    AllocatePage() (PageID, VAddr)
    FreePage(pageID PageID)
    UpdateMapping(pageID PageID, vaddr VAddr)
    PageCount() uint64
    Iter(fn func(pageID PageID, vaddr VAddr))
    Flush() error
}
```

### 4.2 Index Structure Options

**Dense Array (Default)**:
- O(1) lookup and insert
- Zero overhead per entry
- Best for densely-allocated PageIDs

**Radix Tree (For Sparse PageIDs)**:
- 4-level tree with 16-bit splits per level
- ~24 bytes overhead per entry
- O(4) lookup worst case

### 4.3 Allocation Strategy

```
AllocatePage():
    1. If FreeList.Len() > 0:
           pageID = FreeList.Pop()
    2. Else:
           pageID = nextPageID++
    3. vaddr = AllocateVAddr()
    4. Index.Put(pageID, vaddr)
    5. Return (pageID, vaddr)
```

---

## 5. External Value Store

### 5.1 Storage Format

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
│  └───────────────────────────────────────────────────────────┘  │
│  Page 1-N (4096 bytes each, if value spans multiple pages)      │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Interface

```go
type ExternalValueStore interface {
    Store(value []byte) (VAddr, error)
    Retrieve(vaddr VAddr) ([]byte, error)
    RetrieveAt(vaddr VAddr, offset, length uint64) ([]byte, error)
    Delete(vaddr VAddr) error
    GetValueSize(vaddr VAddr) (uint64, error)
}

const DefaultMaxValueSize = 64 * 1024 * 1024  // 64 MB
```

---

## 6. API Layer

### 6.1 Core Interface

```go
type KVStore interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
    Close() error
}
```

### 6.2 Error Types

```go
var (
    ErrKeyNotFound      = errors.New("key not found")
    ErrStoreClosed      = errors.New("store is closed")
    ErrTransactionAborted = errors.New("transaction aborted")
    ErrStoreFull        = errors.New("store is full")
    ErrKeyTooLarge      = errors.New("key too large")
    ErrValueTooLarge    = errors.New("value too large")
)
```

### 6.3 Transaction Support

```go
type KVStoreWithTransactions interface {
    KVStore
    Begin() (Transaction, error)
    BeginWithOptions(opts TransactionOptions) (Transaction, error)
}

type Transaction interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
    Commit() error
    Rollback()
    TxID() uint64
}
```

---

## 7. Caching Strategy

### 7.1 OS Page Cache (Default)

**Design Decision**: Use OS Page Cache as the primary caching mechanism rather than an integrated cache.

| Component | Access Pattern | Rationale |
|-----------|---------------|-----------|
| WAL | Buffered | Sequential writes; kernel optimized |
| Segments | Buffered | Mixed reads/writes; kernel handles |
| External Values | Buffered | Sequential reads; kernel efficient |
| Index | Mmap | Random access, rare reads; pointer access via mmap |

### 7.2 Access Pattern Configuration

```go
var FileTypeAccessPattern = map[FileType]AccessPattern{
    FileTypeWAL:           AccessBuffered,  // Sequential writes
    FileTypeSegment:       AccessBuffered,  // Mixed reads/writes
    FileTypeExternalValue: AccessBuffered,  // Sequential reads
    FileTypeIndex:         AccessMapped,   // Random access
    FileTypeCheckpoint:    AccessMapped,   // Read rarely
}
```

### 7.3 Why OS Page Cache Over Integrated Cache

| Factor | OS Page Cache | Integrated Cache |
|--------|---------------|------------------|
| Code complexity | Simpler (no O_DIRECT) | Complex (alignment, epoch tracking) |
| Memory overhead | Minimal | Additional metadata per entry |
| Eviction control | Kernel-controlled | User-controlled |
| Read performance | Good for sequential | Better for random hot spots |
| Write throughput | Good | O_DIRECT may be better |
| **Overall score** | 64/100 | 66/100 |

**Conclusion**: OS Page Cache is the default. Integrated Cache is available as an alternative for specialized high-performance scenarios.

### 7.4 fsync Ordering

```
WAL must be synced before data pages for same transaction:
1. Sync WAL record
2. Sync data pages
This ensures durability ordering: committed transactions are recoverable.
```

---

## 8. Concurrency Model

### 8.1 Single-Writer, Multi-Reader

```go
type ConcurrencyModel struct {
    WriterMutex  sync.Mutex  // Serializes all write operations
    ReaderCount  atomic.Int64  // Tracks active readers
    WriteGate    chan struct{}  // Coordinate external operations
}
```

**Why single-writer?**
1. B-link splits require coordinating parent/child updates atomically
2. Single writer eliminates distributed deadlock detection
3. Append-only storage naturally serializes writes (unique VAddrs)
4. Write throughput is bounded by append bandwidth, not lock contention

### 8.2 B-link Latch Protocol

```go
type LatchMode int
const (
    LatchRead  LatchMode = iota  // Shared latch
    LatchWrite                    // Exclusive latch
)

// Latch crabbing: Acquire top-down, release bottom-up
type CrabbingProtocol struct {
    manager BLinkLatchManager
}
```

**Crabbing phases**:
1. **Search**: Acquire read latches, descend
2. **Modify**: If split: acquire write latch on parent, release children
3. **Update**: Update leaf, propagate changes upward

### 8.3 Lock-Free Read Optimization

B-link trees support lock-free reads via sibling chains:
- During split: all keys in left < splitKey < all keys in right
- Reader following sibling chain will eventually find correct key
- Limited retry on concurrent splits (typical: 1-2 retries)

---

## 9. Crash Recovery

### 9.1 Write-Ahead Log

```go
type WALRecord struct {
    LSN        uint64         // Monotonically increasing
    RecordType WALRecordType
    Length     uint32
    Checksum   uint32
    Payload    []byte
}

const (
    WALPageAlloc    WALRecordType = iota
    WALPageFree
    WALNodeWrite
    WALExternalValue
    WALRootUpdate
    WALCheckpoint
)
```

**Why WAL when storage is append-only?**
1. Append-only provides persistence but not transactional atomicity
2. Transaction may span multiple pages/nodes
3. WAL ensures we can recover partial transactions (redo)
4. Checkpoint can be verified against WAL for consistency

### 9.2 Checkpoint Strategy

```go
type Checkpoint struct {
    ID           uint64
    LSN          uint64  // All WAL records before this are included
    TreeRoot     VAddr
    PageManager  PageManagerSnapshot
    ExternalVals ExternalValueSnapshot
    Timestamp    uint64
}
```

**Checkpoint capture order**:
```
1. Capture segment manifest (all existing segments)
2. Capture index snapshot
3. Capture B-link tree root VAddr
4. Write checkpoint record to WAL
5. fsync WAL
```

### 9.3 Recovery Algorithm

```
Recover():
    1. Find latest valid checkpoint
    2. Load checkpoint state
    3. Replay WAL from checkpoint LSN to end
    4. Verify tree integrity
    5. Truncate WAL past checkpoint LSN
```

**Why redo-only, not undo?**
- Append-only storage never overwrites data
- Deleted data uses tombstones, not in-place removal
- Therefore, we only need to redo committed operations

---

## 10. Compaction Strategy

### 10.1 Compaction Triggers

```go
type CompactionTrigger struct {
    SpaceUsageThreshold float64  // Default: 40% garbage
    TimeInterval        Duration // Default: 1 hour
    MinSegmentCount     int      // Default: 3 sealed segments
}
```

### 10.2 Generational Compaction

```
Sealed Segments (ordered by age):
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│ Gen 0   │ │ Gen 1   │ │ Gen 2   │ │ Gen 3   │
│ (newest)│ │         │ │         │ │ (oldest)│
└─────────┘ └─────────┘ └─────────┘ └─────────┘
     │            │            │            │
     ▼            ▼            ▼            ▼
More garbage   Some garbage   Less garbage  Mostly garbage
(compact now)  (compact)      (maybe later) (definitely compact)
```

**Algorithm**:
```
Compact():
    1. Evaluate triggers
    2. Select oldest segments with highest garbage ratio
    3. Begin compaction pass (write to new active segment)
    4. Scan selected segments, extract live data
    5. Commit: swap old segments for compacted output
```

### 10.3 Epoch-Based Snapshot Safety

```go
const EpochGracePeriod = 3  // epochs

type EpochManager interface {
    RegisterEpoch() EpochID
    UnregisterEpoch(epoch EpochID)
    IsVisible(vaddr VAddr) bool
    IsSafeToReclaim(vaddr VAddr) bool
    MarkCompactionComplete(oldSegments []SegmentID)
}
```

**Why epochs instead of reference counting?**
- Reference counting requires tracking every VAddr across tree nodes
- Epochs amortize tracking cost: one epoch covers all VAddrs visible to that snapshot
- Simpler to implement correctly; fewer edge cases

### 10.4 Non-Blocking Design

```
┌─────────────────────────────────────────────────────────────────┐
│  Writer Pipeline                                                 │
│  ───────────────                                                │
│  Put(key, value):                                               │
│      1. Append to active segment                                │
│      2. Return success                                         │
│      (Compaction runs independently, in background)              │
└─────────────────────────────────────────────────────────────────┘
```

**Critical constraint**: Writer pipeline never blocks due to compaction. Segment rotation is synchronous; actual compaction happens asynchronously.

---

## 11. Index Persistence

### 11.1 DenseArray Storage Format

```
┌─────────────────────────────────────────────────────────────────┐
│  DenseArray Index File                                          │
├─────────────────────────────────────────────────────────────────┤
│  Header (64 bytes)                                              │
│  ├── Magic: "DAIDX\0\0\0"                                       │
│  ├── Version, IndexType, CheckpointLSN                          │
│  ├── PageIDBase, EntryCount, LiveEntryCount                     │
├─────────────────────────────────────────────────────────────────┤
│  Entry Array (24 bytes × Capacity)                             │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Entry: PageID(8) │ VAddr(16)                             │  │
│  └────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Footer (32 bytes)                                              │
│  ├── Checksum: uint64                                           │
│  └── EntryCountDuplicate                                        │
└─────────────────────────────────────────────────────────────────┘
```

### 11.2 RadixTree Node Format

```
RadixTreeNode (stored as page in segment):
┌─────────────────────────────────────────────────────────────────┐
│  NodeHeader (16 bytes)                                          │
│  ├── Magic: "RADX\0\0\0\0"                                       │
│  ├── NodeType, Height, EntryCount                               │
├─────────────────────────────────────────────────────────────────┤
│  For Internal Nodes (256 branches max):                        │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Branch: key_prefix(8) │ child_vaddr(16)                    │ │
│  └────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  For Leaf Nodes (256 slots max):                               │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Slot: page_id_low(8) │ vaddr(16)                         │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 11.3 Checkpoint Integration

**Critical invariant**: Index state at checkpoint LSN L only references segments that are included at L.

**Correct order**:
1. Capture segment manifest (includes all existing segments)
2. Capture index snapshot (can only reference manifest segments)
3. Write checkpoint record

---

## 12. Invariant Summary

### 12.1 VAddr Invariants

```go
// 1. VAddr uniqueness: No two allocations produce the same VAddr.
// 2. VAddr stability: A VAddr never changes after allocation.
// 3. Segment ID monotonicity: New segments have higher IDs.
// 4. Offset bounds: Offset < MaxSegmentSize for active segments.
// 5. Page alignment: All offsets are multiples of PageSize.
```

### 12.2 B-link-tree Invariants

```go
// 1. NodeImmutability: Once written, a node is never modified.
// 2. SiblingChain: Nodes form chains via HighSibling pointers.
// 3. KeyOrdering: Keys within a node are strictly increasing.
// 4. SplitBoundary: Left ≤ splitKey < right.
// 5. HighKeyInvariant: Internal node's HighKey = max key in rightmost child.
```

### 12.3 Concurrency Invariants

```go
// 1. WriterMutex held by exactly one goroutine at any time.
// 2. Latch acquisition order: root → leaf (top-down).
// 3. Latch release order: leaf → root (bottom-up).
// 4. Lock-free reads may retry; bounded by RetryLimit.
```

### 12.4 Recovery Invariants

```go
// 1. WAL records are never overwritten except via Truncate.
// 2. Checkpoint LSN indicates all prior records are durable.
// 3. Recovery from checkpoint + WAL replays exactly committed transactions.
// 4. Append-only storage ensures no partial page writes.
```

### 12.5 Compaction Invariants

```go
// 1. Snapshot safety: IsVisible checked before rewriting any entry.
// 2. Tombstone preservation: Tombstones rewritten until grace period expires.
// 3. No writer blocking: Compaction runs in background goroutine.
// 4. VAddr uniqueness: Compaction never reuses VAddrs.
```

### 12.6 Cache Invariants (OS Page Cache)

```go
// 1. Each file uses exactly one AccessPattern for its lifetime.
// 2. No file mixes mmap with write(2) or O_DIRECT with buffered I/O.
// 3. DirectIO buffers aligned to AlignmentBytes.
// 4. WAL must be synced before data pages for same transaction.
```

---

## Cross-Document References

All referenced documents use relative paths from `docs/`:

| Document | Reference |
|----------|-----------|
| API Layer | api-layer.md |
| B-link-tree | blinktree-node-format.md |
| VAddr Format | vaddr-format.md |
| Page Manager | page-manager.md |
| External Value Store | external-value-store.md |
| Concurrency & Recovery | concurrency-recovery.md |
| Compaction Strategy | compaction-strategy.md |
| OS Page Cache | os-page-cache.md |
| Integrated Cache | integrated-cache-strategy.md |
| Index Persistence | fixed-size-kvindex-persist.md |
| KV Store Design | kv-store-design.md |

---

## Type Summary

```go
// Core types
type VAddr struct { SegmentID uint64; Offset uint64 }  // 16 bytes
type PageID uint64
type SegmentID uint64

// Constants
const (
    PageSize              = 4096
    MaxInlineValueSize    = 48
    DefaultMaxValueSize   = 64 * 1024 * 1024
    EpochGracePeriod      = 3
)

// File types
const (
    FileTypeWAL           FileType = iota
    FileTypeSegment
    FileTypeExternalValue
    FileTypeIndex
    FileTypeCheckpoint
)
```

---

*Document Status: Integrated Solution*
*Incorporates: vaddr-format.md, blinktree-node-format.md, page-manager.md, fixed-size-kvindex-persist.md, external-value-store.md, api-layer.md, concurrency-recovery.md, compaction-strategy.md, os-page-cache.md, integrated-cache-strategy.md, kv-store-design.md*
*Last Updated: 2024*
