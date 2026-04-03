# Solution-A: Integrated KV Storage Architecture

## 1. Overview

This document integrates all design specifications into a coherent architecture for a high-performance key-value storage engine. The system uses B-link-tree as the primary index with append-only storage to achieve sequential write performance while maintaining efficient random reads.

### 1.1 Key Design Decisions (Finalized)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage Model | Append-only | Sequential writes, no random I/O, crash-safe |
| Primary Index | B-link-tree | Range queries, lock-free reads, right-biased splits |
| Address Format | VAddr = SegmentID(8) + Offset(8) | 16 bytes, extensible to 2^64 segments |
| Inline Value Threshold | ≤48 bytes | Fits in B-link node entry slot (56 bytes data area) |
| Page Size | 4KB | OS-aligned, efficient I/O |
| Concurrency | Single-writer, multi-reader | Simpler than multi-writer; append-only serializes writes |
| Recovery | WAL + Checkpoint, redo-only | Append-only needs no undo |
| Compaction | Generational segmented, epoch-based MVCC | Non-blocking, respects snapshot semantics |
| Cache Strategy | **OS Page Cache (CHOSEN)** | Score: 64/100 vs 66/100 for integrated; simpler, less code |

### 1.2 Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Layer                                │
│           (Get / Put / Delete / Scan / Batch / Transactions)   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     B-link-tree Index                           │
│   • 4KB page-aligned nodes                                     │
│   • Right-biased splits, latch crabbing                        │
│   • Inline values ≤48 bytes; larger via External Value Store   │
│   • Sibling chains for lock-free range traversal               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Page Manager                                 │
│   • FixedSizeKVIndex: PageID → VAddr mapping                   │
│   • DenseArray (default) or RadixTree for sparse IDs           │
│   • Persisted via checkpoint + WAL replay                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Append-only Storage                           │
│   • Segments: Active → Sealed → Compacted → Archived           │
│   • VAddr = SegmentID(8) + Offset(8)                           │
│   • OS Page Cache (buffered I/O) for all file types            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                 External Value Store                            │
│   • Values >48 bytes stored externally                         │
│   • VAddr stored inline in B-link node                         │
│   • Multi-page for large values (4064 bytes per page)          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Address Space (from vaddr-format.md)

### 2.1 VAddr Binary Format

```go
// VAddr encodes a physical address in the append-only address space.
// Invariant: VAddr is 16 bytes, never zero (SegmentID 0 is reserved).
type VAddr struct {
    SegmentID uint64  // Identifies the segment file (big-endian)
    Offset    uint64  // Byte offset within the segment (big-endian)
}

const (
    VAddrInvalid  = VAddr{SegmentID: 0, Offset: 0}  // Reserved for null
    VAddrMinValid = VAddr{SegmentID: 1, Offset: 0}  // Smallest valid address
)
```

**Why 16 bytes?**
- Allows 2^64 segments with 2^64 bytes per segment
- Big-endian enables natural segment comparisons
- Matches 4KB page alignment (Offset is always multiple of PageSize)

### 2.2 Segment Organization

```
┌─────────────────────────────────────────────────────────────────┐
│  Segment Header (32 bytes)                                     │
│  ├── Magic: "FASTSEG\0" (8 bytes)                             │
│  ├── Version: uint16                                           │
│  ├── SegmentID: uint64 (big-endian)                            │
│  ├── CreatedAt: uint64 (unix timestamp nanos)                  │
│  ├── Flags: uint16 (0x01=sealed, 0x02=archived)               │
│  └── Reserved: 6 bytes                                        │
├─────────────────────────────────────────────────────────────────┤
│  Data Pages (4KB aligned, variable count)                     │
├─────────────────────────────────────────────────────────────────┤
│  Segment Trailer (32 bytes)                                    │
│  ├── PageCount: uint64                                         │
│  ├── DataSize: uint64                                         │
│  ├── Checksum: uint64 (CRC64 of header + data)                │
│  └── Reserved: 8 bytes                                         │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Segment Lifecycle

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Active  │────▶│  Sealed  │────▶│ Compact   │────▶│ Archived │
│  (write) │     │  (read)  │     │ (target)  │     │  (cold)  │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
     │                │                │                │
  Only one         Full, no         Segments         Read-only,
  at a time       new writes       being rewritten  maybe moved
```

---

## 3. Storage Layer (from page-manager.md, os-page-cache.md)

### 3.1 Page Manager Interface

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

// PageManagerIndexEntry is fixed-size (24 bytes: 8 + 16).
type PageManagerIndexEntry struct {
    PageID PageID  // 8 bytes
    VAddr  VAddr   // 16 bytes
}
```

### 3.2 FixedSizeKVIndex Options

| Index Type | Structure | Lookup | Best For |
|------------|-----------|--------|----------|
| DenseArray | `[]VAddr` indexed by `pageID - 1` | O(1) | Dense, sequential PageIDs |
| RadixTree | 4-level tree, 16-bit splits | O(4) | Sparse PageIDs |

**Default**: DenseArray (simpler, ~25MB per 1M pages)

### 3.3 OS Page Cache Strategy (CHOSEN)

**Decision Rationale**: OS Page Cache scored 64/100 vs Integrated Cache 66/100. Marginally lower performance but significantly simpler implementation with less code.

```go
// File Type Access Patterns (from os-page-cache.md)
var FileTypeAccessPattern = map[FileType]AccessPattern{
    FileTypeWAL:           AccessBuffered,   // Sequential writes; kernel efficient
    FileTypeSegment:       AccessBuffered,   // Mixed reads/writes
    FileTypeExternalValue: AccessBuffered,   // Large sequential reads
    FileTypeIndex:         AccessMapped,     // Random access; mmap
    FileTypeCheckpoint:    AccessMapped,     // Read rarely
}
```

**Critical Invariant**: Only ONE caching strategy per file type. No double-buffering.

**If Integrated Cache is later adopted**:
- Must use O_DIRECT to bypass OS page cache
- Avoids double buffering between user-space and kernel caches

---

## 4. Index Layer (from blinktree-node-format.md)

### 4.1 B-link-tree Node Format

```go
// NodeFormat header (32 bytes, 64-byte aligned)
type NodeFormat struct {
    NodeType     uint8   // 0=Leaf, 1=Internal
    IsDeleted    uint8   // Soft delete flag
    Level        uint8   // 0=leaf, 1+=internal levels
    Count        uint8   // Number of entries
    Capacity     uint16  // Total entry slots
    Reserved     uint16
    
    HighSibling  VAddr   // Pointer to next node at same level
    LowSibling   VAddr   // Pointer to previous node
    HighKey      PageID  // Max key in subtree (internal only)
    Checksum     uint32  // CRC32c
}

// InlineValue: 64 bytes total
type InlineValue struct {
    Length [8]byte   // Big-endian (top bit = is_external flag)
    Data   [56]byte  // Inline data or VAddr
}

const (
    MaxInlineValueSize = 55    // 56 - 1 length byte
    ExternalThreshold  = 48    // Values >48 bytes get externalized
)
```

### 4.2 Node Capacity Calculation

```
Page size: 4096 bytes
Header: 32 bytes
Entry slots: (4096 - 32) / (8 + 64) = 4064 / 72 ≈ 56 entries max

Leaf entry: Key(8) + InlineValue(64) = 72 bytes
Internal entry: Key(8) + VAddr(16) = 24 bytes
```

### 4.3 Concurrency Protocol

```go
// Latch Crabbing Protocol (from concurrency-recovery.md)
//
// Write path:
//   1. Acquire write latch on root
//   2. Descend, acquiring write latches (latch crabbing)
//   3. At leaf: perform insert
//   4. If split: create new node, update parent
//   5. Release all latches bottom-up
//
// Read path (lock-free optimization):
//   1. Load root (may be stale if split in progress)
//   2. Search for key (descend via child pointers)
//   3. If key > HighKey: follow HighSibling to find split target
//   4. Retry if node splits during read
```

### 4.4 Design Decisions

| Decision | Alternative | Why Not |
|----------|-------------|---------|
| VAddr in entries | PageID | Direct lookup avoids double-indirection |
| HighKey in internal | None | O(1) routing without loading children |
| Right-biased split | Left-biased | Simpler boundary condition |
| InlineValue 64-byte slot | Separate pointer | Cache-friendly for small values |
| Sibling pointers | None | Lock-free internal node traversal |

---

## 5. External Value Store (from external-value-store.md)

### 5.1 Threshold and Encoding

Values ≤48 bytes: stored inline in B-link node's InlineValue.Data field.
Values >48 bytes: stored externally, VAddr stored in InlineValue.Data.

```go
// InlineValue encoding for external reference
func (v *InlineValue) IsExternal() bool {
    return v.Length[0]&0x80 != 0  // Top bit set = external
}

func (v *InlineValue) GetExternalVAddr() VAddr {
    // Extract VAddr from Data field
}
```

### 5.2 External Value Page Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  External Value Page (4096 bytes)                              │
├─────────────────────────────────────────────────────────────────┤
│  ExternalValueHeader (32 bytes)                                │
│  ├── Magic: "EXTVAL\0\0" (8 bytes)                             │
│  ├── Version: uint16                                           │
│  ├── ValueSize: uint64 (total bytes)                           │
│  ├── Flags: uint16                                              │
│  └── Reserved: [4]byte                                          │
├─────────────────────────────────────────────────────────────────┤
│  ValueData (4064 bytes per page)                               │
│  └── First portion of value data                               │
└─────────────────────────────────────────────────────────────────┘
```

**Multi-page values**: Contiguous pages allocated; header on first page only.

---

## 6. API Layer (from api-layer.md)

### 6.1 Core Interface

```go
type KVStore interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
    Close() error
}

// Iterator provides sequential access with snapshot semantics.
type Iterator interface {
    Next() bool
    Key() []byte
    Value() []byte
    Error() error
    Close()
}
```

### 6.2 Error Types

```go
var (
    ErrKeyNotFound    = errors.New("key not found")
    ErrStoreClosed    = errors.New("store is closed")
    ErrTransactionAborted = errors.New("transaction aborted")
    ErrStoreFull      = errors.New("store is full")
    ErrKeyTooLarge    = errors.New("key too large")
    ErrValueTooLarge  = errors.New("value too large")
)
```

### 6.3 Transaction Model

```go
type Transaction interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
    Commit() error
    Rollback()
    TxID() uint64
}

type KVStoreWithTransactions interface {
    KVStore
    Begin() (Transaction, error)
}
```

**Why snapshot isolation?**
- MVCC pairs naturally with append-only storage
- Each transaction reads from a snapshot (root VAddr at Begin)
- Writers create new versions; old versions persist until compaction

---

## 7. Concurrency Model (from concurrency-recovery.md)

### 7.1 Single-Writer/Multi-Reader

```go
type ConcurrencyModel struct {
    WriterMutex sync.Mutex    // Serializes all writes
    ReaderCount atomic.Int64  // Active readers for writer notification
    WriteGate   chan struct{} // Block writers during critical sections
}
```

**Why single-writer?**
1. B-link splits require coordinating parent/child updates atomically
2. Single writer eliminates distributed deadlock detection
3. Append-only storage naturally serializes writes (unique VAddrs)

### 7.2 B-link Latch Protocol

```go
type BLinkLatchManager interface {
    Acquire(vaddr VAddr, mode LatchMode)    // LatchRead or LatchWrite
    Release(vaddr VAddr, mode LatchMode)
    TryAcquire(vaddr VAddr, mode LatchMode) bool
    Upgrade(vaddr VAddr) error
}
```

**Key invariant**: Latches acquired top-down, released bottom-up.

---

## 8. Recovery System (from concurrency-recovery.md, fixed-size-kvindex-persist.md)

### 8.1 Write-Ahead Log Format

```go
type WALRecord struct {
    LSN        uint64       // Log sequence number
    RecordType WALRecordType
    Length     uint32
    Checksum   uint32
    Payload    []byte
}

const (
    WALPageAlloc     WALRecordType = iota  // Page allocation
    WALPageFree                            // Page deallocation
    WALNodeWrite                           // B-link node written
    WALExternalValue                       // External value stored
    WALRootUpdate                          // Tree root changed
    WALCheckpoint                          // Checkpoint marker
)
```

**Why WAL when storage is append-only?**
1. Append-only provides persistence but not transactional atomicity
2. A transaction may span multiple pages/nodes
3. WAL ensures we can recover partial transactions (redo)
4. Checkpoint can be verified against WAL for consistency

### 8.2 Checkpoint Strategy

```go
type CheckpointPolicy struct {
    Interval       time.Duration  // Default: 30 seconds
    WALSizeLimit   uint64         // Default: 64 MB
    DirtyPageLimit int            // Default: 1000 pages
}
```

### 8.3 Recovery Algorithm

```
Recover():
    1. Find latest valid checkpoint
    2. Load checkpoint state (tree root, index snapshot, segment manifest)
    3. Replay WAL from checkpoint LSN to end (redo-only)
    4. Verify tree integrity
    5. Truncate WAL past checkpoint LSN
```

### 8.4 Index Persistence

```go
// DenseArray (default): Full snapshot at checkpoint
// RadixTree: Only root VAddr persisted; WAL replay required

// Checkpoint capture order (CRITICAL):
// 1. Capture segment manifest first (includes all existing segments)
// 2. Capture index snapshot (can only reference manifest segments)
// 3. Write checkpoint record
// 4. fsync WAL
```

---

## 9. Compaction Strategy (from compaction-strategy.md)

### 9.1 Compaction Triggers

```go
type CompactionTrigger struct {
    SpaceUsageThreshold float64  // Default: 40%
    TimeInterval        time.Duration  // Default: 1 hour
    MinSegmentCount     int      // Default: 3
}
```

### 9.2 Generational Compaction Algorithm

```
Compact():
    1. Evaluate triggers; return if not satisfied
    2. Select oldest sealed segments (highest garbage ratio)
    3. Begin compaction pass (new output segment)
    4. Scan selected segments, extract live entries
    5. For each live entry:
       - Rewrite to new segment
       - Update PageManager mapping (oldVAddr → newVAddr)
    6. Commit: swap old segments for compacted output
    7. Mark old segments as archived
```

### 9.3 Epoch-Based MVCC

```go
type EpochManager interface {
    RegisterEpoch() EpochID
    UnregisterEpoch(epoch EpochID)
    IsVisible(vaddr VAddr) bool
    IsSafeToReclaim(vaddr VAddr) bool
}

const EpochGracePeriod = 3  // Epochs
```

**Why epoch-based instead of reference counting?**
- Reference counting requires tracking every VAddr across tree nodes (complex)
- Epochs amortize tracking: one epoch per snapshot
- Grace period absorbs slow readers
- Simpler to implement correctly

### 9.4 Critical Invariant: Non-Blocking Compaction

```
Writer Pipeline:          Compaction:
     │                        │
     ▼                        ▼
  Allocate                 Read sealed
  VAddr                     segments
     │                        │
     ▼                        ▼
  Append to              Write live data
  active segment          to new segment
     │                        │
     ▼                        ▼
  Return success         Mark old archived
                         (async, background)
```

**Separation of concerns**: Writer never blocks on compaction; compaction reads sealed segments only.

---

## 10. Cache Strategy Summary

### 10.1 OS Page Cache (CHOSEN)

| File Type | Access Pattern | Rationale |
|-----------|---------------|-----------|
| WAL | Buffered | Sequential writes; kernel efficient |
| Segments | Buffered | Mixed reads/writes |
| External Values | Buffered | Large sequential reads |
| Index | Mmap | Random access |
| Checkpoint | Mmap | Read rarely |

**Performance Score**: 64/100 (vs 66/100 for integrated cache)

**Advantages**:
- Simpler implementation
- Kernel optimized for common patterns
- No memory management complexity
- Less code to maintain

### 10.2 Double-Buffering Prevention

**Critical Invariant**: If Integrated Cache is later adopted, must use O_DIRECT to bypass OS page cache.

```go
// File access pattern selection
var FileTypeAccessPattern = map[FileType]AccessPattern{
    // All files use OS page cache (CHOSEN)
    FileTypeWAL:           AccessBuffered,
    FileTypeSegment:       AccessBuffered,
    FileTypeExternalValue: AccessBuffered,
    FileTypeIndex:         AccessMapped,
    FileTypeCheckpoint:    AccessMapped,
}

// Future: Integrated Cache option would require:
FileTypeSegment: AccessDirectIO  // O_DIRECT bypasses kernel cache
```

---

## 11. Data Flow

### 11.1 Write Path

```
Put(key, value):
    1. Validate key/value size
    2. Register new epoch (if transaction)
    3. Allocate PageID and VAddr
    4. If value > 48 bytes:
       - Store externally, get VAddr
    5. Encode as InlineValue
    6. B-link insert (with latch crabbing):
       - Search to leaf
       - Insert entry
       - Split if needed (right-biased)
    7. Write WAL record (PageAlloc, NodeWrite)
    8. Update PageManager mapping
    9. Sync WAL (for durability)
    10. Return success
```

### 11.2 Read Path

```
Get(key):
    1. Load root VAddr (snapshot semantics)
    2. B-link search (lock-free or with latches):
       - Descend via child pointers
       - If key > HighKey: follow HighSibling
    3. At leaf: find key
    4. If InlineValue.IsExternal():
       - Retrieve from External Value Store
    5. Return value or ErrKeyNotFound
```

### 11.3 Recovery Path

```
Recover():
    1. Find latest checkpoint
    2. Load tree root, index snapshot, segment manifest
    3. Replay WAL from checkpoint LSN:
       - PageAlloc: Rebuild PageManager mapping
       - NodeWrite: Validate node structure
       - RootUpdate: Set new tree root
    4. Verify integrity
    5. Truncate WAL
```

---

## 12. Module Boundaries and Interfaces

### 12.1 Public Interfaces

```go
// Storage layer (vaddr-format.md)
type VAddr struct { SegmentID uint64; Offset uint64 }
type SegmentManager interface { ... }

// Page layer (page-manager.md)
type PageManager interface { GetVAddr, AllocatePage, FreePage, UpdateMapping, Flush }
type FixedSizeKVIndex interface { Get, Put, Len, RangeQuery }

// Index layer (blinktree-node-format.md)
type NodeOperations interface { Search, Insert, Split, Serialize, Deserialize }
type NodeManager interface { CreateLeaf, CreateInternal, Persist, Load }

// External values (external-value-store.md)
type ExternalValueStore interface { Store, Retrieve, Delete, GetValueSize }

// API layer (api-layer.md)
type KVStore interface { Get, Put, Delete, Scan, Close }

// Concurrency (concurrency-recovery.md)
type BLinkLatchManager interface { Acquire, Release, TryAcquire, Upgrade }
type WAL interface { Append, Truncate, Replay }

// Compaction (compaction-strategy.md)
type EpochManager interface { RegisterEpoch, UnregisterEpoch, IsVisible, IsSafeToReclaim }
type CompactionPolicy interface { ShouldCompact, TriggerType }
```

---

## 13. Key Invariants Summary

### Address Space
- VAddr is immutable after assignment (append-only semantics)
- SegmentID > 0 for valid addresses
- Offset always aligned to PageSize (4096 bytes)
- No VAddr reuse after free

### B-link Tree
- Nodes are immutable after creation (append-only)
- Sibling chains maintained for lock-free traversal
- Right-biased splits
- HighKey equals max key in rightmost child subtree

### Concurrency
- WriterMutex held by exactly one goroutine
- Latch acquisition: root → leaf (top-down)
- Latch release: leaf → root (bottom-up)
- Lock-free reads may retry; bounded by RetryLimit

### Recovery
- WAL records never overwritten (except via Truncate)
- Checkpoint LSN indicates all prior records durable
- Redo-only recovery (append-only needs no undo)
- Checkpoint ordering: segments → index → WAL record

### Compaction
- Snapshot safety: IsVisible checked before rewriting
- Tombstones preserved until epoch grace period expires
- Non-blocking: runs in background goroutine
- VAddr uniqueness maintained (no reuse)

---

## 14. Why Not Alternatives

| Alternative | Why Rejected |
|-------------|--------------|
| Integrated cache over OS cache | Double buffering; complexity for 2-point performance gain |
| Single uint64 VAddr | Limits segment size to 2^32 bytes |
| Left-biased B-link splits | More complex boundary conditions |
| Undo-redo recovery | Append-only never overwrites; undo unnecessary |
| Immediate reclamation on Delete | Violates MVCC snapshot guarantees |
| Reference counting per VAddr | Complex, error-prone, high overhead |
| mmap for all file types | Truncation complexity with compaction |

---

## 15. Related Documents

| Document | Key Content |
|----------|-------------|
| vaddr-format.md | VAddr binary format, segment structure |
| page-manager.md | PageID → VAddr mapping, allocation |
| blinktree-node-format.md | B-link node format, operations |
| api-layer.md | Public API, errors, transactions |
| external-value-store.md | Large value storage |
| concurrency-recovery.md | WAL, checkpoint, latch protocol |
| compaction-strategy.md | Epoch-based compaction |
| os-page-cache.md | OS page cache strategy (CHOSEN) |
| integrated-cache-strategy.md | Integrated cache (NOT CHOSEN) |
| kv-store-design.md | High-level overview |
| fixed-size-kvindex-persist.md | Index persistence, recovery |

---

*Document Status: Integrated Solution*
*Last Updated: 2024*
