# Integrated KV Storage Solution

**Status**: Complete Integrated Design  
**Derived from**: All 11 design documents in `docs/*.md`  
**Finalized**: Key architectural decisions resolved

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [VAddr Format and Storage](#2-vaddr-format-and-storage)
3. [B-link-tree Index](#3-b-link-tree-index)
4. [Page Manager](#4-page-manager)
5. [Index Persistence](#5-index-persistence)
6. [External Value Store](#6-external-value-store)
7. [API Layer](#7-api-layer)
8. [Concurrency Model](#8-concurrency-model)
9. [Crash Recovery](#9-crash-recovery)
10. [Compaction Strategy](#10-compaction-strategy)
11. [Cache Strategy](#11-cache-strategy)
12. [Type Definitions Summary](#12-type-definitions-summary)
13. [Invariant Summary](#13-invariant-summary)
14. [Cross-Document References](#14-cross-document-references)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        API Layer                                    │
│              (Get / Put / Delete / Scan / Batch / TX)               │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      B-link-tree Index                              │
│    • Leaf nodes: (key, inline_value) or (key, external_vaddr)       │
│    • Range queries via sibling chains                               │
│    • All mutations append-only                                      │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Append-only Storage                              │
│    • Sequential writes (solves random-write problem)                │
│    • VAddr = SegmentID(8) + Offset(8) = 16 bytes                     │
│    • Segments: Active → Sealed → Archived                           │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Page Manager                                   │
│    • PageID → VAddr mapping (Dense Array default)                   │
│    • 4KB page allocation/deallocation                               │
│    • Fixed-size entries: 24 bytes (8 + 16)                         │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   External Value Store                              │
│    • Values > 48 bytes stored externally                            │
│    • Referenced by VAddr in tree entries                            │
│    • Contiguous page allocation                                     │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Storage Files                                    │
│    • WAL: Write-Ahead Log for crash recovery                        │
│    • Segments: Data storage (appended, never overwritten)           │
│    • Checkpoints: Consistent snapshots for fast recovery            │
│    • OS Page Cache: Default caching strategy                        │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage Model | Append-only | Sequential writes; no random I/O |
| Primary Index | B-link-tree | Range queries; lock-free reads |
| Address Format | VAddr = SegmentID(8) + Offset(8) | 16 bytes; extensible |
| Inline Value | ≤48 bytes | Fits in node entry slot (56 bytes data) |
| Page Mapping | Dense Array | O(1) lookup; best for OS Page Cache path |
| Concurrency | Single-writer, multi-reader | Simpler; append-only serializes writes |
| Recovery | WAL + Checkpoint | Redo-only (append-only needs no undo) |
| Compaction | Generational segmented | Non-blocking; epoch-based MVCC |
| Cache | **OS Page Cache (default)** | Kernel optimized; less code; reliability 64/100 |

---

## 2. VAddr Format and Storage

### 2.1 VAddr Binary Layout

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
func (v VAddr) IsValid() bool {
    return v.SegmentID != 0
}
```

### 2.2 Reserved Values

```go
const (
    VAddrInvalid = VAddr{SegmentID: 0, Offset: 0}  // Null/invalid address
    VAddrMinValid = VAddr{SegmentID: 1, Offset: 0}
)
```

### 2.3 Segment File Format

```
┌─────────────────────────────────────────────────────────────────────┐
│  Segment Header (32 bytes)                                         │
├─────────────────────────────────────────────────────────────────────┤
│  Magic: "FASTSEG" (8 bytes)                                        │
│  Version: uint16                                                    │
│  SegmentID: uint64 (big-endian)                                     │
│  CreatedAt: uint64 (unix timestamp nanos)                           │
│  Flags: uint16 (0x01=sealed, 0x02=archived)                          │
│  Reserved: 6 bytes                                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Data Pages (variable, aligned to 4KB)                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                             │
│  │  Page 0  │ │  Page 1  │ │  Page N  │                             │
│  └──────────┘ └──────────┘ └──────────┘                             │
├─────────────────────────────────────────────────────────────────────┤
│  Segment Trailer (32 bytes)                                        │
├─────────────────────────────────────────────────────────────────────┤
│  PageCount: uint64                                                  │
│  DataSize: uint64                                                   │
│  Checksum: uint64 (CRC64 of header + data)                         │
│  Reserved: 8 bytes                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.4 Segment Lifecycle

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Active   │────▶│ Sealed   │────▶│ Compact  │────▶│ Archived │
│ (write)  │     │ (read)   │     │ (target) │     │ (cold)   │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
     │                │                │                │
  Only one        Full, no         Segments         Read-only,
  at a time       new writes       being rewritten  maybe moved
```

### 2.5 Page Structure

```go
const PageSize = 4096  // Aligned with OS page size

// Page represents a fixed-size data page.
// Invariant: Page is always PageSize bytes.
type Page struct {
    ID       PageID   // Logical identifier
    Checksum uint32   // CRC32 of page data
    Flags    uint16   // Page flags
    Data     [4080]byte
}
```

### 2.6 Invariants

```go
// 1. VAddr uniqueness: No two allocations produce the same VAddr.
//    Enforced by: AllocateVAddr is single-threaded for active segment.

// 2. VAddr stability: A VAddr never changes after allocation.
//    Enforced by: Append-only semantics; segments immutable once sealed.

// 3. Segment ID monotonicity: New segments have higher IDs than old.
//    Enforced by: SegmentID counter is atomic/increment-only.

// 4. Offset bounds: Offset < MaxSegmentSize for active segments.
//    Enforced by: Segment rotation when offset would exceed limit.

// 5. Page alignment: All offsets are multiples of PageSize.
//    Enforced by: AllocateVAddr rounds up to PageSize boundary.
```

---

## 3. B-link-tree Index

### 3.1 Node Format

```go
// NodeFormat is the binary layout for both internal and leaf nodes.
type NodeFormat struct {
    // Common header (64 bytes, cache-line aligned)
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
    
    _            [4]byte // Padding to 64-byte header alignment
}

// LeafEntry stores key-value pairs in leaf nodes.
type LeafEntry struct {
    Key     PageID        // Fixed-size key (8 bytes)
    Value   InlineValue   // Inline value or pointer (64 bytes)
}

// InternalEntry stores separator keys and child pointers.
type InternalEntry struct {
    Key     PageID        // Separator key (8 bytes)
    Child   VAddr         // Pointer to child node (16 bytes)
}

// InlineValue encodes both inline values and external references.
type InlineValue struct {
    Length [8]byte        // Big-endian length (top bit = is_external flag)
    Data   [56]byte       // Inline data or VAddr of external value
}

// ExternalThreshold defines when values are stored externally.
// Values > 48 bytes: stored externally, VAddr stored in InlineValue.Data
// Values ≤ 48 bytes: stored inline in InlineValue.Data
//
// Why 48 bytes?
// InlineValue.Data has 56 bytes. When external:
// - 1 byte for length encoding = 55 bytes remaining
// - 16 bytes for VAddr = 39 bytes remaining
// - 48 leaves room for length prefix + alignment
const ExternalThreshold = 48
```

### 3.2 Node Operations Interface

```go
type NodeOperations interface {
    // Search finds the child index for key K.
    Search(node *NodeFormat, key PageID) int

    // Insert adds (key, value) to leaf node. Returns (newNode, splitKey).
    Insert(node *NodeFormat, key PageID, value InlineValue) (*NodeFormat, PageID)

    // Split divides node at median key. Returns (left, right, splitKey).
    Split(node *NodeFormat) (*NodeFormat, *NodeFormat, PageID)

    // UpdateHighKey recomputes HighKey from rightmost child.
    UpdateHighKey(node *NodeFormat) PageID

    // Serialize returns binary representation for append-only storage.
    Serialize(node *NodeFormat) []byte

    // Deserialize parses binary representation from storage.
    Deserialize(data []byte) (*NodeFormat, error)
}
```

### 3.3 Node Manager Interface

```go
type NodeManager interface {
    // CreateLeaf initializes a new empty leaf node.
    CreateLeaf() (*NodeFormat, VAddr)

    // CreateInternal initializes a new internal node with given level.
    CreateInternal(level uint8) (*NodeFormat, VAddr)

    // Persist appends node to append-only storage.
    Persist(node *NodeFormat) VAddr

    // Load reads node from storage by VAddr.
    Load(vaddr VAddr) (*NodeFormat, error)

    // UpdateParent updates parent node's child pointer after split.
    UpdateParent(parentVAddr VAddr, oldChild, newChild VAddr, splitKey PageID) error
}
```

### 3.4 Concurrency Patterns

**Write Path:**
1. Acquire write latch on node
2. Perform operation
3. If split: create new node, update parent (append)
4. Release latch

**Read Path (optimistic, no latches):**
1. Start at root
2. Search for key
3. If HighSibling exists for target key: load sibling, retry
4. If node was split during read: retry from root

### 3.5 Design Decisions

| Decision | Alternative | Why Not |
|----------|-------------|---------|
| VAddr in entries | PageID | Direct lookup avoids double-indirection |
| HighKey in internal nodes | None | O(1) routing; no child load needed |
| Right-biased split | Left-biased | Simpler boundary condition |
| InlineValue in 64-byte slot | Separate pointer | Cache-friendly for small values |
| Sibling pointers on internal | None | Lock-free internal node traversal |

---

## 4. Page Manager

### 4.1 Type Definitions

```go
// PageID is a logical identifier for a page.
// Invariant: PageID > 0 (0 is reserved for invalid/null).
type PageID uint64

const PageIDInvalid PageID = 0

// PageManagerIndexEntry is a fixed-size key-value pair.
// Invariant: Entry is always 24 bytes (8 + 16).
type PageManagerIndexEntry struct {
    PageID PageID  // 8 bytes
    VAddr  VAddr   // 16 bytes
}
```

### 4.2 Interface Specification

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

### 4.3 Index Structure Options

**4.3.1 Dense Array (Default — Recommended for OS Page Cache)**

```go
// Why Dense Array for OS Page Cache?
// - O(1) lookup is critical for VAddr resolution
// - Sequential PageID allocation (typical in B-link-tree) = dense IDs
// - Simpler than Radix Tree; less memory overhead
// - OS Page Cache handles I/O efficiently for sequential access
```

**4.3.2 Radix Tree (For Sparse Page IDs)**

```go
// Why Radix Tree?
// - Handles sparse page IDs efficiently
// - O(4) lookup (4 levels × 16 bits per level)
// - ~24 bytes overhead per entry
// - Use when PageIDs are non-sequential or sparse
```

### 4.4 Free List Management

```go
// FreeList manages reclaimable pages for reuse.
// Design: Lock-free stack of PageIDs.
// Invariant: Freed pages are reused before allocating new PageIDs.
type FreeList interface {
    Pop() (PageID, bool)   // Returns freed PageID or false if empty
    Push(pageID PageID)     // Adds to free list
    Len() uint64            // Number of pages on free list
    Clear()                 // Remove all entries
}
```

### 4.5 Allocation Strategy

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

---

## 5. Index Persistence

### 5.1 Index Type Specification

```go
// IndexType identifies the underlying index structure.
type IndexType uint8

const (
    IndexTypeDenseArray IndexType = iota  // O(1) lookup, best for dense PageIDs
    IndexTypeRadixTree                    // O(k) lookup, best for sparse PageIDs
)

// Default: IndexTypeDenseArray (better for OS Page Cache path)
```

### 5.2 DenseArray File Format

```
┌─────────────────────────────────────────────────────────────────────┐
│  DenseArray Index File                                               │
├─────────────────────────────────────────────────────────────────────┤
│  Header (64 bytes)                                                   │
│  ├── Magic: "DAIDX\0\0\0" (8 bytes)                                  │
│  ├── Version: uint16                                                │
│  ├── IndexType: uint8 (= IndexTypeDenseArray)                       │
│  ├── CheckpointLSN: uint64                                          │
│  ├── PageIDBase: uint64 (first PageID in this array)                │
│  ├── EntryCount: uint64                                             │
│  ├── LiveEntryCount: uint64                                         │
│  ├── ArrayCapacity: uint64                                          │
│  └── Reserved: 14 bytes                                             │
├─────────────────────────────────────────────────────────────────────┤
│  Entry Array (24 bytes × Capacity)                                   │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  Entry[0]: PageID(8) │ VAddr(16)                             │    │
│  │  Entry[1]: PageID(8) │ VAddr(16)                             │    │
│  │  ...                                                            │    │
│  └──────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│  Footer (32 bytes)                                                   │
│  ├── Checksum: uint64                                                │
│  ├── EntryCountDuplicate: uint64                                     │
│  ├── LiveEntryCountDuplicate: uint64                                │
│  └── Reserved: 8 bytes                                              │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.3 RadixTree File Format

Radix tree nodes are stored in segment files (like B-link nodes). Only the root VAddr is persisted in the manifest:

```go
// RadixTreeManifestEntry is stored in the index manifest.
type RadixTreeManifestEntry struct {
    IndexType       IndexType  // Must be IndexTypeRadixTree
    RootVAddr       VAddr      // VAddr of root RadixNode
    NodeCount       uint64     // Total nodes in tree
    CheckpointLSN   uint64     // LSN of checkpoint
    Height          uint8      // Tree height (typically 4)
}
```

### 5.4 Recovery Algorithm

```
DenseArrayRebuild():
    1. Load index file header
    2. Scan B-link tree from root:
       - For each leaf node, extract all (PageID, VAddr) pairs
       - Build live_entries map
    3. Reconstruct array from live_entries
    4. Replay WAL from CheckpointLSN

RadixTreeRebuild():
    1. Load RadixTreeManifestEntry from index manifest
    2. Load root node from RootVAddr
    3. Replay WAL from CheckpointLSN (required for RadixTree)
```

**Why WAL replay differs:**
- DenseArray: Full snapshot written at checkpoint
- RadixTree: Only root VAddr persisted; updates after checkpoint are lost without WAL

---

## 6. External Value Store

### 6.1 Value Classification

```go
// Value size 1-48 bytes: InlineValue { length: size, data: [size]bytes }
// Value size 49+ bytes:  InlineValue { length: 16 | 0x8000, data: VAddr }
```

### 6.2 External Value Storage Format

```
┌─────────────────────────────────────────────────────────────────────┐
│  External Value Page(s)                                             │
├─────────────────────────────────────────────────────────────────────┤
│  Page 0 (4096 bytes)                                                 │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  ExternalValueHeader (32 bytes)                              │   │
│  │  ├── Magic: "EXTVAL\0\0" (8 bytes)                           │   │
│  │  ├── Version: uint16 (currently 1)                            │   │
│  │  ├── ValueSize: uint64                                       │   │
│  │  └── Reserved: [4]byte                                       │   │
│  ├──────────────────────────────────────────────────────────────┤   │
│  │  ValueData (4064 bytes on page 0)                             │   │
│  └──────────────────────────────────────────────────────────────┘   │
│  Page 1-N (4096 bytes each, if value spans multiple pages)          │
└─────────────────────────────────────────────────────────────────────┘
```

### 6.3 Interface Specification

```go
type ExternalValueStore interface {
    // Store persists a value and returns its VAddr.
    Store(value []byte) (VAddr, error)

    // Retrieve reads the complete value at the given VAddr.
    Retrieve(vaddr VAddr) ([]byte, error)

    // RetrieveAt reads a slice of the value without loading entire value.
    RetrieveAt(vaddr VAddr, offset, length uint64) ([]byte, error)

    // Delete marks a value for future reclamation.
    Delete(vaddr VAddr) error

    // GetValueSize returns the size of the value at vaddr without loading data.
    GetValueSize(vaddr VAddr) (uint64, error)
}
```

### 6.4 Constants

```go
const (
    ExternalValueHeaderSize  = 32
    ExternalValueDataPerPage = PageSize - ExternalValueHeaderSize  // 4064 bytes
    ExternalValueMagic       = "EXTVAL\0\0"
    ExternalValueVersion     = 1
    DefaultMaxValueSize      = 64 * 1024 * 1024  // 64 MB
)
```

---

## 7. API Layer

### 7.1 Core Interface

```go
// KVStore is the primary interface for key-value operations.
// Thread-safe: all operations are safe for concurrent use.
type KVStore interface {
    // Get retrieves the value for key.
    Get(key []byte) ([]byte, error)

    // Put stores a key-value pair.
    // Invariant: Put with value > 48 bytes stores externally per B-link-tree spec.
    Put(key, value []byte) error

    // Delete removes a key-value pair.
    Delete(key []byte) error

    // Scan returns an iterator over keys in range [start, end).
    Scan(start, end []byte) (Iterator, error)

    // Close releases resources held by the store.
    Close() error
}
```

### 7.2 Error Types

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

### 7.3 Iterator Interface

```go
// Iterator provides sequential access to a range of key-value pairs.
type Iterator interface {
    Next() bool
    Key() []byte
    Value() []byte
    Error() error
    Close()
}
```

### 7.4 Transaction Interface

```go
// Transaction provides atomic multi-key operations.
type Transaction interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
    Commit() error
    Rollback()
    TxID() uint64
}

// KVStoreWithTransactions extends KVStore with transaction support.
type KVStoreWithTransactions interface {
    KVStore
    Begin() (Transaction, error)
    BeginWithOptions(opts TransactionOptions) (Transaction, error)
}
```

### 7.5 Batch Interface

```go
// Batch performs multiple operations atomically.
type Batch interface {
    Put(key, value []byte)
    Delete(key []byte)
    Commit() error
    Reset()
}
```

### 7.6 Configuration

```go
type Config struct {
    Directory         string
    MaxKeySize        uint32      // Default: 1024
    MaxValueSize      uint64      // Default: 64 MB
    ReadOnly          bool
    SyncWrites        bool        // Default: true (durable)
    CacheSizeMB       uint32      // Default: 0 (OS Page Cache)
    BLinkTreeNodeSize uint32      // Default: 64 KB
}

// Factory functions
func Open(directory string, config *Config) (KVStore, error)
func OpenWithTransactions(directory string, config *Config) (KVStoreWithTransactions, error)
func Destroy(directory string) error
```

---

## 8. Concurrency Model

### 8.1 Single-Writer/Multi-Reader Model

```go
type ConcurrencyModel struct {
    WriterMutex  sync.Mutex
    ReaderCount  atomic.Int64
    WriteGate    chan struct{}
}
```

**Why single-writer?**
1. B-link splits require coordinating parent/child updates atomically
2. Multiple writers would require distributed locking across splits
3. Append-only storage naturally serializes writes (unique VAddrs)
4. Write throughput is bounded by append bandwidth, not lock contention

**Why multi-reader?**
1. Reads are 10x-100x more frequent than writes in typical KV workloads
2. B-link trees support lock-free reads via sibling chains
3. Readers never block each other; only wait for writers

### 8.2 Latch Protocol

```go
type LatchMode int

const (
    LatchRead  LatchMode = iota  // Shared latch
    LatchWrite                   // Exclusive latch
)

// BLinkLatchManager manages latches on B-link tree nodes.
type BLinkLatchManager interface {
    Acquire(vaddr VAddr, mode LatchMode)
    Release(vaddr VAddr, mode LatchMode)
    TryAcquire(vaddr VAddr, mode LatchMode) bool
    Upgrade(vaddr VAddr) error
}
```

### 8.3 Latch Crabbing Protocol

```
Phase 1 (Search): Acquire read latches, descend
Phase 2 (Modify): If split: acquire write latch on parent, release children
Phase 3 (Update): Update leaf, propagate changes upward
```

### 8.4 Lock-Free Read Optimization

```go
// SearchLockFree traverses tree without latches, using sibling chains.
// During split: all keys in left < splitKey < all keys in right.
// Reader following sibling chain will eventually find correct key.
```

---

## 9. Crash Recovery

### 9.1 Write-Ahead Log

```go
type WALRecordType uint8

const (
    WALPageAlloc      WALRecordType = iota  // Page allocation
    WALPageFree                            // Page deallocation
    WALNodeWrite                           // B-link node written
    WALExternalValue                       // External value stored
    WALRootUpdate                          // Tree root changed
    WALCheckpoint                          // Checkpoint marker
    WALIndexUpdate                         // Index mutation (RadixTree)
    WALIndexRootUpdate                     // RadixTree root changed
)

// WALRecord is a single record in the write-ahead log.
type WALRecord struct {
    LSN        uint64        // Monotonically increasing
    RecordType WALRecordType
    Length     uint32
    Checksum   uint32
    Payload    []byte
}
```

### 9.2 Checkpoint Strategy

```go
type CheckpointPolicy struct {
    Interval        time.Duration  // Default: 30 seconds
    WALSizeLimit    uint64         // Default: 64 MB
    DirtyPageLimit  int            // Default: 1000 pages
}

type Checkpoint struct {
    ID            uint64
    LSN           uint64
    TreeRoot      VAddr
    PageManager   PageManagerSnapshot
    ExternalValues ExternalValueSnapshot
    Timestamp     uint64
}
```

### 9.3 Recovery Algorithm

```
Recover():
    1. Find latest valid checkpoint
    2. Load checkpoint state (TreeRoot, PageManager, ExternalValues)
    3. Replay WAL from checkpoint LSN to end
    4. Verify integrity
    5. Truncate WAL past checkpoint LSN
```

**Why redo-only, not undo?**
- Append-only storage never overwrites data
- Deleted data uses tombstones, not in-place removal
- Therefore, only redo committed operations is needed

---

## 10. Compaction Strategy

### 10.1 Compaction Triggers

```go
type CompactionTrigger struct {
    SpaceUsageThreshold float64      // Default: 40%
    TimeInterval        time.Duration  // Default: 1 hour
    MinSegmentCount     int          // Default: 3
}
```

### 10.2 Generational Compaction

```go
type GenerationalCompactionStrategy struct {
    segmentSelector    SegmentSelector
    epochManager       EpochManager
    compactionWriter   CompactionWriter
    gcThreshold        int  // Default: 3 epochs
}
```

### 10.3 Epoch-Based MVCC

```go
type EpochManager interface {
    RegisterEpoch() EpochID
    UnregisterEpoch(epoch EpochID)
    IsVisible(vaddr VAddr) bool
    IsSafeToReclaim(vaddr VAddr) bool
    MarkCompactionComplete(oldSegments []SegmentID)
}

const EpochGracePeriod = 3  // Epochs
```

**Why epoch-based instead of reference counting?**
- Reference counting requires tracking every VAddr across tree nodes
- Epochs amortize tracking cost: one epoch covers all VAddrs visible to that snapshot
- Grace period absorbs slow readers
- Simpler to implement correctly

### 10.4 Space Reclamation

```go
type ExternalValueReclaimer interface {
    RegisterExternalValue(vaddr VAddr)
    UnregisterExternalValue(vaddr VAddr) error
    TryReclaim(vaddr VAddr) (bool, error)
    BatchTryReclaim(vaddrs []VAddr) ([]VAddr, error)
}
```

---

## 11. Cache Strategy

### 11.1 Decision: OS Page Cache (Default)

**After expert evaluation:**

| Criteria | OS Page Cache | Integrated Cache |
|----------|---------------|------------------|
| Performance | 75 | 72 |
| Complexity | 42 | 58 |
| Reliability | 75 | 68 |
| **Average** | **64** | **66** |

**Recommendation**: Default to OS Page Cache.

Choose Integrated Cache for:
- Write-heavy workloads (>50%)
- Strict memory boundedness required
- P99 latency predictability required

### 11.2 OS Page Cache Architecture

```go
// AccessPattern defines how a file interacts with OS page cache.
type AccessPattern uint8

const (
    AccessBuffered AccessPattern = iota  // Kernel page cache (default)
    AccessDirectIO                        // Bypass kernel cache
    AccessMapped                          // Memory-mapped I/O
)

// File type assignment
var FileTypeAccessPattern = map[FileType]AccessPattern{
    FileTypeWAL:           AccessBuffered,  // Sequential writes
    FileTypeSegment:       AccessBuffered,  // Mixed reads/writes
    FileTypeExternalValue: AccessBuffered,  // Large sequential reads
    FileTypeIndex:         AccessMapped,   // Random access
    FileTypeCheckpoint:    AccessMapped,   // Read rarely
}
```

### 11.3 Known Traps (Double-Checked)

| Trap | Solution |
|------|----------|
| Double buffering | Choose ONE caching strategy per file type |
| O_DIRECT alignment | All pages aligned to 4096 bytes |
| mmap + write(2) mixing | Each file uses ONE access pattern |
| fsync ordering | WAL synced before data pages |
| mmap truncation | Epoch grace period before release |

### 11.4 fsync Ordering Enforcement

```go
type DurabilityCoordinator struct {
    walFile   *BufferedFile
    dataFiles map[SegmentID]*BufferedFile
    mu        sync.Mutex
}

func (dc *DurabilityCoordinator) SyncWALFirst() error {
    // 1. Sync WAL
    // 2. Sync data pages
    // 3. Only after both succeed is transaction durable
}
```

---

## 12. Type Definitions Summary

### Core Types

```go
type VAddr struct {
    SegmentID uint64
    Offset    uint64
}

type PageID uint64

type SegmentID uint64

type SegmentState uint8

const (
    SegmentStateActive   SegmentState = 0x01
    SegmentStateSealed   SegmentState = 0x02
    SegmentStateArchived SegmentState = 0x04
)

type EpochID uint64
```

### Index Types

```go
type IndexType uint8

const (
    IndexTypeDenseArray IndexType = iota
    IndexTypeRadixTree
)
```

### Constants

```go
const (
    PageSize             = 4096
    PageDataSize         = 4080
    ExternalThreshold    = 48  // Max inline value size
    DefaultMaxValueSize  = 64 * 1024 * 1024
    EpochGracePeriod     = 3
)
```

---

## 13. Invariant Summary

### VAddr Invariants
- VAddr uniqueness: No two allocations produce the same VAddr
- VAddr stability: A VAddr never changes after allocation
- Segment ID monotonicity: New segments have higher IDs than old
- Offset bounds: Offset < MaxSegmentSize for active segments
- Page alignment: All VAddr.Offset values are multiples of PageSize

### B-link-tree Invariants
- NodeImmutability: Once written, a node is never modified
- SiblingChain: Leaf nodes form doubly-linked list via HighSibling
- KeyOrdering: Keys within a node are strictly increasing
- SplitBoundary: For split at key K, left contains ≤K, right contains >K
- HighKeyInvariant: Internal node's HighKey equals max key in rightmost child

### Page Manager Invariants
- PageID uniqueness: No two allocations return same PageID
- VAddr uniqueness: No two allocations return same VAddr
- Entry size: Index entries are always 24 bytes (8 + 16)

### Concurrency Invariants
- WriterMutex held by exactly one goroutine at any time
- Latch acquisition order: root → leaf (top-down)
- Latch release order: leaf → root (bottom-up)

### Recovery Invariants
- WAL records never overwritten except via Truncate
- Checkpoint LSN indicates all prior records durable
- Redo-only recovery (append-only needs no undo)

### Cache Invariants
- Each file uses exactly one AccessPattern for its lifetime
- WAL synced before data pages for same transaction
- Only one sync operation runs at a time

---

## 14. Cross-Document References

| From | To | Interface/Type |
|------|----|----------------|
| API Layer | B-link-tree | KVStore → NodeManager |
| B-link-tree | VAddr | NodePersist → VAddr |
| B-link-tree | External Value | InlineValue → ExternalValueStore |
| B-link-tree | Page Manager | Uses PageID for keys |
| Page Manager | VAddr | PageID → VAddr mapping |
| Page Manager | Index Persistence | DenseArray/RadixTree |
| Compaction | Epoch Manager | IsVisible/IsSafeToReclaim |
| Recovery | WAL | WALRecord types |
| Cache | OS Page Cache | AccessBuffered/AccessMapped |

---

## Document History

This document integrates the following source documents:

| Source | Key Content |
|--------|-------------|
| `vaddr-format.md` | VAddr 16-byte format, segment lifecycle |
| `blinktree-node-format.md` | B-link-tree node format, operations |
| `page-manager.md` | PageID → VAddr mapping, index types |
| `fixed-size-kvindex-persist.md` | Index persistence, recovery |
| `external-value-store.md` | External value storage (>48 bytes) |
| `api-layer.md` | Public API interfaces |
| `concurrency-recovery.md` | Single-writer model, WAL, checkpoint |
| `compaction-strategy.md` | Generational compaction, epochs |
| `os-page-cache.md` | OS Page Cache (default strategy) |
| `integrated-cache-strategy.md` | Integrated Cache (alternative) |
| `kv-store-design.md` | Architecture overview |
| `reliability-review-contract.md` | Expert evaluation scores |

---

*Document Status: Finalized Integrated Design*  
*Last Updated: 2024*
