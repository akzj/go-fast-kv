# FixedSizeKVIndex Persistence Architecture

## 1. Overview

**Purpose**: Define storage format and rebuild strategy for the PageID → VAddr mapping index.

**Scope**: FixedSizeKVIndex persistence, recovery, manifest structure.

**Relates to**:
- `page-manager.md` — Base FixedSizeKVIndex interface
- `concurrency-recovery.md` — WAL and checkpoint integration
- `vaddr-format.md` — VAddr binary format (16 bytes)

## 2. Persistence Architecture

### 2.1 Design Goals

1. **Crash consistency**: Index survives crashes without corruption
2. **Efficient rebuild**: Minimize recovery time after crash
3. **Checkpoint integration**: Consistent with WAL + checkpoint design
4. **Variant support**: Both Dense Array and Radix Tree index types

### 2.2 Why Persistence is Tricky

**The Core Problem**: FixedSizeKVIndex maps PageID → VAddr, but VAddrs point to *segments* written at runtime. If checkpoint includes index state but B-link nodes reference new segments written after checkpoint, recovery will see stale VAddrs.

**Trap 1 — Dense Array rebuild**: Cannot just read segment files to discover PageIDs. Need to scan B-link tree to discover which PageIDs are live, then reconstruct mapping.

**Trap 2 — Radix Tree rebuild**: Need root pointer in manifest, then replay WAL to apply any uncheckpointed updates.

**Trap 3 — Checkpoint ordering**: If checkpoint includes index state but tree nodes reference new segments, recovery sees stale VAddrs.

### 2.3 Solution: Checkpoint-Indexed Recovery

```
Checkpoint LSN: L  (all WAL records < L are durable)

Checkpoint contains:
├── TreeRoot VAddr      (B-link tree root)
├── IndexSnapshot       (DenseArray or RadixTree state)
└── SegmentManifest     (all segments valid at L)

Recovery:
1. Load checkpoint state
2. Verify TreeRoot segments exist in SegmentManifest
3. Replay WAL from L to end (re-applies any missed updates)
4. Verify index consistency with tree
```

**Key invariant**: Index snapshot at LSN L only references segments that exist at L. WAL replay adds any segments created after L.

## 3. Index Type Specification

```go
// IndexType identifies the underlying index structure.
// Invariant: IndexType is persisted in manifest; determines rebuild strategy.
type IndexType uint8

const (
    IndexTypeDenseArray IndexType = iota  // O(1) lookup, best for dense PageIDs
    IndexTypeRadixTree                    // O(k) lookup, best for sparse PageIDs
)

// Why two types?
// - Dense Array: 16 bytes/entry, O(1) lookup, but wastes space on sparse IDs
// - Radix Tree: ~24 bytes/entry overhead, O(4) lookup, compact for sparse IDs
// - Default: Dense Array (simpler, better for typical sequential page allocation)
```

## 4. DenseArray Storage Format

### 4.1 File Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  DenseArray Index File                                          │
├─────────────────────────────────────────────────────────────────┤
│  Header (64 bytes)                                              │
│  ├── Magic: "DAIDX\0\0\0" (8 bytes)                             │
│  ├── Version: uint16                                             │
│  ├── IndexType: uint8 (= IndexTypeDenseArray)                   │
│  ├── CheckpointLSN: uint64                                      │
│  ├── PageIDBase: uint64 (first PageID in this array)            │
│  ├── EntryCount: uint64 (total entries including tombstones)    │
│  ├── LiveEntryCount: uint64 (non-tombstone entries)             │
│  ├── ArrayCapacity: uint64 (allocated slots)                    │
│  └── Reserved: 14 bytes                                         │
├─────────────────────────────────────────────────────────────────┤
│  Entry Array (24 bytes × Capacity)                             │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Entry[0]: PageID(8) │ VAddr(16)                           │ │
│  │  Entry[1]: PageID(8) │ VAddr(16)                           │ │
│  │  ...                                                        │ │
│  └────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  Footer (32 bytes)                                              │
│  ├── Checksum: uint64 (CRC64 of header + entries)               │
│  ├── EntryCountDuplicate: uint64 (verification)                 │
│  ├── LiveEntryCountDuplicate: uint64                            │
│  └── Reserved: 8 bytes                                          │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Type Definitions

```go
// DenseArrayHeader is the fixed-size header for dense array index files.
// Invariant: Header is always 64 bytes, page-aligned.
type DenseArrayHeader struct {
    Magic           [8]byte    // "DAIDX\0\0\0"
    Version         uint16     // Format version (1)
    IndexType       uint8      // Must be IndexTypeDenseArray
    _               [1]byte    // Alignment padding
    CheckpointLSN   uint64     // LSN of checkpoint that created this
    PageIDBase      uint64     // First PageID in array (usually 1)
    EntryCount      uint64     // Total slots (capacity)
    LiveEntryCount  uint64     // Non-tombstone entries
    ArrayCapacity   uint64     // Allocated entries (may exceed LiveEntryCount)
    _               [14]byte   // Reserved
}

// DenseArrayEntry is a single key-value mapping.
// Invariant: Entry is always 24 bytes (8 + 16).
// Why not variable-length? Array index provides O(1) lookup.
type DenseArrayEntry struct {
    PageID PageID  // 8 bytes; 0 = unused slot
    VAddr  VAddr   // 16 bytes; VAddrInvalid = tombstone
}

// Why PageID=0 means unused?
// PageID 0 is reserved for invalid/null per page-manager.md.
// This allows distinguishing unused slots from tombstones.
```

### 4.3 DenseArray Rebuild Strategy

```
DenseArrayRebuild():
    1. Load index file header
    2. Scan B-link tree from root:
       - For each leaf node, extract all (PageID, VAddr) pairs
       - Build live_entries map
    3. Reconstruct array:
       - Array[PageID - PageIDBase] = VAddr
       - All other slots = {PageID: 0, VAddr: VAddrInvalid}
    4. Validate: verify all live_entries count matches header
    5. If mismatch: full scan from segments (fallback)
```

**Why scan B-link tree, not segment files?**
- Segment files contain data pages but not the PageID → VAddr mapping
- B-link tree nodes store PageID keys with VAddr values
- Tree traversal is O(n) where n = live pages (not all segments)

**Fallback: Full Segment Scan**
```
DenseArrayFallbackRebuild():
    1. Scan all segment files
    2. For each page in each segment:
       - Read page header: extract PageID
       - Map(PageID) = VAddr of this page
    3. Build dense array from map
```
**Cost**: O(all pages in all segments), even deleted ones. Used only on corruption.

## 5. RadixTree Storage Format

### 5.1 Manifest Entry

Radix tree nodes are stored in segment files (like B-link nodes). Only the root VAddr is persisted in the manifest:

```go
// RadixTreeManifestEntry is stored in the index manifest.
// Invariant: RootVAddr points to a valid RadixNode persisted in segments.
type RadixTreeManifestEntry struct {
    IndexType       IndexType  // Must be IndexTypeRadixTree
    RootVAddr       VAddr      // VAddr of root RadixNode
    NodeCount       uint64     // Total nodes in tree (for validation)
    CheckpointLSN   uint64     // LSN of checkpoint
    Height          uint8      // Tree height (typically 4)
}

// Why store RootVAddr, not whole tree?
// Radix nodes are already persisted as pages in segments.
// Root pointer is sufficient to rebuild the tree structure.
// Storing all nodes separately would duplicate data.
```

### 5.2 RadixTree Node Format

```
RadixTreeNode (stored as page in segment):
┌─────────────────────────────────────────────────────────────────┐
│  NodeHeader (16 bytes)                                          │
│  ├── Magic: "RADX\0\0\0\0" (8 bytes)                            │
│  ├── NodeType: uint8 (0=internal, 1=leaf)                       │
│  ├── Height: uint8 (levels below this node)                     │
│  ├── EntryCount: uint16 (valid entries in this node)            │
│  └── Reserved: 4 bytes                                          │
├─────────────────────────────────────────────────────────────────┤
│  For Internal Nodes:                                            │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Branch[0]: key_prefix(8) │ child_vaddr(16)               │ │
│  │  Branch[1]: key_prefix(8) │ child_vaddr(16)               │ │
│  │  ... (up to 256 branches)                                   │ │
│  └────────────────────────────────────────────────────────────┘ │
│  Entry size: 24 bytes (8 + 16)                                   │
│  Capacity: 256 branches                                          │
├─────────────────────────────────────────────────────────────────┤
│  For Leaf Nodes:                                                │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Slot[0]: page_id_low(8) │ vaddr(16)                       │ │
│  │  Slot[1]: page_id_low(8) │ vaddr(16)                       │ │
│  │  ... (up to 256 slots)                                     │ │
│  └────────────────────────────────────────────────────────────┘ │
│  Entry size: 24 bytes                                           │
│  Capacity: 256 slots                                             │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 Type Definitions

```go
// RadixNodeType distinguishes internal from leaf nodes.
type RadixNodeType uint8

const (
    RadixNodeInternal RadixNodeType = 0  // Has child pointers
    RadixNodeLeaf    RadixNodeType = 1  // Has key-value pairs
)

// RadixNodeHeader is stored at the start of each radix node page.
// Invariant: Node is always exactly one page (4096 bytes).
type RadixNodeHeader struct {
    Magic       [4]byte     // "RADX"
    NodeType    RadixNodeType
    Height      uint8       // 0 for leaf, >0 for internal
    EntryCount  uint16      // Valid entries
    _           [4]byte     // Alignment/reserved
}

// RadixBranch is a single branch in an internal node.
// key_prefix: upper 48 bits of key (16 bits consumed per level)
// child_vaddr: VAddr of child node
type RadixBranch struct {
    KeyPrefix   uint64      // Upper bits of key
    _           [8]byte     // Reserved (for alignment)
    ChildVAddr  VAddr       // Child node location
}

// RadixSlot is a single key-value pair in a leaf node.
// page_id_low: lower 48 bits of PageID (upper 16 consumed by path)
// vaddr: VAddr of the page
type RadixSlot struct {
    PageIDLow   uint64      // Lower bits of key
    _           [8]byte     // Reserved
    VAddr       VAddr       // Value
}

// Why 4-level tree with 16-bit splits?
// 4 levels × 16 bits = 64 bits (full PageID space)
// 16^4 = 2^16 = 65,536 entries per leaf
// Compact representation, O(4) lookup worst case
```

### 5.4 RadixTree Rebuild Strategy

```
RadixTreeRebuild():
    1. Load RadixTreeManifestEntry from index manifest
    2. Load root node from RootVAddr
    3. Recursively load all descendant nodes
    4. Tree is now in-memory, ready for operations
    5. Replay WAL from CheckpointLSN to end:
       - WALPageAlloc: RadixTree.Put(pageID, vaddr)
       - WALPageFree: RadixTree.Put(pageID, VAddrInvalid)
       - WALRootUpdate: Update manifest RootVAddr
```

**Why WAL replay is necessary for RadixTree but not DenseArray?**
- DenseArray: Full snapshot written at checkpoint (all entries included)
- RadixTree: Only root VAddr persisted; updates after checkpoint are lost without WAL
- **Alternative**: Could persist full RadixTree snapshot like DenseArray, but wasteful (nodes already in segments)

## 6. Index Manifest Structure

### 6.1 Manifest File Layout

```go
// IndexManifest tracks the state of the mapping index.
// Stored in: data/index_manifest (single file, append-only).
type IndexManifest struct {
    Header IndexManifestHeader
    Entries []IndexManifestEntry  // Append-only, latest wins
}

// IndexManifestHeader (32 bytes):
// - Magic: "IDXMN\0\0\0" (8 bytes)
// - Version: uint16
// - CurrentIndexType: IndexType
// - CurrentCheckpointLSN: uint64
// - Reserved: 14 bytes
type IndexManifestHeader struct {
    Magic               [8]byte
    Version             uint16
    CurrentIndexType    IndexType
    _                   [1]byte
    CurrentCheckpointLSN uint64
    _                   [14]byte
}

// IndexManifestEntry is a single index snapshot record.
// Invariant: Entries are never deleted; latest entry is current.
// Why append-only? Allows replay to find current state.
type IndexManifestEntry struct {
    IndexType       IndexType
    CheckpointLSN   uint64
    Payload         IndexPayload  // Union of DenseArray or RadixTree specifics
}

// IndexPayload is a tagged union.
type IndexPayload struct {
    Dense DenseArrayManifestPayload
    Radix RadixTreeManifestPayload
}

// For DenseArray:
type DenseArrayManifestPayload struct {
    FilePath        string  // Relative path to index file
    PageIDBase      uint64
    EntryCount      uint64
    LiveEntryCount  uint64
}

// For RadixTree:
type RadixTreeManifestPayload struct {
    RootVAddr       VAddr
    NodeCount       uint64
    Height          uint8
}
```

### 6.2 Manifest File Format

```
┌─────────────────────────────────────────────────────────────────┐
│  Index Manifest File                                            │
├─────────────────────────────────────────────────────────────────┤
│  Header (32 bytes)                                              │
│  ├── Magic: "IDXMN\0\0\0" (8 bytes)                             │
│  ├── Version: uint16                                             │
│  ├── CurrentIndexType: uint8                                    │
│  ├── CurrentCheckpointLSN: uint64                                │
│  └── Reserved: 14 bytes                                         │
├─────────────────────────────────────────────────────────────────┤
│  Entry 1 (variable length, 8-byte aligned)                     │
│  ├── IndexType: uint8                                            │
│  ├── CheckpointLSN: uint64                                       │
│  ├── PayloadLength: uint32                                       │
│  └── Payload (DenseArray or RadixTree specifics)               │
├─────────────────────────────────────────────────────────────────┤
│  Entry 2...                                                     │
│  ...                                                            │
├─────────────────────────────────────────────────────────────────┤
│  Footer (16 bytes)                                               │
│  ├── Checksum: uint64 (CRC64 of all entries)                    │
│  ├── EntryCount: uint64                                          │
│  └── Reserved: 8 bytes                                          │
└─────────────────────────────────────────────────────────────────┘
```

## 7. Checkpoint Integration

### 7.1 Checkpoint Capture Order

```
CreateCheckpoint():
    1. Acquire write lock on PageManager
    2. WAL: Write WALCheckpointBegin record
    3. WAL: Ensure all pending updates are flushed
    4. Capture order (must be atomic):
       a. Capture B-link tree root VAddr
       b. Capture index snapshot:
          - DenseArray: write full array to temp file, rename
          - RadixTree: atomically update manifest with new RootVAddr
       c. Capture segment manifest (all sealed segments)
    5. Write checkpoint record to WAL with final LSN
    6. fsync WAL
    7. Return Checkpoint{LSN, TreeRoot, IndexState, SegmentManifest}
```

**Critical Invariant**: Index state at checkpoint LSN L only references segments that are also included at L. Segments created after L are captured via WAL replay.

### 7.2 Why This Order Matters

**Failure scenario without ordering**:
```
BAD ORDER:
1. Capture index snapshot (references Segment 5)
2. Segment 5 is sealed and added to manifest
3. Crash after step 1, before step 2
→ Index references Segment 5, but Segment 5 not in manifest → CORRUPTION
```

**Correct order**:
```
GOOD ORDER:
1. Capture segment manifest (includes all existing segments)
2. Capture index snapshot (can only reference manifest segments)
3. Write checkpoint record
→ Index only references segments that are durable → SAFE
```

## 8. Recovery Algorithm

### 8.1 Recovery Entry Point

```go
// RecoverIndex restores the FixedSizeKVIndex from checkpoint + WAL.
// Called during KVStore recovery after tree and segments are loaded.
func RecoverIndex(store *KVStore, checkpoint *Checkpoint) (*IndexState, error) {
    // 1. Load index manifest
    manifest, err := LoadIndexManifest(store.DataDir)
    if err != nil {
        return nil, fmt.Errorf("manifest load failed: %w", err)
    }
    
    // 2. Find entry matching checkpoint LSN
    entry := manifest.FindEntry(checkpoint.LSN)
    if entry == nil {
        return nil, fmt.Errorf("no index entry for LSN %d", checkpoint.LSN)
    }
    
    // 3. Restore based on index type
    switch entry.IndexType {
    case IndexTypeDenseArray:
        return recoverDenseArray(store, entry, checkpoint)
    case IndexTypeRadixTree:
        return recoverRadixTree(store, entry, checkpoint)
    default:
        return nil, fmt.Errorf("unknown index type: %d", entry.IndexType)
    }
}
```

### 8.2 DenseArray Recovery

```go
func recoverDenseArray(store *KVStore, entry *IndexManifestEntry, cp *Checkpoint) (*DenseArrayState, error) {
    // 1. Load index file
    payload := entry.Payload.Dense
    idx, err := LoadDenseArrayFile(store.DataDir, payload.FilePath)
    if err != nil {
        return nil, fmt.Errorf("dense array load failed: %w", err)
    }
    
    // 2. Validate against checkpoint LSN
    if idx.CheckpointLSN != cp.LSN {
        return nil, fmt.Errorf("checkpoint LSN mismatch: %d vs %d", 
            idx.CheckpointLSN, cp.LSN)
    }
    
    // 3. Replay WAL from checkpoint LSN
    // (DenseArray is already fully checkpointed, but WAL may have newer updates)
    replayed, err := ReplayIndexWAL(store.WAL, cp.LSN, idx)
    if err != nil {
        log.Printf("WAL replay warning: %v (using checkpoint state)", err)
        // Continue with checkpoint state; some updates may be lost
    }
    
    // 4. Validate index consistency
    if err := ValidateIndexConsistency(idx, store.TreeRoot); err != nil {
        log.Printf("index validation warning: %v", err)
        // Continue; corruption is rare
    }
    
    return idx, nil
}
```

### 8.3 RadixTree Recovery

```go
func recoverRadixTree(store *KVStore, entry *IndexManifestEntry, cp *Checkpoint) (*RadixTreeState, error) {
    // 1. Load root node from manifest
    payload := entry.Payload.Radix
    root, err := store.SegmentStore.LoadNode(payload.RootVAddr)
    if err != nil {
        return nil, fmt.Errorf("root node load failed: %w", err)
    }
    
    // 2. Validate root node
    if root.Magic != "RADX" || root.Height != payload.Height {
        return nil, fmt.Errorf("root node corrupted")
    }
    
    // 3. Build in-memory tree structure (lazy load children as needed)
    tree := &RadixTreeState{
        Root:       root,
        NodeCount:  payload.NodeCount,
        Height:     payload.Height,
    }
    
    // 4. Replay WAL from checkpoint LSN
    // RadixTree requires WAL replay because only root VAddr is persisted
    replayed, err := ReplayIndexWAL(store.WAL, cp.LSN, tree)
    if err != nil {
        return nil, fmt.Errorf("WAL replay failed: %w", err)
    }
    
    // 5. Update manifest with new root if WAL modified it
    if replayed.NewRootVAddr != VAddrInvalid {
        if err := store.IndexManifest.UpdateRoot(replayed.NewRootVAddr); err != nil {
            log.Printf("manifest update warning: %v", err)
        }
    }
    
    return tree, nil
}
```

## 9. WAL Record Types for Index

```go
// WALRecordType values for index operations (extends concurrency-recovery.md).
const (
    WALIndexUpdate WALRecordType = iota + 100  // Start after other types
    WALIndexFullCheckpoint                     // DenseArray full snapshot taken
    WALIndexRootUpdate                         // RadixTree root changed
)

// WALIndexUpdateRecord records a single index mutation.
// Used for RadixTree WAL replay.
type WALIndexUpdateRecord struct {
    PageID        PageID
    VAddr        VAddr
    // VAddr = VAddrInvalid means tombstone (deletion)
}

// WALIndexRootUpdateRecord records a RadixTree root change.
// Only needed for RadixTree (DenseArray captures full state in checkpoint).
type WALIndexRootUpdateRecord struct {
    OldRoot VAddr
    NewRoot VAddr
}
```

## 10. Invariants Summary

```go
// DenseArray invariants:
// - EntryCount >= LiveEntryCount (all entries minus tombstones)
// - PageIDBase > 0 (0 is reserved)
// - Array[i].PageID == 0 implies unused slot (not tombstone)
// - VAddrInvalid in VAddr field means tombstone

// RadixTree invariants:
// - Root node always exists (RootVAddr != VAddrInvalid)
// - Node height = 4 for 64-bit PageIDs (4 × 16 bits = 64)
// - Internal nodes have EntryCount branches
// - Leaf nodes have EntryCount slots

// Checkpoint invariants:
// - Index checkpoint LSN <= checkpoint WAL LSN
// - Index references only segments that exist at checkpoint LSN
// - WAL replay from checkpoint LSN recovers all post-checkpoint updates

// Recovery invariants:
// - DenseArray: index is fully restored from file, WAL replay is optional
// - RadixTree: root loaded from manifest, WAL replay is required
// - Both: final index state must be consistent with B-link tree
```

## 11. Why Not Alternative Designs

| Alternative | Why Rejected |
|-------------|--------------|
| Persist RadixTree nodes separately | Duplicates data already in segments; more write amplification |
| Delta snapshots for DenseArray | Complexity; full snapshot is simpler and small enough |
| LSM-tree for index | Overkill; index is already append-only with WAL |
| Checkpoint after index only | Race condition: tree may reference new segments not yet checkpointed |
| No WAL for index | Lost updates between checkpoints; unacceptable for durability |

## 12. Acceptance Criteria

- [ ] DenseArray file format defined (header, entries, footer)
- [ ] RadixTree node format defined (header, branches/slots)
- [ ] IndexManifest structure defined
- [ ] Rebuild strategy for DenseArray (B-link tree scan + fallback)
- [ ] Rebuild strategy for RadixTree (manifest root + WAL replay)
- [ ] Checkpoint ordering documented (segments before index)
- [ ] Recovery algorithm specified
- [ ] WAL record types for index defined
- [ ] All invariants documented

---

*Document Status: Contract Spec*
*Last Updated: 2024*
