# High-Performance KV Storage System Design

## 1. Overview

**Goal**: A high-performance key-value storage engine using B-link-tree as the primary index structure, with append-only storage to solve the random write problem inherent in B-tree variants.

**Core Challenge**: 
- B-link-tree provides good read and range query performance
- But B-tree updates cause random writes (overwrites)
- Solution: Append-only storage + internal mapping layer

## 2. Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                      API Layer                              │
│         (Get / Put / Delete / Scan / Batch)                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    B-link-tree Index                         │
│    - Leaf nodes store (key, value_ptr) or (key, value)      │
│    - Supports range queries                                 │
│    - All node operations → append to storage                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Append-only Storage                        │
│    - All writes are append operations                        │
│    - Generates physical addresses (vaddr)                   │
│    - Provides durability and sequential write performance   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Page Manager                              │
│    - Manages page allocation/deallocation                   │
│    - page_id → vaddr mapping (internal KV index)           │
│    - Fixed-size keys enable optimization                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                External Value Store                          │
│    - Stores values too large for B-link-tree pages          │
│    - Referenced by pointer from B-link-tree entry           │
└─────────────────────────────────────────────────────────────┘
```

## 3. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage Model | Append-only | Sequential writes, no random I/O |
| Primary Index | B-link-tree | Range queries, lock-free reads |
| Address Format | VAddr = SegmentID(8) + Offset(8) | 16 bytes, extensible |
| Inline Value | ≤48 bytes | Fits in node entry slot |
| Page Mapping | Dense Array / Radix Tree | Fixed-size keys, O(1) or O(k) lookup |
| Concurrency | Single-writer, multi-reader | Simpler than multi-writer |
| Recovery | WAL + Checkpoint, redo-only | Append-only needs no undo |
| Compaction | Generational segmented | Non-blocking, epoch-based MVCC |
| Cache | OS Page Cache (default) | Kernel optimized, less code |

## 4. Module Summary

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
| Cache | os-page-cache.md | BufferedFile, MmapFile, DirectIOFile |

---

*Document Status: Architecture Overview*
*Last Updated: 2024*
