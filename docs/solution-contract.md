# Solution-C: Integrated Architecture Contract

## Purpose
This document defines the contract for the integrated solution. All implementation must conform to these specifications.

---

## 1. Integrated Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                      API Layer                              │
│         (Get / Put / Delete / Scan / Batch / Tx)          │
│  File: api-layer.md                                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    B-link-tree Index                        │
│    - Leaf nodes store (key, value_ptr) or (key, value)     │
│    - Sibling chain traversal for lock-free reads            │
│    - ≤48 bytes inline; >48 bytes → ExternalValueStore      │
│  File: blinktree-node-format.md                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Append-only Storage                        │
│    - All writes are append operations                        │
│    - VAddr = SegmentID(8) + Offset(8) = 16 bytes           │
│    - Segments: Active → Sealed → Compact → Archived        │
│  File: vaddr-format.md                                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Page Manager                               │
│    - PageID → VAddr mapping (DenseArray or RadixTree)      │
│    - 4KB page allocation, free list management              │
│  File: page-manager.md, fixed-size-kvindex-persist.md      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                External Value Store                          │
│    - Values >48 bytes stored externally                     │
│    - Contiguous page allocation, header-based retrieval     │
│  File: external-value-store.md                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              OS Page Cache (Default)                         │
│    - Buffered I/O for WAL, segments, external values       │
│    - Mmap for index files (rarely accessed)                 │
│    - fsync ordering: WAL before data                        │
│  File: os-page-cache.md                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Key Type Definitions

```go
// VAddr: Physical address in append-only address space
// Layout: [SegmentID: uint64, Offset: uint64], big-endian, 16 bytes total
// Invariant: SegmentID != 0 (0 is reserved for invalid)
type VAddr struct {
    SegmentID uint64
    Offset    uint64
}

// PageID: Logical identifier for a page
type PageID uint64

// InlineValue threshold: ≤48 bytes stored inline in B-link nodes
const MaxInlineValueSize = 48

// Page size: aligned with OS page size
const PageSize = 4096
```

---

## 3. Caching Strategy Decision

**Recommended**: OS Page Cache (simpler, default)

| Component | Access Pattern | Rationale |
|-----------|---------------|-----------|
| WAL | Buffered | Sequential writes; kernel optimized |
| Segments | Buffered | Mixed reads/writes; kernel handles |
| External Values | Buffered | Sequential reads; kernel efficient |
| Index (rare) | Mmap | Random access, rare reads |

**Why NOT Integrated Cache**:
- More complex (O_DIRECT alignment, epoch tracking, redirection maps)
- OS page cache already handles most workloads well
- Added complexity for marginal benefit (64/100 vs 66/100 evaluation)
- Integrated cache reserved for specialized high-performance scenarios

---

## 4. Concurrency Model

- **Single-writer, multi-reader**: Serializes writes via mutex
- **Latch crabbing**: Top-down acquire, bottom-up release
- **Lock-free reads**: Sibling chain traversal allows concurrent reads

---

## 5. Recovery Model

- **WAL + Checkpoint**: Redo-only recovery (no undo needed for append-only)
- **Checkpoint ordering**: Segments manifest → Index snapshot → Checkpoint record
- **Recovery algorithm**: Load checkpoint → Replay WAL from LSN → Verify integrity

---

## 6. Compaction Model

- **Generational segmented**: Oldest segments first
- **Epoch-based safety**: 3-epoch grace period before reclamation
- **Non-blocking**: Runs in background, never blocks writer
- **Triggers**: Space usage (>40% garbage), time interval (1hr), segment count (≥3)

---

## 7. Cross-Doc References (Corrected)

All references should use relative paths from `docs/`:

| Document | Correct Reference |
|----------|-------------------|
| compaction-strategy.md | `api-layer.md`, `blinktree-node-format.md`, etc. (NOT `docs/architecture/`) |

---

## 8. Acceptance Criteria for solution-c.md

- [ ] All 11 design docs referenced and integrated
- [ ] VAddr format: 16 bytes (SegmentID + Offset)
- [ ] Inline value threshold: ≤48 bytes
- [ ] Cache strategy: OS Page Cache as default, Integrated Cache as alternative documented
- [ ] Concurrency: Single-writer, multi-reader, latch crabbing
- [ ] Recovery: WAL + Checkpoint, redo-only
- [ ] Compaction: Generational, epoch-based, non-blocking
- [ ] Path references corrected (no `docs/architecture/` prefix)
- [ ] Internal consistency maintained
