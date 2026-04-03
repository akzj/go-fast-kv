# Architecture Documentation Index

**Project**: go-fast-kv - High-Performance KV Storage  
**Status**: Design Complete

---

## Core Architecture

| Document | Purpose |
|----------|---------|
| [kv-store-design.md](./kv-store-design.md) | Architecture overview, layer decomposition, data flow |

---

## Storage Layer

| Document | Purpose |
|----------|---------|
| [vaddr-format.md](./vaddr-format.md) | VAddr format (16 bytes: SegmentID + Offset), segment lifecycle |
| [page-manager.md](./page-manager.md) | PageID → VAddr mapping, FixedSizeKVIndex interface |

---

## Index Layer

| Document | Purpose |
|----------|---------|
| [blinktree-node-format.md](./blinktree-node-format.md) | B-link-tree node format, operations interface |
| [fixed-size-kvindex-persist.md](./fixed-size-kvindex-persist.md) | Mapping index persistence, rebuild strategy |

---

## Value Storage

| Document | Purpose |
|----------|---------|
| [external-value-store.md](./external-value-store.md) | Large value externalization (>48 bytes) |

---

## API & Operations

| Document | Purpose |
|----------|---------|
| [api-layer.md](./api-layer.md) | Public API interface (Get/Put/Delete/Scan/TX) |

### API Module Contracts

| Module | Contract File | Purpose |
|--------|---------------|---------|
| [vaddr](./api/vaddr/api.go) | `api/vaddr/api.go` | VAddr types, Segment/Offset operations |
| [pagemanager](./api/pagemanager/api.go) | `api/pagemanager/api.go` | Page allocation, mapping, WAL |
| [blinktree](./api/blinktree/api.go) | `api/blinktree/api.go` | B-link-tree index operations |
| [external-value](./api/external-value/api.go) | `api/external-value/api.go` | Large value storage |
| [concurrency](./api/concurrency/api.go) | `api/concurrency/api.go` | Latches, transactions, recovery |
| [compaction](./api/compaction/api.go) | `api/compaction/api.go` | Compaction, garbage collection |
| [cache](./api/cache/api.go) | `api/cache/api.go` | Cache interface, eviction policies |
| [kvstore](./api/kvstore/api.go) | `api/kvstore/api.go` | Top-level KVStore public API |
| [concurrency-recovery.md](./concurrency-recovery.md) | Concurrency model, crash recovery, WAL |

---

## Maintenance

| Document | Purpose |
|----------|---------|
| [compaction-strategy.md](./compaction-strategy.md) | Garbage collection, space reclamation |

---

## Cache Strategy (Evaluated)

| Document | Purpose | Score |
|----------|---------|-------|
| [os-page-cache.md](./os-page-cache.md) | OS kernel page cache strategy | **64/100** (Recommended) |
| [integrated-cache-strategy.md](./integrated-cache-strategy.md) | In-process buffer pool | **66/100** (Specialized) |

### Cache Strategy Decision

| Expert | OS Page Cache | Integrated Cache |
|--------|--------------|-----------------|
| Performance | 75 | 72 |
| Complexity | 42 | 58 |
| Reliability | 75 | 68 |
| **Average** | **64** | **66** |

**Recommendation**: Default to OS Page Cache. Choose Integrated Cache for:
- Write-heavy workloads (>50%)
- Strict memory boundedness required
- P99 latency predictability required

---

## Design Decision Summary

| Decision | Choice |
|----------|--------|
| Storage Model | Append-only (sequential writes) |
| Primary Index | B-link-tree (4KB pages) |
| Address Format | VAddr = SegmentID(8) + Offset(8) |
| Inline Value | ≤48 bytes |
| Page Mapping | Dense Array (default) or Radix Tree |
| Concurrency | Single-writer, multi-reader, latch crabbing |
| Recovery | WAL + Checkpoint, redo-only |
| Compaction | Generational segmented, epoch-based MVCC |
| Cache | OS Page Cache (default) |

---

## Core Principles

**⚠️ READ THIS FIRST: [CORE-PRINCIPLES.md](./CORE-PRINCIPLES.md)**

Module isolation, interface-first design, layer rules — mandatory constraints.

---

## Document Statistics

```
Total documents: 12 design docs + 1 mandatory principles doc
Total lines: ~6000
```

---

*Last Updated: 2024*
