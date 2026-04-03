# Module Dependencies

Analysis of the `github.com/akzj/go-fast-kv` project with 8 internal modules.

## Verification Results

| Check | Status |
|-------|--------|
| Single root `go.mod` | ✅ (1 file found) |
| `go list -m all` | ✅ Passes |
| `go build ./...` | ✅ Passes |

## Module List

| Module | Path | Role |
|--------|------|------|
| vaddr | `internal/vaddr/` | Base storage layer (VAddr, Segment, Page management) |
| pagemanager | `internal/pagemanager/` | PageID → VAddr mapping and allocation |
| cache | `internal/cache/` | OS page cache integration and eviction policies |
| blinktree | `internal/blinktree/` | B-link tree index implementation |
| external-value | `internal/external-value/` | Large value storage (>48 bytes) |
| compaction | `internal/compaction/` | Segment compaction and space reclamation |
| concurrency | `internal/concurrency/` | WAL, checkpoints, latch management |
| kvstore | `internal/kvstore/` | Top-level KV store public API |

## Dependency Graph

```
                    ┌─────────────────┐
                    │     vaddr       │  (base layer)
                    │ internal/vaddr/ │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌──────────────┐  ┌─────────────┐  ┌─────────────┐
    │  pagemanager │  │    cache    │  │  blinktree  │
    │ (no imports) │  │ vaddr/api   │  │   vaddr     │
    └──────────────┘  └─────────────┘  └─────────────┘
           │                 │                 │
           │                 │                 │
           │                 ▼                 ▼
           │          ┌─────────────┐  ┌─────────────────┐
           │          │  external-   │  │    kvstore      │
           │          │    value     │  │     vaddr       │
           │          │   vaddr      │  └─────────────────┘
           │          └─────────────┘
           │                 │
           ▼                 ▼
    ┌─────────────┐  ┌─────────────────┐
    │ concurrency │  │   compaction    │
    │   vaddr     │  │   vaddr/api     │
    └─────────────┘  └─────────────────┘
```

**Invariant:** The dependency graph is **acyclic** (DAG).

- `vaddr` has no dependencies — it is the foundation.
- All other modules depend only on `vaddr`.
- No circular imports exist.

## Import Analysis

### vaddr (`internal/vaddr/api/api.go`)
- **Dependencies:** None (root module)
- **Exports:** `VAddr`, `PageID`, `SegmentID`, `EpochID`, `Storage`, `Segment`, `Page`, `PageManager`, `FixedSizeKVIndex`, `FreeList`, `EpochManager`

### pagemanager (`internal/pagemanager/api/api.go`)
- **Dependencies:** None (uses raw `[16]byte` for VAddr to avoid import)
- **Exports:** `PageManager`, `FixedSizeKVIndex`, `FreeList`, `PageManagerConfig`
- **Note:** Uses `[16]byte` instead of `vaddr.VAddr` to keep API self-contained and mockable

### cache (`internal/cache/api/api.go`)
- **Dependencies:** `internal/vaddr/api`
- **Exports:** `Cache`, `IntegratedCache`, `CacheCoordinator`, `EvictionPolicy`, `AccessPattern`, `DurabilityManager`

### blinktree (`internal/blinktree/api/api.go`)
- **Dependencies:** `internal/vaddr`
- **Exports:** `Tree`, `TreeMutator`, `NodeOperations`, `NodeManager`, `TreeIterator`

### external-value (`internal/external-value/api/api.go`)
- **Dependencies:** `internal/vaddr`
- **Exports:** `ExternalValueStore`, `Metrics`, `Config`

### compaction (`internal/compaction/api/api.go`)
- **Dependencies:** `internal/vaddr/api`
- **Exports:** `Compactor`, `CompactionTrigger`, `CompactionWriter`, `Reclaimer`, `CompactionStrategy`, `EpochManager`

### concurrency (`internal/concurrency/api/api.go`)
- **Dependencies:** `internal/vaddr`
- **Exports:** `WAL`, `CheckpointManager`, `RecoveryManager`, `LatchManager`, `SingleWriterModel`

### kvstore (`internal/kvstore/api/api.go`)
- **Dependencies:** `internal/vaddr`
- **Exports:** `KVStore`, `KVStoreWithTransactions`, `Transaction`, `Iterator`, `Batch`, `Config`

## Interface Coverage Analysis

Each module follows the **interface-only API pattern**:

| Module | Interface File | Implementation Location |
|--------|---------------|------------------------|
| vaddr | `api/api.go` | `internal/vaddr/` |
| pagemanager | `api/api.go` | `internal/pagemanager/` |
| cache | `api/api.go` | `internal/cache/` |
| blinktree | `api/api.go` | `internal/blinktree/` |
| external-value | `api/api.go` | `internal/external-value/` |
| compaction | `api/api.go` | `internal/compaction/` |
| concurrency | `api/api.go` | `internal/concurrency/` |
| kvstore | `api/api.go` | `internal/kvstore/` |

**Invariant:** `api/api.go` contains interfaces only; implementation is in sibling `internal/` packages.

## Cross-Module Interface Dependencies

| Interface | Depends On | Purpose |
|-----------|-----------|---------|
| `kvstore.KVStore` | `vaddr.VAddr` (via type alias) | Address types |
| `blinktree.Tree` | `vaddr.VAddr`, `vaddr.PageID` | Node addressing |
| `compaction.EpochManager` | `vaddr.EpochID`, `vaddr.SegmentID` | Epoch tracking |
| `cache.CacheKey` | `vaddr.SegmentID`, `vaddr.PageID` | Cache indexing |
| `concurrency.WAL` | `vaddr.VAddr` | Recovery |
| `pagemanager.PageManager` | (self-contained `[16]byte`) | Avoids coupling |

## Build Verification

```bash
$ go list -m all
github.com/akzj/go-fast-kv

$ go build ./...
# (no output = success)

$ find . -name "go.mod" | wc -l
1
```

## Conclusion

- ✅ Single root `go.mod` at project root
- ✅ Dependency graph is acyclic (DAG)
- ✅ All modules use interface-only `api/api.go` pattern
- ✅ Build passes with `go build ./...`
- ✅ No circular import dependencies
