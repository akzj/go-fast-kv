# Solution-A Integration Contract

## Purpose
Integrate all design documents into a single coherent architecture.

## Input Documents (11 total)
1. `vaddr-format.md` — VAddr = SegmentID(8) + Offset(8) = 16 bytes
2. `page-manager.md` — PageID → VAddr mapping, 4KB pages
3. `blinktree-node-format.md` — B-link-tree nodes, right-biased splits, sibling chains
4. `api-layer.md` — Public API, operations, error handling, transactions
5. `concurrency-recovery.md` — Single-writer/multi-reader, WAL + checkpoint
6. `compaction-strategy.md` — Generational compaction, epoch-based MVCC
7. `external-value-store.md` — Values >48 bytes stored externally
8. `integrated-cache-strategy.md` — In-process cache (alternative to OS page cache)
9. `os-page-cache.md` — OS page cache strategy
10. `kv-store-design.md` — High-level overview
11. `fixed-size-kvindex-persist.md` — PageID→VAddr index persistence

## Output
`docs/solution-a.md` — Complete integrated design document

## Integration Requirements

### Section Structure
1. **Overview** — System architecture summary, key design decisions
2. **Address Space** — VAddr format, segment structure, page layout (from vaddr-format.md)
3. **Storage Layer** — Page manager, segment allocation, OS page cache strategy (page-manager.md, os-page-cache.md)
4. **Index Layer** — B-link-tree structure, node format, concurrency (blinktree-node-format.md)
5. **External Values** — Threshold, encoding, storage (external-value-store.md)
6. **API Layer** — Public interface, operations, error semantics (api-layer.md)
7. **Concurrency Model** — Single-writer, latch protocol, epoch safety (concurrency-recovery.md)
8. **Recovery** — WAL format, checkpoint, recovery procedure (concurrency-recovery.md)
9. **Compaction** — When/how to compact, epoch-based reclamation (compaction-strategy.md)
10. **Cache Strategy** — OS page cache (CHOSEN) vs integrated cache rationale
11. **Data Flow** — Write path, read path, recovery path
12. **Module Boundaries** — Public interfaces between layers

## Known Traps to Address
1. **Cache Selection**: Must choose OS page cache with rationale (slightly lower perf but simpler)
2. **Inline Threshold**: ≤48 bytes inline; external values via external-value-store
3. **Double Buffering**: If integrated cache chosen, must use O_DIRECT (NOT choosing this)
4. **Epoch Safety**: Redirected VAddrs need grace period before invalidation

## Key Finalized Decisions (must match)
- [x] Append-only storage
- [x] B-link-tree index with 4KB pages
- [x] VAddr = SegmentID(8)+Offset(8) = 16 bytes
- [x] Single-writer multi-reader concurrency
- [x] WAL + checkpoint recovery (redo-only)
- [x] Generational compaction with epoch-based MVCC
- [x] OS page cache (DEFAULT CHOICE — 64/100 vs 66/100)

## Verification Commands
```bash
# All 4 keywords must appear in solution-a.md
grep -l "vaddr-format\|blinktree\|page-manager\|api-layer" docs/*.md
# Must include VAddr definition
head -50 docs/solution-a.md | grep -q "VAddr\|SegmentID" || echo "MISSING vaddr-format"
```

## Anti-Patterns to Avoid
- Don't invent new VAddr formats — use the 16-byte format from vaddr-format.md
- Don't re-litigate OS vs integrated cache — choose OS with rationale
- Don't contradict existing invariants from individual docs
- Don't add implementation details — this is a design document
