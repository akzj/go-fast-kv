# Known Issues

Issues identified during the deep code review (2026). Organized by severity.
Status legend: ✅ FIXED | ⚠️ ACCEPTED | ⏸️ DEFERRED | 🔴 UNRESOLVED

---

## KV Layer

### KI-1: Vacuum Reads/Writes Without Page Locks (CRITICAL) — ✅ FIXED

**Location**: `internal/vacuum/internal/vacuum.go`

Previously: Vacuum traversed B-tree leaves without acquiring per-page write locks, risking corruption.

**Status**: Issue identified as previously fixed (vacuum uses proper locking).

---

### KI-2: Iterator Use After Store Close (HIGH) — ✅ FIXED

**Location**: `internal/btree/internal/btree.go`
**Commit**: c167412, d9d9da0

Previously: Iterators held references to closed file descriptors after store close.

**Fix**: Added `closed` flag check inside `Next()` inner loop + guard in all iterator operations.

---

### KI-3: goroutineID Fragility (MEDIUM) — ⚠️ ACCEPTED

**Location**: `internal/goid/`

The fast goroutine ID implementation uses TLS assembly to read the goroutine ID directly from the Go runtime's internal `g` struct. This depends on the offset of the `goid` field, which is not part of Go's public API and could change between Go versions.

**Mitigation**: The offset is verified at init time. If it changes, the fallback is `runtime.Stack()` parsing (2564x slower).

**Decision**: Acceptable trade-off. Fast path is correct for current Go versions; fallback is safe.

---

### KI-4: GC Not Integrated (MEDIUM) — ✅ FIXED

**Location**: `internal/gc/`, `internal/kvstore/`
**Commit**: 299d90e

Previously: `kvstore` never called `CollectOne()`. Sealed segments accumulated forever.

**Fix**: Wired in `checkAutoGC()` with periodic trigger. GC now properly integrated into kvstore lifecycle.

---

### KI-5: B-link Tree Delete Does Not Merge Underflow Pages (LOW) — ⏸️ DEFERRED

**Location**: `internal/btree/internal/btree.go`

Delete marks entries with `TxnMax` (MVCC tombstone) but never merges underfull pages.

**Mitigation**: Vacuum eventually removes tombstoned entries, which helps.

**Decision**: LOW priority. Full merge logic complexity high; vacuum provides partial mitigation.

---

### KI-6: No WAL Size-Based Auto-Checkpoint (LOW) — ✅ FIXED

**Location**: `internal/kvstore/internal/checkpoint.go`
**Commit**: 299d90e

Previously: Auto-checkpoint when WAL reaches 16MB was not implemented.

**Fix**: Implemented WAL-size based auto-checkpoint trigger.

---

### KI-7: LSM Compaction Not Concurrent-Safe with Reads (MEDIUM) — ⏸️ DEFERRED

**Location**: `internal/lsm/internal/lsm.go`

LSM compaction replaces SSTable files. The current implementation relies on file descriptor caching and OS-level reference counting (deleted files remain readable until all fds are closed).

**Mitigation**: Works correctly on Linux due to fd reference counting.

**Decision**: MEDIUM priority. Full fix requires reference counting table or atomic file replacement. Linux behavior is acceptable for current use cases.

---

### KI-8: Checkpoint File Has No Version Migration (LOW) — ✅ FIXED

**Location**: `internal/kvstore/internal/checkpoint.go`
**Commit**: 299d90e

Previously: Checkpoint format V1–V4 had no automatic migration.

**Fix**: Implemented checkpoint version detection for proper migration support.

---

### KI-9: BlobStore Dense Array Grows Monotonically (LOW) — ✅ FIXED

**Location**: `internal/blobstore/internal/blobstore.go`
**Commit**: d9d9da0

Previously: Blob IDs were never reused; `[]BlobMeta` grew monotonically.

**Fix**: Implemented free list for BlobID recycling. Deleted BlobIDs are now reused instead of growing the array.

---

## SQL Layer

### Parser Limitations

#### ORDER BY Only Accepts Column References (P1) — ⏸️ DEFERRED
**Location**: `parser/internal/parser.go:1785`

`ORDER BY` rejects expressions, positional references, and aliases. Only bare column names accepted.

#### No Block Comment Support (`/* ... */`) (P2) — ⏸️ DEFERRED
**Location**: `parser/internal/lexer.go:237-249`

Only `--` line comments handled.

#### No Quoted Identifier Support (P1) — ⏸️ DEFERRED
**Location**: `parser/internal/lexer.go`

Neither double-quoted nor backtick identifiers supported. Keywords as identifiers cannot be escaped.

#### No Scientific Notation in Numeric Literals (P2) — ⏸️ DEFERRED
**Location**: `parser/internal/lexer.go:296-314`

`readNumber` handles integers and decimals but not scientific notation.

### Planner Limitations

#### CTE Column Types Are All TypeBlob (P1) — ✅ FIXED
**Location**: `planner/internal/planner.go:745-748`
**Commit**: a730428

All CTE columns now correctly inferred from SELECT expressions.

### Catalog Limitations

#### Schema Cache Returns Mutable Pointer (P1) — ✅ FIXED
**Location**: `catalog/internal/catalog_impl.go:134`
**Commit**: a730428

`getTableImpl` now returns immutable copy, preventing cache corruption.

#### Schema Cache Is FIFO, Not LRU (P2) — ✅ FIXED
**Location**: `catalog/internal/catalog_impl.go:152-160`
**Commit**: a730428

Schema cache now properly implements LRU with promotion on hit.

### Executor Limitations

#### Arithmetic Operators Double-Evaluate Operands (P2) — ⏸️ DEFERRED
**Location**: `executor/internal/eval.go:~306-355`

`evalBinaryExpr` evaluates operands twice. Wasteful but not a correctness bug for side-effect-free expressions.

#### HAVING Without GROUP BY Accepted in Some Paths (P2) — ⏸️ DEFERRED

Main `execSelect` rejects it but some join paths may not.

### Not Reviewed

The following areas were not covered by the deep code review and may contain additional issues:

- Full-text search executor paths
- Window function computation correctness
- Savepoint rollback under complex scenarios
- Row locking under contention
- Expression index evaluation
- Recursive CTE termination guarantees
- Concurrent access safety

---

## Summary

| Layer | Fixed | Accepted | Deferred |
|-------|-------|----------|----------|
| KV | 6 | 1 | 2 |
| SQL | 3 | 0 | 5 |

**Commits**: c167412, d9d9da0, a730428, 299d90e
