# Known Issues

Issues identified during the deep code review (2026) that were not fixed. Organized by severity.

---

## KV Layer

### KI-1: Vacuum Reads/Writes Without Page Locks (CRITICAL)

**Location**: `internal/vacuum/internal/vacuum.go`

Vacuum traverses B-tree leaves to find old MVCC versions to clean up. It reads and rewrites leaf pages without acquiring the per-page write lock from the B-link tree's lock manager. Concurrent Put/Delete operations on the same leaf can produce corrupted pages.

**Workaround**: Vacuum runs during low-traffic periods. The auto-vacuum throttle (min 2s interval) reduces but does not eliminate the race window.

---

### KI-2: Iterator Use After Store Close (HIGH)

**Location**: `internal/btree/internal/btree.go`

Iterators created by `Scan()` hold references to the page provider and segment manager. If the store is closed while an iterator is still in use, subsequent `Next()` calls read from closed file descriptors or unmapped memory.

**Workaround**: Application must ensure all iterators are closed before calling `store.Close()`.

---

### KI-3: goroutineID Fragility (MEDIUM)

**Location**: `internal/goid/`

The fast goroutine ID implementation uses TLS assembly to read the goroutine ID directly from the Go runtime's internal `g` struct. This depends on the offset of the `goid` field, which is not part of Go's public API and could change between Go versions.

**Workaround**: The offset is verified at init time. If it changes, the fallback is `runtime.Stack()` parsing (2564x slower).

---

### KI-4: GC Not Integrated (MEDIUM)

**Location**: `internal/gc/`, `internal/kvstore/`

The GC package (`internal/gc/`) implements page and blob garbage collection, but `kvstore` never calls `CollectOne()`. Sealed segments accumulate forever. Disk space from overwritten pages/blobs is never reclaimed.

**Status**: The vacuum system handles MVCC version cleanup (logical layer), but physical segment compaction (GC) is not wired in.

---

### KI-5: B-link Tree Delete Does Not Merge Underflow Pages (LOW)

**Location**: `internal/btree/internal/btree.go`

Delete marks entries with `TxnMax` (MVCC tombstone) but never merges underfull pages. Over time, after many deletes, the tree accumulates half-empty pages. This wastes memory and I/O but does not affect correctness.

**Mitigation**: Vacuum eventually removes tombstoned entries, which helps. But pages that become completely empty are never freed or merged with siblings.

---

### KI-6: No WAL Size-Based Auto-Checkpoint (LOW)

**Location**: `internal/kvstore/internal/checkpoint.go`

The design document specifies auto-checkpoint when WAL reaches 16MB. This is not implemented. Checkpoints are only created on explicit `store.Checkpoint()` or `store.Close()`. Without checkpoints, WAL replay on recovery processes the entire WAL history.

---

### KI-7: LSM Compaction Not Concurrent-Safe with Reads (MEDIUM)

**Location**: `internal/lsm/internal/lsm.go`

LSM compaction replaces SSTable files. If a read is in progress on an SSTable that gets compacted away, the read may fail. The current implementation relies on file descriptor caching and OS-level reference counting (deleted files remain readable until all fds are closed), which works on Linux but is not portable.

---

### KI-8: Checkpoint File Has No Version Migration (LOW)

**Location**: `internal/kvstore/internal/checkpoint.go`

The checkpoint format has evolved through V1–V4 but there is no automatic migration. If the checkpoint format changes again, old checkpoint files will fail to deserialize, requiring a manual recovery procedure.

---

### KI-9: BlobStore Dense Array Grows Monotonically (LOW)

**Location**: `internal/blobstore/internal/blobstore.go`

Blob IDs are never reused. The dense array (`[]BlobMeta`) grows monotonically. After many blob writes and deletes, the array contains many zero entries (deleted blobs) that waste memory. For long-running stores with heavy blob churn, this could become significant.

---

## SQL Layer

### Parser Limitations

#### ORDER BY Only Accepts Column References (P1)
**Location**: `parser/internal/parser.go:1785`

`ORDER BY` rejects expressions, positional references, and aliases. Only bare column names accepted.

#### No Block Comment Support (`/* ... */`) (P2)
**Location**: `parser/internal/lexer.go:237-249`

Only `--` line comments handled.

#### No Quoted Identifier Support (P1)
**Location**: `parser/internal/lexer.go`

Neither double-quoted nor backtick identifiers supported. Keywords as identifiers cannot be escaped.

#### No Scientific Notation in Numeric Literals (P2)
**Location**: `parser/internal/lexer.go:296-314`

`readNumber` handles integers and decimals but not scientific notation.

### Planner Limitations

#### CTE Column Types Are All TypeBlob (P1)
**Location**: `planner/internal/planner.go:745-748`

All CTE columns assigned `TypeBlob` regardless of actual type.

### Catalog Limitations

#### Schema Cache Returns Mutable Pointer (P1)
**Location**: `catalog/internal/catalog_impl.go:134`

`getTableImpl` returns cached `*TableSchema` pointer directly. Any mutation corrupts the cache.

#### Schema Cache Is FIFO, Not LRU (P2)
**Location**: `catalog/internal/catalog_impl.go:152-160`

Comment says "LRU" but implementation is FIFO. No promotion on hit.

### Executor Limitations

#### Arithmetic Operators Double-Evaluate Operands (P2)
**Location**: `executor/internal/eval.go:~306-355`

`evalBinaryExpr` evaluates operands twice. Wasteful but not a correctness bug for side-effect-free expressions.

#### HAVING Without GROUP BY Accepted in Some Paths (P2)

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
