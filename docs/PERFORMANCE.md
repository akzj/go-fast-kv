# Performance

Benchmark results, optimization history, and analysis for go-fast-kv.

---

## Benchmark Environment

- **CPU**: Intel Core i7-14700KF
- **OS**: Linux
- **Go**: 1.23.0
- **GOMAXPROCS**: 4
- **Date**: 2026

---

## Comparative Benchmarks

### vs BoltDB vs Badger (Same Durability: SyncAlways)

| Benchmark | go-fast-kv | BoltDB | Badger | Winner |
|-----------|-----------|--------|--------|--------|
| **PutSequential** | **2,594 μs** | 4,117 μs | 3,027 μs | go-fast-kv (1.6x) |
| **PutConcurrent×10** | **421 μs** | 4,137 μs | — | go-fast-kv (10x) |
| **BatchPut×100** | 9,862 μs | **6,141 μs** | — | BoltDB (1.6x) |
| **GetSequential** | 2.98 μs | **0.75 μs** | 34.9 μs | BoltDB (4x) |
| **GetConcurrent×10** | 11.8 μs | **1.23 μs** | — | BoltDB (10x) |
| **Mixed 5W+5R** | **473 μs** | 2,096 μs | — | go-fast-kv (4.4x) |
| **Scan×100** | 105 μs | **7.1 μs** | 129 μs | BoltDB (15x) |

### SyncNone Mode (Async Writes)

| Store | μs/op | Notes |
|-------|-------|-------|
| **go-fast-kv** (SyncNone) | **60** | Faster than Badger default |
| Badger (SyncWrites=false) | 70 | Badger's default mode |

### Where go-fast-kv Wins

- **Concurrent writes**: 10x faster than BoltDB — per-page locking vs single-writer serialization
- **Mixed read/write**: 4.4x faster than BoltDB — readers and writers don't block each other
- **Sequential writes**: 1.6x faster than BoltDB — WAL group commit amortizes fsync
- **Async writes**: 1.16x faster than Badger at same durability level

### Where go-fast-kv Loses

- **Point reads**: BoltDB is 4x faster — mmap provides zero-copy, zero-syscall reads
- **Scans**: BoltDB is 15x faster — sequential mmap access vs page cache + deserialization

**Note**: Zero-copy node deserialization (`dev-v2`) removes CRC32 checksum overhead and eliminates memory allocations for keys/values, significantly reducing the deserialization gap. MemPageProvider bypasses CRC32 at the storage layer entirely.

---

## Optimization History

### Phase 1 → Phase 2 Improvements

**Phase 2 optimizations**: ~200% throughput improvement over Phase 1 baseline.

| Optimization | Throughput Gain |
|---|---|
| Fast Goroutine ID | baseline |
| Slotted Page Rewrite | +33.5% |
| Dynamic Page Size | +20% |
| WAL Group Commit | +40% |
| Hot Node Cache | +15% |
| Binary Search in B-tree | +10% |
| Zero-copy Deserialize | +25-50% (MemPageProvider) |

**Commit**: `e5990e4` (dev-v2) — Zero-copy Deserialize: remove CRC32 checksum, eliminate allocations in `nodeSerializer.Deserialize()`

| Metric | Phase 1 (baseline) | Phase 2 (optimized) | Improvement |
|--------|--------------------|--------------------|-------------|
| PutSingle | 5,060 μs | 2,350 μs | **2.2x** |
| PutConcurrent×10 | 3,240 μs | 190 μs | **17x** |
| GetConcurrent×10 | 20.3 μs | 11.8 μs | **42% faster** |
| Mixed 5W+5R | 19,600 μs | 4,300 μs | **4.6x** |
| BatchPut×100 | 372,718 μs | 9,862 μs | **37.8x** |

### Key Optimizations Applied

#### 1. Fast Goroutine ID (2564x faster)
**Commit**: `a4f7f89`

Replaced `runtime.Stack()` parsing with TLS assembly-based goroutine ID lookup. This was the single largest bottleneck — every KV operation called `goroutineID()` for WAL collector lookup.

- Before: ~12.5 μs per goroutineID() call
- After: ~4.9 ns per call
- Impact: Write throughput 18K → 54K ops/s (+200%)

#### 2. Slotted Page Rewrite (33.5% throughput gain)
**Commits**: `a0214eb`, `19ffc08`

Replaced serialize/deserialize cycle with direct `[]byte` manipulation using a slotted page format. Eliminated all `Node` struct allocations on the read path.

- Before: 84K ops/s
- After: 112K ops/s
- Impact: Zero-copy reads, reduced GC pressure

#### 3. Dynamic Page Size (Compact VAddr)
**Commits**: `554465a`, `6245214`–`3b61028`

Variable-length page serialization with compact VAddr encoding (20:30:14 bit layout). Pages are serialized at their actual size instead of fixed 4096 bytes.

- Impact: +51% write throughput (combined with B-link fix and cloneBytes elimination)

#### 4. Buffer Pooling
**Commits**: `6869bb3`, `aeab2f3`, `7ae3a05`, `c807263`

Pooled allocations for:
- Page serialize buffer (4096 bytes) — eliminated ~4.1GB alloc/1M writes
- PageStore record buffer (4108 bytes) — eliminated ~4.8GB alloc/1M writes
- WAL batch/collector objects — eliminated ~3M allocs/1M writes

#### 5. WAL Group Commit
Channel-based producer-consumer pattern. fsync latency (~2ms) is the natural batching window.

- Impact: 24.7x throughput improvement under 50 concurrent writers

#### 6. Hot Node Cache
**Commit**: `3310118`

In-memory cache for recently accessed B-tree pages. Avoids segment reads for hot pages.

#### 7. Binary Search in B-tree Operations
**Commits**: `541968c`, `dbcff14`

- `mvccInsert` and `findInsertPos`: binary search instead of linear scan
- `findChild` in internal nodes: binary search O(log k) instead of O(k)

#### 8. Vacuum Optimizations
**Commits**: `e99099f`, `5ceb08f`

- Compact entries in-place instead of allocating new slices (~2GB saved)
- Reuse `keep[]` and `blobsToFree[]` buffers (~21.3M allocs eliminated)

---

## Bulk Loading Performance

| Method | 1,000 entries | 100,000 entries | Complexity |
|--------|---------------|-----------------|------------|
| `Put` (per-key) | ~2,400 ms | ~240,000 ms | O(n log n) |
| `WriteBatch` | ~200 ms | ~20,000 ms | O(n log n), 1 fsync |
| **`BulkLoad`** | **~50 ms** | **~500 ms** | O(n) |

BulkLoad builds the B-tree bottom-up in memory, then atomically swaps the root pointer.

---

## Architecture Impact on Performance

### Why Writes Are Fast

1. **Per-page locking**: Multiple writers operate on different pages concurrently. BoltDB's single-writer lock serializes all writes.
2. **WAL group commit**: Multiple concurrent writes share a single fsync. Under 10 concurrent writers, each write's amortized fsync cost is ~1/10.
3. **Append-only segments**: No read-modify-write cycle. Just append and update the mapping.

### Why Reads Are Slower Than BoltDB

1. **No mmap for reads**: BoltDB maps the entire database file. Page reads are pointer dereferences (no syscall). go-fast-kv reads through the page cache or falls back to `ReadAt()`.
2. **Indirection**: go-fast-kv resolves `pageID → VAddr → segment read`. BoltDB resolves `pageID → mmap offset` (single multiplication).
3. **Deserialization**: go-fast-kv deserializes slotted pages from `[]byte`. BoltDB casts mmap'd memory directly to page structs.

### Why Scans Are Slower Than BoltDB

BoltDB's B-tree leaves are stored contiguously in the mmap'd file. Sequential leaf traversal benefits from OS prefetching. go-fast-kv's leaves are scattered across segments, causing random I/O patterns.

---

## SQL Layer Performance

### FTS (Full-Text Search)

| Operation | Data Scale | Performance | Memory |
|-----------|-----------|-------------|--------|
| IndexDocument (small) | 1 doc | 2.86 ms/op | 81 KB/op |
| IndexDocument (medium) | 1 doc | 3.97 ms/op | 936 KB/op |
| Search_Term | 100 docs | 135 μs/op | 59 KB/op |
| Search_Term | 1,000 docs | 3.57 ms/op | 2.8 MB/op |
| Search_AND | 1,000 docs | 1.45 ms/op | 1.3 MB/op |
| Tokenize (small) | — | 1.6 μs/op | 1.4 KB/op |
| PorterStem | 1 word | 0.5 μs/op | 0 allocs |

### Known Bottlenecks

1. **FTS memory at scale**: 5,000-doc search allocates 16MB. Needs streaming scan or result caching.
2. **No query plan caching**: Every SQL query is parsed and planned from scratch. Prepared statement cache helps but doesn't eliminate planning cost.
3. **In-memory ORDER BY**: All result rows materialized before sorting. Large result sets can cause OOM.

---

## Running Benchmarks

```bash
# Full comparison suite (go-fast-kv vs BoltDB vs Badger)
GOMAXPROCS=4 go test ./internal/bench/ \
  -bench='(GoFastKV|BoltDB|Badger)' \
  -benchtime=500x -count=1 -timeout=10m

# KVStore internal benchmarks
go test ./internal/kvstore/internal/ \
  -bench=BenchmarkKVStore -benchtime=3s -count=1

# WAL group commit benchmarks
go test ./internal/wal/internal/ \
  -bench=BenchmarkWAL -benchtime=3s -count=1

# SQL / FTS benchmarks
go test ./internal/sql/engine/internal -bench=FTS -benchmem -benchtime=100ms
```
