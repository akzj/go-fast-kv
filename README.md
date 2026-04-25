# go-fast-kv

A high-performance embedded key-value store written in Go.

**go-fast-kv** uses a B-link tree with MVCC versioning, append-only segment storage, and a WAL with channel-based group commit. It is designed for concurrent read/write workloads where traditional B-tree stores (like BoltDB) become bottlenecked by single-writer serialization.

## Features

- **B-link tree** with per-page locking — true concurrent reads and writes
- **MVCC + SSI** — readers never block writers; Serializable Snapshot Isolation prevents write skew
- **WAL group commit** — fsync latency is the natural batching window, zero artificial delay
- **WAL segmentation** — O(k) deletion instead of O(n), enables efficient WAL pruning
- **WriteBatch API** — group multiple Put/Delete into one atomic batch with one fsync
- **Bulk loading** — fast B-tree bulk import with optional MVCC visibility
- **SyncMode** — choose between maximum durability (`SyncAlways`) or maximum performance (`SyncNone`)
- **Page checksums** — CRC32-IEEE on every page record, detects torn writes and silent corruption
- **LRU page cache** — hot page caching reduces disk I/O for repeated reads
- **Bloom filter** — skip irrelevant segments during point lookups
- **Auto compaction** — background compaction when fragmentation exceeds threshold
- **Append-only storage** — no in-place updates, crash-safe by design
- **Checkpoint + WAL recovery** — full crash recovery with bounded replay window
- **Incremental vacuum** — non-blocking segment compaction, process in configurable batches
- **Metrics** — Prometheus-compatible metrics for WAL, compaction, transactions, and operations
- **GC / Vacuum** — reclaim space from deleted and overwritten entries

## SQL Layer

go-fast-kv includes a full SQL query engine for relational queries over your key-value data.

### Supported Features

| Category | Features |
|----------|----------|
| **DDL** | `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX` |
### Quick Example

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT)
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
SELECT name FROM users WHERE id > 1
-- Returns: Bob

SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 1
```

### Supported SQL Features

#### Data Definition (DDL)
| Feature | Status | Notes |
|---------|--------|-------|
| `CREATE TABLE` | ✅ | PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT |
| `DROP TABLE` | ✅ | IF EXISTS |
| `ALTER TABLE` | ✅ | ADD COLUMN, DROP COLUMN, RENAME COLUMN |
| `CREATE INDEX` | ✅ | UNIQUE, IF NOT EXISTS |
| `DROP INDEX` | ✅ | IF EXISTS |

#### Data Manipulation (DML)
| Feature | Status | Notes |
|---------|--------|-------|
| `INSERT` | ✅ | Multi-row, INSERT ... SELECT |
| `UPDATE` | ✅ | Multi-column SET |
| `DELETE` | ✅ | WHERE clause |

#### Queries
| Feature | Status | Notes |
|---------|--------|-------|
| `SELECT` | ✅ | *, expressions, DISTINCT |
| `WHERE` | ✅ | =, <>, <, >, <=, >=, AND, OR, NOT |
| `IN`, `NOT IN` | ✅ | With literals |
| `BETWEEN` | ✅ | Inclusive range |
| `LIKE` | ✅ | %, _ wildcards |
| `IS NULL`, `IS NOT NULL` | ✅ | |
| `JOIN` | ✅ | INNER, LEFT, RIGHT, CROSS |
| `GROUP BY` | ✅ | |
| `HAVING` | ✅ | |
| `ORDER BY` | ✅ | ASC, DESC, multi-column |
| `LIMIT`, `OFFSET` | ✅ | |
| Subqueries | ✅ | Scalar, IN, EXISTS, correlated |
| Derived tables | ✅ | (SELECT ...) AS alias |

#### Aggregates
| Function | Status |
|----------|--------|
| `COUNT(*)` | ✅ |
| `SUM` | ✅ |
| `AVG` | ✅ |
| `MIN` | ✅ |
| `MAX` | ✅ |

#### Set Operations
| Operation | Status |
|-----------|--------|
| `UNION` | ✅ |
| `UNION ALL` | ✅ |
| `INTERSECT` | ✅ |
| `EXCEPT` | ✅ |

#### Expressions
| Feature | Status |
|---------|--------|
| `CASE`/`WHEN`/`ELSE`/`END` | ✅ |
| `COALESCE` | ✅ |
| `NULLIF` | ✅ |
| `CAST` | ✅ |
| `SUBSTRING`, `CONCAT` | ✅ |
| `UPPER`, `LOWER` | ✅ |
| `LENGTH`, `TRIM` | ✅ |

#### Transaction Control
| Feature | Status | Notes |
|---------|--------|-------|
| `BEGIN` | ✅ | Starts transaction |
| `COMMIT` | ✅ | Flushes WAL, updates CLOG |
| `ROLLBACK` | ✅ | Rollback pending writes |
| `SELECT ... FOR UPDATE` | ✅ | NOWAIT, SKIP LOCKED |
| Isolation Level | ✅ | SSI (Serializable Snapshot Isolation) |

#### Query Analysis
| Feature | Status |
|---------|--------|
| `EXPLAIN` | ✅ |
| `EXPLAIN ANALYZE` | ✅ |

#### Data Types
| Type | Status |
|------|--------|
| `INT` / `INTEGER` | ✅ |
| `TEXT` | ✅ |
| `FLOAT` | ✅ |
| `BLOB` | ✅ |

### Known Limitations

| Feature | Status | Notes |
|---------|--------|-------|
| `FOREIGN KEY` | ✅ | Multi-column supported |
| `AUTOINCREMENT` | ✅ | INT PRIMARY KEY AUTOINCREMENT |
| `SAVEPOINT` | ✅ | SAVEPOINT, ROLLBACK TO SAVEPOINT, RELEASE SAVEPOINT |
| Named parameters | ❌ | Only `$1`, `$2`, ... positional |
| `CHECK` constraints | ✅ | Column and table level CHECK |
| Isolation level configuration | ❌ | Only SERIALIZABLE (SSI) |
| `LastInsertId()` | ⚠️ | Always returns 0 (KV stores don't support this) |

See [docs/sql.md](docs/sql.md) for complete SQL language documentation.

## Quick Start

```go
package main

import (
    kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
    kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
)

func main() {
    // Open a store
    store, err := kvstore.Open(kvstoreapi.Config{
        Dir: "/tmp/my-kv-store",
    })
    if err != nil {
        panic(err)
    }
    defer store.Close()

    // Put / Get / Delete
    store.Put([]byte("hello"), []byte("world"))

    val, err := store.Get([]byte("hello"))
    // val == []byte("world")

    store.Delete([]byte("hello"))

    // Scan (range query)
    iter := store.Scan([]byte("a"), []byte("z"))
    defer iter.Close()
    for iter.Next() {
        _ = iter.Key()
        _ = iter.Value()
    }

    // WriteBatch (atomic, one fsync for N operations)
    batch := store.NewWriteBatch()
    for i := 0; i < 100; i++ {
        batch.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
    }
    batch.Commit()

    // Checkpoint (snapshot for faster recovery)
    store.Checkpoint()
}
```

## Configuration

```go
store, err := kvstore.Open(kvstoreapi.Config{
    Dir:             "/data/my-store",    // Required: data directory
    MaxSegmentSize:  64 * 1024 * 1024,    // Optional: segment file size (default 64MB)
    InlineThreshold: 256,                 // Optional: max inline value size (default 256B)
    SyncMode:        kvstoreapi.SyncAlways, // Optional: durability mode (default SyncAlways)
})
```

### SyncMode

| Mode | Durability | Performance | Use Case |
|------|-----------|-------------|----------|
| `SyncAlways` (default) | ✅ No data loss on crash | ~2,350 μs/op single write | Financial data, critical state |
| `SyncNone` | ⚠️ Recent writes may be lost on crash | ~60 μs/op single write | Caches, analytics, rebuilable data |

`Close()` and `Checkpoint()` always fsync regardless of SyncMode.

## Performance

All benchmarks on Intel Core i7-14700KF, Linux, GOMAXPROCS=4.

### vs BoltDB vs Badger (same durability mode)

| Benchmark | go-fast-kv | BoltDB | Badger | Notes |
|-----------|-----------|--------|--------|-------|
| **PutSequential** | **2,594 μs** | 4,117 μs | 3,027 μs | All with fsync per write |
| **PutConcurrent×10** | **421 μs** | 4,137 μs | — | go-fast-kv: 10x faster than BoltDB |
| **BatchPut×100** | **9,862 μs** | 6,141 μs | — | WriteBatch API, 1.7x vs BoltDB |
| **GetSequential** | 2.98 μs | **0.75 μs** | 34.9 μs | BoltDB wins via mmap zero-copy |
| **GetConcurrent×10** | 11.8 μs | **1.23 μs** | — | BoltDB wins via mmap |
| **Mixed 5W+5R** | **473 μs** | 2,096 μs | — | go-fast-kv: 4.4x faster |
| **Scan×100** | 105 μs | **7.1 μs** | 129 μs | BoltDB wins via sequential mmap |

### SyncNone mode (async writes, like Badger default)

| Store | μs/op | Notes |
|-------|-------|-------|
| **go-fast-kv** (SyncNone) | **60** | Faster than Badger |
| Badger (SyncWrites=false) | 70 | Badger's default mode |

### Where go-fast-kv wins

- **Concurrent writes**: 10x faster than BoltDB — per-page locking vs single-writer serialization
- **Mixed read/write**: 4.4x faster than BoltDB — readers and writers don't block each other
- **Sequential writes**: 1.6x faster than BoltDB — WAL group commit amortizes fsync
- **Async writes**: 1.16x faster than Badger at same durability level

### Where go-fast-kv loses

- **Point reads**: BoltDB is 4x faster — mmap provides zero-copy, zero-syscall reads
- **Scans**: BoltDB is 15x faster — sequential mmap access vs page cache + deserialization

### Optimization journey

| Metric | Phase 1 (baseline) | Phase 2 (optimized) | Improvement |
|--------|--------------------|--------------------|-------------|
| PutSingle | 5,060 μs | 2,350 μs | **2.2x** |
| PutConcurrent×10 | 3,240 μs | 190 μs | **17x** |
| GetConcurrent×10 | 20.3 μs | 11.8 μs | **42% faster** |
| Mixed 5W+5R | 19,600 μs | 4,300 μs | **4.6x** |
| BatchPut×100 | 372,718 μs | 9,862 μs | **37.8x** |

### Bulk Loading Performance

For large data imports, BulkLoad provides dramatic speedups over incremental writes:

| Method | 1,000 entries | 100,000 entries | Complexity |
|--------|---------------|-----------------|------------|
| `Put` (per-key) | ~2,400 ms | ~240,000 ms | O(n log n) |
| `WriteBatch` | ~200 ms | ~20,000 ms | O(n log n), 1 fsync |
| **`BulkLoad`** | **~50 ms** | **~500 ms** | O(n) |

## Bulk Loading

BulkLoad bypasses the normal O(log n) insert path by building the B-tree bottom-up in memory, then atomically swapping the root pointer.

### How it works

```
BulkLoad(entries):
  1. Sort entries by key (pre-sorted input required)
  2. Build leaf pages in memory (4KB chunks)
  3. Build internal nodes bottom-up
  4. Atomically update rootPageID
  5. Write single WAL entry for crash recovery
```

### API

```go
import "github.com/akzj/go-fast-kv/internal/btree/api"

// BulkLoad: fast mode, visible to all readers immediately
// Data must be pre-sorted by key
err := store.BulkLoad([]btreeapi.KVPair{
    {Key: []byte("a"), Value: []byte("1")},
    {Key: []byte("b"), Value: []byte("2")},
    // ... must be sorted
})

// BulkLoadMVCC: with MVCC versioning
// Entries visible only to transactions >= startTxnID
startTxnID := txnManager.NextTxnID()
err := store.BulkLoadMVCC(pairs, startTxnID)
```

### Restrictions

| Restriction | Description |
|-------------|-------------|
| **Sorted input** | Entries must be sorted by key in ascending order |
| **Exclusive write** | BulkLoad holds a write lock during operation |
| **Concurrent reads** | Get/Scan can run concurrently (snapshot semantics) |
| **WAL optimization** | Only root change is logged (not individual pages) |

### Fast vs MVCC Mode

| Mode | txnID | Visibility | Use Case |
|------|-------|------------|----------|
| `BulkLoad` | 0 | Immediately visible to all | Initial data load, migrations |
| `BulkLoadMVCC` | startTxnID | Visible to transactions ≥ startTxnID | Historical data, versioned imports |

### When to use BulkLoad

✅ **Use BulkLoad for:**
- Initial database population
- Batch data migrations
- Importing sorted datasets
- ETL pipelines

❌ **Use WriteBatch for:**
- Small batches (< 1,000 entries)
- Unordered data
- Real-time writes
- Operations needing transaction boundaries

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   KVStore                        │  User API
│        Put / Get / Delete / Scan / WriteBatch    │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│              B-link Tree (MVCC)                   │  Index
│    per-page RwLock · LRU page cache (1024)       │
│    CRC32 checksums on every page record          │
└────────┬───────────────────────────┬────────────┘
         │                           │
┌────────▼─────────┐       ┌────────▼─────────┐
│    PageStore      │       │    BlobStore      │   Storage mapping
│  PageID → VAddr   │       │  BlobID → VAddr   │   (dense array)
└────────┬─────────┘       └────────┬─────────┘
         │                           │
┌────────▼───────────────────────────▼────────────┐
│              Segment Manager                      │  Append-only files
│         Append / ReadAt / Rotate / Sync          │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│                     WAL                          │  Durability
│  Channel-based group commit · SyncMode config    │
│  fsync latency = natural batching window         │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│              Checkpoint + Recovery                │  Crash recovery
│  Full state snapshot · Bounded WAL replay        │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│               GC / Vacuum                        │  Space reclamation
│  Page GC · Blob GC · Segment compaction          │
└─────────────────────────────────────────────────┘
```

### Key Design Decisions

**Why B-link tree instead of LSM-tree?**
- B-link tree provides O(log N) point reads without compaction overhead
- Per-page locking enables true concurrent access (not just concurrent memtable writes)
- Predictable latency — no background compaction stalls

**Why append-only segments instead of mmap?**
- Append-only is crash-safe by design — no partial in-place overwrites
- Works on all platforms without mmap complexity
- Trade-off: reads are slower than mmap (4x vs BoltDB), but writes are faster

**Why WAL group commit via channels?**
- fsync latency (~2ms) is the natural batching window — no artificial delay needed
- Producer-consumer pattern: writers submit to buffered channel, single consumer batches write + fsync
- Result: 24.7x throughput improvement under 50 concurrent writers

**Why CRC32 checksums?**
- SyncNone mode can produce torn writes on crash
- Without checksums, torn writes cause silent data corruption in the B-tree
- CRC32-IEEE overhead is ~4μs per 4KB page — negligible vs 61μs tree.Put cost

## Module Structure

```
internal/
├── kvstore/          # Top-level KVStore API (Put/Get/Delete/Scan/WriteBatch)
│   └── api/          # Config, options, exported types
├── btree/            # B-link tree with MVCC, per-page locks, LRU cache, bulk loading
│   └── api/          # Public API
│   └── internal/     # Implementation: bulk.go (BulkLoad / BulkLoadMVCC)
├── pagestore/        # PageID → VAddr mapping + CRC32 checksums + WAL segments
│   └── api/          # Config
│   └── internal/     # Implementation: LRU page cache
├── vacuum/           # Segment compaction and space reclamation
│   └── api/          # RunIncremental API
│   └── internal/     # Incremental vacuum implementation
├── wal/              # Write-ahead log with channel-based group commit + CheckpointManager
├── txn/              # MVCC transaction manager + SSI (SIndex / TIndex)
├── metrics/          # Prometheus-compatible metrics (WAL, compaction, txn, operations)
└── bench/            # Benchmark suite (vs BoltDB, vs Badger)
```

Each module follows the convention:
```
internal/{module}/
├── api/api.go        # Public interfaces and types
├── internal/*.go     # Implementation (unexported)
└── {module}.go       # Re-export layer (factory functions)
```

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
```

## Running Tests

```bash
# All tests
go test ./... -count=1

# With race detector
go test ./internal/kvstore/internal/ -race -count=1
go test ./internal/btree/internal/ -race -count=1
go test ./internal/wal/internal/ -race -count=1
```

**168 tests** across 11 packages, all passing, race-clean.

## Project Stats

| Metric | Value |
|--------|-------|
| Production code | ~15,000 lines |
| Test code | ~10,000 lines |
| Test packages | 15 |
| Modules | 10 |
| Go version | 1.23+ |

## Documentation

- [Architecture](docs/architecture.md) — Full system architecture (KV + SQL layers)
- [Bug Fixes](docs/BUGFIXES.md) — All bugs found and fixed with root cause analysis
- [Performance](docs/PERFORMANCE.md) — Benchmark results, optimizations, and analysis
- [Known Issues](docs/KNOWN_ISSUES.md) — Identified issues not yet fixed
- [Design Notes](docs/DESIGN_NOTES.md) — Patterns and pitfalls for future developers
- [Design](docs/DESIGN.md) — Original KV design document
- [SQL Design](docs/DESIGN_SQL.md) — Original SQL design document

## License

MIT
