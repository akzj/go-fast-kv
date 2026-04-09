# go-fast-kv

A high-performance embedded key-value store written in Go.

**go-fast-kv** uses a B-link tree with MVCC versioning, append-only segment storage, and a WAL with channel-based group commit. It is designed for concurrent read/write workloads where traditional B-tree stores (like BoltDB) become bottlenecked by single-writer serialization.

## Features

- **B-link tree** with per-page locking — true concurrent reads and writes
- **MVCC** — readers never block writers, writers never block readers
- **WAL group commit** — fsync latency is the natural batching window, zero artificial delay
- **WriteBatch API** — group multiple Put/Delete into one atomic batch with one fsync
- **SyncMode** — choose between maximum durability (`SyncAlways`) or maximum performance (`SyncNone`)
- **Page checksums** — CRC32-IEEE on every page record, detects torn writes and silent corruption
- **Append-only storage** — no in-place updates, crash-safe by design
- **Checkpoint + WAL recovery** — full crash recovery with bounded replay window
- **GC / Vacuum** — reclaim space from deleted and overwritten entries

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
├── btree/            # B-link tree with MVCC, per-page locks, LRU cache
├── pagestore/        # PageID → VAddr mapping + CRC32 checksums
├── blobstore/        # BlobID → VAddr mapping (large values)
├── segment/          # Append-only segment file manager
├── wal/              # Write-ahead log with channel-based group commit
├── txn/              # MVCC transaction manager (BeginTxn/Commit/Abort)
├── gc/               # Garbage collection (page GC + blob GC)
├── vacuum/           # Segment compaction and space reclamation
├── lock/             # Per-page read-write lock manager
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
| Production code | 6,380 lines |
| Test code | 7,237 lines |
| Test count | 168 |
| Modules | 10 |
| Go version | 1.23+ |

## License

MIT
