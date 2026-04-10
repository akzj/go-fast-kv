# go-fast-kv Examples

Standalone examples demonstrating go-fast-kv's key features.

## Quick Start

All examples use a temp directory and clean up after themselves. Run any example with:

```
go run github.com/akzj/go-fast-kv/examples/{name}
```

Or from the repo root:
```
go run ./examples/{name}
```

## Examples

### [basic](./basic/main.go)
Fundamental KV operations: Open, Put, Get, Delete, Scan.
```
go run ./examples/basic
```

### [batch](./batch/main.go)
WriteBatch vs single Put performance. WriteBatch groups multiple operations into
one atomic transaction with ONE WAL fsync — **~30-85x faster** for bulk writes.
```
go run ./examples/batch
```

### [concurrent](./concurrent/main.go)
Multi-goroutine concurrent read/write. Demonstrates go-fast-kv's thread-safety —
multiple writers and readers operate simultaneously without data loss.
```
go run ./examples/concurrent
```

### [iterator](./iterator/main.go)
Range scan with iterator. Insert 100 keys, scan ranges like `[user:020, user:030)`,
iterate with `Next()`, `Key()`, `Value()`.
```
go run ./examples/iterator
```

### [largevalue](./largevalue/main.go)
Large value storage via BlobStore. Values > 256 bytes are transparently stored
in BlobStore and retrieved seamlessly.
```
go run ./examples/largevalue
```

### [vacuum](./vacuum/main.go)
Auto-vacuum (lazy async goroutine) vs manual `RunVacuum()`. Auto-vacuum triggers
after `AutoVacuumThreshold` operations (default 1000), cleans up dead MVCC versions,
and exits. Manual `RunVacuum()` always available.
```
go run ./examples/vacuum
```

### [checkpoint](./checkpoint/main.go)
Checkpoint and crash recovery. Checkpoint writes a full snapshot to disk, enabling
fast recovery on restart. Without checkpoint, WAL replay is used.
```
go run ./examples/checkpoint
```

### [syncmode](./syncmode/main.go)
`SyncAlways` (fsync per write, maximum durability) vs `SyncNone` (no per-write
fsync, **~55x faster**, risk of data loss on crash). Best for bulk import and
rebuildable data.
```
go run ./examples/syncmode
```

### [config](./config/main.go)
Complete `kvstoreapi.Config` reference. All configuration options documented:
`Dir`, `MaxSegmentSize`, `InlineThreshold`, `SyncMode`, `AutoVacuumThreshold`.
```
go run ./examples/config
```

## Running All Examples

```bash
for ex in basic batch concurrent iterator largevalue vacuum checkpoint syncmode config; do
    echo "=== $ex ==="
    go run ./examples/$ex
    echo
done
```

## Build All at Once

```bash
go build ./examples/basic
go build ./examples/batch
go build ./examples/concurrent
go build ./examples/iterator
go build ./examples/largevalue
go build ./examples/vacuum
go build ./examples/checkpoint
go build ./examples/syncmode
go build ./examples/config
```
