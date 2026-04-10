# go-fast-kv Examples (`cmd/`)

Runnable examples demonstrating all major features of go-fast-kv.

## Running

```bash
# From the go-fast-kv root directory:
go run github.com/akzj/go-fast-kv/cmd/basic
go run github.com/akzj/go-fast-kv/cmd/batch
go run github.com/akzj/go-fast-kv/cmd/vacuum
go run github.com/akzj/go-fast-kv/cmd/checkpoint
go run github.com/akzj/go-fast-kv/cmd/syncmode
go run github.com/akzj/go-fast-kv/cmd/concurrent
go run github.com/akzj/go-fast-kv/cmd/largevalue
go run github.com/akzj/go-fast-kv/cmd/iterator
go run github.com/akzj/go-fast-kv/cmd/config
```

Or build and run:
```bash
go build -o /tmp/gfk-basic github.com/akzj/go-fast-kv/cmd/basic
/tmp/gfk-basic
```

## Index

| Example | Description |
|---------|-------------|
| **basic** | Open, Put, Get, Update, Delete, Scan |
| **batch** | WriteBatch — up to 40x faster for bulk writes |
| **vacuum** | MVCC cleanup — auto-vacuum and manual RunVacuum |
| **checkpoint** | Checkpoint snapshots and crash recovery |
| **syncmode** | SyncAlways vs SyncNone — durability vs performance |
| **concurrent** | Multi-goroutine concurrent reads and writes |
| **largevalue** | Large value storage (>256B) via BlobStore |
| **iterator** | Range queries with Scan — consistent snapshots |
| **config** | All configuration options |
