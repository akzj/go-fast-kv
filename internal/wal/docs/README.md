# WAL Module

## Overview

The `wal` (Write-Ahead Log) module provides durability guarantees for the storage system by logging all mutations before applying them to data.

## Architecture

```
┌─────────────────────────────────────────────┐
│              WAL                             │
│  • Append-only log                          │
│  • Record types: PageAlloc, NodeWrite, etc. │
│  • CRC32c checksums                         │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│        CheckpointManager                      │
│  • Create consistent snapshots              │
│  • Truncate WAL after checkpoint            │
│  • Recovery from latest checkpoint          │
└─────────────────────────────────────────────┘
```

## WAL Record Types

| Type | Description |
|------|-------------|
| WALPageAlloc | Page allocation |
| WALPageFree | Page deallocation |
| WALNodeWrite | B-link tree node write |
| WALExternalValue | External value stored |
| WALRootUpdate | Tree root VAddr change |
| WALCheckpoint | Checkpoint boundary |
| WALIndexUpdate | RadixTree index mutation |
| WALIndexRootUpdate | RadixTree root change |

## Key Interfaces

### WAL
- `Append()` - Add record, return LSN
- `ReadAt()` - Read record by LSN
- `ReadFrom()` - Iterate records from LSN
- `Truncate()` - Remove old records after checkpoint
- `Flush()` - Ensure durability

### CheckpointManager
- `CreateCheckpoint()` - Create consistent snapshot
- `LatestCheckpoint()` - Find most recent checkpoint
- `Recover()` - Restore state from checkpoint
- `TruncateWAL()` - Free space after checkpoint

## Recovery Algorithm

```
Recover():
    1. Find latest valid checkpoint
    2. Load checkpoint state (TreeRoot, PageManager, ExternalValues)
    3. Replay WAL from checkpoint LSN to end
    4. Verify integrity
    5. Truncate WAL past checkpoint LSN
```

## Why Redo-Only?

Append-only storage never overwrites data. Deleted data uses tombstones, not in-place removal. Therefore, only redo committed operations is needed (no undo).

## Usage

```go
import "github.com/akzj/go-fast-kv/internal/wal"

// Open WAL
w, err := wal.OpenWAL(wal.WALConfig{
    Directory:   "/data/db/wal",
    SegmentSize: 64 * 1024 * 1024, // 64MB
    SyncWrites:  true,
})

// Append record
lsn, err := w.Append(&wal.WALRecord{
    RecordType: wal.WALNodeWrite,
    Payload:    nodeData,
})
```

## Module Structure

```
wal/
├── api/api.go       # All interfaces
├── internal/        # Segment format, serialization
├── docs/            # This documentation
└── wal.go           # Re-export from api
```
