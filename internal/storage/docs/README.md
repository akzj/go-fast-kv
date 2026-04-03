# Storage Module

## Overview

The `storage` module manages segment lifecycle and provides low-level file I/O for the append-only storage system.

## Architecture

```
┌─────────────────────────────────────────────┐
│            SegmentManager                    │
│  • Creates/seals/archives segments          │
│  • Manages active segment rotation          │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│              Segment                         │
│  • Header (32 bytes)                        │
│  • Data pages (4KB each, aligned)           │
│  • Trailer (32 bytes)                       │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│          FileOperations                      │
│  • Abstracts OS file I/O                     │
│  • Mockable for testing                      │
└─────────────────────────────────────────────┘
```

## Segment Lifecycle

```
Active ──► Sealed ──► Archived
(Writes)   (Read)     (Cold)
   │          │          │
 Exactly   No new     Read-only,
 one at    writes     may compact
 a time                 or delete
```

## File Format

### Segment Header (32 bytes)
- Magic: "FASTSEG" (8 bytes)
- Version: uint16
- SegmentID: uint64 (big-endian)
- CreatedAt: uint64 (unix nanos)
- Flags: uint16 (state)
- Reserved: 6 bytes

### Segment Trailer (32 bytes)
- PageCount: uint64
- DataSize: uint64
- Checksum: uint64 (CRC64)
- Reserved: 8 bytes

## Key Interfaces

### SegmentManager
- `ActiveSegment()` - Current writable segment
- `CreateSegment()` - Rotate to new segment
- `SealSegment()` - Close segment to writes
- `ArchiveSegment()` - Mark as cold storage

### Segment
- `Append()` - Write data, return VAddr
- `ReadAt()` - Read data at offset
- `State()` - Current lifecycle state

### FileOperations
- `Open()`, `Close()`
- `ReadAt()`, `WriteAt()`
- `Sync()`, `Truncate()`

## Usage

```go
import "github.com/akzj/go-fast-kv/internal/storage"

// Open segment manager
mgr, err := storage.OpenSegmentManager(storage.StorageConfig{
    Directory:   "/data/db",
    SegmentSize: 1 << 30, // 1GB
})

// Get active segment
seg := mgr.ActiveSegment()

// Append data
addr, err := seg.Append([]byte("value"))
```

## Module Structure

```
storage/
├── api/api.go       # All interfaces
├── internal/        # Segment file format, I/O
├── docs/            # This documentation
└── storage.go       # Re-export from api
```
