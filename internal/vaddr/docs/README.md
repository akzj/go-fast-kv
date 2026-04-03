# VAddr Module

## Overview

The `vaddr` module provides foundation types shared by all other modules in the storage system. It has no dependencies on other internal packages, making it safe to import without circular dependencies.

## Types

### VAddr

`VAddr` encodes a physical address in the append-only address space:
- **16 bytes**: SegmentID (8 bytes) + Offset (8 bytes)
- **Invariant**: SegmentID 0 is reserved (invalid)
- **Serialization**: Big-endian byte order

### SegmentID

Unique identifier for a segment file:
- **Invariant**: Monotonically increasing (new segments have higher IDs)
- **Minimum valid ID**: 1

### PageID

Logical identifier for a page in the page manager:
- **Invariant**: PageID > 0 (0 is reserved)
- Sequential allocation produces dense IDs (optimal for dense array index)

### SegmentState

Lifecycle state of a segment:
- **Active**: Accepting new writes (exactly one at a time)
- **Sealed**: No new writes, readable
- **Archived**: Read-only, may be compacted

### EpochID

Identifies a compaction epoch for MVCC-based garbage collection.

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| PageSize | 4096 | Standard OS page size |
| ExternalThreshold | 48 | Max inline value size |
| MaxSegmentSize | 1GB | Target size before rotation |
| EpochGracePeriod | 3 | Epochs before VAddr reclamation |

## Usage

```go
import vaddr "github.com/akzj/go-fast-kv/internal/vaddr"

// Create a VAddr
addr := vaddr.VAddr{
    SegmentID: 1,
    Offset: 4096,
}

// Check validity
if addr.IsValid() {
    // Use address
}
```

## Module Structure

```
vaddr/
├── api/api.go    # All type definitions
├── internal/     # (empty - types only)
├── docs/         # This documentation
└── vaddr.go      # Re-export from api
```
