# Module Skeleton Design

**Status**: Phase 1 - Interface Definition  
**Mission**: `phase1-skeleton`  
**Date**: 2024

---

## 1. Module List with Responsibilities

| Module | Responsibility | Key Interface |
|--------|---------------|--------------|
| `vaddr` | Foundation types: VAddr, PageID, SegmentID, SegmentState | (types only, no interface) |
| `storage` | Segment lifecycle (Active→Sealed→Archived), file I/O | `SegmentManager`, `FileOperations` |
| `pagemanager` | PageID → VAddr mapping, page allocation | `PageManager`, `FixedSizeKVIndex`, `FreeList` |
| `blinktree` | B-link-tree index, node operations | `Tree`, `TreeMutator`, `NodeOperations`, `NodeManager` |
| `external-value` | External value storage (>48 bytes) | `ExternalValueStore` |
| `wal` | Write-Ahead Log, checkpoint | `WAL`, `CheckpointManager` |
| `concurrency` | Latches, single-writer model | `LatchManager` |
| `compaction` | Epoch-based MVCC, space reclamation | `EpochManager`, `Compactor` |
| `cache` | OS Page Cache management | `Cache` |
| `kvstore` | Public API | `KVStore`, `Transaction`, `Batch` |

---

## 2. Directory Layout

```
internal/
├── vaddr/
│   ├── api/
│   │   └── api.go          # VAddr, PageID, SegmentID, SegmentState types
│   ├── internal/
│   │   └── internal.go     # Stub
│   ├── docs/
│   │   └── README.md
│   └── vaddr.go            # Re-export
│
├── storage/
│   ├── api/
│   │   └── api.go          # SegmentManager, FileOperations interfaces
│   ├── internal/
│   │   └── internal.go     # Stub
│   ├── docs/
│   │   └── README.md
│   └── storage.go          # Re-export
│
├── wal/
│   ├── api/
│   │   └── api.go          # WAL, CheckpointManager interfaces
│   ├── internal/
│   │   └── internal.go     # Stub
│   ├── docs/
│   │   └── README.md
│   └── wal.go              # Re-export
│
├── pagemanager/             # (exists, verify completeness)
├── blinktree/               # (exists, verify completeness)
├── external-value/          # (exists, verify completeness)
├── concurrency/             # (exists, verify completeness)
├── compaction/             # (exists, verify completeness)
├── cache/                  # (exists, verify completeness)
└── kvstore/                # (exists, verify completeness)
```

---

## 3. Interface Definitions

### 3.1 vaddr/api/api.go - Foundation Types

```go
package vaddr

// VAddr encodes a physical address in the append-only address space.
// Invariant: VAddr is 16 bytes, never zero (SegmentID 0 is reserved).
// Why 16 bytes? Allows segment IDs up to uint64_max with 8-byte offsets.
// Why big-endian? Natural byte ordering for segment comparisons.
type VAddr struct {
    SegmentID uint64  // Identifies the segment file (1..N)
    Offset    uint64  // Byte offset within the segment
}

// IsValid returns true if this VAddr represents a valid address.
func (v VAddr) IsValid() bool {
    return v.SegmentID != 0
}

// IsZero returns true if both fields are zero.
func (v VAddr) IsZero() bool {
    return v.SegmentID == 0 && v.Offset == 0
}

// SegmentID type.
type SegmentID uint64

const (
    SegmentIDInvalid SegmentID = 0
    SegmentIDMin     SegmentID = 1
)

// PageID is the logical identifier for a page.
type PageID uint64

const PageIDInvalid PageID = 0

// SegmentState represents the lifecycle state of a segment.
type SegmentState uint8

const (
    SegmentStateActive   SegmentState = 0x01  // Accepting writes
    SegmentStateSealed   SegmentState = 0x02  // No new writes
    SegmentStateArchived SegmentState = 0x04  // Read-only, possibly compacted
)

// EpochID identifies a compaction epoch for MVCC.
type EpochID uint64

// Constants.
const (
    PageSize          = 4096
    ExternalThreshold = 48  // Max inline value size
)
```

### 3.2 storage/api/api.go - Storage Interface

```go
package storage

import (
    vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// FileType categorizes files for cache strategy assignment.
type FileType uint8

const (
    FileTypeWAL            FileType = iota  // Write-Ahead Log
    FileTypeSegment                         // Data segments
    FileTypeExternalValue                   // External value pages
    FileTypeIndex                           // Page manager index
    FileTypeCheckpoint                      // Checkpoint files
)

// FileOperations provides low-level file I/O.
// Why separate interface? Allows mock implementations for testing.
type FileOperations interface {
    // Open opens or creates a file.
    Open(path string) error

    // Close closes the file.
    Close() error

    // ReadAt reads len(p) bytes into p starting at offset.
    ReadAt(p []byte, offset int64) (int, error)

    // WriteAt writes len(p) bytes from p starting at offset.
    WriteAt(p []byte, offset int64) (int, error)

    // Sync flushes writes to durable storage.
    Sync() error

    // Size returns the current file size.
    Size() (int64, error)

    // Truncate changes the file size.
    Truncate(size int64) error
}

// SegmentManager manages segment lifecycle.
// Invariant: Exactly one segment is Active at any time.
// Invariant: Segments transition: Active → Sealed → Archived (never backwards).
type SegmentManager interface {
    // ActiveSegment returns the current active segment for writing.
    // Invariant: Returns non-nil unless store is closed.
    ActiveSegment() Segment

    // GetSegment returns segment by ID.
    // Returns nil if segment doesn't exist.
    GetSegment(id vaddr.SegmentID) Segment

    // CreateSegment creates a new active segment.
    // Invariant: New segment ID > all existing segment IDs.
    // Invariant: Only one active segment at a time.
    CreateSegment() (Segment, error)

    // SealSegment marks a segment as sealed (no new writes).
    // Invariant: Segment must be Active.
    // Invariant: After Seal, segment transitions to Sealed state.
    SealSegment(id vaddr.SegmentID) error

    // ArchiveSegment marks a segment as archived (read-only, may be compacted).
    // Invariant: Segment must be Sealed.
    ArchiveSegment(id vaddr.SegmentID) error

    // ListSegments returns all segments in ID order.
    ListSegments() []Segment

    // SegmentCount returns the number of segments.
    SegmentCount() int

    // Close releases all resources.
    Close() error
}

// Segment represents a single segment file.
// Invariant: Segment state never changes after Sealed or Archived.
type Segment interface {
    // ID returns the segment's unique identifier.
    ID() vaddr.SegmentID

    // State returns the current segment state.
    State() vaddr.SegmentState

    // Append appends data to the segment.
    // Returns the VAddr where data was written.
    // Invariant: Segment must be Active.
    // Invariant: Returned Offset is monotonically increasing.
    Append(data []byte) (vaddr.VAddr, error)

    // ReadAt reads data at offset.
    // Invariant: Offset + length must be within segment bounds.
    ReadAt(offset int64, length int) ([]byte, error)

    // Size returns current data size (excluding header/trailer).
    Size() int64

    // Sync ensures data is durable.
    Sync() error

    // Close closes the segment file.
    Close() error
}

// StorageConfig holds initialization parameters.
type StorageConfig struct {
    Directory    string
    SegmentSize  uint64  // Target size before rotation (default: 1GB)
    FileType     FileType
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() StorageConfig {
    return StorageConfig{
        SegmentSize: 1 << 30, // 1 GB
    }
}
```

### 3.3 wal/api/api.go - WAL Interface

```go
package wal

import (
    vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// WALRecordType identifies the type of WAL record.
type WALRecordType uint8

const (
    WALPageAlloc        WALRecordType = iota  // Page allocation
    WALPageFree                              // Page deallocation
    WALNodeWrite                             // B-link node written
    WALExternalValue                        // External value stored
    WALRootUpdate                            // Tree root changed
    WALCheckpoint                           // Checkpoint marker
    WALIndexUpdate                           // Index mutation (RadixTree)
    WALIndexRootUpdate                       // RadixTree root changed
)

// WALRecord is a single record in the write-ahead log.
type WALRecord struct {
    LSN        uint64         // Monotonically increasing log sequence number
    RecordType WALRecordType
    Length     uint32
    Checksum   uint32
    Payload    []byte
}

// WAL provides write-ahead logging for crash recovery.
// Invariant: WAL records are never overwritten except via Truncate.
// Invariant: All mutations are recorded before being applied to data.
type WAL interface {
    // Append appends a record to the WAL.
    // Returns the LSN assigned to this record.
    // Invariant: LSN is monotonically increasing.
    Append(record *WALRecord) (uint64, error)

    // ReadAt reads the record at the given LSN.
    // Returns nil if LSN doesn't exist.
    ReadAt(lsn uint64) (*WALRecord, error)

    // ReadFrom reads all records from startLSN to end of WAL.
    // Iterator must be closed by caller.
    ReadFrom(startLSN uint64) (WALIterator, error)

    // Truncate removes all records up to and including truncateLSN.
    // Invariant: truncateLSN must be ≤ last LSN.
    Truncate(truncateLSN uint64) error

    // LastLSN returns the LSN of the last record.
    LastLSN() uint64

    // Flush ensures WAL is durable.
    Flush() error

    // Close closes the WAL.
    Close() error
}

// WALIterator provides sequential access to WAL records.
type WALIterator interface {
    Next() bool
    Record() *WALRecord
    Error() error
    Close()
}

// CheckpointManager manages checkpoint creation and recovery.
type CheckpointManager interface {
    // CreateCheckpoint creates a consistent snapshot.
    // Returns the LSN of the checkpoint.
    CreateCheckpoint() (uint64, error)

    // LatestCheckpoint returns the most recent valid checkpoint.
    LatestCheckpoint() (*Checkpoint, error)

    // Recover restores state from the latest checkpoint.
    Recover() error

    // Checkpoint returns information about a checkpoint.
    Checkpoint(lsn uint64) (*Checkpoint, error)
}

// Checkpoint represents a consistent snapshot of the system.
type Checkpoint struct {
    ID          uint64
    LSN         uint64              // All WAL records ≤ LSN are durable
    TreeRoot    vaddr.VAddr         // B-link tree root VAddr
    PageManager PageManagerSnapshot // Page manager state
    ExternalStore ExternalValueSnapshot
    Timestamp   uint64              // Unix nanoseconds
}

// PageManagerSnapshot is a checkpoint of the page manager index.
type PageManagerSnapshot struct {
    RootVAddr     vaddr.VAddr
    LivePageCount uint64
    CheckpointLSN uint64
}

// ExternalValueSnapshot is a checkpoint of the external value store.
type ExternalValueSnapshot struct {
    ActiveVAddrs []vaddr.VAddr
    CheckpointLSN uint64
}

// WALConfig holds WAL initialization parameters.
type WALConfig struct {
    Directory    string
    SegmentSize  uint64  // WAL segment size before rotation (default: 64MB)
    SyncWrites   bool    // Sync after each write (default: true)
}
```

### 3.4 Concurrency Interface (concurrency/api/api.go - existing, verify)

See existing implementation for:
- `LatchManager` interface
- `LatchMode` constants (Read/Write)
- Single-writer/multi-reader model

### 3.5 Cross-Module Dependencies (Interface-Based)

```
kvstore
    └── blinktree.Tree
            └── blinktree.NodeManager → storage.SegmentManager
            └── blinktree.NodeOperations (internal)
    └── externalvalue.ExternalValueStore
    └── wal.WAL

blinktree
    └── vaddr.VAddr (types only)
    └── storage.SegmentManager (for NodeManager)
    └── concurrency.LatchManager

pagemanager
    └── storage.SegmentManager
    └── wal.WAL (for logging mutations)

externalvalue
    └── vaddr.VAddr (types only)
    └── storage.SegmentManager

storage      (no dependencies on other modules)
wal          (no dependencies on other modules)
vaddr        (no dependencies - foundation types)
```

---

## 4. Verification Command

```bash
cd /home/ubuntu/workspace/go-fast-kv && \
find . -name "api.go" -path "*/api/*" | sort && \
echo "---" && \
find . -type d -not -path "./.git/*" -not -path "./.backups/*" | sort
```

Expected output includes:
- `internal/vaddr/api/api.go`
- `internal/storage/api/api.go`
- `internal/wal/api/api.go`
- All existing modules with proper structure

---

## 5. Acceptance Criteria

1. All modules have `api/api.go` defining public interfaces
2. All modules have `internal/` directory with stub implementation
3. All modules have `docs/` directory
4. All modules have `{mod}.go` re-exporting from api
5. Cross-module dependencies use interfaces only (no internal coupling)
6. All `api.go` files compile without errors
7. vaddr module provides foundation types for all other modules
