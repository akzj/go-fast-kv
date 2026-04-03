# OS Page Cache Strategy

## Overview

Controls how the KV store interacts with the operating system's page cache. Determines whether to rely on kernel caching (buffered I/O), bypass it entirely (O_DIRECT), or map files into process address space (mmap).

**Relates to:**
- `page-manager.md` — Page allocation, VAddr mapping, alignment requirements
- `concurrency-recovery.md` — WAL durability, fsync ordering, checkpoint strategy
- `compaction-strategy.md` — Background compaction, segment lifecycle, truncation
- `kv-store-design.md` — Overall architecture context

---

## 1. Design Principles

### 1.1 The Double-Buffering Trap

**Problem**: If user-space cache + OS page cache both hold data, writes may not be durable when expected.

**Solution**: Choose ONE caching strategy per file type, never both.

### 1.2 O_DIRECT Alignment Trap

**Problem**: O_DIRECT requires aligned buffers (typically 512-byte or 4096-byte aligned). Misaligned access returns EINVAL.

**Solution**: All pages are 4KB-aligned (from `page-manager.md`). Use `mmap` with `MAP_SYNC` or allocate with `posix_memalign`/`aligned_alloc`.

### 1.3 mmap + write(2) Coherence Trap

**Problem**: Calling both mmap and write(2) on the same file causes undefined behavior.

**Solution**: Each file chooses ONE access pattern:
- **mmap files**: Use only pointer dereferences + msync
- **buffered files**: Use only read/write syscalls
- **direct files**: Use only O_DIRECT reads/writes

### 1.4 fsync Ordering Trap

**Problem**: After WAL records are synced, data pages must also be synced to ensure durability ordering.

**Solution**: WAL-first ordering enforced by `DurabilityManager`.

---

## 2. Access Pattern Selection

```go
// AccessPattern defines how a file interacts with OS page cache.
// Invariant: Each file uses exactly one AccessPattern for its lifetime.
// Invariant: Mixing patterns on same file causes undefined behavior.
type AccessPattern uint8

const (
    // Buffered relies on kernel page cache for reads and writes.
    // Data may be in OS cache before reaching disk.
    // Use for: WAL segments, random-read data files.
    AccessBuffered AccessPattern = iota

    // DirectIO bypasses kernel page cache entirely.
    // Requires aligned buffers; provides predictable I/O.
    // Use for: Large sequential reads, write-intensive workloads.
    AccessDirectIO

    // Mapped maps file into process address space.
    // OS handles paging; msync ensures durability.
    // Use for: Index files, B-link tree nodes.
    AccessMapped
)

// FileAccessConfig defines access pattern for a file type.
// Invariant: Config is set at file creation and never changed.
type FileAccessConfig struct {
    Pattern        AccessPattern
    AlignmentBytes int  // Must be >= PageSize for DirectIO
    BufferAllocator BufferAllocator  // For AccessDirectIO
    SyncMode       MmapSyncMode      // For AccessMapped
}

type MmapSyncMode uint8
const (
    MmapSyncPeriodic MmapSyncMode = iota  // msync every N seconds
    MmapSyncOnDemand                      // msync only on explicit request
    MmapSyncCooperative                   // msync with MADV_DONTNEED hints
)
```

### 2.1 File Type Assignment

```go
// FileType identifies the purpose of a storage file.
// Invariant: Each file type has a fixed AccessPattern.
type FileType uint8

const (
    FileTypeWAL FileType = iota      // Write-Ahead Log
    FileTypeSegment                  // Data segment (B-link nodes)
    FileTypeExternalValue            // External large values
    FileTypeIndex                    // Page manager index
    FileTypeCheckpoint               // Checkpoint metadata
)

var FileTypeAccessPattern = map[FileType]AccessPattern{
    FileTypeWAL:           AccessBuffered,  // Sequential writes; rely on kernel
    FileTypeSegment:       AccessBuffered,  // Mixed reads/writes; rely on kernel
    FileTypeExternalValue: AccessBuffered,  // Large sequential reads; kernel efficient
    FileTypeIndex:         AccessMapped,   // Random access; mmap enables pointer access
    FileTypeCheckpoint:    AccessMapped,   // Read rarely; mmap for easy loading
}
```

---

## 3. Buffered I/O (Kernel Page Cache)

```go
// BufferedFile provides buffered I/O using kernel page cache.
// Invariant: All writes go through kernel page cache before disk.
// Invariant: fsync is required for durability.
type BufferedFile interface {
    io.ReaderAt
    io.WriterAt
    io.Closer

    // Sync ensures data is durable on disk.
    // Invariant: After Sync returns, all prior writes are durable.
    Sync() error

    // DataSync is like Sync but may skip metadata (faster).
    DataSync() error
}

// DurabilityManager ensures WAL-first ordering.
// Invariant: WAL must be synced before data pages.
type DurabilityManager interface {
    SyncWAL() (lsn uint64, err error)
    SyncData() error
    SyncAll() error
}
```

---

## 4. Direct I/O

```go
// DirectIOFile provides O_DIRECT I/O bypassing kernel page cache.
// Invariant: All buffers passed to Read/Write are aligned to AlignmentBytes.
// Invariant: All read/write sizes are multiples of AlignmentBytes.
type DirectIOFile interface {
    io.ReaderAt
    io.WriterAt
    io.Closer
    Sync() error
    DataSync() error
}

type DirectIOConfig struct {
    AlignmentBytes int
    AIOEvents      bool
    MaxIOBytes     int
}

// AlignedBuffer is a buffer suitable for O_DIRECT.
type AlignedBuffer interface {
    Data() []byte
    Size() int
    Reset()
    Free()
}

type BufferAllocator interface {
    Alloc(size int) (AlignedBuffer, error)
    AllocHuge(size int) (AlignedBuffer, error)
}

// EnsureAlignment adjusts offset/size to meet alignment requirements.
func EnsureAlignment(offset, size, alignment int) (alignedOffset, alignedSize int)
```

---

## 5. Memory-Mapped I/O

```go
// MmapFile maps a file into process address space.
// Invariant: File size must not change while mmap is active.
type MmapFile interface {
    Data() []byte
    Slice(start, end int) []byte
    PageAt(offset int64) ([]byte, error)
    Msync(mode int) error
    Advise(hint int) error
    Close() error
}

type MmapConfig struct {
    PageSize   int
    Advice     int
    Trampoline bool
}
```

### 5.1 Compaction Truncation Coordination

```go
// MmapManager tracks all mmap'd regions and coordinates truncation.
// Invariant: No region is unmapped while any pointer may reference it.
type MmapManager interface {
    Map(file *os.File, config *MmapConfig) (*MmapRegion, error)
    MarkSegmentCompacted(segmentID SegmentID)
    TryRelease() int
    ActiveSegments() []SegmentID
}

type MmapRegion struct {
    SegmentID    SegmentID
    Data         []byte
    MappedAt     time.Time
    Reclaimable  bool
}

type ReleasePolicy struct {
    GracePeriod      int   // Default: 3 epochs
    MaxMappedRegions int64 // Default: 1GB
}
```

---

## 6. Segment File Access

```go
// SegmentAccessMode selects the access pattern for data segments.
type SegmentAccessMode uint8

const (
    SegmentAccessAuto      SegmentAccessMode = iota  // Auto-select based on characteristics
    SegmentAccessBuffered                             // Kernel page cache (default)
    SegmentAccessDirect                               // O_DIRECT
    SegmentAccessMapped                               // mmap
)

type SegmentFile interface {
    ReadAt(buf []byte, offset int64) (n int, err error)
    WriteAt(buf []byte, offset int64) (n int, err error)
    Size() (int64, error)
    Sync() error
    Close() error
}

type SegmentFileFactory interface {
    Open(segmentID SegmentID, mode SegmentAccessMode) (SegmentFile, error)
    Create(segmentID SegmentID, mode SegmentAccessMode) (SegmentFile, error)
}
```

---

## 7. Default Configuration

```go
var DefaultPageCacheConfig = &PageCacheConfig{
    WAL: BufferedAccessConfig{
        SyncMode: SyncEveryRecord,
    },
    Segments: BufferedAccessConfig{
        SyncMode: SyncOnRotation,
    },
    Index: MmapAccessConfig{
        SyncMode:    MmapSyncCooperative,
        Advice:      MADV_RANDOM,
        Trampoline:  true,
    },
    ExternalValues: BufferedAccessConfig{
        SyncMode: SyncOnClose,
    },
    Checkpoint: MmapAccessConfig{
        SyncMode: MmapSyncOnDemand,
    },
}

type SyncMode uint8
const (
    SyncEveryRecord SyncMode = iota  // Every write (WAL default)
    SyncOnRotation                    // On segment rotation (data default)
    SyncOnClose                       // On file close
    SyncOnDemand                      // Explicit sync only
    SyncCooperative                   // Batch syncs; hint pages after access
)
```

---

## 8. fsync Ordering Enforcement

```go
// DurabilityCoordinator ensures proper sync ordering.
// Invariant: WAL synced before data pages.
type DurabilityCoordinator struct {
    walFile   *BufferedFile
    dataFiles map[SegmentID]*BufferedFile
    mu        sync.Mutex
}

func (dc *DurabilityCoordinator) SyncWALFirst() error {
    dc.mu.Lock()
    defer dc.mu.Unlock()

    if err := dc.walFile.Sync(); err != nil {
        return fmt.Errorf("wal sync: %w", err)
    }

    for _, f := range dc.dataFiles {
        if err := f.DataSync(); err != nil {
            return fmt.Errorf("data sync: %w", err)
        }
    }
    return nil
}
```

---

## 9. Invariants Summary

```go
// Access pattern invariants:
// 1. Each file uses exactly one AccessPattern for its lifetime.
// 2. No file mixes mmap with write(2) or O_DIRECT with buffered I/O.
// 3. DirectIO buffers are aligned to AlignmentBytes.

// Alignment invariants:
// 1. All VAddr.Offset values are multiples of PageSize (4096).
// 2. DirectIO reads/writes use aligned offsets and sizes.

// fsync ordering invariants:
// 1. WAL must be synced before data pages for same transaction.
// 2. Only one sync operation runs at a time.

// Mmap truncation invariants:
// 1. Segment is marked as compacted before truncation.
// 2. No mmap region is released before epoch grace period.
// 3. File is truncated only after region is unmapped.
```

---

## 10. Why Not Alternatives

| Alternative | Why Rejected |
|-------------|--------------|
| All DirectIO | No read caching; bad for random reads |
| All Buffered | Double buffering; unpredictable latency |
| All Mmap | Truncation complexity; coherence issues |
| User-space cache | Complex; reinventing kernel page cache |
| fsync every page | Terrible performance; overkill |

---

*Document Status: Contract Spec*
*Last Updated: 2024*
