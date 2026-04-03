# Lock Analysis and Optimization Proposals

## Overview

This document analyzes mutex usage across the go-fast-kv codebase and proposes improvements for reducing contention and improving concurrency.

---

## 1. WALImpl (`internal/wal/internal/wal.go`)

### Current Implementation

```go
type WALImpl struct {
    mu        sync.Mutex    // ⚠️ PROBLEM: Mutex used for all operations
    ...
    currentSegmentFile *os.File
    currentOffset      int64
}
```

### Issues Identified

| Method | Lock Mode | Issue |
|--------|-----------|-------|
| `Append()` | `sync.Mutex` | ✅ Correct - serializes writes |
| `ReadAt()` | `sync.Mutex` | ❌ **Unnecessary contention** - reads don't modify state |
| `ReadFrom()` | `sync.Mutex` | ❌ **Unnecessary contention** - reads don't modify state |
| `Flush()` | `sync.Mutex` | ✅ Correct - syncs current file |
| `Close()` | `sync.Mutex` | ✅ Correct - closing requires write |

### Contention Analysis

- **Write path**: `Append()` acquires `mu` for LSN generation, segment rotation, file writes
- **Read path**: `ReadAt()` and `ReadFrom()` also acquire `mu`, serializing with writers
- **Impact**: High - read operations (replication, recovery) are blocked by writes

### Improvement Proposals

#### Proposal 1: Use RWMutex for WALImpl
```go
type WALImpl struct {
    mu        sync.RWMutex    // Change: read-heavy ops use RLock
    ...
}
```
- `Append()`: `mu.Lock()` (write)
- `ReadAt()`: `mu.RLock()` (read)
- `ReadFrom()`: `mu.RLock()` (read)
- `Flush()`: `mu.RLock()` (reads state, not modifies)
- **Expected improvement**: 2-5x read throughput increase under write load

#### Proposal 2: Lock-Free LSN Generation
```go
type WALImpl struct {
    mu            sync.Mutex
    nextLSN       atomic.Uint64  // Change: atomic counter
}
```
- Replace `mu.Lock()` + `nextLSN++` with `atomic.AddUint64(&nextLSN, 1)`
- Reduces append lock scope to file operations only

#### Proposal 3: Segment-Level Locking (Advanced)
- Each WAL segment file has independent mutex
- LSN-to-segment mapping via sharding
- Eliminates single lock bottleneck for concurrent readers/writers

---

## 2. Segment (`internal/storage/internal/segment.go`)

### Current Implementation

```go
type segment struct {
    mu        sync.RWMutex    // ✅ Good: uses RWMutex
    ...
}
```

### Lock Usage Analysis

| Method | Lock Mode | Duration | Assessment |
|--------|-----------|----------|------------|
| `Append()` | `mu.Lock()` | Full I/O | ✅ Acceptable - exclusive write |
| `ReadAt()` | `mu.RLock()` | Full read | ⚠️ Could be optimized |
| `State()` | `mu.RLock()` | Very short | ✅ OK |
| `Size()` | `mu.RLock()` | Very short | ✅ OK |
| `Sync()` | `mu.RLock()` | Full sync | ✅ OK |

### Issues Identified

1. **No write batching**: Each `Append()` is a separate I/O operation
2. **Lock held during I/O**: `mu.Lock()` held while `WriteAt()` executes
3. **Single-writer limitation**: Only one concurrent writer per segment

### Improvement Proposals

#### Proposal 1: Batch Writer Integration (IMPLEMENTED)
See `batch_segment.go` - provides channel-based batching:
```go
type BatchSegment struct {
    segment *segment
    bw      *batchwriter.BatchWriter
}
```
- Multiple small writes batched into single I/O
- Event-driven (no timers)
- Thread-safe via channel

#### Proposal 2: Per-Field Locking (Future)
```go
type segment struct {
    dataMu    sync.Mutex  // Protects dataSize, pageCount
    stateMu   sync.RWMutex // Protects state
    file      api.FileOperations
}
```
- Reduces lock scope
- File I/O can proceed without holding dataMu

---

## 3. SegmentManager (`internal/storage/internal/manager.go`)

### Current Implementation

```go
type segmentManager struct {
    mu           sync.RWMutex    // ✅ Good
    segments     map[vaddr.SegmentID]*segment
    ...
}
```

### Lock Usage Analysis

| Method | Lock Mode | Duration | Assessment |
|--------|-----------|----------|------------|
| `ActiveSegment()` | `mu.RLock()` | Short | ✅ OK |
| `GetSegment()` | `mu.RLock()` | Short | ✅ OK |
| `CreateSegment()` | `mu.Lock()` | Long | ✅ Necessary |
| `SealSegment()` | `mu.Lock()` | Medium | ✅ Necessary |
| `ListSegments()` | `mu.RLock()` | Medium | ✅ OK |

### Issues Identified

1. **Global map access**: All segment lookups go through single mutex
2. **Active segment hot path**: `ActiveSegment()` called frequently

### Improvement Proposals

#### Proposal 1: Per-Segment Locking with Manager Lock
```go
type segmentManager struct {
    managerMu sync.RWMutex  // Manager-level lock for map
    segments  map[vaddr.SegmentID]*segment
    activeID  vaddr.SegmentID  // Atomic - no lock needed
}
```
- Most operations don't need manager lock
- Individual segment locks protect segment state

---

## 4. LatchManager (`internal/blinktree/internal/latch.go`)

### Current Implementation

```go
type latchManagerImpl struct {
    latches map[vaddr.VAddr]*nodeLatch
    mu      sync.RWMutex
}

type nodeLatch struct {
    mu      sync.RWMutex
    readers int
    writer  bool
}
```

### Assessment

| Aspect | Status | Notes |
|--------|--------|-------|
| Per-key locking | ✅ Excellent | No global contention |
| Read/write separation | ✅ Good | Per-key RWMutex |
| Latch table growth | ⚠️ Note | Map grows unbounded |

### Improvement Proposals

#### Proposal 1: Sharded Latch Tables
```go
type latchManagerImpl struct {
    shards    [256]*latchShard
    shardMask uint64 = 255
}

type latchShard struct {
    mu      sync.RWMutex
    latches map[vaddr.VAddr]*nodeLatch
}
```
- Reduces contention in high-concurrency scenarios
- Consistent hashing for lock distribution

#### Proposal 2: Lock-Free Latch Table
- Use `sync.Map` for latch table
- Eliminated map mutex entirely
- Per-key locking only

---

## 5. PageManager (`internal/pagemanager/internal/pagemanager.go`)

### Current Implementation

```go
type pageManager struct {
    mu             sync.RWMutex    // ✅ Good
    index          *denseArray
    freeList       *freeList
    segmentManager storage.SegmentManager
    ...
}
```

### Lock Usage Analysis

| Method | Lock Mode | Duration | Assessment |
|--------|-----------|----------|------------|
| `AllocatePage()` | `mu.Lock()` | Long | ⚠️ Calls segment manager |
| `FreePage()` | `mu.Lock()` | Short | ✅ OK |
| `GetVAddr()` | `mu.RLock()` | Short | ✅ OK |
| `UpdateMapping()` | `mu.Lock()` | Medium | ✅ OK |

### Issues Identified

1. **Allocation contention**: `AllocatePage()` holds lock during segment allocation
2. **DenseArray operations**: Index updates require full lock

### Improvement Proposals

#### Proposal 1: Split Free List Lock
```go
type pageManager struct {
    mu             sync.RWMutex
    freeListMu     sync.Mutex  // Separate lock for free list
    freeList       *freeList
}
```
- Free list operations don't block other operations

#### Proposal 2: Async Allocation
```go
func (pm *pageManager) AllocatePageAsync() (PageID, [16]byte, <-chan error) {
    // Returns immediately, completes in background
}
```

---

## Summary of Recommendations

### Priority 1 (High Impact, Low Risk)
1. ✅ **WALImpl RWMutex** - Simple change, high read throughput improvement
2. ✅ **BatchSegment** - Already implemented, reduces I/O overhead

### Priority 2 (Medium Impact, Medium Risk)
3. **Lock-Free LSN Generation** - Atomic operations, reduces lock scope
4. **Per-Field Segment Locking** - Reduces lock scope

### Priority 3 (Advanced, Future)
5. **Sharded Latch Tables** - For extreme concurrency
6. **Async Allocation** - Complex, requires careful error handling

---

## Performance Impact Estimates

| Optimization | Write Throughput | Read Throughput | Risk |
|--------------|------------------|-----------------|------|
| WAL RWMutex | +10% (less contention) | +200% (parallel reads) | Low |
| BatchSegment | +50% (batched I/O) | N/A | Low |
| Lock-Free LSN | +15% (shorter critical section) | N/A | Low |
| Sharded Latches | +5% | +30% | Medium |
