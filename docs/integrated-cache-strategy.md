# Integrated Cache Strategy

## 1. Overview

Design for in-process page/value cache managed by the KV store itself. Addresses cache layers, eviction policy, memory management, and epoch-aware invalidation for compaction safety.

**Relates to:**
- `page-manager.md` — 4KB page allocation, PageID→VAddr mapping
- `vaddr-format.md` — VAddr format, segment lifecycle
- `compaction-strategy.md` — Epoch-based reclamation, segment lifecycle
- `external-value-store.md` — External value storage, threshold (48 bytes)

## 2. Cache Layer Architecture

### 2.1 Why Not Double Caching

The OS already maintains a page cache for file-backed storage. Adding an internal buffer pool risks:
1. **Redundant copies**: Data lives in both OS cache and internal cache
2. **Memory bloat**: Total cache pressure exceeds available RAM
3. **Consistency complexity**: OS cache and internal cache may diverge

**Solution**: Use **direct I/O** (O_DIRECT) to bypass OS page cache. The internal buffer pool is the **sole caching layer**.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Cache Layer Architecture                                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Application                                                         │
│      │                                                               │
│      ▼                                                               │
│  ┌─────────────────────────────────────┐                           │
│  │         Integrated Cache            │                           │
│  │  ┌─────────────┐  ┌─────────────┐   │                           │
│  │  │ PageCache   │  │ ValueCache  │   │  ← Sole caching layer     │
│  │  │ (4KB pages) │  │ (inline/    │   │                           │
│  │  │             │  │  external) │   │                           │
│  │  └─────────────┘  └─────────────┘   │                           │
│  └─────────────────────────────────────┘                           │
│      │                                                               │
│      ▼                                                               │
│  ┌─────────────────────────────────────┐                           │
│  │         Direct I/O                  │  ← Bypasses OS page cache  │
│  │     (O_DIRECT for reads/writes)      │                           │
│  └─────────────────────────────────────┘                           │
│      │                                                               │
│      ▼                                                               │
│  Segment Files (appended, never overwritten)                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Why O_DIRECT Instead of mmap

| Approach | Why Not |
|----------|---------|
| mmap + OS page cache | Double caching; OS may evict pages we want to keep |
| mmap + madvise(DONTNEED) | Complexity; still relies on OS for eviction decisions |
| O_DIRECT | We control eviction; predictable memory usage; no double caching |

**Trade-off**: O_DIRECT requires aligned buffers (page-aligned). Acceptable for 4KB page boundaries.

## 3. Cache Structure

### 3.1 Two-Tier Design

```go
// IntegratedCache combines page and value caching.
// Invariant: Cache is the sole caching layer (O_DIRECT I/O bypasses OS cache).
// Invariant: Cache respects epoch boundaries for compaction safety.
type IntegratedCache interface {
    // PageCache returns the page-level cache.
    // All page I/O goes through this.
    PageCache() PageCache

    // ValueCache returns the value-level cache.
    // Caches hot inline values and external value metadata.
    ValueCache() ValueCache

    // SetMemoryLimit sets the total memory budget for all caches.
    // Invariant: Actual memory may exceed limit briefly during bursts.
    SetMemoryLimit(bytes uint64)

    // MemoryStats returns current memory usage.
    MemoryStats() CacheMemoryStats

    // Flush forces dirty pages to storage.
    // Used during checkpoint.
    Flush() error

    // Close releases all cache resources.
    Close() error
}

// CacheMemoryStats tracks cache memory usage.
type CacheMemoryStats struct {
    PageCacheUsed  uint64  // Bytes used by page cache
    ValueCacheUsed uint64  // Bytes used by value cache
    TotalUsed      uint64  // Sum of above
    PageCacheHits  uint64  // Hit count (for metrics)
    ValueCacheHits uint64  // Hit count (for metrics)
    Evictions      uint64  // Total evictions
}
```

### 3.2 PageCache Interface

```go
// PageCache caches 4KB pages, aligned with PageManager's page size.
// Invariant: Cache entry is always exactly PageSize bytes.
// Invariant: Cache keys are VAddrs (not PageIDs) — aligns with storage layer.
// Thread-safe: Multiple goroutines can read/write concurrently.
type PageCache interface {
    // Get returns a cached page by VAddr.
    // Returns (page, true) if found, (nil, false) if not cached.
    // Invariant: Page data is immutable while cached (no in-place modification).
    Get(vaddr VAddr) (Page, bool)

    // Put stores a page in the cache.
    // If cache is full, evicts least recently used entry.
    // Invariant: Put does not block; eviction is non-blocking.
    Put(vaddr VAddr, page Page)

    // Invalidate removes a VAddr from the cache.
    // Called when VAddr is redirected (compaction) or freed.
    // Invariant: Invalidate is idempotent.
    Invalidate(vaddr VAddr)

    // InvalidateSegment removes all cache entries for a segment.
    // Called when segment is archived/reclaimed.
    // Why invalidate whole segment? Faster than per-VAddr invalidation.
    InvalidateSegment(segmentID SegmentID)

    // Capacity returns the maximum number of pages cached.
    Capacity() uint64

    // Len returns the current number of cached pages.
    Len() uint64
}

// Why VAddr as key, not PageID?
// - VAddr is what we read from storage
// - Compaction tracks VAddr redirections, not PageID mappings
// - PageManager already provides PageID→VAddr lookup when needed
```

### 3.3 ValueCache Interface

```go
// ValueCache caches hot values at the value level (above page level).
// Caches:
// - Inline values (≤48 bytes) that fit in tree nodes
// - External value headers (32 bytes) for quick size checks
// - Small external values (≤4KB) to avoid full page reads
//
// Invariant: ValueCache is a layer ABOVE PageCache.
// Reads go: ValueCache → PageCache → Direct I/O
// Writes go: Direct I/O (value cache is read-through)
type ValueCache interface {
    // Get returns a cached value by key.
    // key is typically the tree key (e.g., user key for the value).
    // Returns (value, true) if found, (nil, false) if not cached.
    // Invariant: Returned value is immutable copy; caller may modify freely.
    Get(key []byte) (ValueCacheEntry, bool)

    // Put stores a value in the cache.
    // For inline values: stores full data.
    // For external values: stores header + small body if ≤ SmallValueThreshold.
    // Invariant: Put does not block; eviction is non-blocking.
    Put(key []byte, entry ValueCacheEntry)

    // Invalidate removes a key from the cache.
    // Called on Delete or Update for the key.
    // Invariant: Invalidate is idempotent.
    Invalidate(key []byte)

    // Capacity returns the maximum number of entries cached.
    Capacity() uint64

    // Len returns the current number of cached entries.
    Len() uint64
}

// ValueCacheEntry represents a cached value.
type ValueCacheEntry struct {
    // Value is the actual data bytes.
    // For inline: the full value.
    // For external: first bytes or header only.
    Value []byte

    // IsExternal indicates this references an external value.
    // If true, Value contains VAddr, not data.
    IsExternal bool

    // ExternalVAddr is set if IsExternal is true.
    ExternalVAddr VAddr

    // Size is the original value size (may exceed len(Value)).
    Size uint64

    // CachedAt records when this entry was cached.
    // Used for LRU eviction and epoch tracking.
    CachedAt EpochID
}

// SmallValueThreshold is the maximum size for fully-cached external values.
// Values larger than this are not cached as bodies (only headers).
const SmallValueThreshold = 4096  // 4KB, one page

// Why cache external value headers?
// Allows GetValueSize() to return size without full value read.
// Useful for range scans that need to estimate space.
```

## 4. Eviction Policy

### 4.1 Clock Algorithm (Approximate LRU)

```go
// CacheEvictionPolicy defines the eviction algorithm.
// Why Clock instead of true LRU?
// - True LRU requires O(1) modification per access (list manipulation)
// - Clock is O(1) per access with negligible constant factor
// - Good enough approximation for most workloads
// - Lock-free variants exist for high concurrency
type CacheEvictionPolicy interface {
    // OnAccess updates state when an entry is accessed.
    // Non-blocking: returns immediately.
    OnAccess(key interface{})

    // Evict returns entries to evict until fn returns false or list is exhausted.
    // fn(key) should return true to evict, false to stop.
    // Returns number of entries evicted.
    Evict(fn func(key interface{}) bool) int

    // RecordPut is called when a new entry is added.
    RecordPut(key interface{})

    // RecordRemove is called when an entry is explicitly removed.
    RecordRemove(key interface{})
}

// ClockEvictor implements approximate LRU using clock hand.
// Structure: circular buffer of "referenced" bits; hand advances each eviction check.
type ClockEvictor struct {
    mu       sync.Mutex
    entries  []clockEntry      // Ordered by insertion (approximate recency)
    hand     int               // Clock hand position
    refBits  []bool            // Referenced bit for each entry
    capacity int
}

type clockEntry struct {
    key    interface{}
    hash   uint64  // For quick comparison
}

// OnAccess sets the referenced bit for the entry.
// If entry not found, this is a no-op (use RecordPut for new entries).
func (c *ClockEvictor) OnAccess(key interface{}) {
    // Find entry, set refBits[idx] = true
    // Use hash map for O(1) lookup: key → index
}

// Evict advances the clock hand, evicting entries with refBits = false.
// Entries with refBits = true have their bit cleared and are skipped.
func (c *ClockEvictor) Evict(fn func(key interface{}) bool) int {
    evicted := 0
    for evicted < c.capacity {
        if c.refBits[c.hand] {
            c.refBits[c.hand] = false  // Clear bit, advance
            c.hand = (c.hand + 1) % len(c.entries)
            continue
        }
        // Entry not referenced recently — evict it
        entry := c.entries[c.hand]
        if !fn(entry.key) {
            break
        }
        evicted++
        c.removeAt(c.hand)
        // Don't advance hand — next entry slides into this slot
    }
    return evicted
}
```

### 4.2 Why Not Other Eviction Policies

| Policy | Why Rejected |
|--------|--------------|
| True LRU | O(1) per access requires list manipulation + mutex |
| LFU | Frequency tracking overhead; doesn't handle access patterns changes |
| Random | Simple but poor hit rate for non-uniform access |
| ARC | Good but complex; Clock is simpler with similar hit rates |

## 5. Memory Management

### 5.1 Budget Allocation

```go
// CacheConfig holds memory budget and behavior parameters.
type CacheConfig struct {
    // TotalMemoryLimit is the maximum memory for all caches.
    // Includes page cache, value cache, and metadata overhead.
    // If 0, uses DefaultCacheMemoryLimit.
    // Default: 256 MB.
    TotalMemoryLimit uint64

    // PageCacheBudget is the memory budget for page cache.
    // If 0, uses TotalMemoryLimit / 2.
    PageCacheBudget uint64

    // ValueCacheBudget is the memory budget for value cache.
    // If 0, uses TotalMemoryLimit / 2.
    ValueCacheBudget uint64

    // PageCacheEntries is the max number of cached pages.
    // Calculated from PageCacheBudget / PageSize if 0.
    PageCacheEntries uint64

    // ValueCacheEntries is the max number of cached values.
    // If 0, uses DefaultValueCacheEntries (10000).
    ValueCacheEntries uint64

    // DirectIOEnabled bypasses OS page cache.
    // Default: true.
    // Set false for testing or when OS cache is desired.
    DirectIOEnabled bool
}

// Defaults
const (
    DefaultCacheMemoryLimit   = 256 * 1024 * 1024  // 256 MB
    DefaultValueCacheEntries  = 10000
    DefaultPageCacheFraction  = 0.5  // PageCache gets 50% of budget
)
```

### 5.2 Memory Tracking

```go
// cacheMemoryTracker tracks memory usage with atomic counters.
// Invariant: Tracked memory ≈ actual memory (accounting for allocation overhead).
type cacheMemoryTracker struct {
    usedBytes    atomic.Int64
    usedEntries  atomic.Int64
    limitBytes   atomic.Int64
    limitEntries atomic.Int64
}

// Alloc returns true if allocation of size bytes would exceed limit.
// Non-blocking: uses atomic compare.
func (t *cacheMemoryTracker) Alloc(size int64) bool {
    for {
        current := t.usedBytes.Load()
        if current+size > t.limitBytes.Load() {
            return false
        }
        if t.usedBytes.CompareSwap(current, current+size) {
            return true
        }
    }
}

// Free releases memory. Idempotent (safe to over-release on error paths).
func (t *cacheMemoryTracker) Free(size int64) {
    t.usedBytes.Add(-size)
}

// EvictUntilReady attempts to free memory until Alloc would succeed.
// Returns true if ready, false if still over limit (rare).
func (t *cacheMemoryTracker) EvictUntilReady(size int64) bool {
    if t.Alloc(size) {
        return true
    }
    // Signal eviction goroutine (see 5.3)
    t.evictSignal <- struct{}{}
    return false
}
```

### 5.3 Background Eviction

```go
// backgroundEvictor runs eviction in a background goroutine.
// Prevents latency spikes from synchronous eviction.
type backgroundEvictor struct {
    pageCache   PageCache
    valueCache  ValueCache
    trigger     chan struct{}  // Signal from Alloc failure
    done        chan struct{}
}

// Run starts the eviction loop.
func (e *backgroundEvictor) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case <-e.trigger:
            e.evictSome()
        case <-time.After(100 * time.Millisecond):
            // Periodic cleanup even without trigger
            e.evictSome()
        }
    }
}

// evictSome evicts a batch of entries.
// Batch size tuned for latency vs progress.
func (e *backgroundEvictor) evictSome() {
    // Evict 10% of capacity per batch
    batchSize := int(e.pageCache.Capacity() / 10)
    if batchSize < 1 {
        batchSize = 1
    }

    evicted := 0
    for evicted < batchSize {
        // Try page cache first (higher impact)
        // Use evictor's clock algorithm
        n := e.pageCacheEvictor.Evict(func(key interface{}) bool {
            vaddr := key.(VAddr)
            e.pageCache.Invalidate(vaddr)
            return true
        })
        if n == 0 {
            break
        }
        evicted += n
    }
}
```

## 6. Epoch-Aware Invalidation (Compaction Safety)

### 6.1 The Core Problem

Compaction redirects VAddrs: old VAddr → new VAddr. If the cache still holds data at the old VAddr:
1. **Stale reads**: Cache returns old data instead of new
2. **Epoch violation**: Old VAddr may be reclaimed while cache holds it

### 6.2 Solution: Visibility Windows

```go
// CacheEntryMetadata tracks epoch visibility for cache entries.
// Invariant: Entry is visible only to epochs that existed when entry was cached.
type CacheEntryMetadata struct {
    // VAddr is the storage address of this cached data.
    VAddr VAddr

    // CachedAtEpoch is when this entry was cached.
    // Entry is invisible to epochs registered before CachedAtEpoch.
    CachedAtEpoch EpochID

    // VisibilityWindow tracks which epochs can see this VAddr.
    // An epoch E can see VAddr if E >= CachedAtEpoch AND VAddr is not redirected.
    VisibilityWindow

    // IsRedirected is true if VAddr has been redirected by compaction.
    // Stale entries remain until epoch grace period.
    IsRedirected bool

    // RedirectedAtEpoch records when compaction redirected this VAddr.
    // Used to compute when entry becomes reclaimable.
    RedirectedAtEpoch EpochID
}

// PageCacheWithEpochs extends PageCache with epoch awareness.
type PageCacheWithEpochs interface {
    PageCache  // Embeds basic interface

    // PutWithEpoch stores entry with epoch metadata.
    // Called by I/O path after reading from storage.
    PutWithEpoch(vaddr VAddr, page Page, epoch EpochID)

    // MarkRedirected records that oldVAddr has been redirected to newVAddr.
    // Cache may still serve oldVAddr until epoch grace period expires.
    // Why keep serving? Active snapshots may still reference oldVAddr.
    MarkRedirected(oldVAddr, newVAddr VAddr)

    // IsStale checks if entry at vaddr should be considered stale.
    // Returns true if entry exists but vaddr has been redirected
    // AND current epoch exceeds redirection grace period.
    IsStale(vaddr VAddr, currentEpoch EpochID) bool

    // PurgeStale removes all entries that are both redirected and past grace period.
    // Called periodically or during epoch unregistration.
    PurgeStale(currentEpoch EpochID) int
}

// Why not weak references?
// Go has no safe weak reference API for general use.
// Epoch tracking gives us explicit control over visibility.
```

### 6.3 Epoch Grace Period

```go
const (
    // CacheEpochGracePeriod is how many epochs a redirected VAddr stays in cache.
    // Default: 3 (same as compaction's gcThreshold).
    // Why 3? Allows clock skew, slow readers, and snapshot chains.
    // ValueCache uses same grace period as PageCache.
    CacheEpochGracePeriod = 3
)

// IsSafeToEvict returns true if cache entry can be evicted.
func (m *CacheEntryMetadata) IsSafeToEvict(currentEpoch EpochID) bool {
    if m.IsRedirected {
        // Only safe after grace period
        return (currentEpoch - m.RedirectedAtEpoch) >= CacheEpochGracePeriod
    }
    // Non-redirected entries can be evicted anytime (LRU)
    return true
}
```

### 6.4 Redirection Tracking

```go
// redirectionMap tracks VAddr redirections for cache invalidation.
// Invariant: Redirection entries expire after grace period.
type redirectionMap struct {
    mu         sync.RWMutex
    oldToNew   map[VAddr]VAddr  // old VAddr → new VAddr
    timestamp  map[VAddr]EpochID  // When redirected (for grace period)
}

// MarkRedirected records that oldVAddr now lives at newVAddr.
func (m *redirectionMap) MarkRedirected(oldVAddr, newVAddr VAddr, epoch EpochID) {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.oldToNew[oldVAddr] = newVAddr
    m.timestamp[oldVAddr] = epoch
}

// GetRedirected returns newVAddr if oldVAddr was redirected, VAddrInvalid otherwise.
func (m *redirectionMap) GetRedirected(oldVAddr VAddr) VAddr {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.oldToNew[oldVAddr]
}

// Cleanup removes entries past grace period.
func (m *redirectionMap) Cleanup(currentEpoch EpochID) int {
    m.mu.Lock()
    defer m.mu.Unlock()

    removed := 0
    for vaddr, epoch := range m.timestamp {
        if (currentEpoch - epoch) >= CacheEpochGracePeriod {
            delete(m.oldToNew, vaddr)
            delete(m.timestamp, vaddr)
            removed++
        }
    }
    return removed
}

// PageCache integration:
// When reading a page:
//   1. Check cache (Get)
//   2. If miss, read from storage
//   3. Check redirectionMap.Get(vaddr)
//   4. If redirected and IsStale, read new VAddr instead
//   5. PutWithEpoch(new_vaddr, page, currentEpoch)
```

## 7. Integration with Compaction

### 7.1 Compaction Callback Interface

```go
// CacheCoordinator coordinates cache invalidation with compaction.
// Invariant: Cache is always consistent with compaction's epoch tracking.
type CacheCoordinator interface {
    // CompactionStarted notifies cache that compaction is beginning.
    // Cache should prepare for redirections (e.g., disable prefetching old segments).
    CompactionStarted(sealedSegments []SegmentID)

    // CompactionRedirected notifies cache of VAddr redirections.
    // Called during compaction commit phase, after all redirects are recorded.
    // Invariant: Called BEFORE old segments become reclaimable.
    CompactionRedirected(redirects []VAddrRedirect)

    // CompactionCompleted notifies cache that compaction has finished.
    // Old segments are now archived; cache can purge their entries.
    CompactionCompleted(archivedSegments []SegmentID)

    // EpochUnregistered notifies cache that an epoch has expired.
    // Cache should purge any entries that are now past grace period.
    EpochUnregistered(epoch EpochID)
}

// VAddrRedirect records a single VAddr redirection during compaction.
type VAddrRedirect struct {
    OldVAddr VAddr
    NewVAddr VAddr
    Epoch    EpochID  // When redirection occurred
}
```

### 7.2 Compaction Integration Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  Compaction → Cache Coordination Flow                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Compaction Pipeline:                                                │
│                                                                      │
│  1. Begin Compaction                                                │
│     ├─ CompactionCoordinator.CompactionStarted([segments])           │
│     │   └─ PageCache: mark segments as "compacting"                 │
│     │   └─ ValueCache: disable prefetch for these segments          │
│     │                                                                  │
│  2. Rewrite Live Data                                               │
│     │   ├─ For each live VAddr:                                      │
│     │   │   ├─ Read from old location                                │
│     │   │   ├─ Write to new location                                  │
│     │   │   └─ Update PageManager mapping                            │
│     │   └─ CompactionCoordinator.CompactionRedirected([redirects])   │
│     │       └─ RedirectionMap: record all old→new mappings           │
│     │       └─ PageCache: mark entries as redirected                │
│     │       └─ ValueCache: mark entries as redirected                │
│     │                                                                  │
│  3. Commit Compaction                                               │
│     ├─ CompactionCoordinator.CompactionCompleted([archived])         │
│     │   └─ PageCache.InvalidateSegment(segmentID) for each           │
│     │   └─ RedirectionMap: entries now eligible for cleanup          │
│     │                                                                  │
│  4. Background: Epoch Expiration                                    │
│     ├─ When epoch expires:                                          │
│     │   └─ CompactionCoordinator.EpochUnregistered(epoch)            │
│     │       └─ CacheCoordinator.PurgeStale(currentEpoch)             │
│     │                                                                  │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.3 Cache Consistency Protocol

```go
// CacheConsistencyProtocol ensures cache never returns stale data.
//
// Invariant 1: Cache entry for VAddr X is valid iff:
//   - VAddr X is not in RedirectionMap, OR
//   - CurrentEpoch < RedirectedAtEpoch + GracePeriod
//
// Invariant 2: When compaction redirects X → Y:
//   - Cache may still serve X until grace period expires
//   - New reads of X should prefer Y (if redirect is stable)
//
// Invariant 3: When epoch expires:
//   - All entries with RedirectedAtEpoch + GracePeriod <= epoch become purgeable

// Read path with consistency:
// Read(key):
//     1. pageID = PageManager.Lookup(key)
//     2. vaddr = PageManager.GetVAddr(pageID)
//     3. If cache.Get(vaddr) hits and !cache.IsStale(vaddr, currentEpoch):
//            return cache entry
//     4. page = ReadFromStorage(vaddr)
//     5. cache.PutWithEpoch(vaddr, page, currentEpoch)
//     6. return page
//
// Why check IsStale?
// Prevents serving redirected entries past grace period.
```

## 8. Type Definitions Summary

```go
// Core cache types
type IntegratedCache interface { ... }
type PageCache interface { ... }
type ValueCache interface { ... }
type ValueCacheEntry struct { ... }

// Eviction
type CacheEvictionPolicy interface { ... }
type ClockEvictor struct { ... }

// Memory management
type CacheConfig struct { ... }
type CacheMemoryStats struct { ... }

// Epoch awareness
type CacheEntryMetadata struct { ... }
type PageCacheWithEpochs interface { ... }

// Compaction coordination
type CacheCoordinator interface { ... }
type VAddrRedirect struct { ... }
type redirectionMap struct { ... }

// Constants
const (
    PageSize                  = 4096
    SmallValueThreshold       = 4096
    CacheEpochGracePeriod     = 3
    DefaultCacheMemoryLimit   = 256 * 1024 * 1024
    DefaultValueCacheEntries  = 10000
    DefaultPageCacheFraction  = 0.5
)
```

## 9. Invariants Summary

```go
// Cache invariants:

// 1. Memory bound: Cache never exceeds configured memory limit.
//    Enforced by: memoryTracker.Alloc() check before Put

// 2. No stale reads: Get returns valid data for current VAddr.
//    Enforced by: IsStale() check; redirected entries purged after grace period

// 3. Epoch visibility: Entry is visible only to epochs that existed when cached.
//    Enforced by: CachedAtEpoch tracking

// 4. Compaction consistency: Cache invalidation happens before reclamation.
//    Enforced by: CompactionRedirected called before CompactionCompleted

// 5. Eviction determinism: Eviction is non-blocking and eventually makes space.
//    Enforced by: backgroundEvictor; batch eviction

// 6. Thread safety: Concurrent Get/Put is safe without external locking.
//    Enforced by: internal synchronization (lock-free where possible)

// 7. Cache key uniqueness: Each VAddr has at most one entry in PageCache.
//    Enforced by: Put overwrites existing entry, doesn't duplicate

// 8. Value isolation: Returned values are immutable copies.
//    Enforced by: copy on Get; no in-place modification
```

## 10. Why Not Alternatives

| Alternative | Why Rejected |
|-------------|--------------|
| OS page cache only | No control over eviction; may evict hot pages |
| mmap with madvise | Complexity; eviction still OS-controlled |
| Per-segment cache | No cross-segment sharing; wastes memory |
| No value cache | Inline values hit page cache every read; inefficient |
| Synchronous eviction | Latency spike on cache full |
| Reference counting | High overhead per access; complex for VAddr chains |

## 11. Related Specifications

- **Page Manager**: Cache aligns with 4KB pages; uses VAddr keys
- **VAddr Format**: O_DIRECT bypasses OS cache; VAddr stability guarantee
- **Compaction**: Epoch tracking enables grace period; coordinator interface
- **External Value Store**: ValueCache handles headers vs full values

---

*Document Status: Contract Spec*
*Last Updated: 2024*
