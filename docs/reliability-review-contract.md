# Reliability Expert Review: Cache Strategies

## Evaluation Contract

### Purpose
Score Integrated Cache vs OS Page Cache strategies on **reliability** (0-100 each).

---

## 1. Reliability Evaluation Criteria

### 1.1 Crash Consistency (25 points)
- **Criterion**: What happens on power failure or crash?
- **Questions**:
  - Is durability ordering enforced correctly (WAL before data)?
  - Are writes durable when expected?
  - Can data be recovered correctly?
  - Are there scenarios where data loss is silent?

### 1.2 Memory Safety (20 points)
- **Criterion**: Can cache cause memory corruption or crashes?
- **Questions**:
  - Buffer alignment correctness (O_DIRECT requires alignment)
  - Use-after-free risks
  - Race conditions in concurrent access
  - Memory limit enforcement

### 1.3 Corruption Risk (20 points)
- **Criterion**: Can data become internally corrupted?
- **Questions**:
  - Double-buffering inconsistencies
  - mmap + write(2) mixing
  - Partial writes
  - Stale data serving

### 1.4 Recovery Complexity (20 points)
- **Criterion**: How hard is recovery after failure?
- **Questions**:
  - Is recovery deterministic?
  - How much state needs to be replayed?
  - Are there edge cases in recovery?
  - Complexity of checkpoint + WAL replay

### 1.5 Edge Cases (15 points)
- **Criterion**: How are tricky scenarios handled?
- **Questions**:
  - O_DIRECT alignment violations
  - mmap truncation during compaction
  - fsync ordering violations
  - Epoch grace period correctness
  - Redirection tracking correctness

---

## 2. Output Format

```go
// ReliabilityReview contains the expert review output.
type ReliabilityReview struct {
    IntegratedCacheScore int    // 0-100
    IntegratedCacheRationale string  // Detailed justification
    
    OSPageCacheScore int    // 0-100
    OSPageCacheRationale string  // Detailed justification
    
    Recommendation string  // Which strategy is more reliable, and why
}
```

---

## 3. Invariants to Verify

### Integrated Cache Invariants
- [ ] Memory bound: Cache never exceeds configured memory limit
- [ ] No stale reads: Get returns valid data for current VAddr
- [ ] Epoch visibility: Entry visible only to epochs that existed when cached
- [ ] Compaction consistency: Cache invalidation happens before reclamation
- [ ] Eviction determinism: Eviction is non-blocking and eventually makes space
- [ ] Thread safety: Concurrent Get/Put is safe without external locking
- [ ] Cache key uniqueness: Each VAddr has at most one entry in PageCache

### OS Page Cache Invariants
- [ ] Each file uses exactly one AccessPattern for its lifetime
- [ ] No file mixes mmap with write(2) or O_DIRECT with buffered I/O
- [ ] DirectIO buffers are aligned to AlignmentBytes
- [ ] All VAddr.Offset values are multiples of PageSize (4096)
- [ ] WAL synced before data pages for same transaction
- [ ] Only one sync operation runs at a time
- [ ] Segment marked as compacted before truncation
- [ ] No mmap region released before epoch grace period

---

## 4. Documents Under Review

| Document | Path | Key Components |
|----------|------|----------------|
| Integrated Cache | `docs/architecture/integrated-cache-strategy.md` | O_DIRECT, PageCache, ValueCache, ClockEvictor, EpochTracking, CacheCoordinator |
| OS Page Cache | `docs/architecture/os-page-cache.md` | Buffered/DirectIO/Mapped, DurabilityManager, MmapManager, DurabilityCoordinator |

---

## 5. Known Reliability Traps (From LTM)

### Trap: Double Buffering
- User-space cache + OS page cache both hold data
- Writes may not be durable when expected
- Solution: Choose ONE caching strategy per file type

### Trap: O_DIRECT Alignment
- Misaligned access returns EINVAL
- All pages must be aligned (4096 bytes)

### Trap: mmap + write(2) Coherence
- Mixing mmap and write(2) causes undefined behavior
- Solution: Each file chooses ONE access pattern

### Trap: fsync Ordering
- WAL must be synced BEFORE data pages
- Wrong order causes data loss on crash

### Trap: mmap Truncation
- Truncating mmap'd file causes SIGSEGV
- Solution: Generational MmapManager tracking

---

*Contract Status: Ready for Evaluation*
*Created: For delegation to branch/reliability-expert*