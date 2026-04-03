# Compaction Strategy

## Overview

Design for space reclamation in an append-only KV storage system. Addresses when to compact, how to compact, and how to safely reclaim space while respecting MVCC snapshot semantics.

**Relates to:**
- `docs/architecture/api-layer.md` — MVCC snapshot isolation, tombstone semantics
- `docs/architecture/blinktree-node-format.md` — B-link node format, IsDeleted flag
- `docs/architecture/external-value-store.md` — External value tombstoning and reclamation
- `docs/architecture/vaddr-format.md` — Segment lifecycle (Active → Sealed → Archived)
- `docs/architecture/page-manager.md` — PageID → VAddr mapping, free list management
- `docs/architecture/concurrency-recovery.md` — Single-writer model, checkpoint strategy

---

## Core Problem

Append-only storage accumulates garbage:
1. **Overwritten values**: Old inline/external versions from Put(key, newValue)
2. **Tombstoned entries**: Deleted keys (IsDeleted flag in node, VAddrInvalid mapping)
3. **Obsolete nodes**: B-link tree nodes replaced during splits

Space reclamation must:
- Not violate MVCC snapshot guarantees
- Not reclaim in-use VAddrs (active snapshots, in-flight reads)
- Not block the single-writer pipeline
- Produce valid B-link tree nodes in finalized format

---

## Segment Lifecycle (Foundation)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Segment States and Compaction Interaction                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐   │
│   │ Active   │────▶│ Sealed   │────▶│ Compact  │────▶│ Archived │   │
│   │ (write)  │     │ (read)   │     │ (target) │     │ (cold)   │   │
│   └──────────┘     └──────────┘     └──────────┘     └──────────┘   │
│        │                │                │                │          │
│        │                │                │                │          │
│   Only one          Full, no          Segments          Read-only,   │
│   at a time         new writes        being rewritten   maybe moved  │
│                                                                      │
│   Compaction reads Sealed segments, writes to Compact segment        │
│   Compact becomes new Active; Archived is immutable reference        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Compaction Triggers

```go
// CompactionTrigger defines when compaction should run.
// Invariant: At least one trigger must be satisfied to start compaction.
// Invariant: Compaction does not run if another compaction is in progress.
type CompactionTrigger struct {
    // SpaceUsageThreshold triggers when garbage exceeds this percentage.
    // If 0, uses DefaultSpaceUsageThreshold.
    // Default: 40% (60% live data efficiency target).
    // Why 40%? Balance between frequency and space savings.
    SpaceUsageThreshold float64

    // TimeInterval triggers compaction after this duration since last compaction.
    // If 0, uses DefaultCompactionInterval.
    // Default: 1 hour.
    // Why time-based? Prevents unbounded garbage accumulation.
    TimeInterval time.Duration

    // MinSegmentCount triggers when sealed segment count exceeds this.
    // If 0, uses DefaultMinSegmentCount.
    // Default: 3 segments.
    // Why minimum? Compaction overhead not worth it for 1-2 segments.
    MinSegmentCount int

    // ManualTrigger allows explicit compaction request.
    ManualTrigger chan struct{}
}

// Constants
const (
    DefaultSpaceUsageThreshold = 0.40  // 40% garbage triggers compaction
    DefaultCompactionInterval  = 1 * time.Hour
    DefaultMinSegmentCount      = 3
)

// CompactionPolicy combines triggers into a single decision function.
// Invariant: Returns true if any trigger condition is satisfied.
// Invariant: Returns false if compaction is already running.
type CompactionPolicy interface {
    // ShouldCompact evaluates all triggers and returns true if compaction should run.
    ShouldCompact(stats *StorageStats) bool

    // TriggerType returns which trigger caused ShouldCompact to return true.
    // Used for logging and metrics.
    TriggerType(stats *StorageStats) CompactionTriggerType
}

type CompactionTriggerType uint8

const (
    TriggerSpaceUsage CompactionTriggerType = iota
    TriggerTimeInterval
    TriggerSegmentCount
    TriggerManual
)

// StorageStats provides current storage metrics for trigger evaluation.
type StorageStats struct {
    LiveBytes     uint64  // Bytes in non-tombstoned entries
    GarbageBytes  uint64  // Bytes in tombstones and overwritten values
    SegmentCount  int     // Number of sealed segments
    LastCompaction time.Time
}
```

### Why These Triggers

| Trigger | Why Needed | Why Not Alternative |
|---------|------------|---------------------|
| SpaceUsageThreshold | Direct measure of inefficiency | Only time-based: wastes space |
| TimeInterval | Prevents unbounded accumulation | Only space-based: unpredictable |
| SegmentCount | Compaction overhead amortization | Only time: may compact too frequently |

---

## Compaction Strategy: Generational/Segmented

### Strategy Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  Generational Compaction                                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Sealed Segments (ordered by age):                                   │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                     │
│  │ Gen 0   │ │ Gen 1   │ │ Gen 2   │ │ Gen 3   │  ...              │
│  │ (newest)│ │         │ │         │ │ (oldest)│                   │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘                   │
│       │            │            │            │                      │
│       ▼            ▼            ▼            ▼                      │
│  More garbage   Some garbage   Less garbage  Mostly garbage        │
│  (compact now)  (compact)      (maybe later) (definitely compact)   │
│                                                                      │
│  Compaction reads oldest segments first (highest garbage ratio)     │
│  Writes live data to new active segment                              │
│  Oldest segments become "archived" and eligible for space reclaim   │
└─────────────────────────────────────────────────────────────────────┘
```

```go
// GenerationalCompactionStrategy implements segment-based compaction.
// Invariant: Compaction runs in background goroutine, never blocking writer.
// Invariant: Only one compaction pass at a time (serialized).
type GenerationalCompactionStrategy struct {
    // segmentSelector selects which segments to compact.
    // Default: Oldest N segments with highest garbage ratio.
    segmentSelector SegmentSelector

    // epochManager tracks active snapshots for safe reclamation.
    epochManager EpochManager

    // compactionWriter writes compacted data to new segments.
    compactionWriter CompactionWriter

    // gcThreshold is the minimum epoch age to consider VAddrs reclaimable.
    // Default: 3 epochs.
    // Why 3? Allows for clock skew, slow readers, and snapshot chains.
    gcThreshold int
}

// SegmentSelector determines which sealed segments to compact next.
type SegmentSelector interface {
    // Select returns segment IDs to compact, ordered by priority.
    // Returns empty slice if no segments need compaction.
    // 
    // Selection criteria:
    // 1. Oldest segments first (higher garbage ratio expected)
    // 2. Segments below efficiency threshold (garbage > 50%)
    // 3. Never select segments with recent active snapshots
    Select(sealedSegments []SegmentManifestEntry, stats *StorageStats) []SegmentID
}

// CompactionWriter writes compacted data during compaction.
// Invariant: Writes go to a new active segment, never to existing sealed segments.
type CompactionWriter interface {
    // BeginCompaction starts a new compaction pass.
    // Returns a CompactionContext for this pass.
    BeginCompaction() (*CompactionContext, error)

    // WriteLiveEntry writes a live key-value pair during compaction.
    // Returns VAddr where entry was written.
    WriteLiveEntry(ctx *CompactionContext, key []byte, value []byte) (VAddr, error)

    // WriteLiveNode writes a B-link node during compaction.
    WriteLiveNode(ctx *CompactionContext, node *NodeFormat) (VAddr, error)

    // CommitCompaction finalizes the compaction pass.
    // Marks old segments as compacted/archived.
    CommitCompaction(ctx *CompactionContext) error

    // AbortCompaction cancels the compaction pass.
    // Rolls back any partial writes.
    AbortCompaction(ctx *CompactionContext) error
}

// CompactionContext tracks state for a single compaction pass.
type CompactionContext struct {
    // Output segment for compacted data
    outputSegmentID SegmentID

    // TombstoneRewritten counts tombstones rewritten (for metrics)
    TombstoneRewritten uint64

    // LiveEntriesRewritten counts live entries rewritten
    LiveEntriesRewritten uint64

    // BytesWritten tracks space usage of output
    BytesWritten uint64

    // OldSegments references segments being replaced
    OldSegments []SegmentID
}
```

### Compaction Algorithm

```go
// Compact performs a generational compaction pass.
// Invariant: Does not block writer; runs in background.
// Invariant: Produces valid B-link tree nodes in finalized format.
func (s *GenerationalCompactionStrategy) Compact() error {
    // 1. Evaluate triggers
    if !s.policy.ShouldCompact(s.stats) {
        return nil
    }

    // 2. Select segments to compact
    segments := s.segmentSelector.Select(s.sealedSegments, s.stats)
    if len(segments) == 0 {
        return nil
    }

    // 3. Begin compaction pass
    ctx, err := s.compactionWriter.BeginCompaction()
    if err != nil {
        return fmt.Errorf("begin compaction: %w", err)
    }
    ctx.OldSegments = segments

    // 4. Scan selected segments, extract live data
    for _, segID := range segments {
        if err := s.compactSegment(ctx, segID); err != nil {
            s.compactionWriter.AbortCompaction(ctx)
            return fmt.Errorf("compact segment %d: %w", segID, err)
        }
    }

    // 5. Commit: swap old segments for compacted output
    if err := s.compactionWriter.CommitCompaction(ctx); err != nil {
        return fmt.Errorf("commit compaction: %w", err)
    }

    // 6. Update epoch tracking (see EpochManager below)
    s.epochManager.MarkCompactionComplete(ctx.OldSegments)

    return nil
}

// compactSegment scans a segment and rewrites live entries.
func (s *GenerationalCompactionStrategy) compactSegment(ctx *CompactionContext, segID SegmentID) error {
    // 1. Open segment for reading
    seg, err := s.openSegment(segID)
    if err != nil {
        return err
    }
    defer seg.Close()

    // 2. Iterate over B-link tree nodes in segment
    for nodeVAddr := seg.FirstNode(); nodeVAddr != VAddrInvalid; nodeVAddr = seg.NextNode(nodeVAddr) {
        node, err := s.nodeManager.Load(nodeVAddr)
        if err != nil {
            return fmt.Errorf("load node %v: %w", nodeVAddr, err)
        }

        // 3. Classify entries
        for i := 0; i < int(node.Count); i++ {
            entry := node.Entries[i]
            
            // Check visibility: is this entry visible to any active epoch?
            if s.epochManager.IsVisible(entry.VAddr) {
                // 4a. Rewrite live entry to new segment
                newVAddr, err := s.compactionWriter.WriteLiveEntry(ctx, entry.Key, entry.Value)
                if err != nil {
                    return fmt.Errorf("write live entry: %w", err)
                }
                // 5. Update PageManager mapping (see Space Reclamation)
                s.pageManager.UpdateMapping(entry.PageID, newVAddr)
                ctx.LiveEntriesRewritten++
            } else {
                // 4b. Entry is invisible to all epochs — it's garbage
                ctx.GarbageBytesFreed += estimateSize(entry)
            }
        }

        // 6. Rewrite tombstones that are still visible
        //    (Cannot drop tombstones until all snapshots expire)
        if node.IsDeleted {
            // Tombstone remains until epochManager confirms safe to drop
            s.compactionWriter.WriteTombstone(ctx, node.PageID)
            ctx.TombstoneRewritten++
        }
    }

    return nil
}
```

---

## Epoch-Based Snapshot Tracking

### Core Problem

Tombstones are necessary for MVCC — active snapshots may need to see deleted keys. We cannot simply drop tombstones during compaction without coordination.

### Solution: Epoch Manager

```go
// EpochManager tracks active snapshot epochs for safe garbage collection.
// 
// Invariant: VAddr is reclaimable only after all epochs that could see it have expired.
// Invariant: Epochs are created at transaction/snapshot boundaries.
// Invariant: Epochs expire after a grace period (no active readers).
//
// Why epoch-based instead of reference counting?
// - Reference counting requires tracking every VAddr across tree nodes
// - Epochs amortize tracking cost: one epoch covers all VAddrs visible to that snapshot
// - Simpler to implement correctly; fewer edge cases
type EpochManager interface {
    // RegisterEpoch creates a new epoch for a snapshot/transaction.
    // Returns epoch ID that callers must use when reading.
    // Invariant: Epoch IDs are monotonically increasing.
    RegisterEpoch() EpochID

    // UnregisterEpoch marks an epoch as expired.
    // After Unregister, the epoch's VAddrs become eligible for reclamation.
    // Invariant: Unregister is idempotent.
    UnregisterEpoch(epoch EpochID)

    // IsVisible returns true if vaddr is visible to any active epoch.
    // Used during compaction to determine if entry should be rewritten.
    // 
    // Invariant: If IsVisible returns false, no active snapshot can see vaddr.
    // Invariant: IsVisible is called from compaction, not hot path.
    IsVisible(vaddr VAddr) bool

    // IsSafeToReclaim returns true if vaddr can be reclaimed.
    // Combines IsVisible with epoch age check.
    // 
    // Invariant: If IsSafeToReclaim returns true, vaddr is invisible to all epochs
    //            AND epoch grace period has passed.
    IsSafeToReclaim(vaddr VAddr) bool

    // MarkCompactionComplete records that old segments were compacted.
    // Old VAddrs from those segments become eligible for reclaim after gcThreshold epochs.
    MarkCompactionComplete(oldSegments []SegmentID)
}

type EpochID uint64

const (
    // EpochGracePeriod is how long an expired epoch's VAddrs remain protected.
    // Default: 3 epochs.
    // Why grace period? Prevents reclaiming VAddrs seen by slow readers.
    EpochGracePeriod = 3
)

// Why not reference counting?
// Reference counting requires:
//   - Tracking every VAddr in every tree node (complex)
//   - Atomic increments/decrements on every read (overhead)
//   - Handling cycles in external value references
// Epochs trade precision for simplicity:
//   - One epoch per snapshot, not one counter per VAddr
//   - Grace period absorbs slow readers
//   - Good enough for most workloads
```

### Epoch Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│  Epoch Lifecycle and Garbage Collection                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   Time ─────────────────────────────────────────────────────────▶   │
│                                                                      │
│   Epoch 1 created (snapshot started)                                 │
│        │                                                             │
│        ├─ Epoch 1 active: all VAddrs visible                        │
│        │                                                             │
│        ├─ Epoch 1 unregistered (snapshot closed)                    │
│        │                                                             │
│        ├─ Epoch 1 enters grace period (3 epochs)                     │
│        │   │                                                         │
│        │   ├─ Compaction: VAddrs from Epoch 1 still protected       │
│        │   │                                                         │
│        │   ├─ Epoch 4 created (new snapshot)                        │
│        │   │                                                         │
│        │   └─ Epoch 4 unregistered                                   │
│        │                                                             │
│        └─ Epoch 1 exits grace period                                 │
│            │                                                         │
│            └─ VAddrs from Epoch 1 are now reclaimable                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Space Reclamation

### External Value Reclamation

```go
// ExternalValueReclaimer manages reclamation of external values.
// Invariant: External values are reclaimed only when IsSafeToReclaim returns true.
// Invariant: Reclamation is idempotent (safe to call multiple times).
type ExternalValueReclaimer interface {
    // RegisterExternalValue records that vaddr holds an external value.
    // Called when storing a new external value.
    RegisterExternalValue(vaddr VAddr)

    // UnregisterExternalValue marks vaddr as deleted (but not yet reclaimable).
    // Called from Delete() in external-value-store.md.
    UnregisterExternalValue(vaddr VAddr) error

    // TryReclaim attempts to reclaim vaddr if safe.
    // Returns true if reclaimed, false if not yet safe.
    // 
    // Invariant: If TryReclaim returns true, vaddr was previously unregistered
    //            AND epoch manager confirms safe to reclaim.
    TryReclaim(vaddr VAddr) (reclaimed bool, err error)

    // BatchTryReclaim attempts to reclaim multiple vaddrs efficiently.
    // Returns list of actually reclaimed vaddrs.
    BatchTryReclaim(vaddrs []VAddr) ([]VAddr, error)
}

// Reclamation is called during compaction commit phase:
// 
// CommitCompaction():
//   1. Build list of VAddrs that were NOT rewritten (garbage)
//   2. For each garbage VAddr:
//        - If external value: BatchTryReclaim([vaddr])
//        - If page mapping: PageManager.FreePage(pageID)
//   3. Mark old segments as archived
//   4. Done
```

### Page Manager Integration

```go
// PageManager.ReclaimPage marks page as freed after compaction.
// 
// Invariant: Page is reclaimable only after:
//   1. PageManager.UpdateMapping(pageID, newVAddr) called (redirect complete)
//   2. All in-flight reads using oldVAddr have drained (epoch grace period)
//   3. Compaction commit has completed
//
// Why separate ReclaimPage from FreePage?
// FreePage is immediate (caller knows page is dead).
// ReclaimPage is deferred (must wait for epoch grace period).
type ReclaimablePage struct {
    PageID     PageID
    OldVAddr   VAddr
    ReclaimAt  EpochID  // Earliest epoch ID that can reclaim
}

// ReclaimPages releases all pages whose reclaim epoch has passed.
// Called periodically or during compaction commit.
func (pm *PageManager) ReclaimPages(currentEpoch EpochID) int {
    reclaimed := 0
    pm.mu.Lock()
    defer pm.mu.Unlock()

    for _, page := range pm.reclaimablePages {
        if page.ReclaimAt <= currentEpoch {
            pm.freeList.Push(page.PageID)
            reclaimed++
        }
    }
    pm.reclaimablePages = filterReclaimable(pm.reclaimablePages, currentEpoch)
    return reclaimed
}
```

---

## Writer Pipeline Integration

### Critical Constraint: Non-Blocking Compaction

The single-writer pipeline must never block due to compaction. Solution: **separation of concerns**.

```go
// CompactionCoordinator orchestrates background compaction without blocking writer.
// 
// Invariant: Writer pipeline and compaction are completely independent.
// Invariant: Compaction reads sealed segments; writer writes to active segment.
// Invariant: Coordination happens only at segment boundaries (seal/rotate).
type CompactionCoordinator struct {
    // BackgroundCompactor runs compaction in separate goroutine.
    backgroundCompactor *BackgroundCompactor

    // segmentRotator handles Active → Sealed transitions.
    // Called by writer when active segment is full.
    segmentRotator SegmentRotator

    // epochManager shared with writer for snapshot tracking.
    epochManager EpochManager
}

// SegmentRotator seals current active segment and creates new active.
// Called synchronously by writer (fast operation).
// 
// Why seal synchronously?
// Segment rotation is just updating manifest pointers.
// Actual compaction happens later, asynchronously.
func (sr *SegmentRotator) Rotate() error {
    // 1. Close current active segment (seal)
    if err := sr.sealCurrentSegment(); err != nil {
        return fmt.Errorf("seal segment: %w", err)
    }

    // 2. Create new active segment
    if err := sr.createActiveSegment(); err != nil {
        return fmt.Errorf("create active: %w", err)
    }

    // 3. Notify compaction coordinator (non-blocking)
    select {
    case sr.compactionReady <- struct{}{}:
    default:
        // Compactor will pick up new segment on next check
    }

    return nil
}

// BackgroundCompactor watches for sealed segments and compacts asynchronously.
// Invariant: Never blocks; runs in dedicated goroutine.
func (bc *BackgroundCompactor) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case <-bc.compactionReady:
            bc.evaluateAndCompact()
        case <-time.After(bc.checkInterval):
            bc.evaluateAndCompact()
        }
    }
}

// Writer pipeline is completely unaffected:
//   Put(key, value):
//       1. Append to active segment
//       2. Return success
//       (Compaction runs independently, in background)
```

---

## Compaction Configuration

```go
// CompactionConfig holds compaction tuning parameters.
type CompactionConfig struct {
    // Enabled turns compaction on/off.
    // Default: true.
    Enabled bool

    // Trigger defines when compaction runs.
    // If nil, uses DefaultCompactionPolicy.
    Trigger CompactionPolicy

    // Parallelism is the number of concurrent segment readers.
    // Default: 2 (balance I/O vs CPU).
    // Why limited? Compaction is I/O bound, not CPU bound.
    Parallelism int

    // WriteBufferSize is the buffer size for compaction output.
    // Default: 4MB.
    // Larger = better sequential write, more memory.
    WriteBufferSize int

    // MaxSegmentAge is the maximum age of segments to compact together.
    // Default: 10 segments.
    // Why limit? Older segments have more garbage, but compacting too many
    // at once increases latency variance.
    MaxSegmentAge int

    // GcThreshold is the minimum epoch age for VAddr reclamation.
    // Default: 3 epochs.
    GcThreshold int
}

// Defaults
const (
    DefaultCompactionEnabled     = true
    DefaultCompactionParallelism = 2
    DefaultWriteBufferSize       = 4 * 1024 * 1024  // 4 MB
    DefaultMaxSegmentAge         = 10
)
```

---

## Invariants Summary

```go
// Compaction invariants:

// 1. Snapshot safety: IsVisible is called before rewriting any entry.
//    Proved by: epochManager tracks all active snapshots.

// 2. Tombstone preservation: Tombstones are rewritten until epoch grace period expires.
//    Proved by: IsSafeToReclaim combines visibility + grace period.

// 3. External value safety: TryReclaim checks epochManager before reclaiming.
//    Proved by: IsSafeToReclaim is precondition for reclamation.

// 4. No writer blocking: Compaction runs in background goroutine.
//    Proved by: Segment rotation is synchronous; compaction is async.

// 5. B-link node validity: Compaction produces nodes in finalized format.
//    Proved by: compactionWriter uses same NodeManager as writer.

// 6. VAddr uniqueness: Compaction never reuses VAddrs.
//    Proved by: AllocateVAddr always returns fresh addresses.

// 7. Page mapping consistency: UpdateMapping called before old VAddr becomes reclaimable.
//    Proved by: CompactionContext tracks old→new mappings, commit waits for update.
```

---

## Why Not Alternatives

| Alternative | Why Rejected |
|-------------|--------------|
| Immediate reclamation on Delete | Violates MVCC; active snapshots may need deleted keys |
| Reference counting per VAddr | Complex, error-prone, high overhead |
| Whole-database rewrite | Blocks writer, unacceptable latency |
| On-demand compaction | Unpredictable, may starve compaction under load |
| Lock-free compaction | Too complex for single-writer model; not needed |
| Priority queue of tombstones | Epoch manager handles this implicitly |

---

## Related Specifications

- **API Layer**: Delete() creates tombstones; epoch tracking begins at transaction start
- **B-link Tree**: Nodes are immutable; compaction produces new nodes
- **External Value Store**: VAddrs tracked for reclamation; Delete() tombstones values
- **VAddr Format**: Segments have lifecycle; compacted segments become archived
- **Page Manager**: PageID mappings updated during compaction; free list for reuse
- **Concurrency/Recovery**: Checkpoints sync with compaction; WAL records segment rotation

---

*Document Status: Contract Definition*
*Last Updated: 2024*
