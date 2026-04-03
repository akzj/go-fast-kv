# Concurrency Model and Crash Recovery

## Overview

Defines the concurrency control strategy (single-writer/multi-reader with B-link latch protocol) and crash recovery system (checkpoint + write-ahead log).

**Relates to:**
- `vaddr-format.md` — VAddr format (SegmentID + Offset, 16 bytes)
- `kv-store-design.md` — Architecture context

## Concurrency Model

### 1. Single-Writer/Multi-Reader Model

```go
// ConcurrencyModel defines the threading strategy for the KV store.
// Invariant: Exactly one goroutine holds the write lock at any time.
// Invariant: Multiple goroutines can hold read locks concurrently.
// Invariant: Writers have priority to prevent write starvation.
type ConcurrencyModel struct {
    // WriterMutex serializes all write operations.
    // Why not multiple writers? B-link split coordination is simpler single-writer.
    // Multiple writers would require distributed locking across splits.
    WriterMutex sync.Mutex

    // ReaderCount tracks active readers for writer notification.
    // Why atomic? Readers and writers coordinate through this counter.
    ReaderCount atomic.Int64

    // WriteGate allows readers to block writers during critical sections.
    // Why not reader-writer lock? B-link's latch crabbing handles tree latching.
    // This is for coordinating external operations (compaction, checkpoint).
    WriteGate chan struct{}
}

// Why single-writer?
// 1. B-link splits require coordinating parent/child updates atomically.
// 2. Single writer eliminates distributed deadlock detection.
// 3. Append-only storage naturally serializes writes (unique VAddrs).
// 4. Write throughput is bounded by append bandwidth, not lock contention.
//
// Why multi-reader?
// 1. Reads are 10x-100x more frequent than writes in typical KV workloads.
// 2. B-link trees support lock-free reads via sibling chains.
// 3. Readers never block each other; only wait for writers.
```

### 2. B-link Latch Protocol Integration

```go
// LatchMode defines how a node is latched during an operation.
// Invariant: Latches are held for minimum duration (latch crabbing).
// Invariant: Latches are never acquired in descending order (prevents deadlock).
type LatchMode int

const (
    LatchRead  LatchMode = iota  // Shared latch; multiple readers allowed
    LatchWrite                    // Exclusive latch; single writer only
)

// BLinkLatchManager manages latches on B-link tree nodes.
// Why separate from NodeManager? Different concerns:
// - NodeManager: node lifecycle, persistence
// - BLinkLatchManager: in-memory concurrency control
type BLinkLatchManager interface {
    // Acquire gets a latch on node at vaddr with specified mode.
    // Blocks if latch is held by another goroutine.
    // Invariant: Acquire is always paired with Release.
    // Invariant: Writers acquire LatchWrite, readers acquire LatchRead.
    Acquire(vaddr VAddr, mode LatchMode)

    // Release frees a previously acquired latch.
    // Precondition: caller must have held the latch.
    // Invariant: Release is always called, even on panic (defer pattern).
    Release(vaddr VAddr, mode LatchMode)

    // TryAcquire attempts to acquire latch without blocking.
    // Returns true if acquired, false if contended.
    // Why TryAcquire? Used in乐观锁 paths for non-blocking operations.
    TryAcquire(vaddr VAddr, mode LatchMode) bool

    // Upgrade attempts to upgrade a read latch to write latch.
    // Returns error if another reader holds the latch.
    // Why not always write latch? Read latches allow concurrent readers.
    Upgrade(vaddr VAddr) error
}

// BLinkLatches is the global latch table for all nodes.
// Invariant: Latch table is fixed size; vaddr % table_size determines slot.
// Why hash table? Latches are created/destroyed dynamically.
type BLinkLatches struct {
    slots   []atomic.Pointer[latchSlot]
    mask    uint64  // slots mask (power of 2 - 1)
    pool    sync.Pool  // reuse latch objects
}

type latchSlot struct {
    mu     sync.Mutex
    mode   LatchMode
    count  int  // shared latch count
    holder uint64  // goroutine ID for write latch
}

// Why slot-based not per-node?
// B-link trees have thousands of nodes; per-node mutexes waste memory.
// Slot-based with vaddr hash provides O(1) average latch acquisition.
```

### 3. Latch Crabbing Protocol

```go
// CrabbingProtocol defines the standard B-link tree traversal with latching.
// Invariant: Latches are acquired top-down, released bottom-up.
// Invariant: Child latch acquired before parent latch released (except splits).
//
// Crabbing phases:
// Phase 1 (Search): Acquire read latches, descend
// Phase 2 (Modify): If split: acquire write latch on parent, release children
// Phase 3 (Update): Update leaf, propagate changes upward
//
// Why not always write latches?
// Read-only operations (Get, Scan) only need read latches.
// This allows concurrent readers during writes.
type CrabbingProtocol struct {
    manager BLinkLatchManager
}

// SearchWithLatches traverses tree to find key, acquiring read latches.
// Returns leaf node and index where key should be.
// Invariant: Caller must release all latches via ReleasePath.
func (cp *CrabbingProtocol) SearchWithLatches(root VAddr, key PageID) (
    leaf *NodeFormat, index int, path []VAddr, err error) {
    // 1. Start at root with read latch
    // 2. Search node for child pointer
    // 3. Acquire child latch
    // 4. Release parent latch (latch crabbing)
    // 5. Repeat until leaf
    // 6. Return leaf with read latch held
}

// WriteWithLatches performs insert/update with write latches.
// Returns new root if tree height increased.
// Invariant: Holds write latch on leaf, read latches on ancestors.
func (cp *CrabbingProtocol) WriteWithLatches(root VAddr, key PageID, value InlineValue) (
    newRoot VAddr, err error) {
    // 1. Start at root with write latch (upgrade if needed)
    // 2. Descend, acquiring write latches (latch crabbing)
    // 3. At leaf: perform insert
    // 4. If split: propagate upward
    // 5. Release all latches bottom-up
}

// Why release bottom-up?
// Child latch must be held while updating parent.
// Otherwise: concurrent reader might see inconsistent state.
```

### 4. Lock-Free Read Optimization

```go
// LockFreeReadPath enables reads without acquiring tree latches.
// Invariant: Lock-free reads may see stale data but never corrupt data.
// Invariant: Lock-free reads must handle concurrent splits gracefully.
//
// Why lock-free?
// B-link property: during split, all keys in left < splitKey < all keys in right.
// Reader following sibling chain will eventually find correct key.
//
// Limitations:
// - May retry if node splits during read
// - May see value before split propagation completes
// - Cannot guarantee read-your-writes consistency
type LockFreeReadPath struct {
    // RetryLimit prevents infinite retry on pathological contention.
    // Why 3? Typical splits happen at most 1-2 times per key.
    RetryLimit int = 3
}

// SearchLockFree traverses tree without latches, using sibling chains.
func (lf *LockFreeReadPath) SearchLockFree(root VAddr, key PageID) ([]byte, error) {
    // 1. Load root node (may be stale if split in progress)
    // 2. Search for key (descend via child pointers)
    // 3. If at non-leaf and key > HighKey:
    //      - Follow HighSibling to find split target
    //      - Retry search from new node
    // 4. If at leaf and key not found:
    //      - Follow HighSibling and retry
    // 5. Return value or NotFound
}

// Why HighSibling on internal nodes?
// During internal node split, parent update is delayed.
// Without HighSibling, reader might descend wrong child.
// With HighSibling, reader follows chain until finding correct subtree.
```

### 5. Write Ordering and Visibility

```go
// WriteBarrier ensures writes are visible in correct order.
// Why needed? Goroutines may execute concurrently, out-of-order.
//
// Invariant: All stores before a barrier are visible after the barrier.
// Invariant: Stores after a barrier are not visible before the barrier.
var WriteBarrier = sync.LazyFunc(func() {
    runtime.Gosched()  // Force memory ordering
})

// WriteTransaction represents an atomic set of modifications.
// Invariant: All modifications in transaction are visible atomically.
// Invariant: Transaction either commits fully or not at all.
//
// Why not nested transactions?
// Single-level transactions are sufficient for KV semantics.
// Multi-key transactions would require 2PC or similar complexity.
type WriteTransaction struct {
    ops []WriteOp  // Pending operations
    
    // Reservation phase: allocate VAddrs for all modifications
    // Execution phase: append all nodes to storage
    // Commit phase: atomically update tree root
    //
    // Why reservation first?
    // If execution fails (crash), we know which VAddrs were reserved.
    // Recovery can reclaim unused VAddrs.
}

// Commit atomically applies transaction to the tree.
// Returns new root VAddr.
func (tx *WriteTransaction) Commit() (VAddr, error) {
    // 1. Validate all operations
    // 2. Persist all new nodes (append-only)
    // 3. Atomic update of root pointer
    // 4. Return new root
}
```

## Crash Recovery System

### 6. Write-Ahead Log (WAL)

```go
// WALRecordType identifies the type of WAL record.
type WALRecordType uint8

const (
    WALPageAlloc     WALRecordType = iota  // Page allocation
    WALPageFree                           // Page deallocation
    WALNodeWrite                          // B-link node written
    WALExternalValue                     // External value stored
    WALRootUpdate                         // Tree root changed
    WALCheckpoint                         // Checkpoint marker
)

// WALRecord is a single record in the write-ahead log.
// Invariant: WAL is append-only; records never overwritten.
// Invariant: WAL record contains all info needed to redo the operation.
type WALRecord struct {
    // Header (16 bytes)
    LSN        uint64    // Log sequence number (monotonically increasing)
    RecordType WALRecordType
    Length     uint32    // Payload length
    Checksum   uint32    // CRC32c of payload
    
    // Payload (variable)
    Payload    []byte    // Type-specific data
    
    // Why not store LSN in payload?
    // LSN is used for recovery position; must be accessible without parsing payload.
}

// WALFormat defines the on-disk layout.
// Invariant: WAL is always appended; never written in-place.
// Invariant: WAL pages are PageSize bytes for alignment.
type WALFormat struct {
    Header struct {
        Magic       [8]byte  // "WAL\0\0\0\0\0"
        Version     uint16
        SegmentID   uint64   // Which segment this WAL belongs to
        FirstLSN    uint64   // LSN of first record in this segment
        LastLSN     uint64   // LSN of last record (updated on append)
    }
    
    // Records are appended after header, page-aligned.
    // Each page has its own mini-header for recovery.
}

// Why WAL when storage is append-only?
// 1. Append-only storage provides persistence but not transactional atomicity.
// 2. A transaction may span multiple pages/nodes.
// 3. WAL ensures we can recover partial transactions (redo or undo).
// 4. Checkpoint can be verified against WAL for consistency.
//
// Why not shadow paging?
// Shadow paging requires updating pointers atomically.
// With B-link tree, root update is already atomic (single pointer).
// WAL is simpler and provides better recovery granularity.
```

### 7. WAL Record Types

```go
// WALPageAllocRecord records a page allocation.
type WALPageAllocRecord struct {
    PageID    PageID
    VAddr     VAddr
    Timestamp uint64  // Monotonic clock for ordering
}

// WALNodeWriteRecord records a B-link node write.
// Why store full node? Allows redo recovery without accessing original.
type WALNodeWriteRecord struct {
    NodeVAddr  VAddr
    NodeType   uint8  // Leaf or Internal
    Level      uint8
    HighKey    PageID
    HighSibling VAddr
    EntryCount uint8
    // Entry data follows in Payload
}

// WALRootUpdateRecord marks a transaction boundary.
// Only this record type requires fsync before acknowledging write.
type WALRootUpdateRecord struct {
    OldRoot VAddr  // For rollback if needed
    NewRoot VAddr
}

// WALCheckpointRecord marks the beginning of a checkpoint.
// Recovery can start from here instead of scanning full WAL.
type WALCheckpointRecord struct {
    CheckpointID    uint64
    RootVAddr       VAddr
    PageManagerState PageManagerSnapshot
    ExternalValueState ExternalValueSnapshot
    Timestamp       uint64
}
```

### 8. Checkpoint Strategy

```go
// CheckpointPolicy defines when checkpoints are taken.
type CheckpointPolicy struct {
    // Interval triggers checkpoint after this duration.
    Interval time.Duration  // Default: 30 seconds
    
    // WALSize triggers checkpoint when WAL exceeds this size.
    // Default: 64 MB
    WALSizeLimit uint64
    
    // DirtyPages triggers checkpoint when dirty page count exceeds.
    // Default: 1000 pages
    DirtyPageLimit int
    
    // ForceCheckpoint immediately checkpoints (for shutdown).
    ForceCheckpoint chan struct{}
}

// Why multiple triggers?
// Time-based: ensures bounded recovery time.
// Size-based: prevents WAL from growing unbounded.
// Page-based: balances checkpoint cost vs recovery cost.
```

```go
// Checkpoint represents a consistent point-in-time snapshot.
// Invariant: Checkpoint is crash-consistent; all data up to LSN is durable.
// Invariant: Checkpoint can be recovered without replaying WAL from start.
type Checkpoint struct {
    ID            uint64
    LSN           uint64  // All WAL records before this LSN are included
    
    // Components captured atomically:
    TreeRoot      VAddr   // B-link tree root VAddr
    PageManager   PageManagerSnapshot
    ExternalValues ExternalValueSnapshot
    
    // Checkpoint metadata
    Timestamp     uint64
    Duration      time.Duration  // Time to create checkpoint
    PagesWritten  uint64
}

// CheckpointManager orchestrates checkpoint creation.
// Invariant: Only one checkpoint runs at a time.
// Invariant: Checkpoint does not block readers or writers (copy-on-write).
type CheckpointManager interface {
    // CreateCheckpoint performs a full checkpoint.
    // Returns Checkpoint descriptor on success.
    // Invariant: All data up to returned LSN is durable.
    CreateCheckpoint() (*Checkpoint, error)
    
    // GetLatestCheckpoint returns the most recent checkpoint.
    // Used by recovery to determine starting point.
    GetLatestCheckpoint() (*Checkpoint, error)
    
    // DeleteCheckpoint removes old checkpoint data.
    // Called after WAL is truncated past checkpoint LSN.
    DeleteCheckpoint(id uint64) error
}

// Why copy-on-write checkpoint?
// 1. Concurrent readers can continue using old checkpoint.
// 2. Writer is not blocked during checkpoint I/O.
// 3. Checkpoint failure doesn't corrupt active database.
// 4. Multiple checkpoints can exist (for backup/clone).
```

### 9. Recovery Algorithm

```go
// RecoveryState tracks progress through recovery phases.
type RecoveryState struct {
    // Phase 1: Find checkpoint
    Checkpoint     *Checkpoint
    LastCheckpoint LSN
    
    // Phase 2: Truncate WAL
    TruncationPoint LSN
    
    // Phase 3: Apply WAL
    RedoCount      int
    RedoFailed     []WALRecord
    
    // Phase 4: Verify
    VerifiedNodes  int
    CorruptNodes    []VAddr
}

// Recover performs crash recovery to restore consistent state.
// Returns to caller when recovery is complete.
func Recover(store *KVStore) (*RecoveryState, error) {
    state := &RecoveryState{}
    
    // Phase 1: Find latest valid checkpoint
    // ─────────────────────────────────────
    cp, err := FindLatestCheckpoint(store.CheckpointDir)
    if err != nil {
        return nil, fmt.Errorf("no checkpoint found: %w", err)
    }
    state.Checkpoint = cp
    state.LastCheckpoint = cp.LSN
    
    // Phase 2: Load checkpoint state
    // ─────────────────────────────────────
    store.TreeRoot = cp.TreeRoot
    if err := store.PageManager.RestoreSnapshot(cp.PageManagerState); err != nil {
        return nil, fmt.Errorf("page manager restore failed: %w", err)
    }
    if err := store.ExternalValueStore.RestoreSnapshot(cp.ExternalValueState); err != nil {
        return nil, fmt.Errorf("external value store restore failed: %w", err)
    }
    
    // Phase 3: Replay WAL from checkpoint LSN
    // ─────────────────────────────────────
    if err := ReplayWAL(store.WAL, cp.LSN, state); err != nil {
        return nil, fmt.Errorf("WAL replay failed: %w", err)
    }
    
    // Phase 4: Verify integrity
    // ─────────────────────────────────────
    if err := VerifyTreeIntegrity(store); err != nil {
        log.Printf("warning: integrity issues detected: %v", err)
        // Continue; tree is still recoverable via repair mode
    }
    
    // Phase 5: Truncate WAL
    // ─────────────────────────────────────
    if err := store.WAL.Truncate(cp.LSN); err != nil {
        log.Printf("warning: WAL truncation failed: %v", err)
        // Non-fatal; WAL will be truncated on next checkpoint
    }
    
    return state, nil
}

// ReplayWAL applies all WAL records from startLSN to end of log.
func ReplayWAL(wal *WAL, startLSN uint64, state *RecoveryState) error {
    // 1. Open WAL from startLSN
    // 2. For each record:
    //      - Validate checksum
    //      - Apply redo (always; no undo needed for append-only)
    //      - Update LSN tracking
    // 3. Return on EOF or corrupt record
}

// Why redo-only, not undo?
// Append-only storage never overwrites data.
// Deleted data uses tombstones, not in-place removal.
// Therefore, we only need to redo committed operations.
```

### 10. Recovery from Append-Only Storage

```go
// RecoverFromSegments rebuilds state by scanning all segments.
// Used when no checkpoint exists (first startup or corruption).
func RecoverFromSegments(store *KVStore) error {
    // 1. Scan all segment files
    // 2. Build page_id → vaddr map from segment metadata
    // 3. Identify B-link tree nodes by magic bytes
    // 4. Find root node (node with no parent in map)
    // 5. Validate node structure (checksum, key ordering)
    // 6. Rebuild external value index
    
    // This is O(n) scan but handles complete failure modes.
}

// Why not always use segment scan?
// 1. Slow for large databases (millions of pages).
// 2. Cannot recover uncommitted transactions.
// 3. WAL provides faster, more precise recovery.
```

## Invariants Summary

```go
// Concurrency invariants:
// - WriterMutex is held by exactly one goroutine at any time.
// - Latch acquisition order: root → leaf (top-down).
// - Latch release order: leaf → root (bottom-up).
// - Lock-free reads may retry; bounded by RetryLimit.

// Recovery invariants:
// - WAL records are never overwritten or deleted except via Truncate.
// - Checkpoint LSN indicates all prior records are durable.
// - Recovery from checkpoint + WAL replays exactly committed transactions.
// - Append-only storage ensures no partial page writes.

// Crash consistency invariants:
// - All writes are durable after fsync returns.
// - Tree root update is atomic (single VAddr write).
// - External value store is crash-consistent (all-or-nothing page writes).
// - Page Manager index can be rebuilt from segment scan.
```

## Why These Design Choices

| Decision | Alternative | Why Not |
|----------|-------------|---------|
| Single writer | Multiple writers | B-link split coordination simpler; append-only serializes anyway |
| B-link latches | Fine-grained locks | Latch crabbing already handles tree traversal; extra granularity adds complexity |
| Lock-free reads | All reads latched | Reads are 10x-100x more common; avoid reader contention |
| WAL + Checkpoint | Shadow paging | WAL provides transaction atomicity; simpler than atomic pointer updates |
| Redo-only recovery | Undo-redo | Append-only storage never overwrites; no undo needed |
| Checkpoint before WAL truncate | Truncate immediately | Checkpoint verifies data before reclaiming log space |

---

*Document Status: Contract Definition*
*Last Updated: 2024*
