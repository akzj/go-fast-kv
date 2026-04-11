// Package vacuumapi defines the interface for the Vacuum process.
//
// Vacuum cleans up old MVCC versions from B-tree leaf nodes that are
// no longer visible to any active transaction. This is the logical
// cleanup layer — it removes entries from leaves and frees associated
// blobs. The physical space reclamation (segment file cleanup) is
// handled separately by GC.
//
// Vacuum runs periodically and processes all leaf pages by traversing
// the B-tree from the leftmost leaf via Next (right-link) pointers.
//
// Two cleanup cases:
//
//  1. Committed delete/overwrite: entry.TxnMax != MaxUint64 AND
//     entry.TxnMax < safeXID AND clog.Get(entry.TxnMax) == Committed
//     → physically remove the entry (and free its blob if any)
//
//  2. Aborted creator: clog.Get(entry.TxnMin) == Aborted
//     → physically remove the entry (and free its blob if any)
//     → if the aborted entry had overwritten a previous version
//     (prevEntry.TxnMax == entry.TxnMin), restore prevEntry.TxnMax
//     to MaxUint64
//
// Design reference: docs/DESIGN.md §3.9.6
package vacuumapi

import "errors"

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrNoLeaves is returned when the B-tree is empty (no root or no leaves).
	ErrNoLeaves = errors.New("vacuum: no leaves to vacuum")
)

// ─── Stats ──────────────────────────────────────────────────────────

// VacuumStats reports the results of a vacuum run.
type VacuumStats struct {
	LeavesScanned  int // total leaf pages visited
	LeavesModified int // leaf pages that had entries removed
	EntriesRemoved int // total entries physically removed
	BlobsFreed     int // blobs freed via BlobStore.Delete
}

// ─── PageLocker ─────────────────────────────────────────────────────

// PageLocker abstracts page-level locking for vacuum.
// Vacuum acquires write locks on each leaf page during processing,
// and read locks when navigating the internal node hierarchy,
// preventing concurrent Put/Delete operations from corrupting data.
//
// The B-link tree guarantees that at most one lock is held at a time
// per goroutine, making deadlocks impossible.
type PageLocker interface {
	RLock(pageID uint64)
	RUnlock(pageID uint64)
	WLock(pageID uint64)
	WUnlock(pageID uint64)
}

// ─── Interface ──────────────────────────────────────────────────────

// Vacuum cleans up old MVCC versions from B-tree leaves.
//
// Thread safety: Vacuum acquires write locks on each leaf page
// individually (one at a time) to avoid blocking concurrent reads
// for extended periods. It does NOT hold a global lock.
//
// Design reference: docs/DESIGN.md §3.9.6
type Vacuum interface {
	// Run performs a full vacuum pass over all B-tree leaves.
	//
	// Algorithm:
	//   1. safeXID = txnMgr.GetMinActive()
	//   2. Navigate from root to leftmost leaf (descend Children[0])
	//   3. For each leaf (following Next links):
	//      a. Read leaf entries
	//      b. Identify removable entries (Case 1 + Case 2)
	//      c. If any removed: rewrite the leaf page, write WAL batch
	//   4. Return stats
	//
	// Returns ErrNoLeaves if the tree is empty.
	Run() (*VacuumStats, error)

	// RunIncremental performs an incremental vacuum pass.
	// It processes at most targetPages leaf pages.
	// If this is the start of a new pass (internal state reset),
	// it starts from the leftmost leaf. Otherwise it continues
	// from where the last call left off.
	//
	// Use Run() for a full pass, RunIncremental() for non-blocking
	// batch processing when the tree is too large for a single pass.
	//
	// Returns ErrNoLeaves if the tree is empty.
	RunIncremental(targetPages int) (*VacuumStats, error)
}
