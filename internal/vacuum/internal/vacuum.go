// Package vacuum implements the Vacuum process — MVCC old version cleanup.
//
// Vacuum traverses all B-tree leaf pages (leftmost leaf → Next chain)
// and removes entries that are no longer visible to any active transaction.
//
// Two cleanup cases:
//  1. Committed delete/overwrite: TxnMax < safeXID and committed
//  2. Aborted creator: TxnMin was aborted → remove + restore prev version
//
// Design reference: docs/DESIGN.md §3.9.6
package internal

import (
	"bytes"
	"math"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	vacuumapi "github.com/akzj/go-fast-kv/internal/vacuum/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// Compile-time interface check.
var _ vacuumapi.Vacuum = (*vacuumer)(nil)

// vacuumer implements vacuumapi.Vacuum.
type vacuumer struct {
	rootPageIDFn      func() uint64
	pages             btreeapi.PageProvider
	txnMgr            txnapi.TxnManager
	blobStore         blobstoreapi.BlobStore
	wal               walapi.WAL
	segSync           func() error
	registerCollector func() (*[]pagestoreapi.WALEntry, func()) // per-goroutine WAL entry collector
	pageLocks         vacuumapi.PageLocker                      // page-level locks for concurrent safety

	// Incremental vacuum progress: last processed leaf PID.
	// 0 = start of a new pass (will find leftmost leaf).
	lastLeafPID uint64

	// passComplete is true when the last incremental pass reached the end.
	// When true, next RunIncremental starts fresh from leftmost leaf.
	passComplete bool
}

// New creates a new Vacuum instance.
//
// Parameters:
//   - rootPageIDFn: returns the B-tree root PageID (e.g. tree.RootPageID)
//   - pages: PageProvider for reading/writing leaf nodes
//   - txnMgr: TxnManager for GetMinActive() + CLOG access
//   - blobStore: BlobStore for deleting blobs of removed entries
//   - wal: WAL for atomic batch writes
//   - segSync: function to fsync the page segment (e.g. pageSegMgr.Sync)
//   - registerCollector: registers a per-goroutine WAL entry collector.
//     Returns a pointer to the entry slice (populated by WritePage calls)
//     and an unregister function. This ensures vacuum's WritePage WAL
//     entries are isolated from concurrent Put/Delete operations, preventing
//     the shared-buffer stealing bug where DrainWALEntries could drain
//     entries belonging to other operations.
//   - pageLocks: PageLocker for acquiring page locks during vacuum.
//     This prevents concurrent Put/Delete from corrupting data while
//     vacuum rewrites leaf pages.
func New(
	rootPageIDFn func() uint64,
	pages btreeapi.PageProvider,
	txnMgr txnapi.TxnManager,
	blobStore blobstoreapi.BlobStore,
	wal walapi.WAL,
	segSync func() error,
	registerCollector func() (*[]pagestoreapi.WALEntry, func()),
	pageLocks vacuumapi.PageLocker,
) vacuumapi.Vacuum {
	return &vacuumer{
		rootPageIDFn:      rootPageIDFn,
		pages:             pages,
		txnMgr:            txnMgr,
		blobStore:         blobStore,
		wal:               wal,
		segSync:           segSync,
		registerCollector: registerCollector,
		pageLocks:         pageLocks,
	}
}

// Run performs a full vacuum pass over all B-tree leaves.
func (v *vacuumer) Run() (*vacuumapi.VacuumStats, error) {
	stats := &vacuumapi.VacuumStats{}

	// 1. Determine safe cleanup boundary
	safeXID := v.txnMgr.GetMinActive()
	if safeXID == math.MaxUint64 {
		// No active transactions — use NextXID as the boundary
		safeXID = v.txnMgr.NextXID()
	}

	// 2. Check root
	rootPID := v.rootPageIDFn()
	if rootPID == 0 {
		return nil, vacuumapi.ErrNoLeaves
	}

	// 3. Navigate to leftmost leaf
	leafPID, err := v.findLeftmostLeaf(rootPID)
	if err != nil {
		return nil, err
	}

	// Register a per-goroutine WAL entry collector. This ensures vacuum's
	// WritePage calls route entries to our own collector (keyed by goroutine ID),
	// NOT the shared legacy buffer. This prevents the shared-buffer stealing bug.
	pageEntries, unregCollector := v.registerCollector()
	defer unregCollector()

	// Collect blob WAL entries across all leaves
	var blobWALEntries []blobstoreapi.WALEntry

	// 4. Iterate all leaves via Next links, with page locks for safety
	for leafPID != 0 {
		// Acquire write lock before reading — prevents concurrent Put/Delete
		// from modifying the page while we analyze and potentially rewrite it.
		v.pageLocks.WLock(leafPID)

		node, err := v.pages.ReadPage(leafPID)
		if err != nil {
			v.pageLocks.WUnlock(leafPID)
			return nil, err
		}
		if !node.IsLeaf {
			// Safety: should not happen if tree is well-formed
			v.pageLocks.WUnlock(leafPID)
			break
		}

		stats.LeavesScanned++

		// Process this leaf under the write lock
		removed, blobEntries, err := v.processLeaf(node, leafPID, safeXID)
		if err != nil {
			v.pageLocks.WUnlock(leafPID)
			return nil, err
		}

		if removed > 0 {
			// Rewrite the leaf page under the write lock.
			// WritePage routes WAL entries to our registered collector
			// (via goroutine ID), not the shared buffer.
			if err := v.pages.WritePage(leafPID, node); err != nil {
				v.pageLocks.WUnlock(leafPID)
				return nil, err
			}
			stats.LeavesModified++
			stats.EntriesRemoved += removed
			stats.BlobsFreed += len(blobEntries)
			blobWALEntries = append(blobWALEntries, blobEntries...)
		}

		// Release lock and move to next leaf.
		// The next PID was captured under lock, so it's consistent.
		// If the leaf was split concurrently, nextPID points to the new
		// right sibling — we will visit it in the next iteration.
		nextPID := node.Next
		v.pageLocks.WUnlock(leafPID)
		leafPID = nextPID
	}

	// 5. If any leaves were modified, flush WAL batch
	if stats.LeavesModified > 0 {
		if err := v.segSync(); err != nil {
			return nil, err
		}

		// Build WAL batch: page mapping updates + blob free entries
		batch := walapi.NewBatch()

		// Page WAL entries (collected by our per-goroutine collector
		// during WritePage calls above — isolated from concurrent ops)
		for _, e := range *pageEntries {
			batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
		}

		// Blob free WAL entries
		for _, e := range blobWALEntries {
			batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
		}

		if batch.Len() > 0 {
			if _, err := v.wal.WriteBatch(batch); err != nil {
				return nil, err
			}
		}
	}

	return stats, nil
}

// RunIncremental performs an incremental vacuum pass.
// It processes at most targetPages leaf pages.
// If lastLeafPID=0 (start of a new pass), it finds the leftmost leaf first.
// Otherwise it continues from where the last call left off.
func (v *vacuumer) RunIncremental(targetPages int) (*vacuumapi.VacuumStats, error) {
	stats := &vacuumapi.VacuumStats{}

	// 1. Determine safe cleanup boundary
	safeXID := v.txnMgr.GetMinActive()
	if safeXID == math.MaxUint64 {
		safeXID = v.txnMgr.NextXID()
	}

	// 2. Check root
	rootPID := v.rootPageIDFn()
	if rootPID == 0 {
		return nil, vacuumapi.ErrNoLeaves
	}

	// 3. Find starting leaf
	// If passComplete=true, the last pass completed - nothing to do.
	// If lastLeafPID=0, start fresh from leftmost leaf.
	// Otherwise continue from lastLeafPID.
	if v.passComplete {
		// Already completed a full pass, nothing new to do
		return stats, nil
	}
	if v.lastLeafPID == 0 {
		leftmost, err := v.findLeftmostLeaf(rootPID)
		if err != nil {
			return nil, err
		}
		v.lastLeafPID = leftmost
		if v.lastLeafPID == 0 {
			// Empty tree - mark complete
			v.passComplete = true
			return stats, nil
		}
	}

	// 4. Register per-goroutine WAL collector
	pageEntries, unregCollector := v.registerCollector()
	defer unregCollector()

	// Collect blob WAL entries across processed leaves
	var blobWALEntries []blobstoreapi.WALEntry

	// 5. Process up to targetPages leaves
	leafPID := v.lastLeafPID
	pagesProcessed := 0

	for leafPID != 0 && pagesProcessed < targetPages {
		v.pageLocks.WLock(leafPID)

		node, err := v.pages.ReadPage(leafPID)
		if err != nil {
			v.pageLocks.WUnlock(leafPID)
			return nil, err
		}
		if !node.IsLeaf {
			v.pageLocks.WUnlock(leafPID)
			break
		}

		stats.LeavesScanned++

		// Process this leaf
		removed, blobEntries, err := v.processLeaf(node, leafPID, safeXID)
		if err != nil {
			v.pageLocks.WUnlock(leafPID)
			return nil, err
		}

		if removed > 0 {
			if err := v.pages.WritePage(leafPID, node); err != nil {
				v.pageLocks.WUnlock(leafPID)
				return nil, err
			}
			stats.LeavesModified++
			stats.EntriesRemoved += removed
			stats.BlobsFreed += len(blobEntries)
			blobWALEntries = append(blobWALEntries, blobEntries...)
		}

		// Capture next PID under lock, then unlock
		nextPID := node.Next
		v.pageLocks.WUnlock(leafPID)

		leafPID = nextPID
		pagesProcessed++

		// If we reached the end of the tree, reset for next pass
		if leafPID == 0 {
			v.lastLeafPID = 0
			v.passComplete = true
		} else {
			v.lastLeafPID = leafPID
		}
	}

	// 6. If we stopped early (pagesProcessed >= targetPages), save position
	// lastLeafPID is already set above

	// 7. If any leaves were modified, flush WAL batch
	if stats.LeavesModified > 0 {
		if err := v.segSync(); err != nil {
			return nil, err
		}

		batch := walapi.NewBatch()
		for _, e := range *pageEntries {
			batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
		}
		for _, e := range blobWALEntries {
			batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
		}

		if batch.Len() > 0 {
			if _, err := v.wal.WriteBatch(batch); err != nil {
				return nil, err
			}
		}
	}

	return stats, nil
}

// findLeftmostLeaf descends from the given node to the leftmost leaf,
// always following Children[0].
func (v *vacuumer) findLeftmostLeaf(pid uint64) (uint64, error) {
	for {
		v.pageLocks.RLock(pid)
		node, err := v.pages.ReadPage(pid)
		if err != nil {
			v.pageLocks.RUnlock(pid)
			return 0, err
		}
		if node.IsLeaf {
			v.pageLocks.RUnlock(pid)
			return pid, nil
		}
		if len(node.Children) == 0 {
			v.pageLocks.RUnlock(pid)
			return 0, vacuumapi.ErrNoLeaves
		}
		nextPID := node.Children[0]
		v.pageLocks.RUnlock(pid)
		pid = nextPID
	}
}

// processLeaf processes a single leaf node, removing dead entries.
// Returns the number of entries removed and any blob WAL entries generated.
// If entries were removed, the leaf is rewritten via pages.WritePage.
func (v *vacuumer) processLeaf(
	node *btreeapi.Node,
	leafPID uint64,
	safeXID uint64,
) (int, []blobstoreapi.WALEntry, error) {
	clog := v.txnMgr.CLOG()

	// Catalog key prefix - vacuum skips catalog entries to protect metadata
	const catalogPrefix = "_sql:"

	// First pass: identify which entries to remove.
	// NOTE: We collect blob IDs to free but DO NOT free them yet.
	// We free them AFTER rewriting the node to ensure entries are physically
	// removed before their blobs are freed.
	removed := 0
	var blobsToFree []blobstoreapi.BlobID // BlobIDs to free AFTER node rewrite
	keep := make([]bool, len(node.Entries))

	for i := range node.Entries {
		e := &node.Entries[i]

		// SKIP catalog entries - vacuum must not process metadata
		if len(e.Key) >= len(catalogPrefix) && string(e.Key[:len(catalogPrefix)]) == catalogPrefix {
			keep[i] = true
			continue
		}

		// Case 1: Committed delete/overwrite
		if e.TxnMax != math.MaxUint64 &&
			e.TxnMax < safeXID &&
			clog.Get(e.TxnMax) == txnapi.TxnCommitted {
			keep[i] = false
			removed++
			// Collect blob ID to free (will free AFTER node rewrite)
			if e.Value.BlobID > 0 {
				blobsToFree = append(blobsToFree, e.Value.BlobID)
			}
			continue
		}

		// Case 2: Aborted creator
		if clog.Get(e.TxnMin) == txnapi.TxnAborted {
			keep[i] = false
			removed++
			// Collect blob ID to free (will free AFTER node rewrite)
			if e.Value.BlobID > 0 {
				blobsToFree = append(blobsToFree, e.Value.BlobID)
			}
			// Restore previous version's TxnMax if it was overwritten by this aborted txn
			for j := range node.Entries {
				if j == i {
					continue
				}
				prev := &node.Entries[j]
				if bytes.Equal(prev.Key, e.Key) && prev.TxnMax == e.TxnMin {
					prev.TxnMax = math.MaxUint64
					break
				}
			}
			continue
		}

		// Keep this entry
		keep[i] = true
	}

	// Case 3: Duplicate committed versions — safety net for the concurrent Put
	// bug (H4) where two writers could both create entries with TxnMax=∞.
	// For each key with multiple committed TxnMax=∞ entries, keep only the
	// one with the highest TxnMin (most recent writer).
	type bestEntry struct {
		index  int
		txnMin uint64
	}
	bestMap := make(map[string]bestEntry)
	for i := range node.Entries {
		if !keep[i] {
			continue
		}
		e := &node.Entries[i]
		// SKIP catalog entries
		if len(e.Key) >= len(catalogPrefix) && string(e.Key[:len(catalogPrefix)]) == catalogPrefix {
			continue
		}
		if e.TxnMax != math.MaxUint64 {
			continue
		}
		// Only consider committed entries for dedup
		if clog.Get(e.TxnMin) != txnapi.TxnCommitted {
			continue
		}
		keyStr := string(e.Key)
		if prev, ok := bestMap[keyStr]; ok {
			// Keep the one with highest TxnMin (most recent committed version)
			if e.TxnMin > prev.txnMin {
				// Current entry is newer — remove the previous best
				keep[prev.index] = false
				removed++
				// Collect blob ID to free (will free AFTER node rewrite)
				if node.Entries[prev.index].Value.BlobID > 0 {
					blobsToFree = append(blobsToFree, node.Entries[prev.index].Value.BlobID)
				}
				bestMap[keyStr] = bestEntry{index: i, txnMin: e.TxnMin}
			} else {
				// Previous best is newer — remove current entry
				keep[i] = false
				removed++
				// Collect blob ID to free (will free AFTER node rewrite)
				if e.Value.BlobID > 0 {
					blobsToFree = append(blobsToFree, e.Value.BlobID)
				}
			}
		} else {
			bestMap[keyStr] = bestEntry{index: i, txnMin: e.TxnMin}
		}
	}

	if removed == 0 {
		return 0, nil, nil
	}

	// Build new entry list (physically remove dead entries from node)
	newEntries := make([]btreeapi.LeafEntry, 0, len(node.Entries)-removed)
	for i, e := range node.Entries {
		if keep[i] {
			newEntries = append(newEntries, e)
		}
	}
	node.Entries = newEntries
	node.Count = uint16(len(newEntries))

	// NOW free blobs — entries are physically removed from the tree.
	// This ensures no reader can find an entry with a freed blob.
	var blobEntries []blobstoreapi.WALEntry
	for _, blobID := range blobsToFree {
		entry := v.blobStore.Delete(blobID)
		blobEntries = append(blobEntries, entry)
	}

	// Note: caller (Run) handles WritePage under the page lock.
	return removed, blobEntries, nil
}
