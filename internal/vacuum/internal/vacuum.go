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
	rootPageIDFn func() uint64
	pages        btreeapi.PageProvider
	txnMgr       txnapi.TxnManager
	blobStore    blobstoreapi.BlobStore
	wal          walapi.WAL
	segSync      func() error
	drainPageWAL func() []pagestoreapi.WALEntry
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
//   - drainPageWAL: function to drain page WAL entries (e.g. provider.DrainWALEntries)
func New(
	rootPageIDFn func() uint64,
	pages btreeapi.PageProvider,
	txnMgr txnapi.TxnManager,
	blobStore blobstoreapi.BlobStore,
	wal walapi.WAL,
	segSync func() error,
	drainPageWAL func() []pagestoreapi.WALEntry,
) vacuumapi.Vacuum {
	return &vacuumer{
		rootPageIDFn: rootPageIDFn,
		pages:        pages,
		txnMgr:       txnMgr,
		blobStore:    blobStore,
		wal:          wal,
		segSync:      segSync,
		drainPageWAL: drainPageWAL,
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

	// Drain any stale page WAL entries before starting
	v.drainPageWAL()

	// Collect blob WAL entries across all leaves
	var blobWALEntries []blobstoreapi.WALEntry

	// 4. Iterate all leaves via Next links
	for leafPID != 0 {
		node, err := v.pages.ReadPage(leafPID)
		if err != nil {
			return nil, err
		}
		if !node.IsLeaf {
			// Safety: should not happen if tree is well-formed
			break
		}

		stats.LeavesScanned++

		// Process this leaf
		removed, blobEntries, err := v.processLeaf(node, leafPID, safeXID)
		if err != nil {
			return nil, err
		}

		if removed > 0 {
			stats.LeavesModified++
			stats.EntriesRemoved += removed
			stats.BlobsFreed += len(blobEntries)
			blobWALEntries = append(blobWALEntries, blobEntries...)
		}

		// Move to next leaf
		leafPID = node.Next
	}

	// 5. If any leaves were modified, flush WAL batch
	if stats.LeavesModified > 0 {
		if err := v.segSync(); err != nil {
			return nil, err
		}

		// Build WAL batch: page mapping updates + blob free entries
		batch := walapi.NewBatch()

		// Page WAL entries (collected by RealPageProvider during WritePage calls)
		for _, e := range v.drainPageWAL() {
			batch.Add(walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
		}

		// Blob free WAL entries
		for _, e := range blobWALEntries {
			batch.Add(walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
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
		node, err := v.pages.ReadPage(pid)
		if err != nil {
			return 0, err
		}
		if node.IsLeaf {
			return pid, nil
		}
		if len(node.Children) == 0 {
			return 0, vacuumapi.ErrNoLeaves
		}
		pid = node.Children[0]
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

	// First pass: identify which entries to remove and handle aborted-creator restoration
	removed := 0
	var blobEntries []blobstoreapi.WALEntry
	keep := make([]bool, len(node.Entries))

	for i := range node.Entries {
		e := &node.Entries[i]

		// Case 1: Committed delete/overwrite
		if e.TxnMax != math.MaxUint64 &&
			e.TxnMax < safeXID &&
			clog.Get(e.TxnMax) == txnapi.TxnCommitted {
			keep[i] = false
			removed++
			// Free blob if any
			if e.Value.BlobID > 0 {
				entry := v.blobStore.Delete(e.Value.BlobID)
				blobEntries = append(blobEntries, entry)
			}
			continue
		}

		// Case 2: Aborted creator
		if clog.Get(e.TxnMin) == txnapi.TxnAborted {
			keep[i] = false
			removed++
			// Free blob if any
			if e.Value.BlobID > 0 {
				entry := v.blobStore.Delete(e.Value.BlobID)
				blobEntries = append(blobEntries, entry)
			}
			// Restore previous version's TxnMax if it was overwritten by this aborted txn
			// Look for a previous version with same key where TxnMax == e.TxnMin
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

	if removed == 0 {
		return 0, nil, nil
	}

	// Build new entry list
	newEntries := make([]btreeapi.LeafEntry, 0, len(node.Entries)-removed)
	for i, e := range node.Entries {
		if keep[i] {
			newEntries = append(newEntries, e)
		}
	}
	node.Entries = newEntries
	node.Count = uint16(len(newEntries))

	// Rewrite the leaf page
	if err := v.pages.WritePage(leafPID, node); err != nil {
		return 0, nil, err
	}

	return removed, blobEntries, nil
}
