package internal

import (
	"os"
	"path/filepath"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// recover loads checkpoint + replays WAL to restore store state.
// Called during Open.
//
// Recovery flow (DESIGN.md §3.6):
//  1. Load checkpoint (if exists) → restore CLOG, nextXID, nextPageID, nextBlobID
//  2. Replay WAL entries from beginning (LSN 0) to restore all page→VAddr mappings
//     via LSM's in-memory state (SSTables rebuilt from disk by RecoveryStore.Build)
//  3. MarkInProgressAsAborted → any in-flight txn at crash is aborted
//  4. Set btree root
func (s *store) recover() error {
	cpPath := filepath.Join(s.dir, "checkpoint")

	// Recovery interfaces for PageStore and BlobStore
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	bsRecovery := s.blobStore.(blobstoreapi.BlobStoreRecovery)
	lsmRecovery := psRecovery.LSMLifecycle()

	var afterLSN uint64
	var rootPageID uint64
	var maxTxnXID uint64

	cpData, err := loadCheckpoint(cpPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// No checkpoint — replay WAL from the beginning (LSN 0).
		afterLSN = 0
		rootPageID = 0
		maxTxnXID = 0
	} else {
		// Checkpoint exists — restore state from it.
		// Page→VAddr mappings: no longer in checkpoint file (LSM handles persistence).
		// CLOG, XID, page IDs, blob IDs, root: still restored from checkpoint.
		psRecovery.SetNextPageID(cpData.NextPageID)
		bsRecovery.LoadMapping(cpData.toBlobMappings())
		bsRecovery.SetNextBlobID(cpData.NextBlobID)
		s.txnMgr.LoadCLOG(cpData.toCLOGEntries())
		s.txnMgr.SetNextXID(cpData.NextXID)
		afterLSN = cpData.LSN
		rootPageID = cpData.RootPageID
		maxTxnXID = cpData.NextXID
		lsmRecovery.SetCheckpointLSN(cpData.LSN)

		// Load GC stats from checkpoint
		s.gcStats.LoadAll(cpData.Stats)

		// For v3 checkpoint: load LSM segment list from checkpoint.
		// The LSM manifest will be initialized with these segments,
		// skipping the rebuild from WAL for pre-checkpoint entries.
		if lsmSegs := cpData.lsmSegments; len(lsmSegs) > 0 {
			psRecovery.SetLSMSegments(lsmSegs)
		}
	}

	// Replay WAL entries. Only records with LSN > checkpoint.LSN are replayed.
	// The checkpoint file already contains the full state snapshot at that LSN,
	// so pre-checkpoint records are redundant. The active WAL segment (which
	// holds post-checkpoint records) is never deleted by Truncate().
	// (LSM entries are ModuleLSM; blob/tree entries are ModuleTree or Type=0.)
	err = s.wal.Replay(afterLSN, func(r walapi.Record) error {

		switch r.ModuleType {
		case walapi.ModuleTree, 0: // ModuleTree or legacy records
			switch r.Type {
			case walapi.RecordPageMap:
				// Old page map record — apply to LSM
				lsmRecovery.ApplyPageMapping(r.ID, r.VAddr)
				s.updateNextPageID(psRecovery, r.ID)
			case walapi.RecordPageFree:
				lsmRecovery.ApplyPageDelete(r.ID)
			case walapi.RecordBlobMap:
				bsRecovery.ApplyBlobMap(r.ID, r.VAddr, r.Size)
				s.updateNextBlobID(bsRecovery, r.ID)
			case walapi.RecordBlobFree:
				bsRecovery.ApplyBlobFree(r.ID)
			case walapi.RecordSetRoot:
				rootPageID = r.ID
			case walapi.RecordTxnCommit:
				s.txnMgr.CLOG().Set(r.ID, txnapi.TxnCommitted)
				if r.ID >= maxTxnXID {
					maxTxnXID = r.ID + 1
				}
			case walapi.RecordTxnAbort:
				s.txnMgr.CLOG().Set(r.ID, txnapi.TxnAborted)
				if r.ID >= maxTxnXID {
					maxTxnXID = r.ID + 1
				}
			}
		case walapi.ModuleLSM:
			// New LSM page/blob mapping records
			switch r.Type {
			case walapi.RecordPageMap:
				lsmRecovery.ApplyPageMapping(r.ID, r.VAddr)
				s.updateNextPageID(psRecovery, r.ID)
			case walapi.RecordPageFree:
				lsmRecovery.ApplyPageDelete(r.ID)
			case walapi.RecordBlobMap:
				lsmRecovery.ApplyBlobMapping(r.ID, r.VAddr, r.Size)
			case walapi.RecordBlobFree:
				lsmRecovery.ApplyBlobDelete(r.ID)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Update nextXID if WAL replay found higher XIDs
	initialNextXID := uint64(0)
	if cpData != nil {
		initialNextXID = cpData.NextXID
	}
	if maxTxnXID > initialNextXID {
		s.txnMgr.SetNextXID(maxTxnXID)
	}

	// Mark in-progress transactions as aborted
	s.txnMgr.MarkInProgressAsAborted()

	// Set btree root (may be 0 if truly fresh start with empty WAL)
	if rootPageID != 0 {
		s.tree.SetRootPageID(rootPageID)
	}

	return nil
}

// updateNextPageID ensures nextPageID > pageID after WAL replay.
func (s *store) updateNextPageID(psRecovery pagestoreapi.PageStoreRecovery, pageID uint64) {
	nextID := s.pageStore.NextPageID()
	if pageID >= nextID {
		psRecovery.SetNextPageID(pageID + 1)
	}
}

// updateNextBlobID ensures nextBlobID > blobID after WAL replay.
func (s *store) updateNextBlobID(bsRecovery blobstoreapi.BlobStoreRecovery, blobID uint64) {
	nextID := s.blobStore.NextBlobID()
	if blobID >= nextID {
		bsRecovery.SetNextBlobID(blobID + 1)
	}
}
