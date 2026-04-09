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
//  1. Load checkpoint (if exists) → restore mappings, CLOG, nextXID, rootPageID
//  2. Replay WAL after checkpoint LSN (or from 0 if no checkpoint)
//  3. MarkInProgressAsAborted → any in-flight txn at crash is aborted
//  4. Set btree root
func (s *store) recover() error {
	cpPath := filepath.Join(s.dir, "checkpoint")

	// Recovery interfaces for PageStore and BlobStore
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	bsRecovery := s.blobStore.(blobstoreapi.BlobStoreRecovery)

	var afterLSN uint64
	var rootPageID uint64
	var maxTxnXID uint64

	cpData, err := loadCheckpoint(cpPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// No checkpoint — replay WAL from the very beginning (LSN 0).
		// afterLSN=0 means Replay will process all records with LSN > 0,
		// which is every record in the WAL.
		afterLSN = 0
		rootPageID = 0
		maxTxnXID = 0
	} else {
		// Checkpoint exists — restore state from it, then replay WAL
		// for records after the checkpoint LSN.
		psRecovery.LoadMapping(cpData.toPageMappings())
		psRecovery.SetNextPageID(cpData.NextPageID)

		bsRecovery.LoadMapping(cpData.toBlobMappings())
		bsRecovery.SetNextBlobID(cpData.NextBlobID)

		s.txnMgr.LoadCLOG(cpData.toCLOGEntries())
		s.txnMgr.SetNextXID(cpData.NextXID)

		afterLSN = cpData.LSN
		rootPageID = cpData.RootPageID
		maxTxnXID = cpData.NextXID
	}

	// Replay WAL after the checkpoint LSN (or from 0 if no checkpoint).
	err = s.wal.Replay(afterLSN, func(r walapi.Record) error {
		switch r.Type {
		case walapi.RecordPageMap:
			psRecovery.ApplyPageMap(r.ID, r.VAddr)
			s.updateNextPageID(psRecovery, r.ID)
		case walapi.RecordPageFree:
			psRecovery.ApplyPageFree(r.ID)
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
