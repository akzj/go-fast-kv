package kvstore

import (
	"os"
	"path/filepath"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// recover loads checkpoint + replays WAL to restore store state.
// Called during Open. If no checkpoint exists, this is a fresh start (no-op).
//
// Recovery flow (DESIGN.md §3.6):
//  1. Load checkpoint → restore mappings, CLOG, nextXID, rootPageID
//  2. Replay WAL after checkpoint LSN → apply incremental changes
//  3. MarkInProgressAsAborted → any in-flight txn at crash is aborted
//  4. Set btree root
func (s *store) recover() error {
	cpPath := filepath.Join(s.dir, "checkpoint")

	cpData, err := loadCheckpoint(cpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh start — nothing to recover
		}
		return err
	}

	// 1. Restore PageStore mapping
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	psRecovery.LoadMapping(cpData.toPageMappings())
	psRecovery.SetNextPageID(cpData.NextPageID)

	// 2. Restore BlobStore mapping
	bsRecovery := s.blobStore.(blobstoreapi.BlobStoreRecovery)
	bsRecovery.LoadMapping(cpData.toBlobMappings())
	bsRecovery.SetNextBlobID(cpData.NextBlobID)

	// 3. Restore CLOG + nextXID
	s.txnMgr.LoadCLOG(cpData.toCLOGEntries())
	s.txnMgr.SetNextXID(cpData.NextXID)

	// 4. Restore root PageID
	rootPageID := cpData.RootPageID

	// 5. Replay WAL after checkpoint LSN
	maxTxnXID := cpData.NextXID
	err = s.wal.Replay(cpData.LSN, func(r walapi.Record) error {
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
	if maxTxnXID > cpData.NextXID {
		s.txnMgr.SetNextXID(maxTxnXID)
	}

	// 6. Mark in-progress transactions as aborted
	s.txnMgr.MarkInProgressAsAborted()

	// 7. Set btree root
	s.tree.SetRootPageID(rootPageID)

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
