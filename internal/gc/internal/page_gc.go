// Package gc implements garbage collection for page and blob segments.
//
// GC reclaims space in sealed segment files by copying live data to the
// active segment and removing the old segment file. It operates at the
// physical storage layer — it does NOT touch B-tree entries.
//
// Design reference: docs/DESIGN.md §3.7
package internal

import (
	"encoding/binary"

	gcapi "github.com/akzj/go-fast-kv/internal/gc/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// Compile-time interface check.
var _ gcapi.PageGC = (*pageGC)(nil)

// pageGC implements gcapi.PageGC.
type pageGC struct {
	segMgr   segmentapi.SegmentManager
	ps       pagestoreapi.PageStore
	recovery pagestoreapi.PageStoreRecovery
	wal      walapi.WAL
}

// NewPageGC creates a new Page GC instance.
//
// Parameters:
//   - pageSegMgr: the SegmentManager used by PageStore (page segments)
//   - pageStore: the PageStore (for reading current mapping)
//   - pageStoreRecovery: the PageStoreRecovery (for updating mapping after copy)
//   - wal: the shared WAL (for durably recording mapping updates)
func NewPageGC(
	pageSegMgr segmentapi.SegmentManager,
	pageStore pagestoreapi.PageStore,
	pageStoreRecovery pagestoreapi.PageStoreRecovery,
	wal walapi.WAL,
) gcapi.PageGC {
	return &pageGC{
		segMgr:   pageSegMgr,
		ps:       pageStore,
		recovery: pageStoreRecovery,
		wal:      wal,
	}
}

// CollectOne selects the first sealed page segment and collects it.
//
// Algorithm (per DESIGN.md §3.7):
//  1. Get sealed segments; pick the first one (simplest heuristic).
//  2. Get segment size; iterate through all 4104-byte page records.
//  3. For each record, extract pageID and check liveness against the
//     current mapping table.
//  4. Live pages are re-appended to the active segment; dead pages skipped.
//  5. After all live data is copied: sync, write WAL batch, update
//     in-memory mapping, remove old segment.
func (gc *pageGC) CollectOne() (*gcapi.GCStats, error) {
	// 1. Find a sealed segment to collect.
	sealed := gc.segMgr.SealedSegments()
	if len(sealed) == 0 {
		return nil, gcapi.ErrNoSegmentsToGC
	}
	segID := sealed[0] // simplest heuristic: pick the oldest

	// 2. Get segment size.
	segSize, err := gc.segMgr.SegmentSize(segID)
	if err != nil {
		return nil, err
	}

	// 3. Build liveness map from current PageStore mapping.
	//    map[pageID] → packed VAddr
	entries := gc.recovery.ExportMapping()
	liveMap := make(map[uint64]uint64, len(entries))
	for _, e := range entries {
		liveMap[e.PageID] = e.VAddr
	}

	// 4. Scan all page records in the segment.
	stats := &gcapi.GCStats{
		SegmentID: segID,
	}

	// Collect WAL entries and mapping updates for live pages.
	type mappingUpdate struct {
		pageID uint64
		vaddr  uint64
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()

	var offset uint32
	recordSize := uint32(pagestoreapi.PageRecordSize) // 4104

	for int64(offset)+int64(recordSize) <= segSize {
		// Read the full 4104-byte record.
		addr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
		record, err := gc.segMgr.ReadAt(addr, recordSize)
		if err != nil {
			return nil, err
		}

		stats.TotalRecords++

		// Extract pageID from first 8 bytes (big-endian).
		pageID := binary.BigEndian.Uint64(record[:8])

		// Check liveness: is the current mapping for this pageID
		// pointing to this exact VAddr?
		currentVAddr := addr.Pack()
		if mappedVAddr, ok := liveMap[pageID]; ok && mappedVAddr == currentVAddr {
			// Live — copy to active segment.
			newAddr, err := gc.segMgr.Append(record)
			if err != nil {
				return nil, err
			}

			newPacked := newAddr.Pack()
			batch.Add(walapi.RecordPageMap, pageID, newPacked, 0)
			updates = append(updates, mappingUpdate{pageID: pageID, vaddr: newPacked})

			// Update liveMap so subsequent records for the same pageID
			// in this segment won't also be considered live.
			liveMap[pageID] = newPacked

			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(recordSize)
		}

		offset += recordSize
	}

	// 5. Sync segment data, write WAL batch, apply mapping updates, remove old segment.
	if err := gc.segMgr.Sync(); err != nil {
		return nil, err
	}

	if batch.Len() > 0 {
		if _, err := gc.wal.WriteBatch(batch); err != nil {
			return nil, err
		}
	}

	// Apply mapping updates to in-memory state.
	for _, u := range updates {
		gc.recovery.ApplyPageMap(u.pageID, u.vaddr)
	}

	// Remove the old segment.
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}

	return stats, nil
}
