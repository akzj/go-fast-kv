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
//  2. Get segment size; iterate through all 4108-byte page records.
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

	// 4. Scan all page records in the segment.
	stats := &gcapi.GCStats{
		SegmentID: segID,
	}

	// Collect WAL entries and mapping updates for live pages.
	type mappingUpdate struct {
		pageID   uint64
		oldVAddr uint64 // VAddr in the old (being-collected) segment
		newVAddr uint64 // VAddr in the new (active) segment
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()

	var offset uint32
	recordSize := uint32(pagestoreapi.PageRecordSize) // 4108

	for int64(offset)+int64(recordSize) <= segSize {
		// Read the full 4108-byte record.
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
		oldVAddr := addr.Pack()
		if mappedVAddr, ok := gc.recovery.LSMLifecycle().GetPageMapping(pageID); ok && mappedVAddr == oldVAddr {
			// Live — copy to active segment.
			newAddr, err := gc.segMgr.Append(record)
			if err != nil {
				return nil, err
			}

			newPacked := newAddr.Pack()
			batch.Add(walapi.ModuleTree, walapi.RecordPageMap, pageID, newPacked, 0)
			updates = append(updates, mappingUpdate{pageID: pageID, oldVAddr: oldVAddr, newVAddr: newPacked})

			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(recordSize)
		}

		offset += recordSize
	}

	// 5. Sync segment data, write WAL batch, apply CAS mapping updates, remove old segment.
	// First Sync: flush the old sealed segment being collected.
	if err := gc.segMgr.Sync(); err != nil {
		return nil, err
	}
	// Second Sync: flush the active segment with newly copied live data
	// BEFORE writing WAL record that references it. Without this, a crash
	// after WAL write but before the next segment sync would lose the data
	// referenced by the WAL record (silent data corruption).
	if err := gc.segMgr.Sync(); err != nil {
		return nil, err
	}

	if batch.Len() > 0 {
		if _, err := gc.wal.WriteBatch(batch); err != nil {
			return nil, err
		}
	}

	// Apply mapping updates using CAS to prevent race with concurrent writes.
	// If CAS fails (mapping was concurrently updated), the page data in the old
	// segment is now stale — it will be handled in the next GC cycle.
	// This prevents blind SetPageMapping from silently overwriting user data.
	for _, u := range updates {
		// Use CompareAndSetPageMapping: only update if current value still equals oldVAddr.
		// This prevents the race between GC's mapping check and the update.
		if gc.recovery.LSMLifecycle().CompareAndSetPageMapping(u.pageID, u.oldVAddr, u.newVAddr) {
			// CAS succeeded — invalidate any cached stale entry for this page.
			gc.ps.InvalidatePage(u.pageID)
		}
		// If CAS fails: concurrent write updated the mapping after our scan.
		// The old segment's data for this page is now stale — skip update.
		// GC will clean up the stale data in the next cycle.
	}

	// Check CanDelete before removing the segment.
	// This prevents GC from deleting SSTables that are pinned by an in-flight checkpoint.
	// Only pre-snapshot SSTables have refcount > 0 and are blocked.
	// New SSTables created after checkpoint snapshot have refcount == 0 and can be deleted.
	// Skip check if manifest is nil (e.g., in tests with mock LSM).
	manifest := gc.recovery.LSMLifecycle().Manifest()
	if manifest != nil {
		// Use TryDelete to atomically check refcount AND delete.
		// This prevents the race: GC checks CanDelete (refcount=0) → checkpoint pins → GC deletes.
		// TryDelete holds write lock during check+delete, blocking concurrent Pin/Unpin.
		if !manifest.TryDelete(gc.segMgr, segID) {
			// Segment is pinned by checkpoint or not found - skip deletion, GC will retry later.
			return stats, nil
		}
		return stats, nil
	}

	// Manifest is nil - fall back to direct removal (for tests with mock LSM).
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}

	return stats, nil
}

// CollectOneCompact collects a sealed segment containing variable-length
// compact page records (written by WriteCompact).
//
// Record format: [PageID:8][DataLen:2][Data:N][CRC32:4] = N+14 bytes.
// The scan reads the 10-byte header first to determine record size.
//
// Packed VAddr uses the 20:30:14 encoding (SegmentID:Offset:RecordLen).
func (gc *pageGC) CollectOneCompact() (*gcapi.GCStats, error) {
	sealed := gc.segMgr.SealedSegments()
	if len(sealed) == 0 {
		return nil, gcapi.ErrNoSegmentsToGC
	}
	segID := sealed[0]

	segSize, err := gc.segMgr.SegmentSize(segID)
	if err != nil {
		return nil, err
	}

	stats := &gcapi.GCStats{SegmentID: segID}

	type mappingUpdate struct {
		pageID   uint64
		oldVAddr uint64
		newVAddr uint64
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()

	var offset uint32
	headerBuf := make([]byte, pagestoreapi.PageRecordHeaderSize) // 10 bytes

	for int64(offset)+int64(pagestoreapi.PageRecordHeaderSize) <= segSize {
		// 1. Read 10-byte header: [PageID:8][DataLen:2]
		headerAddr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
		if err := gc.segMgr.ReadAtInto(headerAddr, headerBuf); err != nil {
			return nil, err
		}

		pageID := binary.BigEndian.Uint64(headerBuf[:8])
		dataLen := int(binary.BigEndian.Uint16(headerBuf[8:10]))
		recordSize := uint32(pagestoreapi.PageRecordOverhead + dataLen) // 14 + N

		// Validate record fits in segment.
		if int64(offset)+int64(recordSize) > segSize {
			break // Truncated record at end of segment
		}

		// 2. Read full record.
		record, err := gc.segMgr.ReadAt(headerAddr, recordSize)
		if err != nil {
			return nil, err
		}

		stats.TotalRecords++

		// 3. Construct packed VAddr for liveness check.
		oldPacked := segmentapi.PackPageVAddr(segID, offset, uint16(recordSize))

		// Check liveness against LSM mapping.
		if mappedVAddr, ok := gc.recovery.LSMLifecycle().GetPageMapping(pageID); ok && mappedVAddr == oldPacked {
			// Live — copy to active segment.
			newAddr, err := gc.segMgr.Append(record)
			if err != nil {
				return nil, err
			}

			newPacked := segmentapi.PackPageVAddr(newAddr.SegmentID, newAddr.Offset, uint16(recordSize))
			batch.Add(walapi.ModuleTree, walapi.RecordPageMap, pageID, newPacked, 0)
			updates = append(updates, mappingUpdate{pageID: pageID, oldVAddr: oldPacked, newVAddr: newPacked})

			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(recordSize)
		}

		offset += recordSize
	}

	// Sync + WAL + CAS mapping updates.
	if err := gc.segMgr.Sync(); err != nil {
		return nil, err
	}
	if err := gc.segMgr.Sync(); err != nil {
		return nil, err
	}
	if batch.Len() > 0 {
		if _, err := gc.wal.WriteBatch(batch); err != nil {
			return nil, err
		}
	}
	for _, u := range updates {
		if gc.recovery.LSMLifecycle().CompareAndSetPageMapping(u.pageID, u.oldVAddr, u.newVAddr) {
			gc.ps.InvalidatePage(u.pageID)
		}
	}

	// Remove old segment.
	manifest := gc.recovery.LSMLifecycle().Manifest()
	if manifest != nil {
		if !manifest.TryDelete(gc.segMgr, segID) {
			return stats, nil
		}
		return stats, nil
	}
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}
	return stats, nil
}
