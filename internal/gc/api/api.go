// Package gcapi defines the interface for the garbage collector (GC).
//
// GC reclaims space in sealed segment files by copying live data to the
// active segment and removing the old segment file. There are two
// independent GC processes: one for page segments and one for blob segments.
//
// GC operates at the physical storage layer — it does NOT touch B-tree
// entries (that's Vacuum's job). GC only cares about whether a VAddr
// in a segment is still referenced by the mapping table.
//
// Design reference: docs/DESIGN.md §3.7
package gcapi

import (
	"encoding/binary"
	"errors"
	"fmt"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrNoSegmentsToGC is returned when there are no sealed segments
	// eligible for garbage collection.
	ErrNoSegmentsToGC = errors.New("gc: no sealed segments to collect")
)

// ─── Stats ──────────────────────────────────────────────────────────

// GCStats reports the results of a GC run.
type GCStats struct {
	SegmentID    uint32 // which segment was collected
	TotalRecords int    // total records scanned in the segment
	LiveRecords  int    // records still alive (copied to new segment)
	DeadRecords  int    // records no longer referenced (skipped)
	BytesFreed   int64  // approximate bytes freed
}

// ─── Interface ──────────────────────────────────────────────────────

// PageGC collects garbage from page segments.
//
// It scans a sealed page segment, checks each page's liveness against
// the PageStore mapping table, copies live pages to the active segment,
// updates the mapping, and removes the old segment.
//
// Thread safety: PageGC must be safe for concurrent use with PageStore
// reads/writes. The mapping updates must go through WAL for crash safety.
//
// Design reference: docs/DESIGN.md §3.7 (Page GC)
type PageGC interface {
	// CollectOne selects the best candidate sealed page segment and
	// collects it. Returns stats about what was collected.
	//
	// "Best candidate" = the sealed segment with the highest estimated
	// dead record ratio. A simple heuristic: pick any sealed segment.
	//
	// Returns ErrNoSegmentsToGC if no sealed segments exist.
	CollectOne() (*GCStats, error)
}

// BlobGC collects garbage from blob segments.
//
// Similar to PageGC but handles variable-length blob records.
//
// Design reference: docs/DESIGN.md §3.7 (Blob GC)
type BlobGC interface {
	// CollectOne selects the best candidate sealed blob segment and
	// collects it. Returns stats about what was collected.
	//
	// Returns ErrNoSegmentsToGC if no sealed segments exist.
	CollectOne() (*GCStats, error)
}

// ─── Constructors ────────────────────────────────────────────────────

// NewPageGC creates a new Page GC instance.
func NewPageGC(
	pageSegMgr segmentapi.SegmentManager,
	pageStore pagestoreapi.PageStore,
	pageStoreRecovery pagestoreapi.PageStoreRecovery,
	wal walapi.WAL,
) PageGC {
	return &gcPageGC{
		segMgr:   pageSegMgr,
		ps:       pageStore,
		recovery: pageStoreRecovery,
		wal:      wal,
	}
}

// NewBlobGC creates a new Blob GC instance.
func NewBlobGC(
	blobSegMgr segmentapi.SegmentManager,
	blobStore blobstoreapi.BlobStore,
	blobStoreRecovery blobstoreapi.BlobStoreRecovery,
	wal walapi.WAL,
) BlobGC {
	return &gcBlobGC{
		segMgr:   blobSegMgr,
		bs:       blobStore,
		recovery: blobStoreRecovery,
		wal:      wal,
	}
}

// ─── Page GC Implementation ──────────────────────────────────────────

// Compile-time interface check.
var _ PageGC = (*gcPageGC)(nil)

// gcPageGC implements PageGC.
type gcPageGC struct {
	segMgr   segmentapi.SegmentManager
	ps       pagestoreapi.PageStore
	recovery pagestoreapi.PageStoreRecovery
	wal      walapi.WAL
}

// CollectOne selects the first sealed page segment and collects it.
func (gc *gcPageGC) CollectOne() (*GCStats, error) {
	sealed := gc.segMgr.SealedSegments()
	if len(sealed) == 0 {
		return nil, ErrNoSegmentsToGC
	}
	segID := sealed[0]

	segSize, err := gc.segMgr.SegmentSize(segID)
	if err != nil {
		return nil, err
	}

	stats := &GCStats{SegmentID: segID}
	type mappingUpdate struct {
		pageID uint64
		vaddr  uint64
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()
	var offset uint32
	recordSize := uint32(pagestoreapi.PageRecordSize)

	for int64(offset)+int64(recordSize) <= segSize {
		addr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
		record, err := gc.segMgr.ReadAt(addr, recordSize)
		if err != nil {
			return nil, err
		}
		stats.TotalRecords++
		pageID := binary.BigEndian.Uint64(record[:8])
		currentVAddr := addr.Pack()
		if mappedVAddr, ok := gc.recovery.LSMLifecycle().GetPageMapping(pageID); ok && mappedVAddr == currentVAddr {
			newAddr, err := gc.segMgr.Append(record)
			if err != nil {
				return nil, err
			}
			newPacked := newAddr.Pack()
			batch.Add(walapi.ModuleTree, walapi.RecordPageMap, pageID, newPacked, 0)
			updates = append(updates, mappingUpdate{pageID: pageID, vaddr: newPacked})
			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(recordSize)
		}
		offset += recordSize
	}

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
		gc.recovery.ApplyPageMap(u.pageID, u.vaddr)
	}
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}
	return stats, nil
}

// ─── Blob GC Implementation ──────────────────────────────────────────

// Compile-time interface check.
var _ BlobGC = (*gcBlobGC)(nil)

// gcBlobGC implements BlobGC.
type gcBlobGC struct {
	segMgr   segmentapi.SegmentManager
	bs       blobstoreapi.BlobStore
	recovery blobstoreapi.BlobStoreRecovery
	wal      walapi.WAL
}

const gcBlobHeaderSize = 12
const gcBlobChecksumSize = 4

// CollectOne selects the first sealed blob segment and collects it.
func (gc *gcBlobGC) CollectOne() (*GCStats, error) {
	sealed := gc.segMgr.SealedSegments()
	if len(sealed) == 0 {
		return nil, ErrNoSegmentsToGC
	}
	segID := sealed[0]

	segSize, err := gc.segMgr.SegmentSize(segID)
	if err != nil {
		return nil, err
	}

	stats := &GCStats{SegmentID: segID}
	type mappingUpdate struct {
		blobID uint64
		vaddr  uint64
		size   uint32
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()
	var offset uint32

	for int64(offset)+int64(gcBlobHeaderSize) <= segSize {
		headerAddr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
		header, err := gc.segMgr.ReadAt(headerAddr, gcBlobHeaderSize)
		if err != nil {
			return nil, err
		}
		blobID := binary.BigEndian.Uint64(header[:8])
		dataSize := binary.BigEndian.Uint32(header[8:12])
		fullRecordSize := uint32(gcBlobHeaderSize) + dataSize + gcBlobChecksumSize
		if int64(offset)+int64(fullRecordSize) > segSize {
			return nil, fmt.Errorf("gc: blob record at offset %d extends beyond segment", offset)
		}
		stats.TotalRecords++
		if _, err := gc.bs.Read(blobstoreapi.BlobID(blobID)); err == nil {
			fullRecord, err := gc.segMgr.ReadAt(headerAddr, fullRecordSize)
			if err != nil {
				return nil, err
			}
			newAddr, err := gc.segMgr.Append(fullRecord)
			if err != nil {
				return nil, err
			}
			newPacked := newAddr.Pack()
			batch.Add(walapi.ModuleBlob, walapi.RecordBlobMap, blobID, newPacked, dataSize)
			updates = append(updates, mappingUpdate{blobID: blobID, vaddr: newPacked, size: dataSize})
			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(fullRecordSize)
		}
		offset += fullRecordSize
	}

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
		gc.recovery.ApplyBlobMap(u.blobID, u.vaddr, u.size)
	}
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}
	return stats, nil
}
