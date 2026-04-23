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
	"golang.org/x/sys/unix"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrNoSegmentsToGC is returned when there are no sealed segments
	// eligible for garbage collection.
	ErrNoSegmentsToGC = errors.New("gc: no sealed segments to collect")
	// ErrInsufficientDiskSpace is returned when there is not enough disk space
	// to safely perform GC (needs at least 2x the size of live data).
	ErrInsufficientDiskSpace = errors.New("gc: insufficient disk space to run GC")
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

	// CollectOneCompact collects a sealed segment containing variable-length
	// compact page records (written by WriteCompact).
	//
	// Record format: [PageID:8][DataLen:2][Data:N][CRC32:4] = N+14 bytes.
	// Scans by reading 10-byte headers to determine record sizes.
	// Uses PackPageVAddr (20:30:14 encoding) for liveness checks.
	//
	// Returns ErrNoSegmentsToGC if no sealed segments exist.
	CollectOneCompact() (*GCStats, error)
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

// getAvailableDiskSpace returns the number of available bytes on the filesystem
// containing the given directory.
func getAvailableDiskSpace(dir string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

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
	type liveRecord struct {
		pageID uint64
		addr   segmentapi.VAddr
		record []byte
	}
	var liveRecords []liveRecord
	var offset uint32
	recordSize := uint32(pagestoreapi.PageRecordSize)

	// First pass: identify all live records and calculate total live size
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
			liveRecords = append(liveRecords, liveRecord{
				pageID: pageID,
				addr:   addr,
				record: record,
			})
			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(recordSize)
		}
		offset += recordSize
	}

	// Disk space pre-check: need at least 2x the size of live data
	totalLiveSize := uint64(len(liveRecords)) * uint64(recordSize)
	available, err := getAvailableDiskSpace(gc.segMgr.StorageDir())
	if err != nil {
		return nil, err
	}
	if available < 2*totalLiveSize {
		return nil, ErrInsufficientDiskSpace
	}

	// Second pass: copy live records to active segment
	type mappingUpdate struct {
		pageID  uint64
		oldVAddr uint64
		newVAddr uint64
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()
	for _, lr := range liveRecords {
		oldVAddr := lr.addr.Pack()
		// Re-check liveness (GetPageMapping acquires internal read lock automatically, thread-safe with concurrent writes)
		mappedVAddr, ok := gc.recovery.LSMLifecycle().GetPageMapping(lr.pageID)
		if !ok || mappedVAddr != oldVAddr {
			// Record was modified/deleted during first pass, skip
			continue
		}
		// Copy the record
		newAddr, err := gc.segMgr.Append(lr.record)
		if err != nil {
			return nil, err
		}

		newPacked := newAddr.Pack()
		batch.Add(walapi.ModuleTree, walapi.RecordPageMap, lr.pageID, newPacked, 0)
		updates = append(updates, mappingUpdate{pageID: lr.pageID, oldVAddr: oldVAddr, newVAddr: newPacked})
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
	// Apply mapping updates to in-memory state with CAS atomic update
	for _, u := range updates {
		// Use CAS atomic update: only set if current mapping still equals oldVAddr
		// If CAS fails, the page was modified by concurrent user writes, skip update
		gc.recovery.LSMLifecycle().CompareAndSetPageMapping(u.pageID, u.oldVAddr, u.newVAddr)
	}
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}
	return stats, nil
}

// CollectOneCompact collects a sealed segment containing variable-length
// compact page records (written by WriteCompact).
//
// Record format: [PageID:8][DataLen:2][Data:N][CRC32:4] = N+14 bytes.
// Scans by reading 10-byte headers to determine record sizes.
func (gc *gcPageGC) CollectOneCompact() (*GCStats, error) {
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
	type liveRecord struct {
		pageID uint64
		packed uint64
		record []byte
	}
	var liveRecords []liveRecord
	var offset uint32
	headerBuf := make([]byte, pagestoreapi.PageRecordHeaderSize) // 10 bytes

	// First pass: identify all live records
	for int64(offset)+int64(pagestoreapi.PageRecordHeaderSize) <= segSize {
		headerAddr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
		if err := gc.segMgr.ReadAtInto(headerAddr, headerBuf); err != nil {
			return nil, err
		}

		pageID := binary.BigEndian.Uint64(headerBuf[:8])
		dataLen := int(binary.BigEndian.Uint16(headerBuf[8:10]))
		recordSize := uint32(pagestoreapi.PageRecordOverhead + dataLen)

		if int64(offset)+int64(recordSize) > segSize {
			break
		}

		record, err := gc.segMgr.ReadAt(headerAddr, recordSize)
		if err != nil {
			return nil, err
		}

		stats.TotalRecords++
		oldPacked := segmentapi.PackPageVAddr(segID, offset, uint16(recordSize))

		if mappedVAddr, ok := gc.recovery.LSMLifecycle().GetPageMapping(pageID); ok && mappedVAddr == oldPacked {
			liveRecords = append(liveRecords, liveRecord{
				pageID: pageID,
				packed: oldPacked,
				record: record,
			})
			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(recordSize)
		}
		offset += recordSize
	}

	// Disk space pre-check
	var totalLiveSize uint64
	for _, lr := range liveRecords {
		totalLiveSize += uint64(len(lr.record))
	}
	available, err := getAvailableDiskSpace(gc.segMgr.StorageDir())
	if err != nil {
		return nil, err
	}
	if available < 2*totalLiveSize {
		return nil, ErrInsufficientDiskSpace
	}

	// Second pass: copy live records
	type mappingUpdate struct {
		pageID   uint64
		oldVAddr uint64
		newVAddr uint64
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()

	for _, lr := range liveRecords {
		mappedVAddr, ok := gc.recovery.LSMLifecycle().GetPageMapping(lr.pageID)
		if !ok || mappedVAddr != lr.packed {
			continue
		}
		newAddr, err := gc.segMgr.Append(lr.record)
		if err != nil {
			return nil, err
		}
		newPacked := segmentapi.PackPageVAddr(newAddr.SegmentID, newAddr.Offset, uint16(len(lr.record)))
		batch.Add(walapi.ModuleTree, walapi.RecordPageMap, lr.pageID, newPacked, 0)
		updates = append(updates, mappingUpdate{pageID: lr.pageID, oldVAddr: lr.packed, newVAddr: newPacked})
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
		gc.recovery.LSMLifecycle().CompareAndSetPageMapping(u.pageID, u.oldVAddr, u.newVAddr)
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
	type liveRecord struct {
		blobID         uint64
		addr           segmentapi.VAddr
		fullRecord     []byte
		fullRecordSize uint32
		dataSize       uint32
	}
	var liveRecords []liveRecord
	var offset uint32

	// First pass: identify all live records and calculate total live size
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
			liveRecords = append(liveRecords, liveRecord{
				blobID:         blobID,
				addr:           headerAddr,
				fullRecord:     fullRecord,
				fullRecordSize: fullRecordSize,
				dataSize:       dataSize,
			})
			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(fullRecordSize)
		}
		offset += fullRecordSize
	}

	// Disk space pre-check: need at least 2x the size of live data
	var totalLiveSize uint64
	for _, lr := range liveRecords {
		totalLiveSize += uint64(lr.fullRecordSize)
	}
	available, err := getAvailableDiskSpace(gc.segMgr.StorageDir())
	if err != nil {
		return nil, err
	}
	if available < 2*totalLiveSize {
		return nil, ErrInsufficientDiskSpace
	}

	// Second pass: copy live records to active segment
	type mappingUpdate struct {
		blobID    uint64
		oldVAddr  uint64
		newVAddr  uint64
		size      uint32
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()
	for _, lr := range liveRecords {
		oldVAddr := lr.addr.Pack()
		// Re-check liveness using blobstore Read (acquires internal blobstore lock automatically, thread-safe with concurrent writes)
		_, err := gc.bs.Read(blobstoreapi.BlobID(lr.blobID))
		if err != nil {
			// Record was deleted during first pass, skip
			continue
		}
		// Copy the record
		newAddr, err := gc.segMgr.Append(lr.fullRecord)
		if err != nil {
			return nil, err
		}

		newPacked := newAddr.Pack()
		batch.Add(walapi.ModuleBlob, walapi.RecordBlobMap, lr.blobID, newPacked, lr.dataSize)
		updates = append(updates, mappingUpdate{blobID: lr.blobID, oldVAddr: oldVAddr, newVAddr: newPacked, size: lr.dataSize})
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
	// Apply mapping updates to in-memory state with CAS atomic update
	for _, u := range updates {
		// Use CAS atomic update: only set if current mapping still equals oldVAddr and size
		// If CAS fails, the blob was modified by concurrent user writes, skip update
		gc.bs.CompareAndSetBlobMapping(u.blobID, u.oldVAddr, u.size, u.newVAddr, u.size)
	}
	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}
	return stats, nil
}
