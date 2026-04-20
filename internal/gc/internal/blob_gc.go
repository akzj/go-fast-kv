package internal

import (
	"encoding/binary"
	"fmt"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	gcapi "github.com/akzj/go-fast-kv/internal/gc/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
	"golang.org/x/sys/unix"
)

// Compile-time interface check.
var _ gcapi.BlobGC = (*blobGC)(nil)

// blobHeaderSize is the size of the header prepended to each blob in a segment:
// 8 bytes blobID (big-endian) + 4 bytes size (big-endian) = 12 bytes.
const blobHeaderSize = 12

// blobChecksumSize is the size of the CRC32 checksum appended to each blob record.
const blobChecksumSize = 4

// blobGC implements gcapi.BlobGC.
type blobGC struct {
	segMgr   segmentapi.SegmentManager
	bs       blobstoreapi.BlobStore
	recovery blobstoreapi.BlobStoreRecovery
	wal      walapi.WAL
}

// NewBlobGC creates a new Blob GC instance.
//
// Parameters:
//   - blobSegMgr: the SegmentManager used by BlobStore (blob segments)
//   - blobStore: the BlobStore (for reading current mapping)
//   - blobStoreRecovery: the BlobStoreRecovery (for updating mapping after copy)
//   - wal: the shared WAL (for durably recording mapping updates)
func NewBlobGC(
	blobSegMgr segmentapi.SegmentManager,
	blobStore blobstoreapi.BlobStore,
	blobStoreRecovery blobstoreapi.BlobStoreRecovery,
	wal walapi.WAL,
) gcapi.BlobGC {
	return &blobGC{
		segMgr:   blobSegMgr,
		bs:       blobStore,
		recovery: blobStoreRecovery,
		wal:      wal,
	}
}

// getAvailableDiskSpace returns the number of available bytes on the filesystem
// containing the given directory.
func getAvailableDiskSpace(dir string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

// CollectOne selects the first sealed blob segment and collects it.
//
// Algorithm (per DESIGN.md §3.7):
//  1. Get sealed segments; pick the first one.
//  2. Get segment size; iterate through variable-length blob records.
//  3. Each record: [blobID:8][size:4][data:size][crc32:4] — total 12+size+4 bytes.
//  4. Check liveness against blob mapping.
//  5. Live blobs are re-appended; dead blobs skipped.
//  6. Disk space pre-check before copying (safety).
//  7. Sync, WAL batch, update mapping, remove old segment.
//
// Thread safety: Uses CompareAndSetBlobMapping which holds per-key sharded lock.
// CAS failure orphan handling: old segment deletion is deferred until after
// all CAS updates complete — if CAS fails, old segment stays and data remains
// accessible via old mapping.
func (gc *blobGC) CollectOne() (*gcapi.GCStats, error) {
	// 1. Find a sealed segment to collect.
	sealed := gc.segMgr.SealedSegments()
	if len(sealed) == 0 {
		return nil, gcapi.ErrNoSegmentsToGC
	}
	segID := sealed[0]

	// 2. Get segment size.
	segSize, err := gc.segMgr.SegmentSize(segID)
	if err != nil {
		return nil, err
	}

	stats := &gcapi.GCStats{
		SegmentID: segID,
	}

	type liveRecord struct {
		blobID         uint64
		addr           segmentapi.VAddr
		fullRecord     []byte
		fullRecordSize uint32
		dataSize       uint32
		oldVAddr       uint64
	}
	var liveRecords []liveRecord

	// First pass: identify all live records and calculate total live size.
	// This two-pass approach enables disk space pre-check before any copying.
	var offset uint32

	for int64(offset)+int64(blobHeaderSize) <= segSize {
		// Read the 12-byte header: [blobID:8][size:4]
		headerAddr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
		header, err := gc.segMgr.ReadAt(headerAddr, blobHeaderSize)
		if err != nil {
			return nil, err
		}

		blobID := binary.BigEndian.Uint64(header[:8])
		dataSize := binary.BigEndian.Uint32(header[8:12])
		fullRecordSize := uint32(blobHeaderSize) + dataSize + blobChecksumSize

		// Validate: record must fit within segment.
		if int64(offset)+int64(fullRecordSize) > segSize {
			return nil, fmt.Errorf("gc: blob record at offset %d extends beyond segment (need %d, seg size %d)",
				offset, fullRecordSize, segSize)
		}

		stats.TotalRecords++

		// Check liveness: if BlobStore can read this blobID, it's live.
		// Capture the current VAddr for CAS: only update if mapping still points to old segment.
		oldMeta := gc.bs.GetMeta(blobstoreapi.BlobID(blobID))
		if oldMeta.VAddr == 0 {
			// Blob not found in mapping — dead.
			stats.DeadRecords++
			stats.BytesFreed += int64(fullRecordSize)
			offset += fullRecordSize
			continue
		}

		// Verify the current mapping points to the old segment (not already moved).
		oldVAddr := headerAddr.Pack()
		if oldMeta.VAddr != oldVAddr {
			// Blob was already moved to a newer segment — this copy in the old
			// segment is now stale. Skip it.
			stats.DeadRecords++
			stats.BytesFreed += int64(fullRecordSize)
			offset += fullRecordSize
			continue
		}

		// Live — read full record for later copying.
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
			oldVAddr:       oldVAddr,
		})
		stats.LiveRecords++

		offset += fullRecordSize
	}

	// Issue 2 fix: Disk space pre-check.
	// Calculate total size of live data that needs to be copied.
	var totalLiveSize uint64
	for _, lr := range liveRecords {
		totalLiveSize += uint64(lr.fullRecordSize)
	}

	// Check available disk space. Need at least 2x the live data size
	// to safely perform GC (room for new segment + original during copy).
	available, err := getAvailableDiskSpace(gc.segMgr.StorageDir())
	if err != nil {
		return nil, err
	}
	if available < 2*totalLiveSize {
		return nil, gcapi.ErrInsufficientDiskSpace
	}

	// Second pass: copy live records to active segment.
	type mappingUpdate struct {
		blobID   uint64
		oldVAddr uint64
		newVAddr uint64
		size     uint32
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()

	for _, lr := range liveRecords {
		// Re-check liveness: use GetMeta to verify mapping still points to old segment.
		// This re-check under GetMeta's lock (for reads) + our eventual CAS ensures
		// we don't copy stale data that was already moved.
		oldMeta := gc.bs.GetMeta(blobstoreapi.BlobID(lr.blobID))
		if oldMeta.VAddr != lr.oldVAddr {
			// Record was modified/moved during first pass, skip
			continue
		}

		// Copy the record to active segment.
		newAddr, err := gc.segMgr.Append(lr.fullRecord)
		if err != nil {
			return nil, err
		}

		newPacked := newAddr.Pack()
		batch.Add(walapi.ModuleBlob, walapi.RecordBlobMap, lr.blobID, newPacked, lr.dataSize)
		updates = append(updates, mappingUpdate{
			blobID:   lr.blobID,
			oldVAddr: lr.oldVAddr,
			newVAddr: newPacked,
			size:     lr.dataSize,
		})
	}

	// Sync, WAL batch, apply CAS updates, remove old segment.
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
	// Issue 3 fix (deferred deletion): We delete the old segment AFTER all CAS
	// updates complete. If CAS fails for a blob, the old segment stays and the
	// blob data remains accessible via the old mapping. No orphan data.
	//
	// Note: CompareAndSetBlobMapping holds per-key sharded lock (not global mu),
	// allowing concurrent CAS on different blobIDs to proceed in parallel.
	var casFailed int
	for _, u := range updates {
		ok := gc.bs.CompareAndSetBlobMapping(u.blobID, u.oldVAddr, u.size, u.newVAddr, u.size)
		if !ok {
			casFailed++
			// CAS failed: concurrent write updated the mapping. The old segment's
			// data for this blob is still accessible via the old mapping — no orphan.
			// The stale new segment data will be cleaned in next GC cycle when
			// a different segment is collected.
		}
	}

	// Issue 3 fix: Only delete old segment if ALL CAS updates succeeded.
	// If ANY CAS failed, old segment stays — blob data remains accessible via
	// old mapping (no orphan). The stale data in the new segment will be
	// cleaned in next GC cycle when a DIFFERENT segment is collected.
	// This prevents infinite loops: a segment is only deleted when its
	// blobs are all successfully remapped.
	if casFailed == 0 {
		if err := gc.segMgr.RemoveSegment(segID); err != nil {
			return nil, err
		}
	} else {
		// CAS failed for some blobs. Segment stays for now.
		// These blobs will be retried in the next GC cycle when this
		// segment is picked again. Eventually the concurrent writes will
		// settle and CAS will succeed.
		// Note: this means blobs that always have CAS failures will cause
		// the same segment to be re-collected forever. This is acceptable
		// because: (a) the old segment is still readable, (b) the blob
		// data is accessible via old mapping, (c) the old segment will be
		// deleted once CAS succeeds.
	}

	return stats, nil
}
