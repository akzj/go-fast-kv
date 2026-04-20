package internal

import (
	"encoding/binary"
	"fmt"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	gcapi "github.com/akzj/go-fast-kv/internal/gc/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
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

// CollectOne selects the first sealed blob segment and collects it.
//
// Algorithm (per DESIGN.md §3.7):
//  1. Get sealed segments; pick the first one.
//  2. Get segment size; iterate through variable-length blob records.
//  3. Each record: [blobID:8][size:4][data:size][crc32:4] — total 12+size+4 bytes.
//  4. Check liveness against blob mapping.
//  5. Live blobs are re-appended; dead blobs skipped.
//  6. Sync, WAL batch, update mapping, remove old segment.
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

	// 4. Scan all blob records in the segment.
	stats := &gcapi.GCStats{
		SegmentID: segID,
	}

	type mappingUpdate struct {
		blobID   uint64
		oldVAddr uint64 // VAddr in the old (being-collected) segment
		newVAddr uint64 // VAddr in the new (active) segment
		size     uint32
	}
	var updates []mappingUpdate
	batch := walapi.NewBatch()

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

		// Live — read full record and copy to active segment.
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
		updates = append(updates, mappingUpdate{blobID: blobID, oldVAddr: oldVAddr, newVAddr: newPacked, size: dataSize})

		stats.LiveRecords++
		offset += fullRecordSize
	}

	// 5. Sync, WAL batch, apply CAS updates, remove old segment.
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
	// CompareAndSetBlobMapping holds BlobStore.mu during the CAS, which serializes
	// with concurrent blob writes (also under BlobStore.mu). If CAS fails,
	// the blob data in the old segment is now stale — handled in next GC cycle.
	for _, u := range updates {
		gc.bs.CompareAndSetBlobMapping(u.blobID, u.oldVAddr, u.size, u.newVAddr, u.size)
		// If CAS fails: concurrent write updated the mapping. Skip update.
		// The old segment's data for this blob is now stale — will be cleaned
		// in the next GC cycle.
	}

	if err := gc.segMgr.RemoveSegment(segID); err != nil {
		return nil, err
	}

	return stats, nil
}
