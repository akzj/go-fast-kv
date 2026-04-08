package gc

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
//  3. Each record: [blobID:8][size:4][data:size] — total 12+size bytes.
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

	// 3. Build liveness map from current BlobStore mapping.
	//    map[blobID] → packed VAddr
	entries := gc.recovery.ExportMapping()
	liveMap := make(map[uint64]uint64, len(entries))
	for _, e := range entries {
		liveMap[e.BlobID] = e.VAddr
	}

	// 4. Scan all blob records in the segment.
	stats := &gcapi.GCStats{
		SegmentID: segID,
	}

	type mappingUpdate struct {
		blobID uint64
		vaddr  uint64
		size   uint32
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
		fullRecordSize := uint32(blobHeaderSize) + dataSize

		// Validate: record must fit within segment.
		if int64(offset)+int64(fullRecordSize) > segSize {
			return nil, fmt.Errorf("gc: blob record at offset %d extends beyond segment (need %d, seg size %d)",
				offset, fullRecordSize, segSize)
		}

		stats.TotalRecords++

		// Check liveness.
		currentVAddr := headerAddr.Pack()
		if mappedVAddr, ok := liveMap[blobID]; ok && mappedVAddr == currentVAddr {
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
			batch.Add(walapi.RecordBlobMap, blobID, newPacked, dataSize)
			updates = append(updates, mappingUpdate{blobID: blobID, vaddr: newPacked, size: dataSize})

			liveMap[blobID] = newPacked

			stats.LiveRecords++
		} else {
			stats.DeadRecords++
			stats.BytesFreed += int64(fullRecordSize)
		}

		offset += fullRecordSize
	}

	// 5. Sync, WAL batch, apply updates, remove old segment.
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
