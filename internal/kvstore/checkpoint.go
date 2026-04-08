package kvstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── Checkpoint data structures ─────────────────────────────────────

// checkpointData holds all state needed to restore the store.
type checkpointData struct {
	LSN        uint64
	NextXID    uint64
	RootPageID uint64
	NextPageID uint64
	NextBlobID uint64
	Pages      []pageMapping
	Blobs      []blobMapping
	CLOGEntries []clogEntry
}

type pageMapping struct {
	PageID uint64
	VAddr  uint64
}

type blobMapping struct {
	BlobID uint64
	VAddr  uint64
	Size   uint32
}

type clogEntry struct {
	XID    uint64
	Status uint8
}

// ─── Checkpoint header layout ───────────────────────────────────────
//
// [0:8]   uint64  LSN
// [8:16]  uint64  NextXID
// [16:24] uint64  RootPageID
// [24:32] uint64  NextPageID
// [32:40] uint64  NextBlobID
// [40:44] uint32  PageCount
// [44:48] uint32  BlobCount
// [48:52] uint32  CLOGCount
// [52:56] uint32  reserved (padding)
//
// Total header: 56 bytes

const checkpointHeaderSize = 56

// ─── Store.Checkpoint ───────────────────────────────────────────────

func (s *store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Collect state
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	bsRecovery := s.blobStore.(blobstoreapi.BlobStoreRecovery)

	pageMappings := psRecovery.ExportMapping()
	blobMappings := bsRecovery.ExportMapping()
	clogEntries := s.txnMgr.CLOG().Entries()

	data := &checkpointData{
		LSN:        s.wal.CurrentLSN(),
		NextXID:    s.txnMgr.NextXID(),
		RootPageID: s.tree.RootPageID(),
		NextPageID: s.pageStore.NextPageID(),
		NextBlobID: s.blobStore.NextBlobID(),
	}

	for _, p := range pageMappings {
		data.Pages = append(data.Pages, pageMapping{PageID: p.PageID, VAddr: p.VAddr})
	}
	for _, b := range blobMappings {
		data.Blobs = append(data.Blobs, blobMapping{BlobID: b.BlobID, VAddr: b.VAddr, Size: b.Size})
	}
	for xid, status := range clogEntries {
		data.CLOGEntries = append(data.CLOGEntries, clogEntry{XID: xid, Status: uint8(status)})
	}

	// Write checkpoint record to WAL
	batch := walapi.NewBatch()
	batch.Add(walapi.RecordCheckpoint, data.LSN, 0, 0)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	// Write checkpoint file (atomic: write temp → rename)
	cpPath := filepath.Join(s.dir, "checkpoint")
	return writeCheckpoint(cpPath, data)
}

// ─── writeCheckpoint ────────────────────────────────────────────────

func writeCheckpoint(path string, data *checkpointData) error {
	buf := serializeCheckpoint(data)

	// Write to temp file, then atomic rename
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return fmt.Errorf("checkpoint: write temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
}

func serializeCheckpoint(data *checkpointData) []byte {
	pageCount := uint32(len(data.Pages))
	blobCount := uint32(len(data.Blobs))
	clogCount := uint32(len(data.CLOGEntries))

	// Calculate total size:
	// header(56) + pages(16 each) + blobs(20 each) + clog(9 each) + crc(4)
	totalSize := checkpointHeaderSize +
		int(pageCount)*16 +
		int(blobCount)*20 +
		int(clogCount)*9 +
		4 // trailing CRC32

	buf := make([]byte, totalSize)
	off := 0

	// Header
	binary.LittleEndian.PutUint64(buf[off:], data.LSN)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], data.NextXID)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], data.RootPageID)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], data.NextPageID)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], data.NextBlobID)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], pageCount)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], blobCount)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], clogCount)
	off += 4
	// reserved padding
	binary.LittleEndian.PutUint32(buf[off:], 0)
	off += 4

	// Page mappings
	for _, p := range data.Pages {
		binary.LittleEndian.PutUint64(buf[off:], p.PageID)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], p.VAddr)
		off += 8
	}

	// Blob mappings
	for _, b := range data.Blobs {
		binary.LittleEndian.PutUint64(buf[off:], b.BlobID)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], b.VAddr)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], b.Size)
		off += 4
	}

	// CLOG entries
	for _, c := range data.CLOGEntries {
		binary.LittleEndian.PutUint64(buf[off:], c.XID)
		off += 8
		buf[off] = c.Status
		off++
	}

	// CRC32-C over everything before the CRC field
	crc := crc32.Checksum(buf[:off], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[off:], crc)

	return buf
}

// ─── loadCheckpoint ─────────────────────────────────────────────────

func loadCheckpoint(path string) (*checkpointData, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(raw) < checkpointHeaderSize+4 {
		return nil, fmt.Errorf("checkpoint: file too small (%d bytes)", len(raw))
	}

	// Verify CRC
	storedCRC := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	computedCRC := crc32.Checksum(raw[:len(raw)-4], crc32.MakeTable(crc32.Castagnoli))
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("checkpoint: CRC mismatch (stored=%x computed=%x)", storedCRC, computedCRC)
	}

	return deserializeCheckpoint(raw)
}

func deserializeCheckpoint(buf []byte) (*checkpointData, error) {
	off := 0
	data := &checkpointData{}

	data.LSN = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	data.NextXID = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	data.RootPageID = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	data.NextPageID = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	data.NextBlobID = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	pageCount := binary.LittleEndian.Uint32(buf[off:])
	off += 4
	blobCount := binary.LittleEndian.Uint32(buf[off:])
	off += 4
	clogCount := binary.LittleEndian.Uint32(buf[off:])
	off += 4
	// skip reserved
	off += 4

	// Validate size
	expected := checkpointHeaderSize +
		int(pageCount)*16 +
		int(blobCount)*20 +
		int(clogCount)*9 +
		4
	if len(buf) != expected {
		return nil, fmt.Errorf("checkpoint: size mismatch (got %d, expected %d)", len(buf), expected)
	}

	// Page mappings
	data.Pages = make([]pageMapping, pageCount)
	for i := range data.Pages {
		data.Pages[i].PageID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Pages[i].VAddr = binary.LittleEndian.Uint64(buf[off:])
		off += 8
	}

	// Blob mappings
	data.Blobs = make([]blobMapping, blobCount)
	for i := range data.Blobs {
		data.Blobs[i].BlobID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Blobs[i].VAddr = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Blobs[i].Size = binary.LittleEndian.Uint32(buf[off:])
		off += 4
	}

	// CLOG entries
	data.CLOGEntries = make([]clogEntry, clogCount)
	for i := range data.CLOGEntries {
		data.CLOGEntries[i].XID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.CLOGEntries[i].Status = buf[off]
		off++
	}

	return data, nil
}

// ─── helper for recovery: convert checkpoint to typed mappings ──────

func (d *checkpointData) toPageMappings() []pagestoreapi.MappingEntry {
	out := make([]pagestoreapi.MappingEntry, len(d.Pages))
	for i, p := range d.Pages {
		out[i] = pagestoreapi.MappingEntry{PageID: p.PageID, VAddr: p.VAddr}
	}
	return out
}

func (d *checkpointData) toBlobMappings() []blobstoreapi.MappingEntry {
	out := make([]blobstoreapi.MappingEntry, len(d.Blobs))
	for i, b := range d.Blobs {
		out[i] = blobstoreapi.MappingEntry{BlobID: b.BlobID, VAddr: b.VAddr, Size: b.Size}
	}
	return out
}

func (d *checkpointData) toCLOGEntries() map[uint64]txnapi.TxnStatus {
	out := make(map[uint64]txnapi.TxnStatus, len(d.CLOGEntries))
	for _, e := range d.CLOGEntries {
		out[e.XID] = txnapi.TxnStatus(e.Status)
	}
	return out
}
