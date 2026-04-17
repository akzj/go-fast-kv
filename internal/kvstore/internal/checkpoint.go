package internal

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
// [0:1]   byte    Version (1 = current)
// [1:9]   uint64  LSN
// [9:17]  uint64  NextXID
// [17:25] uint64  RootPageID
// [25:33] uint64  NextPageID
// [33:41] uint64  NextBlobID
// [41:45] uint32  PageCount
// [45:49] uint32  BlobCount
// [49:53] uint32  CLOGCount
// [53:57] uint32  reserved (padding)
//
// Total header: 57 bytes

const checkpointHeaderSize = 57
const checkpointVersion = 1

// ─── Store.Checkpoint ───────────────────────────────────────────────

func (s *store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	return s.checkpointLocked()
}

// checkpointLocked performs the checkpoint while the caller already holds s.mu.
func (s *store) checkpointLocked() error {
	// Sync segments to ensure all page/blob data is durable before checkpoint.
	// Per-Put/Delete no longer fsyncs segments (WAL provides durability);
	// this is the point where segment data becomes durable on disk.
	if err := s.pageSegMgr.Sync(); err != nil {
		return err
	}
	if err := s.blobSegMgr.Sync(); err != nil {
		return err
	}

	// Collect state
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)

	// Flush LSM WAL entries to the kvstore WAL before checkpoint.
	// SetPageMapping/SetBlobMapping collect entries in a per-goroutine collector.
	// This flushes them to the kvstore WAL for fsync durability.
	lsmRecovery := psRecovery.LSMLifecycle()
	_, err := lsmRecovery.FlushToWAL()
	if err != nil {
		return fmt.Errorf("checkpoint: flush LSM WAL: %w", err)
	}

	// pageMappings: PageStore no longer exports mappings (LSM handles persistence).
	// BlobStore: ExportMapping via public BlobStore interface.
	pageMappings := []pagestoreapi.MappingEntry(nil)
	blobMappings := s.blobStore.ExportMapping()
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
	batch.Add(walapi.ModuleTree, walapi.RecordCheckpoint, data.LSN, 0, 0)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	// Write checkpoint file (atomic: write temp → fsync → rename → dir fsync)
	cpPath := filepath.Join(s.dir, "checkpoint")
	if err := writeCheckpoint(cpPath, data); err != nil {
		return err
	}

	// Truncate WAL entries at or before the checkpoint LSN. The checkpoint
	// file is now durable (data fsync'd + rename + dir fsync'd), so all WAL
	// entries up to data.LSN are recoverable from the checkpoint and no longer
	// needed in the WAL. Without this, the WAL file grows without bound.
	if data.LSN > 0 {
		if err := s.wal.Truncate(data.LSN); err != nil {
			return err
		}
	}

	// Truncate CLOG to reclaim memory. The checkpoint file now contains
	// the full CLOG, so old entries are recoverable from the checkpoint.
	//
	// safeXID = the oldest XID that any active transaction could reference.
	// All CLOG entries below safeXID are no longer needed for visibility
	// checks because no snapshot can reference them.
	//
	// Note: checkpoint holds s.mu.Lock() (exclusive), so no Put/Delete/Get/Scan
	// is running. GetMinActive() returns TxnMaxInfinity when no txns are active.
	// In that case, we use NextXID() — everything below it is fully resolved.
	safeXID := s.txnMgr.GetMinActive()
	if safeXID == txnapi.TxnMaxInfinity {
		safeXID = s.txnMgr.NextXID()
	}
	s.txnMgr.CLOG().Truncate(safeXID)

	return nil
}

// ─── writeCheckpoint ────────────────────────────────────────────────

func writeCheckpoint(path string, data *checkpointData) error {
	buf := serializeCheckpoint(data)

	// Write to temp file, fsync, then atomic rename.
	// The fsync ensures data is durable before rename; without it,
	// a crash after rename could leave a zero-filled or partial checkpoint.
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("checkpoint: create temp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint: write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint: sync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint: close temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}

	// Fsync the parent directory to ensure the rename (directory entry update)
	// is durable. Without this, a power loss after rename could lose the new
	// checkpoint file from the directory — the data is on disk but the
	// directory metadata pointing to it is not.
	dir := filepath.Dir(path)
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("checkpoint: open dir for sync: %w", err)
	}
	if err := dirFile.Sync(); err != nil {
		dirFile.Close()
		return fmt.Errorf("checkpoint: sync dir: %w", err)
	}
	dirFile.Close()

	return nil
}

func serializeCheckpoint(data *checkpointData) []byte {
	pageCount := uint32(len(data.Pages))
	blobCount := uint32(len(data.Blobs))
	clogCount := uint32(len(data.CLOGEntries))

	// Calculate total size:
	// header(57) + pages(16 each) + blobs(20 each) + clog(9 each) + crc(4)
	totalSize := checkpointHeaderSize +
		int(pageCount)*16 +
		int(blobCount)*20 +
		int(clogCount)*9 +
		4 // trailing CRC32

	buf := make([]byte, totalSize)
	off := 0

	// Header: version byte first, then fields shifted by +1
	buf[off] = checkpointVersion // Version at offset 0
	off += 1
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

	// CRC32-C over everything before the CRC field (includes version byte)
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

	// Version byte first
	if len(buf) < 1 {
		return nil, fmt.Errorf("checkpoint: file too small for version byte")
	}
	version := buf[off]
	off += 1
	if version != checkpointVersion {
		return nil, fmt.Errorf("checkpoint: unsupported version %d (expected %d)", version, checkpointVersion)
	}

	// Fields now start at offset 1 (shifted +1 from v0)
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
