package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── Checkpoint data structures ─────────────────────────────────────

// checkpointData holds all state needed to restore the store.
type checkpointData struct {
	LSN         uint64
	NextXID     uint64
	SnapshotXID uint64   // xmax cutoff for MVCC snapshot
	RootPageID  uint64
	NextPageID  uint64
	NextBlobID  uint64
	Pages       []pageMapping
	Blobs       []blobMapping
	CLOGEntries []clogEntry
	Stats       []segmentStatEntry
	// Version 3: LSM manifest snapshot
	lsmSegments []string // pinned segment names at checkpoint time
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

// checkpointCtx holds state for a background checkpoint goroutine.
type checkpointCtx struct {
	stopCh    chan struct{}  // closed to signal abort
	doneCh    chan struct{}  // closed when checkpoint completes (success or failure)
	snapshotXID uint64       // xmax cutoff for MVCC snapshot
	pinnedSegments []string  // segment names pinned during checkpoint
	tempPath string         // path to temp file (for cleanup on abort)
}

// ─── Checkpoint header layout v4 ─────────────────────────────────────
//
// [0:1]   byte    Version (4 = with page mappings for backup recovery)
// [1:9]   uint64  LSN
// [9:17]  uint64  NextXID
// [17:25] uint64  SnapshotXID (xmax cutoff)
// [25:33] uint64  RootPageID
// [33:41] uint64  NextPageID
// [41:49] uint64  NextBlobID
// [49:53] uint32  PageCount
// [53:57] uint32  BlobCount
// [57:61] uint32  CLOGCount
// [61:65] uint32  StatsCount
// [65:69] uint32  LSMSegmentCount
// [69:73] uint32  reserved
//
// Total header: 73 bytes

const checkpointHeaderSizeV4 = 73
const checkpointHeaderSizeV3 = 73 // v3 added SnapshotXID + LSM segment count, same size
const checkpointHeaderSize = 61 // v1/v2 header size
const checkpointVersion = 4 // v4: includes page mappings for backup recovery

// ─── Store.Checkpoint — lock-free background checkpoint ─────────────

// activeCheckpoint tracks the currently running background checkpoint.
// Only one checkpoint runs at a time.
var activeCheckpoint atomic.Pointer[checkpointCtx]

// Checkpoint triggers a background checkpoint goroutine and returns immediately.
// The checkpoint runs asynchronously without blocking user operations (Put/Get/Delete/Scan).
// Checkpoint state is captured at a consistent MVCC snapshot point.
//
// Returns nil immediately (the background goroutine completes independently).
// Returns error only if the store is closed.
func (s *store) Checkpoint() error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return kvstoreapi.ErrClosed
	}
	s.mu.RUnlock()

	// Create checkpoint context with stop channel.
	ctx := &checkpointCtx{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	// Try to set as active checkpoint (only one runs at a time).
	if !activeCheckpoint.CompareAndSwap(nil, ctx) {
		// Another checkpoint is already running — skip this request.
		// The in-flight checkpoint will eventually complete and become
		// the latest state. No need to force-kill it.
		return nil
	}

	// Spawn background goroutine.
	go s.runCheckpoint(ctx)

	return nil
}

// runCheckpoint performs the checkpoint work in a background goroutine.
// It captures state at a consistent MVCC snapshot point, writes the checkpoint
// file, and then unpins SSTables and truncates WAL.
//
// On stopCh signal (from Close()), it cleans up all resources and exits.
func (s *store) runCheckpoint(ctx *checkpointCtx) {
	defer close(ctx.doneCh)

	// Sync segments first (without blocking user operations — sync is async-safe).
	// This ensures all page/blob data is durable before we capture the snapshot.
	_ = s.pageSegMgr.Sync()
	_ = s.blobSegMgr.Sync()

	// Flush LSM WAL entries to the kvstore WAL before checkpoint.
	// SetPageMapping/SetBlobMapping collect entries in a per-goroutine collector.
	// FlushToWAL drains them and writes to the kvstore WAL with fsync.
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	lsmRecovery := psRecovery.LSMLifecycle()
	if lsmRecovery != nil {
		_, _ = lsmRecovery.FlushToWAL()
	}

	// Create MVCC snapshot: record current LSN and xmax cutoff.
	// xmax = nextXID at snapshot time. Transactions with XID >= xmax
	// are invisible to this checkpoint's state.
	lsn := s.wal.CurrentLSN()
	nextXID := s.txnMgr.NextXID()
	ctx.snapshotXID = nextXID

	// Get LSM manifest and pin all current SSTables.
	// This prevents GC from deleting segments while we're capturing state.
	var manifest lsmapi.Manifest
	if lsmStore, ok := lsmRecovery.(interface{ Manifest() lsmapi.Manifest }); ok {
		manifest = lsmStore.Manifest()
	}
	var pinnedSegments []string
	if manifest != nil {
		pinnedSegments = manifest.PinAll()
		ctx.pinnedSegments = pinnedSegments
	}

	// Collect page mappings from LSM (for checkpoint persistence).
	// This is critical for recovery — without page mappings, we can't locate
	// B-tree pages even if the segment files exist. Include ALL mappings so
	// recovery doesn't depend on WAL replay.
	// Note: GetAllPageMappings returns []walapi.Record{ID=pageID, VAddr=vaddr}.
	var pageMappings []pagestoreapi.MappingEntry
	if lsmStore, ok := lsmRecovery.(interface {
		GetAllPageMappings() []walapi.Record
	}); ok {
		for _, rec := range lsmStore.GetAllPageMappings() {
			pageMappings = append(pageMappings, pagestoreapi.MappingEntry{
				PageID: rec.ID,
				VAddr:  rec.VAddr,
			})
		}
	}

	// Collect blob mappings via COW copy (short write lock ~10ns).
	blobMappings := s.blobStore.GetSnapshotMappings()

	// Collect CLOG entries with XID < xmax (consistent snapshot).
	clogEntries := s.txnMgr.CLOG().EntriesUpTo(nextXID)

	// Collect remaining state via atomic reads (no locks blocking user ops).
	data := &checkpointData{
		LSN:         lsn,
		NextXID:     nextXID,
		SnapshotXID: nextXID,
		RootPageID:  s.tree.RootPageID(),
		NextPageID:  s.pageStore.NextPageID(),
		NextBlobID:  s.blobStore.NextBlobID(),
		Stats:       s.gcStats.ExportAll(),
		lsmSegments: pinnedSegments,
	}

	// Convert page mappings to internal format.
	for _, p := range pageMappings {
		data.Pages = append(data.Pages, pageMapping{
			PageID: p.PageID,
			VAddr:  p.VAddr,
		})
	}

	// Convert blob mappings to internal format.
	for _, b := range blobMappings {
		data.Blobs = append(data.Blobs, blobMapping{
			BlobID: b.BlobID,
			VAddr:  b.VAddr,
			Size:   b.Size,
		})
	}

	// Convert CLOG entries to internal format.
	for xid, status := range clogEntries {
		data.CLOGEntries = append(data.CLOGEntries, clogEntry{
			XID:    xid,
			Status: uint8(status),
		})
	}

	// Write checkpoint file (temp → fsync → rename → dir sync).
	cpPath := filepath.Join(s.dir, "checkpoint")
	tmpPath := cpPath + ".tmp"
	ctx.tempPath = tmpPath

	// Check for stop signal before writing.
	select {
	case <-ctx.stopCh:
		// Close() was called — abort checkpoint.
		s.abortCheckpoint(ctx)
		return
	default:
	}

	// Write checkpoint to temp file.
	if err := writeCheckpoint(tmpPath, data); err != nil {
		s.abortCheckpoint(ctx)
		return
	}

	// Write checkpoint record to WAL (marks the checkpoint point for recovery).
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordCheckpoint, lsn, 0, 0)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		s.abortCheckpoint(ctx)
		return
	}

	// Atomically rename temp file to final checkpoint path.
	// After this, the checkpoint is durable (fsync'd + renamed + dir synced).
	if err := os.Rename(tmpPath, cpPath); err != nil {
		s.abortCheckpoint(ctx)
		return
	}

	// Sync directory to ensure rename is durable.
	dir := filepath.Dir(cpPath)
	dirFile, err := os.Open(dir)
	if err == nil {
		dirFile.Sync()
		dirFile.Close()
	}

	// Truncate WAL entries with LSN <= checkpoint LSN.
	// WAL is safe to truncate only AFTER checkpoint file is fully durable.
	if lsn > 0 {
		_ = s.wal.Truncate(lsn)
	}

	// Truncate CLOG to reclaim memory.
	// safeXID = oldest active XID. All CLOG entries below safeXID
	// are no longer needed (no snapshot can reference them).
	safeXID := s.txnMgr.GetMinActive()
	if safeXID == txnapi.TxnMaxInfinity {
		safeXID = s.txnMgr.NextXID()
	}
	s.txnMgr.CLOG().Truncate(safeXID)

	// Unpin all SSTables (checkpoint is complete).
	s.unpinAndClear(ctx)
}

// abortCheckpoint cleans up after a failed or aborted checkpoint.
// Called when stopCh is signaled or an error occurs.
func (s *store) abortCheckpoint(ctx *checkpointCtx) {
	// Delete temp file if it exists.
	if ctx.tempPath != "" {
		os.Remove(ctx.tempPath)
	}

	// Unpin all SSTables.
	s.unpinAndClear(ctx)
}

// unpinAndClear unpins all SSTables and clears the active checkpoint reference.
func (s *store) unpinAndClear(ctx *checkpointCtx) {
	// Unpin all pinned segments.
	if len(ctx.pinnedSegments) > 0 {
		psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
		lsmRecovery := psRecovery.LSMLifecycle()
		if lsmStore, ok := lsmRecovery.(interface{ Manifest() lsmapi.Manifest }); ok {
			lsmStore.Manifest().UnpinAll(ctx.pinnedSegments)
		}
	}

	// Clear active checkpoint reference.
	activeCheckpoint.Store((*checkpointCtx)(nil))
}

// StopCheckpoint signals the active checkpoint goroutine to abort.
// Called by store.Close() to cleanly stop the checkpoint before shutdown.
func (s *store) stopCheckpoint() {
	ctx := activeCheckpoint.Swap(nil)
	if ctx == nil {
		return
	}

	close(ctx.stopCh)

	// Wait for checkpoint to exit (with timeout).
	select {
	case <-ctx.doneCh:
		return
	case <-time.After(2 * time.Second):
		// Timeout — checkpoint goroutine is taking too long.
		// The goroutine will eventually exit when it checks stopCh.
		// Force-clear the reference so Close() can complete.
		return
	}
}

// checkpointLocked performs a synchronous checkpoint (legacy, for testing).
// Called from Close() to ensure checkpoint completes before shutdown.
// Holds s.mu.Lock briefly for final state collection.
func (s *store) checkpointLocked() error {
	// Sync segments.
	if err := s.pageSegMgr.Sync(); err != nil {
		return err
	}
	if err := s.blobSegMgr.Sync(); err != nil {
		return err
	}

	// Flush LSM WAL.
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	lsmRecovery := psRecovery.LSMLifecycle()
	if lsmRecovery != nil {
		_, err := lsmRecovery.FlushToWAL()
		if err != nil {
			return err
		}
	}

	// Collect state.
	blobMappings := s.blobStore.GetSnapshotMappings()
	nextXID := s.txnMgr.NextXID()
	clogEntries := s.txnMgr.CLOG().EntriesUpTo(nextXID)

	// Collect page mappings from LSM (CRITICAL for recovery).
	// Without page mappings, B-tree pages are unfindable after WAL truncation.
	var pageMappings []pagestoreapi.MappingEntry
	if lsmStore, ok := lsmRecovery.(interface {
		GetAllPageMappings() []walapi.Record
	}); ok {
		for _, rec := range lsmStore.GetAllPageMappings() {
			pageMappings = append(pageMappings, pagestoreapi.MappingEntry{
				PageID: rec.ID,
				VAddr:  rec.VAddr,
			})
		}
	}

	data := &checkpointData{
		LSN:         s.wal.CurrentLSN(),
		NextXID:     nextXID,
		SnapshotXID: nextXID,
		RootPageID:  s.tree.RootPageID(),
		NextPageID:  s.pageStore.NextPageID(),
		NextBlobID:  s.blobStore.NextBlobID(),
		Stats:       s.gcStats.ExportAll(),
	}

	for _, b := range blobMappings {
		data.Blobs = append(data.Blobs, blobMapping{
			BlobID: b.BlobID,
			VAddr:  b.VAddr,
			Size:   b.Size,
		})
	}

	// Convert page mappings to internal format.
	for _, p := range pageMappings {
		data.Pages = append(data.Pages, pageMapping{
			PageID: p.PageID,
			VAddr:  p.VAddr,
		})
	}

	for xid, status := range clogEntries {
		data.CLOGEntries = append(data.CLOGEntries, clogEntry{
			XID:    xid,
			Status: uint8(status),
		})
	}

	// Write checkpoint record to WAL.
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordCheckpoint, data.LSN, 0, 0)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	// Write checkpoint file.
	cpPath := filepath.Join(s.dir, "checkpoint")
	if err := writeCheckpoint(cpPath, data); err != nil {
		return err
	}

	// Truncate WAL.
	if data.LSN > 0 {
		if err := s.wal.Truncate(data.LSN); err != nil {
			return err
		}
	}

	// Truncate CLOG.
	safeXID := s.txnMgr.GetMinActive()
	if safeXID == txnapi.TxnMaxInfinity {
		safeXID = s.txnMgr.NextXID()
	}
	s.txnMgr.CLOG().Truncate(safeXID)

	return nil
}

// ─── writeCheckpoint ────────────────────────────────────────────────

func writeCheckpoint(path string, data *checkpointData) error {
	buf := serializeCheckpointV3(data)

	// Write to temp file, fsync, then atomic rename.
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

	// Fsync directory.
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

// serializeCheckpointV3 serializes checkpoint data to bytes (v3 format).
func serializeCheckpointV3(data *checkpointData) []byte {
	pageCount := uint32(len(data.Pages))
	blobCount := uint32(len(data.Blobs))
	clogCount := uint32(len(data.CLOGEntries))
	statsCount := uint32(len(data.Stats))
	lsmCount := uint32(len(data.lsmSegments))

	// Calculate total size.
	totalSize := checkpointHeaderSizeV3 +
		int(pageCount)*16 +
		int(blobCount)*20 +
		int(clogCount)*9 +
		int(statsCount)*20 +
		int(lsmCount)*16 + // segment names (max 16 bytes each)
		4 // trailing CRC32

	buf := make([]byte, totalSize)
	off := 0

	// Header (v3).
	buf[off] = checkpointVersion
	off += 1
	binary.LittleEndian.PutUint64(buf[off:], data.LSN)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], data.NextXID)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], data.SnapshotXID)
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
	binary.LittleEndian.PutUint32(buf[off:], statsCount)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], lsmCount)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], 0) // reserved
	off += 4

	// Page mappings.
	for _, p := range data.Pages {
		binary.LittleEndian.PutUint64(buf[off:], p.PageID)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], p.VAddr)
		off += 8
	}

	// Blob mappings.
	for _, b := range data.Blobs {
		binary.LittleEndian.PutUint64(buf[off:], b.BlobID)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], b.VAddr)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], b.Size)
		off += 4
	}

	// CLOG entries.
	for _, c := range data.CLOGEntries {
		binary.LittleEndian.PutUint64(buf[off:], c.XID)
		off += 8
		buf[off] = c.Status
		off++
	}

	// Stats entries.
	for _, e := range data.Stats {
		binary.LittleEndian.PutUint32(buf[off:], e.SegID)
		off += 4
		binary.LittleEndian.PutUint64(buf[off:], e.AliveCount)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.AliveBytes)
		off += 8
	}

	// LSM segment names.
	for _, name := range data.lsmSegments {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(name)))
		off += 2
		copy(buf[off:], name)
		off += len(name)
		// Pad to 16-byte alignment
		pad := (16 - (len(name)%16)) % 16
		for i := 0; i < pad; i++ {
			buf[off] = 0
			off++
		}
	}

	// CRC32-C.
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

	if len(raw) < checkpointHeaderSizeV3+4 {
		return nil, fmt.Errorf("checkpoint: file too small (%d bytes)", len(raw))
	}

	// Verify CRC.
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

	// Version byte.
	if len(buf) < 1 {
		return nil, fmt.Errorf("checkpoint: file too small for version byte")
	}
	version := buf[off]
	off += 1

	// Version 1-2 use checkpointHeaderSize (61), v3 uses checkpointHeaderSizeV3 (73).
	var pageCount, blobCount, clogCount, statsCount, lsmCount uint32
	var headerSize int

	if version < 3 {
		// v1/v2 format (backward compatible).
		if len(buf) < checkpointHeaderSize+4 {
			return nil, fmt.Errorf("checkpoint: file too small for v%d header", version)
		}
		headerSize = checkpointHeaderSize

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
		pageCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		blobCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		clogCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		statsCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		// reserved
		off += 4
		lsmCount = 0 // v1/v2 don't have LSM manifest
		data.SnapshotXID = data.NextXID // default for v1/v2
	} else if version == 3 {
		// v3 format.
		headerSize = checkpointHeaderSizeV3

		data.LSN = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.NextXID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.SnapshotXID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.RootPageID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.NextPageID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.NextBlobID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		pageCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		blobCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		clogCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		statsCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		lsmCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		// reserved
		off += 4
	} else if version == 3 || version == 4 {
		// v3/v4 format (same header size 73).
		// v4 added page mappings for backup recovery; header layout is identical.
		headerSize = checkpointHeaderSizeV3

		data.LSN = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.NextXID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.SnapshotXID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.RootPageID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.NextPageID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.NextBlobID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		pageCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		blobCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		clogCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		statsCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		lsmCount = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		// reserved
		off += 4
	} else {
		return nil, fmt.Errorf("checkpoint: unsupported version %d", version)
	}

	// Validate size.
	expected := headerSize +
		int(pageCount)*16 +
		int(blobCount)*20 +
		int(clogCount)*9 +
		int(statsCount)*20 +
		int(lsmCount)*16 +
		4
	if len(buf) != expected {
		return nil, fmt.Errorf("checkpoint: size mismatch (got %d, expected %d)", len(buf), expected)
	}

	// Page mappings.
	data.Pages = make([]pageMapping, pageCount)
	for i := range data.Pages {
		data.Pages[i].PageID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Pages[i].VAddr = binary.LittleEndian.Uint64(buf[off:])
		off += 8
	}

	// Blob mappings.
	data.Blobs = make([]blobMapping, blobCount)
	for i := range data.Blobs {
		data.Blobs[i].BlobID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Blobs[i].VAddr = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Blobs[i].Size = binary.LittleEndian.Uint32(buf[off:])
		off += 4
	}

	// CLOG entries.
	data.CLOGEntries = make([]clogEntry, clogCount)
	for i := range data.CLOGEntries {
		data.CLOGEntries[i].XID = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.CLOGEntries[i].Status = buf[off]
		off++
	}

	// Stats entries.
	data.Stats = make([]segmentStatEntry, statsCount)
	for i := range data.Stats {
		data.Stats[i].SegID = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		data.Stats[i].AliveCount = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		data.Stats[i].AliveBytes = binary.LittleEndian.Uint64(buf[off:])
		off += 8
	}

	// LSM segment names (v3 only).
	data.lsmSegments = make([]string, lsmCount)
	for i := uint32(0); i < lsmCount; i++ {
		nameLen := binary.LittleEndian.Uint16(buf[off:])
		off += 2
		name := string(buf[off : off+int(nameLen)])
		off += int(nameLen)
		// Skip padding
		pad := (16 - (int(nameLen)%16)) % 16
		off += int(pad)
		data.lsmSegments[i] = name
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

// Version returns the checkpoint version.
func (d *checkpointData) Version() int {
	if len(d.lsmSegments) > 0 {
		return 3
	}
	return 2
}