// Package blobstore implements the BlobStore interface.
//
// It provides variable-length blob storage with stable BlobID addressing.
// Internally it uses a dense array ([]BlobMeta) mapping BlobID → (VAddr, Size),
// and delegates physical I/O to a SegmentManager.
//
// Record format: [blobID:8][size:4][data:N][crc32:4]
// CRC32 (Castagnoli) covers [blobID:8][size:4][data:N].
//
// Design reference: docs/DESIGN.md §3.3
package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// Compile-time interface checks.
var (
	_ blobstoreapi.BlobStore         = (*blobStore)(nil)
	_ blobstoreapi.BlobStoreRecovery = (*blobStore)(nil)
)

const defaultInitialCapacity = 1024

// blobHeaderSize is the size of the header prepended to each blob in a segment:
// 8 bytes blobID (big-endian) + 4 bytes size (big-endian) = 12 bytes.
const blobHeaderSize = 12

// blobChecksumSize is the size of the CRC32 checksum appended to each blob record.
const blobChecksumSize = 4

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// blobStore implements blobstoreapi.BlobStore and blobstoreapi.BlobStoreRecovery.
type blobStore struct {
	mu         sync.Mutex
	segMgr     segmentapi.SegmentManager
	mapping    []blobstoreapi.BlobMeta // dense array: index = blobID, value = BlobMeta
	nextBlobID uint64
	closed     bool
	statsMgr   interface {
		Increment(segID uint32, count, bytes int64)
		Decrement(segID uint32, count, bytes int64)
	}

	// Per-key sharded CAS locks for fine-grained concurrent access.
	// Reduces contention vs global mu — concurrent CAS on different blobIDs
	// can proceed in parallel. lockCount must be power of 2 for fast bitmask modulo.
	casLocks    []sync.Mutex
	casLockMask uint64
}

const defaultCASLockCount = 256 // must be power of 2

// New creates a new BlobStore backed by the given SegmentManager.
//
// BlobIDs start at 1 (0 is reserved as invalid).
// The mapping table is initialized with cfg.InitialCapacity slots
// (defaults to 1024).
func New(cfg blobstoreapi.Config, segMgr segmentapi.SegmentManager) blobstoreapi.BlobStore {
	cap := cfg.InitialCapacity
	if cap <= 0 {
		cap = defaultInitialCapacity
	}
	bs := &blobStore{
		segMgr:     segMgr,
		mapping:    make([]blobstoreapi.BlobMeta, cap),
		nextBlobID: 1, // 0 is reserved
	}
	if cfg.StatsManager != nil {
		bs.statsMgr = cfg.StatsManager
	}
	// Initialize per-key sharded CAS locks.
	// Using 256 locks reduces contention vs global mu — concurrent CAS on different
	// blobIDs proceeds in parallel. lockCount must be power of 2 for fast bitmask modulo.
	lockCount := uint64(defaultCASLockCount)
	bs.casLocks = make([]sync.Mutex, lockCount)
	bs.casLockMask = lockCount - 1
	return bs
}

// ─── BlobStore interface ────────────────────────────────────────────

// Write allocates a new BlobID and writes the blob data.
//
// Record format: [blobID:8][size:4][data:N][crc32:4]
// CRC32 (Castagnoli) covers bytes [0 : 12+N].
//
// It appends the record to the segment, updates the in-memory mapping,
// and returns a WALEntry for the caller to batch.
func (bs *blobStore) Write(data []byte) (blobstoreapi.BlobID, blobstoreapi.WALEntry, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return 0, blobstoreapi.WALEntry{}, blobstoreapi.ErrClosed
	}

	// Allocate BlobID
	blobID := bs.nextBlobID
	bs.nextBlobID++

	// Build record: [blobID:8][size:4][data:N][crc32:4]
	dataLen := uint32(len(data))
	recordSize := blobHeaderSize + len(data) + blobChecksumSize
	record := make([]byte, recordSize)
	binary.BigEndian.PutUint64(record[:8], blobID)
	binary.BigEndian.PutUint32(record[8:12], dataLen)
	copy(record[12:12+len(data)], data)

	// CRC32 over [blobID:8][size:4][data:N]
	checksum := crc32c(record[:blobHeaderSize+len(data)])
	binary.BigEndian.PutUint32(record[blobHeaderSize+len(data):], checksum)

	// Append to segment. If the active segment is full, rotate and retry once.
	vaddr, err := bs.segMgr.Append(record)
	if err == segmentapi.ErrSegmentFull {
		if rotErr := bs.segMgr.Rotate(); rotErr != nil {
			bs.nextBlobID--
			return 0, blobstoreapi.WALEntry{}, fmt.Errorf("blobstore: rotate on full: %w", rotErr)
		}
		vaddr, err = bs.segMgr.Append(record)
	}
	if err != nil {
		// Roll back the BlobID allocation
		bs.nextBlobID--
		return 0, blobstoreapi.WALEntry{}, err
	}

	packed := vaddr.Pack()

	// Update mapping
	bs.ensureCapacity(blobID)
	bs.mapping[blobID] = blobstoreapi.BlobMeta{VAddr: packed, Size: dataLen}

	// Stats update — increment new segment.
	if bs.statsMgr != nil {
		recordSize := int64(blobHeaderSize) + int64(dataLen) + int64(blobChecksumSize)
		bs.statsMgr.Increment(vaddr.SegmentID, 1, recordSize)
	}

	return blobID, blobstoreapi.WALEntry{
		Type:  2, // RecordBlobMap
		ID:    blobID,
		VAddr: packed,
		Size:  dataLen,
	}, nil
}

// Read reads the blob data for the given BlobID.
//
// Reads the full record [blobID:8][size:4][data:N][crc32:4],
// verifies the CRC32 checksum, and returns the raw blob data.
// Returns ErrBlobNotFound if the blob has not been allocated or was deleted.
// Returns ErrChecksumMismatch if the checksum does not match.
func (bs *blobStore) Read(blobID blobstoreapi.BlobID) ([]byte, error) {
	bs.mu.Lock()
	if bs.closed {
		bs.mu.Unlock()
		return nil, blobstoreapi.ErrClosed
	}
	meta := bs.getMapping(blobID)
	bs.mu.Unlock()

	if meta.IsZero() {
		return nil, blobstoreapi.ErrBlobNotFound
	}

	// Read the full record: header + data + checksum
	totalSize := meta.Size + blobHeaderSize + blobChecksumSize
	vaddr := segmentapi.UnpackVAddr(meta.VAddr)
	raw, err := bs.segMgr.ReadAt(vaddr, totalSize)
	if err != nil {
		return nil, err
	}

	// Verify CRC32 checksum
	payloadEnd := blobHeaderSize + meta.Size
	storedCRC := binary.BigEndian.Uint32(raw[payloadEnd : payloadEnd+blobChecksumSize])
	computedCRC := crc32c(raw[:payloadEnd])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("%w: blobID=%d stored=0x%08x computed=0x%08x",
			blobstoreapi.ErrChecksumMismatch, blobID, storedCRC, computedCRC)
	}

	// Strip header and checksum, return only the blob data
	result := make([]byte, meta.Size)
	copy(result, raw[blobHeaderSize:payloadEnd])
	return result, nil
}

// Delete marks a BlobID as deleted. The mapping is cleared.
// Returns a WALEntry (RecordBlobFree) for the caller to batch.
func (bs *blobStore) Delete(blobID blobstoreapi.BlobID) blobstoreapi.WALEntry {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Look up old meta before clearing (needed for stats decrement).
	oldMeta := bs.getMapping(blobID)

	if blobID < uint64(len(bs.mapping)) {
		bs.mapping[blobID] = blobstoreapi.BlobMeta{}
	}

	// Stats update — decrement old segment.
	if bs.statsMgr != nil && !oldMeta.IsZero() {
		oldSegID := uint32(oldMeta.VAddr >> 32)
		recordSize := int64(oldMeta.Size) + int64(blobHeaderSize) + int64(blobChecksumSize)
		bs.statsMgr.Decrement(oldSegID, 1, recordSize)
	}

	return blobstoreapi.WALEntry{
		Type:  3, // RecordBlobFree
		ID:    blobID,
		VAddr: 0,
		Size:  0,
	}
}

// NextBlobID returns the next BlobID that will be allocated.
func (bs *blobStore) NextBlobID() blobstoreapi.BlobID {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.nextBlobID
}

// GetMeta returns the metadata for a blobID if it exists, or zero BlobMeta otherwise.
func (bs *blobStore) GetMeta(blobID blobstoreapi.BlobID) blobstoreapi.BlobMeta {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.closed {
		return blobstoreapi.BlobMeta{}
	}
	return bs.getMapping(blobID)
}

// CompareAndSetBlobMapping atomically sets a blob mapping only if the current
// value equals expectedVAddr and expectedSize. Returns true if the update was applied.
//
// Uses per-key sharded lock (not global mu) to allow concurrent CAS on different blobIDs.
func (bs *blobStore) CompareAndSetBlobMapping(blobID uint64, expectedVAddr uint64, expectedSize uint32, newVAddr uint64, newSize uint32) bool {
	if bs.closed {
		return false
	}

	// Per-key sharded lock — serializes CAS on this specific blobID.
	lockIdx := blobID & bs.casLockMask
	bs.casLocks[lockIdx].Lock()
	defer bs.casLocks[lockIdx].Unlock()

	// Re-check closed status under lock
	if bs.closed {
		return false
	}

	current := bs.getMapping(blobID)
	if current.VAddr != expectedVAddr || current.Size != expectedSize {
		return false
	}
	bs.mapping[blobID] = blobstoreapi.BlobMeta{VAddr: newVAddr, Size: newSize}
	return true
}

// Close closes the BlobStore. After Close, all operations return ErrClosed.
func (bs *blobStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	bs.closed = true
	return nil
}

// ─── BlobStoreRecovery interface ────────────────────────────────────

// LoadMapping bulk-loads the mapping table from checkpoint data.
func (bs *blobStore) LoadMapping(entries []blobstoreapi.MappingEntry) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	for _, e := range entries {
		bs.ensureCapacity(e.BlobID)
		bs.mapping[e.BlobID] = blobstoreapi.BlobMeta{VAddr: e.VAddr, Size: e.Size}
	}
}

// ExportMapping returns all non-zero blob mappings for checkpoint serialization.
func (bs *blobStore) ExportMapping() []blobstoreapi.MappingEntry {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	var entries []blobstoreapi.MappingEntry
	for i, m := range bs.mapping {
		if !m.IsZero() {
			entries = append(entries, blobstoreapi.MappingEntry{
				BlobID: uint64(i),
				VAddr:  m.VAddr,
				Size:   m.Size,
			})
		}
	}
	return entries
}

// ApplyBlobMap applies a WAL RecordBlobMap record during replay.
func (bs *blobStore) ApplyBlobMap(blobID blobstoreapi.BlobID, vaddr uint64, size uint32) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	bs.ensureCapacity(blobID)
	bs.mapping[blobID] = blobstoreapi.BlobMeta{VAddr: vaddr, Size: size}
}

// ApplyBlobFree applies a WAL RecordBlobFree record during replay.
func (bs *blobStore) ApplyBlobFree(blobID blobstoreapi.BlobID) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if blobID < uint64(len(bs.mapping)) {
		bs.mapping[blobID] = blobstoreapi.BlobMeta{}
	}
}

// SetNextBlobID sets the next allocatable BlobID (used during recovery).
func (bs *blobStore) SetNextBlobID(nextID blobstoreapi.BlobID) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	bs.nextBlobID = nextID
}

// ─── Internal helpers ───────────────────────────────────────────────

// ensureCapacity grows the mapping slice if needed to hold the given blobID.
// Must be called with bs.mu held.
func (bs *blobStore) ensureCapacity(blobID uint64) {
	for blobID >= uint64(len(bs.mapping)) {
		newCap := len(bs.mapping) * 2
		if newCap == 0 {
			newCap = defaultInitialCapacity
		}
		grown := make([]blobstoreapi.BlobMeta, newCap)
		copy(grown, bs.mapping)
		bs.mapping = grown
	}
}

// getMapping returns the BlobMeta for a blobID, or zero BlobMeta if out of range.
// Must be called with bs.mu held.
func (bs *blobStore) getMapping(blobID uint64) blobstoreapi.BlobMeta {
	if blobID >= uint64(len(bs.mapping)) {
		return blobstoreapi.BlobMeta{}
	}
	return bs.mapping[blobID]
}
