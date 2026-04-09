// Package blobstore implements the BlobStore interface.
//
// It provides variable-length blob storage with stable BlobID addressing.
// Internally it uses a dense array ([]BlobMeta) mapping BlobID → (VAddr, Size),
// and delegates physical I/O to a SegmentManager.
//
// Design reference: docs/DESIGN.md §3.3
package internal

import (
	"encoding/binary"
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

// blobStore implements blobstoreapi.BlobStore and blobstoreapi.BlobStoreRecovery.
type blobStore struct {
	mu         sync.Mutex
	segMgr     segmentapi.SegmentManager
	mapping    []blobstoreapi.BlobMeta // dense array: index = blobID, value = BlobMeta
	nextBlobID uint64
	closed     bool
}

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
	return &blobStore{
		segMgr:     segMgr,
		mapping:    make([]blobstoreapi.BlobMeta, cap),
		nextBlobID: 1, // 0 is reserved
	}
}

// ─── BlobStore interface ────────────────────────────────────────────

// Write allocates a new BlobID and writes the blob data.
//
// It prepends a big-endian blobID (8 bytes) + size (4 bytes) header,
// appends the record to the segment, updates the in-memory mapping,
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

	// Build record: [blobID:8][size:4][data]
	dataLen := uint32(len(data))
	record := make([]byte, blobHeaderSize+len(data))
	binary.BigEndian.PutUint64(record[:8], blobID)
	binary.BigEndian.PutUint32(record[8:12], dataLen)
	copy(record[12:], data)

	// Append to segment
	vaddr, err := bs.segMgr.Append(record)
	if err != nil {
		// Roll back the BlobID allocation
		bs.nextBlobID--
		return 0, blobstoreapi.WALEntry{}, err
	}

	packed := vaddr.Pack()

	// Update mapping
	bs.ensureCapacity(blobID)
	bs.mapping[blobID] = blobstoreapi.BlobMeta{VAddr: packed, Size: dataLen}

	return blobID, blobstoreapi.WALEntry{
		Type:  2, // RecordBlobMap
		ID:    blobID,
		VAddr: packed,
		Size:  dataLen,
	}, nil
}

// Read reads the blob data for the given BlobID.
//
// Returns the raw blob data (without blobID/size headers).
// Returns ErrBlobNotFound if the blob has not been allocated or was deleted.
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

	vaddr := segmentapi.UnpackVAddr(meta.VAddr)
	raw, err := bs.segMgr.ReadAt(vaddr, meta.Size+blobHeaderSize)
	if err != nil {
		return nil, err
	}

	// Strip the 12-byte header (blobID + size), return only the blob data
	result := make([]byte, meta.Size)
	copy(result, raw[blobHeaderSize:])
	return result, nil
}

// Delete marks a BlobID as deleted. The mapping is cleared.
// Returns a WALEntry (RecordBlobFree) for the caller to batch.
func (bs *blobStore) Delete(blobID blobstoreapi.BlobID) blobstoreapi.WALEntry {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if blobID < uint64(len(bs.mapping)) {
		bs.mapping[blobID] = blobstoreapi.BlobMeta{}
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
