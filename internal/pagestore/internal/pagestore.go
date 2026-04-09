// Package pagestore implements the PageStore interface.
//
// It provides fixed-size (4KB) page storage with stable PageID addressing.
// Internally it uses a dense array ([]uint64) mapping PageID → packed VAddr,
// and delegates physical I/O to a SegmentManager.
//
// Design reference: docs/DESIGN.md §3.2
package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// Compile-time interface checks.
var (
	_ pagestoreapi.PageStore         = (*pageStore)(nil)
	_ pagestoreapi.PageStoreRecovery = (*pageStore)(nil)
)

const defaultInitialCapacity = 1024

// pageStore implements pagestoreapi.PageStore and pagestoreapi.PageStoreRecovery.
type pageStore struct {
	mu         sync.Mutex
	segMgr     segmentapi.SegmentManager
	mapping    []uint64 // dense array: index = pageID, value = packed VAddr (0 = free/unallocated)
	nextPageID uint64
	closed     bool
}

// New creates a new PageStore backed by the given SegmentManager.
//
// PageIDs start at 1 (0 is reserved as invalid).
// The mapping table is initialized with cfg.InitialCapacity slots
// (defaults to 1024).
func New(cfg pagestoreapi.Config, segMgr segmentapi.SegmentManager) pagestoreapi.PageStore {
	cap := cfg.InitialCapacity
	if cap <= 0 {
		cap = defaultInitialCapacity
	}
	return &pageStore{
		segMgr:     segMgr,
		mapping:    make([]uint64, cap),
		nextPageID: 1, // 0 is reserved
	}
}

// ─── PageStore interface ────────────────────────────────────────────

// Alloc allocates a new PageID. PageIDs are monotonically increasing
// and never reused. The page has no data until Write is called.
func (ps *pageStore) Alloc() pagestoreapi.PageID {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	id := ps.nextPageID
	ps.nextPageID++
	ps.ensureCapacity(id)
	return id
}

// Write writes page data for the given PageID.
//
// It builds a [pageID:8][data:4096][crc32:4] record (4108 bytes),
// appends it to the segment, updates the in-memory mapping,
// and returns a WALEntry for the caller to batch.
func (ps *pageStore) Write(pageID pagestoreapi.PageID, data []byte) (pagestoreapi.WALEntry, error) {
	if len(data) != pagestoreapi.PageSize {
		return pagestoreapi.WALEntry{}, pagestoreapi.ErrInvalidPageSize
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return pagestoreapi.WALEntry{}, pagestoreapi.ErrClosed
	}

	// Build the 4108-byte record: [pageID:8][data:4096][crc32:4]
	record := make([]byte, pagestoreapi.PageRecordSize)
	binary.BigEndian.PutUint64(record[:8], pageID)
	copy(record[8:8+pagestoreapi.PageSize], data)

	// Compute CRC32 over [pageID:8][data:4096] = first 4104 bytes
	checksum := crc32.ChecksumIEEE(record[:8+pagestoreapi.PageSize])
	binary.BigEndian.PutUint32(record[8+pagestoreapi.PageSize:], checksum)

	// Append to segment
	vaddr, err := ps.segMgr.Append(record)
	if err != nil {
		return pagestoreapi.WALEntry{}, err
	}

	packed := vaddr.Pack()

	// Update mapping
	ps.ensureCapacity(pageID)
	ps.mapping[pageID] = packed

	return pagestoreapi.WALEntry{
		Type:  1, // RecordPageMap
		ID:    pageID,
		VAddr: packed,
		Size:  0,
	}, nil
}

// Read reads the page data for the given PageID.
//
// Returns exactly PageSize (4096) bytes — the pageID header and CRC are stripped.
// Verifies the CRC32 checksum; returns ErrChecksumMismatch if data is corrupt.
// Returns ErrPageNotFound if the page has not been allocated or was freed.
func (ps *pageStore) Read(pageID pagestoreapi.PageID) ([]byte, error) {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil, pagestoreapi.ErrClosed
	}
	packed := ps.getMapping(pageID)
	ps.mu.Unlock()

	if packed == 0 {
		return nil, pagestoreapi.ErrPageNotFound
	}

	vaddr := segmentapi.UnpackVAddr(packed)
	raw, err := ps.segMgr.ReadAt(vaddr, pagestoreapi.PageRecordSize)
	if err != nil {
		return nil, err
	}

	// Verify CRC32 checksum
	expected := binary.BigEndian.Uint32(raw[8+pagestoreapi.PageSize:])
	actual := crc32.ChecksumIEEE(raw[:8+pagestoreapi.PageSize])
	if expected != actual {
		return nil, fmt.Errorf("%w: pageID=%d expected=0x%08x actual=0x%08x",
			pagestoreapi.ErrChecksumMismatch, pageID, expected, actual)
	}

	// Strip the 8-byte pageID header and 4-byte CRC, return only the page data
	result := make([]byte, pagestoreapi.PageSize)
	copy(result, raw[8:8+pagestoreapi.PageSize])
	return result, nil
}

// Free marks a PageID as freed. The mapping is cleared.
// Returns a WALEntry (RecordPageFree) for the caller to batch.
func (ps *pageStore) Free(pageID pagestoreapi.PageID) pagestoreapi.WALEntry {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if pageID < uint64(len(ps.mapping)) {
		ps.mapping[pageID] = 0
	}

	return pagestoreapi.WALEntry{
		Type:  4, // RecordPageFree
		ID:    pageID,
		VAddr: 0,
		Size:  0,
	}
}

// NextPageID returns the next PageID that will be allocated.
func (ps *pageStore) NextPageID() pagestoreapi.PageID {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.nextPageID
}

// Close closes the PageStore. After Close, all operations return ErrClosed.
func (ps *pageStore) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.closed = true
	return nil
}

// ─── PageStoreRecovery interface ────────────────────────────────────

// LoadMapping bulk-loads the mapping table from checkpoint data.
func (ps *pageStore) LoadMapping(entries []pagestoreapi.MappingEntry) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, e := range entries {
		ps.ensureCapacity(e.PageID)
		ps.mapping[e.PageID] = e.VAddr
	}
}

// ExportMapping returns all non-zero page mappings for checkpoint serialization.
func (ps *pageStore) ExportMapping() []pagestoreapi.MappingEntry {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	var entries []pagestoreapi.MappingEntry
	for i, v := range ps.mapping {
		if v != 0 {
			entries = append(entries, pagestoreapi.MappingEntry{
				PageID: uint64(i),
				VAddr:  v,
			})
		}
	}
	return entries
}

// ApplyPageMap applies a WAL RecordPageMap record during replay.
func (ps *pageStore) ApplyPageMap(pageID pagestoreapi.PageID, vaddr uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.ensureCapacity(pageID)
	ps.mapping[pageID] = vaddr
}

// ApplyPageFree applies a WAL RecordPageFree record during replay.
func (ps *pageStore) ApplyPageFree(pageID pagestoreapi.PageID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if pageID < uint64(len(ps.mapping)) {
		ps.mapping[pageID] = 0
	}
}

// SetNextPageID sets the next allocatable PageID (used during recovery).
func (ps *pageStore) SetNextPageID(nextID pagestoreapi.PageID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nextPageID = nextID
}

// ─── Internal helpers ───────────────────────────────────────────────

// ensureCapacity grows the mapping slice if needed to hold the given pageID.
// Must be called with ps.mu held.
func (ps *pageStore) ensureCapacity(pageID uint64) {
	for pageID >= uint64(len(ps.mapping)) {
		newCap := len(ps.mapping) * 2
		if newCap == 0 {
			newCap = defaultInitialCapacity
		}
		grown := make([]uint64, newCap)
		copy(grown, ps.mapping)
		ps.mapping = grown
	}
}

// getMapping returns the packed VAddr for a pageID, or 0 if out of range.
// Must be called with ps.mu held.
func (ps *pageStore) getMapping(pageID uint64) uint64 {
	if pageID >= uint64(len(ps.mapping)) {
		return 0
	}
	return ps.mapping[pageID]
}
