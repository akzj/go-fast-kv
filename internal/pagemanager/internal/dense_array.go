package internal

import (
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// denseArray implements FixedSizeKVIndex using a dense array.
// Entry size: 24 bytes (8 bytes PageID + 16 bytes VAddr).
// O(1) lookup by PageID.
type denseArray struct {
	mu            sync.RWMutex
	basePageID    PageID           // First PageID in the array
	entries       []PageManagerIndexEntry // Dense array of entries
	liveCount     uint64           // Count of non-zero (valid) entries
}

// PageManagerIndexEntry is the on-disk format for an index entry.
// 24 bytes: PageID (8 bytes) + VAddr (16 bytes: SegmentID + Offset).
type PageManagerIndexEntry struct {
	PageID PageID
	VAddr  [16]byte // 16 bytes: SegmentID[8] + Offset[8], big-endian
}

// newDenseArray creates a new dense array with initial capacity.
func newDenseArray(initialCapacity uint64) *denseArray {
	if initialCapacity < 1024 {
		initialCapacity = 1024
	}
	return &denseArray{
		basePageID: 1,
		entries:    make([]PageManagerIndexEntry, initialCapacity),
	}
}

// ensureCapacity grows the array if needed to accommodate the given pageID.
func (d *denseArray) ensureCapacity(pageID PageID) {
	if pageID == 0 {
		return
	}

	index := int(pageID - d.basePageID)
	if index >= len(d.entries) {
		// Grow the array
		newCapacity := uint64(len(d.entries))
		for newCapacity <= uint64(index) {
			newCapacity *= 2
		}
		newEntries := make([]PageManagerIndexEntry, newCapacity)
		copy(newEntries, d.entries)
		d.entries = newEntries
	}
}

// Get returns the VAddr for key, or zero VAddr if not present.
func (d *denseArray) Get(key PageID) [16]byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if key == 0 {
		return [16]byte{}
	}

	index := key - d.basePageID
	if int(index) >= len(d.entries) || int(index) < 0 {
		return [16]byte{}
	}

	entry := d.entries[index]
	if entry.PageID != key {
		return [16]byte{} // Slot not used
	}

	return entry.VAddr
}

// Put stores the mapping key → value.
// Invariant: Put is idempotent for the same key.
func (d *denseArray) Put(key PageID, value [16]byte) {
	if key == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Ensure capacity
	d.ensureCapacity(key)

	index := key - d.basePageID
	entry := &d.entries[index]

	// Track live count changes
	wasLive := entry.PageID != 0 && !isZeroVAddr(entry.VAddr)
	isLive := !isZeroVAddr(value)

	if wasLive && !isLive {
		d.liveCount--
	} else if !wasLive && isLive {
		d.liveCount++
	}

	entry.PageID = key
	entry.VAddr = value
}

// isZeroVAddr returns true if the 16-byte VAddr is all zeros.
func isZeroVAddr(b [16]byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// Len returns the number of entries in the index.
func (d *denseArray) Len() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	maxID := PageIDInvalid
	for i := range d.entries {
		if d.entries[i].PageID != 0 {
			maxID = d.entries[i].PageID
		}
	}
	if maxID == PageIDInvalid {
		return 0
	}
	return uint64(maxID - d.basePageID + 1)
}

// RangeQuery returns all entries where start <= pageID < end.
func (d *denseArray) RangeQuery(start, end PageID) []PageManagerIndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []PageManagerIndexEntry

	for i := start; i < end; i++ {
		index := i - d.basePageID
		if int(index) >= len(d.entries) || int(index) < 0 {
			continue
		}

		entry := d.entries[index]
		if entry.PageID == i && !isZeroVAddr(entry.VAddr) {
			result = append(result, entry)
		}
	}

	return result
}

// ByteSize returns the memory footprint of the index.
func (d *denseArray) ByteSize() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 24 bytes per entry * capacity
	return uint64(len(d.entries)) * 24
}

// LiveCount returns the number of entries with non-zero VAddr.
func (d *denseArray) LiveCount() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.liveCount
}

// GetLiveVAddr is like Get but also checks if entry is "live" (non-zero VAddr).
func (d *denseArray) GetLiveVAddr(key PageID) ([16]byte, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if key == 0 {
		return [16]byte{}, false
	}

	index := key - d.basePageID
	if int(index) >= len(d.entries) || int(index) < 0 {
		return [16]byte{}, false
	}

	entry := d.entries[index]
	if entry.PageID != key {
		return [16]byte{}, false
	}

	if isZeroVAddr(entry.VAddr) {
		return [16]byte{}, false
	}

	return entry.VAddr, true
}

// Iter calls fn for each page_id → vaddr mapping.
func (d *denseArray) Iter(fn func(pageID PageID, vaddr [16]byte)) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for i := range d.entries {
		entry := &d.entries[i]
		if entry.PageID != 0 && !isZeroVAddr(entry.VAddr) {
			fn(entry.PageID, entry.VAddr)
		}
	}
}

// convertVAddrToStruct converts a 16-byte VAddr to vaddr.VAddr struct.
func convertVAddrToStruct(b [16]byte) vaddr.VAddr {
	var segID, offset uint64
	for i := 0; i < 8; i++ {
		segID = segID<<8 | uint64(b[i])
		offset = offset<<8 | uint64(b[8+i])
	}
	return vaddr.VAddr{SegmentID: segID, Offset: offset}
}

// convertVAddrToBytes converts vaddr.VAddr to 16-byte representation.
func convertVAddrToBytes(v vaddr.VAddr) [16]byte {
	var b [16]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(v.SegmentID >> (56 - 8*uint(i)))
		b[8+i] = byte(v.Offset >> (56 - 8*uint(i)))
	}
	return b
}
