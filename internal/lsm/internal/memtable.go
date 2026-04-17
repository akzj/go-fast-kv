package internal

import (
	"sync"
)

// ─── Memtable ─────────────────────────────────────────────────────

// memtable is an in-memory skip list for storing mappings.
// Uses sync.Map for concurrent access.
type memtable struct {
	pageMappings sync.Map // key=uint64, value=uint64 (packed vaddr)
	blobMappings sync.Map // key=uint64, value=uint64 (packed blob meta)
	size         int64
	mu           sync.Mutex
}

// packedBlobMeta packs (vaddr, size) into a single uint64.
func packBlobMeta(vaddr uint64, size uint32) uint64 {
	return (vaddr << 32) | uint64(size)
}

// unpackBlobMeta unpacks (vaddr, size) from a single uint64.
func unpackBlobMeta(packed uint64) (vaddr uint64, size uint32) {
	vaddr = packed >> 32
	size = uint32(packed)
	return
}

// newMemtable creates a new memtable.
func newMemtable() *memtable {
	return &memtable{}
}

// Size returns the approximate size of the memtable in bytes.
func (m *memtable) Size() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

// SetPageMapping sets a page mapping.
func (m *memtable) SetPageMapping(pageID uint64, vaddr uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, loaded := m.pageMappings.Load(pageID)
	if loaded {
		m.size -= 16 // Update: remove old entry overhead
	} else {
		m.size += 16 // New entry: key + value
	}
	m.pageMappings.Store(pageID, vaddr)
}

// GetPageMapping gets a page mapping.
func (m *memtable) GetPageMapping(pageID uint64) (vaddr uint64, ok bool) {
	val, ok := m.pageMappings.Load(pageID)
	if !ok {
		return 0, false
	}
	return val.(uint64), true
}

// SetBlobMapping sets a blob mapping.
func (m *memtable) SetBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	packed := packBlobMeta(vaddr, size)
	_, loaded := m.blobMappings.Load(blobID)
	if loaded {
		m.size -= 16
	} else {
		m.size += 16
	}
	m.blobMappings.Store(blobID, packed)
}

// GetBlobMapping gets a blob mapping.
func (m *memtable) GetBlobMapping(blobID uint64) (vaddr uint64, size uint32, ok bool) {
	val, ok := m.blobMappings.Load(blobID)
	if !ok {
		return 0, 0, false
	}
	packed := val.(uint64)
	vaddr = packed >> 32
	size = uint32(packed)
	return vaddr, size, true
}

// DeletePageMapping deletes a page mapping.
func (m *memtable) DeletePageMapping(pageID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.pageMappings.Load(pageID); ok {
		m.pageMappings.Delete(pageID)
		m.size -= 16
	}
}

// DeleteBlobMapping deletes a blob mapping.
func (m *memtable) DeleteBlobMapping(blobID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.blobMappings.Load(blobID); ok {
		m.blobMappings.Delete(blobID)
		m.size -= 16
	}
}

// RangePages iterates over all page mappings.
func (m *memtable) RangePages(fn func(pageID uint64, vaddr uint64) bool) {
	m.pageMappings.Range(func(k, v interface{}) bool {
		return fn(k.(uint64), v.(uint64))
	})
}

// RangeBlobs iterates over all blob mappings.
func (m *memtable) RangeBlobs(fn func(blobID uint64, vaddr uint64, size uint32) bool) {
	m.blobMappings.Range(func(k, v interface{}) bool {
		packed := v.(uint64)
		vaddr, size := unpackBlobMeta(packed)
		return fn(k.(uint64), vaddr, size)
	})
}

// Len returns the number of entries in the memtable.
func (m *memtable) Len() int {
	count := 0
	m.pageMappings.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	m.blobMappings.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}
