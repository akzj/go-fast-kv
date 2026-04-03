package internal

import (
	"fmt"
	"sync"

	storage "github.com/akzj/go-fast-kv/internal/storage"
	api "github.com/akzj/go-fast-kv/internal/pagemanager/api"
)

// pageManager implements the api.PageManager interface.
type pageManager struct {
	mu             sync.RWMutex
	config         PageManagerConfig
	index          *denseArray           // DenseArray for PageID → VAddr
	freeList       *freeList             // Free list for page reuse
	segmentManager storage.SegmentManager // For allocating VAddrs
	nextPageID     api.PageID            // Next PageID to allocate (when not reusing)
	closed         bool
}

// PageManagerConfig holds configuration for the page manager.
type PageManagerConfig struct {
	InitialPageCount uint64
	GrowFactor       float64
	IndexType        api.IndexType
}

// DefaultPageManagerConfig returns the default configuration.
func DefaultPageManagerConfig() PageManagerConfig {
	return PageManagerConfig{
		InitialPageCount: 1024,
		GrowFactor:       1.5,
		IndexType:        api.IndexTypeDenseArray,
	}
}

// NewPageManager creates a new page manager with the given segment manager.
func NewPageManager(segmentManager storage.SegmentManager, config PageManagerConfig) (api.PageManager, error) {
	if config.InitialPageCount == 0 {
		config.InitialPageCount = 1024
	}
	if config.GrowFactor == 0 {
		config.GrowFactor = 1.5
	}
	if config.IndexType == 0 {
		config.IndexType = api.IndexTypeDenseArray
	}

	pm := &pageManager{
		config:         config,
		index:         newDenseArray(config.InitialPageCount),
		freeList:      newFreeList(),
		segmentManager: segmentManager,
		nextPageID:    1, // Start at 1 (0 is invalid)
	}

	return pm, nil
}

// GetVAddr returns the VAddr for pageID, or zero VAddr if not allocated.
func (pm *pageManager) GetVAddr(pageID api.PageID) [16]byte {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.closed {
		return [16]byte{}
	}

	return pm.index.Get(pageID)
}

// AllocatePage allocates a new page and returns its PageID and VAddr.
func (pm *pageManager) AllocatePage() (api.PageID, [16]byte) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.closed {
		return 0, [16]byte{}
	}

	var pageID api.PageID

	// Try to reuse from free list first
	if id, ok := pm.freeList.Pop(); ok {
		pageID = id
	} else {
		// Allocate new PageID
		pageID = pm.nextPageID
		pm.nextPageID++
	}

	// Allocate VAddr from segment manager
	segment := pm.segmentManager.ActiveSegment()
	if segment == nil {
		// Create a new segment if none exists
		var err error
		segment, err = pm.segmentManager.CreateSegment()
		if err != nil {
			// Push pageID back to free list on failure
			pm.freeList.Push(pageID)
			return 0, [16]byte{}
		}
	}

	// Allocate a page-sized chunk (4096 bytes)
	page := make([]byte, 4096)
	vaddr_, err := segment.Append(page)
	if err != nil {
		// Push pageID back to free list on failure
		pm.freeList.Push(pageID)
		return 0, [16]byte{}
	}

	// Convert VAddr to 16-byte format
	vaddrBytes := convertVAddrToBytes(vaddr_)

	// Store mapping
	pm.index.Put(pageID, vaddrBytes)

	return pageID, vaddrBytes
}

// FreePage marks a page as reclaimable.
func (pm *pageManager) FreePage(pageID api.PageID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.closed || pageID == 0 {
		return
	}

	// Get current VAddr
	vaddrBytes := pm.index.Get(pageID)
	if isZeroVAddr(vaddrBytes) {
		return // Already freed
	}

	// Mark as tombstone (zero VAddr)
	pm.index.Put(pageID, [16]byte{})

	// Add to free list for reuse
	pm.freeList.Push(pageID)
}

// UpdateMapping records that pageID now lives at vaddr.
func (pm *pageManager) UpdateMapping(pageID api.PageID, vaddr [16]byte) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.closed || pageID == 0 {
		return
	}

	// Validate VAddr is not zero
	if isZeroVAddr(vaddr) {
		return
	}

	pm.index.Put(pageID, vaddr)
}

// PageCount returns the total number of allocated pages.
// Does not subtract freed pages.
func (pm *pageManager) PageCount() uint64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// This is approximately nextPageID - 1 minus freed pages that haven't been reused
	return uint64(pm.nextPageID - 1)
}

// LivePageCount returns pages that have valid (non-zero) VAddrs.
func (pm *pageManager) LivePageCount() uint64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.index.LiveCount()
}

// Iter calls fn for each page_id → vaddr mapping.
func (pm *pageManager) Iter(fn func(pageID api.PageID, vaddr [16]byte)) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.closed {
		return
	}

	pm.index.Iter(fn)
}

// Flush ensures durable storage of the index.
func (pm *pageManager) Flush() error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.closed {
		return api.ErrClosed
	}

	// Flush the active segment
	segment := pm.segmentManager.ActiveSegment()
	if segment != nil {
		if err := segment.Sync(); err != nil {
			return fmt.Errorf("flush segment: %w", err)
		}
	}

	return nil
}

// Close releases resources held by the PageManager.
func (pm *pageManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.closed {
		return nil
	}
	pm.closed = true

	// Clear the free list
	pm.freeList.Clear()

	return nil
}
