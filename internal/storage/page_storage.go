// Package storage provides page-level storage interfaces.
// PageStorage maintains PageID → VAddr mapping for the B-tree layer.
package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// PageID type alias for convenience in this package.
type PageID = vaddr.PageID

// =============================================================================
// PageStorage Interface
// =============================================================================

// PageStorage provides page-level storage with PageID → VAddr mapping.
// This abstraction allows B-tree to use logical PageIDs while the underlying
// storage can be swapped (local file, Redis, remote API, etc.).
//
// Invariant: PageID is stable across writes (append-only semantics).
// Invariant: VAddr may change after WritePage (old VAddr becomes invalid).
// Invariant: PageID → VAddr mapping is maintained internally.
//
// Why this interface exists:
//   - B-tree should not depend on physical storage details
//   - Allows swapping storage backends without changing B-tree code
//   - Enables future features like remote storage, caching layers
type PageStorage interface {
	// CreatePage creates a new page with initial data.
	// Returns a new unique PageID (monotonically increasing).
	// The page is immediately persisted to storage.
	//
	// Invariant: Returned PageID is unique and never reused.
	// Invariant: Returned VAddr points to the persisted location.
	CreatePage(data []byte) (PageID, vaddr.VAddr, error)

	// WritePage updates an existing page (append-only semantics).
	// The data is appended to storage, VAddr may change.
	// PageID → VAddr mapping is updated.
	//
	// Invariant: pageID must be valid (created by CreatePage or prior WritePage).
	// Invariant: Returned VAddr is the new physical location.
	WritePage(pageID PageID, data []byte) (vaddr.VAddr, error)

	// ReadPage reads a page by PageID.
	// Looks up VAddr from mapping, then reads from storage.
	//
	// Invariant: pageID must be valid.
	// Invariant: Returns error if pageID has no valid VAddr mapping.
	ReadPage(pageID PageID) ([]byte, error)

	// DeletePage marks a page as deleted.
	// The PageID → VAddr mapping is invalidated.
	// Physical data may be retained for garbage collection.
	//
	// Invariant: After DeletePage, ReadPage(pageID) returns error.
	// Why retain mapping info? Allows GC to identify orphaned VAddrs.
	DeletePage(pageID PageID) error

	// GetVAddr returns the current VAddr for a PageID.
	// Used for debugging and testing.
	// Returns false if pageID has no valid mapping.
	GetVAddr(pageID PageID) (vaddr.VAddr, bool)

	// GetPageID returns the PageID for a VAddr.
	// Used by tree iterator to resolve sibling links (HighSibling is VAddr).
	// Returns false if VAddr has no known PageID mapping.
	GetPageID(vaddr vaddr.VAddr) (PageID, bool)

	// Close releases all resources.
	Close() error
}

// =============================================================================
// In-Memory PageStorage (for testing)
// =============================================================================

// MemoryPageStorage implements PageStorage with in-memory storage.
// Used for unit testing and when persistence is not required.
// Thread-safe: all operations are protected by a mutex.
//
// Invariant: All data is lost on Close().
type MemoryPageStorage struct {
	mu          sync.Mutex
	pageToVAddr map[PageID]vaddr.VAddr
	vaddrToData map[vaddr.VAddr][]byte
	nextPageID  PageID
	nextVAddr   uint64
}

func NewMemoryPageStorage() *MemoryPageStorage {
	return &MemoryPageStorage{
		pageToVAddr: make(map[PageID]vaddr.VAddr),
		vaddrToData: make(map[vaddr.VAddr][]byte),
		nextPageID:  1,
		nextVAddr:   1,
	}
}

func (m *MemoryPageStorage) CreatePage(data []byte) (PageID, vaddr.VAddr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pageID := m.nextPageID
	m.nextPageID++

	addr := vaddr.VAddr{SegmentID: 1, Offset: m.nextVAddr}
	m.nextVAddr += uint64(len(data))

	// Store a copy to prevent mutation
	stored := make([]byte, len(data))
	copy(stored, data)

	m.pageToVAddr[pageID] = addr
	m.vaddrToData[addr] = stored

	return pageID, addr, nil
}

func (m *MemoryPageStorage) WritePage(pageID PageID, data []byte) (vaddr.VAddr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.pageToVAddr[pageID]; !exists {
		return vaddr.VAddr{}, ErrPageNotFound
	}

	// Append-only: create new VAddr
	addr := vaddr.VAddr{SegmentID: 1, Offset: m.nextVAddr}
	m.nextVAddr += uint64(len(data))

	// Store a copy
	stored := make([]byte, len(data))
	copy(stored, data)

	m.pageToVAddr[pageID] = addr
	m.vaddrToData[addr] = stored

	return addr, nil
}

func (m *MemoryPageStorage) ReadPage(pageID PageID) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	addr, exists := m.pageToVAddr[pageID]
	if !exists {
		return nil, ErrPageNotFound
	}
	data, exists := m.vaddrToData[addr]
	if !exists {
		return nil, ErrPageNotFound
	}
	// Return a copy to prevent callers from corrupting stored data
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (m *MemoryPageStorage) DeletePage(pageID PageID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pageToVAddr, pageID)
	return nil
}

func (m *MemoryPageStorage) GetPageID(v vaddr.VAddr) (PageID, bool) {
	return PageID(0), false
}

func (m *MemoryPageStorage) GetVAddr(pageID PageID) (vaddr.VAddr, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	physAddr, exists := m.pageToVAddr[pageID]
	return physAddr, exists
}

func (m *MemoryPageStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pageToVAddr = nil
	m.vaddrToData = nil
	return nil
}

// =============================================================================
// Errors
// =============================================================================

var (
	// ErrPageNotFound is returned when PageID has no valid mapping.
	ErrPageNotFound = errors.New("page storage: page not found")

	// ErrPageStorageClosed is returned when storage is closed.
	ErrPageStorageClosed = errors.New("page storage: storage is closed")
)

// =============================================================================
// SegmentManager-backed PageStorage
// =============================================================================

// SegmentManagerPageStorage implements PageStorage using SegmentManager.
// This bridges the old SegmentManager interface with the new PageStorage interface.
//
// Invariant: All pages are appended to the active segment.
// Invariant: PageID is monotonically increasing.
// Invariant: Bidirectional mapping: PageID ↔ VAddr is maintained.
type SegmentManagerPageStorage struct {
	segMgr       SegmentManager
	pageToVAddr  map[PageID]vaddr.VAddr
	vaddrToPage  map[vaddr.VAddr]PageID  // Reverse lookup for sibling traversal
	nextPageID   PageID
	mu           struct {
		sync.Mutex
	}
}

func NewSegmentManagerPageStorage(segMgr SegmentManager) *SegmentManagerPageStorage {
	s := &SegmentManagerPageStorage{
		segMgr:       segMgr,
		pageToVAddr:  make(map[PageID]vaddr.VAddr),
		vaddrToPage:  make(map[vaddr.VAddr]PageID),
		nextPageID:   1,
	}
	// Load persisted mapping from manifest on startup
	s.LoadMapping()
	return s
}

// SaveMapping persists the PageID↔VAddr mapping to a manifest file.
// Must be called while holding mu.
func (s *SegmentManagerPageStorage) SaveMapping() error {
	dir := s.segMgr.Directory()
	if dir == "" {
		return nil // No directory set, skip persistence
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	manifestPath := filepath.Join(dir, "page_storage.manifest")

	// Build manifest from current state (must hold mu)
	data := struct {
		NextPageID PageID         `json:"nextPageID"`
		Forward    []forwardEntry `json:"forward"`
		Reverse    []reverseEntry `json:"reverse"`
	}{
		NextPageID: s.nextPageID,
		Forward:    make([]forwardEntry, 0, len(s.pageToVAddr)),
		Reverse:    make([]reverseEntry, 0, len(s.vaddrToPage)),
	}

	for pageID, v := range s.pageToVAddr {
		data.Forward = append(data.Forward, forwardEntry{
			PageID:    uint64(pageID),
			SegmentID: uint64(v.SegmentID),
			Offset:    v.Offset,
		})
	}

	for v, pageID := range s.vaddrToPage {
		data.Reverse = append(data.Reverse, reverseEntry{
			SegmentID: uint64(v.SegmentID),
			Offset:    v.Offset,
			PageID:    uint64(pageID),
		})
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(manifestPath, jsonData, 0644)
}

// forwardEntry represents a PageID → VAddr mapping entry for JSON serialization.
type forwardEntry struct {
	PageID    uint64 `json:"pageID"`
	SegmentID uint64 `json:"segmentID"`
	Offset    uint64 `json:"offset"`
}

// reverseEntry represents a VAddr → PageID mapping entry for JSON serialization.
type reverseEntry struct {
	SegmentID uint64 `json:"segmentID"`
	Offset    uint64 `json:"offset"`
	PageID    uint64 `json:"pageID"`
}

// LoadMapping restores the PageID↔VAddr mapping from a manifest file.
// Must be called while holding mu.
func (s *SegmentManagerPageStorage) LoadMapping() error {
	dir := s.segMgr.Directory()
	if dir == "" {
		return nil // No directory set, skip loading
	}

	manifestPath := filepath.Join(dir, "page_storage.manifest")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No manifest file yet, this is fine on first run
		}
		return err
	}

	var manifest struct {
		NextPageID PageID         `json:"nextPageID"`
		Forward    []forwardEntry `json:"forward"`
		Reverse    []reverseEntry `json:"reverse"`
	}

	if err := json.Unmarshal(data, &manifest); err != nil {
		return err
	}

	// Restore state
	s.nextPageID = manifest.NextPageID

	for _, entry := range manifest.Forward {
		pageID := PageID(entry.PageID)
		v := vaddr.VAddr{
			SegmentID: entry.SegmentID, // VAddr.SegmentID is uint64
			Offset:    entry.Offset,
		}
		s.pageToVAddr[pageID] = v
	}

	for _, entry := range manifest.Reverse {
		v := vaddr.VAddr{
			SegmentID: entry.SegmentID, // VAddr.SegmentID is uint64
			Offset:    entry.Offset,
		}
		pageID := PageID(entry.PageID)
		s.vaddrToPage[v] = pageID
	}

	return nil
}

func (s *SegmentManagerPageStorage) CreatePage(data []byte) (PageID, vaddr.VAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pageID := s.nextPageID
	s.nextPageID++

	// Pad to page alignment
	pageSize := int(vaddr.PageSize)
	if len(data)%pageSize != 0 {
		padding := pageSize - (len(data) % pageSize)
		data = append(data, make([]byte, padding)...)
	}

	seg := s.segMgr.ActiveSegment()
	if seg == nil {
		return 0, vaddr.VAddr{}, ErrSegmentNotActive
	}

	physAddr, err := seg.Append(data)
	if err != nil {
		return 0, vaddr.VAddr{}, err
	}

	s.pageToVAddr[pageID] = physAddr
	s.vaddrToPage[physAddr] = pageID // Reverse mapping for sibling traversal
	
	// Persist mapping to manifest
	s.SaveMapping()
	
	return pageID, physAddr, nil
}

func (s *SegmentManagerPageStorage) WritePage(pageID PageID, data []byte) (vaddr.VAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.pageToVAddr[pageID]
	if !exists {
		return vaddr.VAddr{}, ErrPageNotFound
	}

	// Pad to page alignment
	pageSize := int(vaddr.PageSize)
	if len(data)%pageSize != 0 {
		padding := pageSize - (len(data) % pageSize)
		data = append(data, make([]byte, padding)...)
	}

	seg := s.segMgr.ActiveSegment()
	if seg == nil {
		return vaddr.VAddr{}, ErrSegmentNotActive
	}

	newPhysAddr, err := seg.Append(data)
	if err != nil {
		return vaddr.VAddr{}, err
	}

	// Update forward mapping to new VAddr
	s.pageToVAddr[pageID] = newPhysAddr
	
	// Keep reverse mapping for OLD VAddr - needed for sibling traversal!
	// Other nodes may reference this page via HighSibling pointing to oldPhysAddr
	// We update it so the latest VAddr is returned, but keep historical mapping
	// so GetPageID(oldPhysAddr) still returns pageID (for sibling resolution)
	
	// Add new VAddr mapping (pageID can have multiple VAddrs pointing to it)
	s.vaddrToPage[newPhysAddr] = pageID
	
	// DO NOT delete oldPhysAddr from vaddrToPage!
	// The old VAddr may still be referenced by sibling links in other pages.
	// When iterating via HighSibling, we need to resolve all historical VAddrs.
	
	// Persist mapping to manifest
	s.SaveMapping()
	
	return newPhysAddr, nil
}

func (s *SegmentManagerPageStorage) ReadPage(pageID PageID) ([]byte, error) {
	s.mu.Lock()
	physAddr, exists := s.pageToVAddr[pageID]
	s.mu.Unlock()

	if !exists {
		return nil, ErrPageNotFound
	}

	seg := s.segMgr.GetSegment(vaddr.SegmentID(physAddr.SegmentID))
	if seg == nil {
		return nil, ErrPageNotFound
	}

	return seg.ReadAt(int64(physAddr.Offset), int(vaddr.PageSize))
}

func (s *SegmentManagerPageStorage) DeletePage(pageID PageID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if physAddr, exists := s.pageToVAddr[pageID]; exists {
		delete(s.vaddrToPage, physAddr)
	}
	delete(s.pageToVAddr, pageID)
	return nil
}

func (s *SegmentManagerPageStorage) GetPageID(physAddr vaddr.VAddr) (PageID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pageID, exists := s.vaddrToPage[physAddr]
	return pageID, exists
}

func (s *SegmentManagerPageStorage) GetVAddr(pageID PageID) (vaddr.VAddr, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	physAddr, exists := s.pageToVAddr[pageID]
	return physAddr, exists
}

func (s *SegmentManagerPageStorage) Close() error {
	return nil
}
