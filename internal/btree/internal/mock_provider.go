package internal

import (
	"fmt"
	"sync"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// MemPageProvider is an in-memory PageProvider for testing.
// It stores pages as raw []byte buffers (slotted page format).
type MemPageProvider struct {
	mu         sync.Mutex
	pages      map[uint64][]byte // pageID → raw page bytes (copy)
	nextPageID uint64
}

// NewMemPageProvider creates a new in-memory PageProvider.
func NewMemPageProvider() *MemPageProvider {
	return &MemPageProvider{
		pages:      make(map[uint64][]byte),
		nextPageID: 1,
	}
}

// AllocPage allocates a new PageID.
func (m *MemPageProvider) AllocPage() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextPageID
	m.nextPageID++
	return id
}

// ReadPage reads a page from the given PageID.
// Returns a *Page wrapping a copy of the stored bytes.
func (m *MemPageProvider) ReadPage(pageID uint64) (*Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.pages[pageID]
	if !ok {
		return nil, fmt.Errorf("mempage: page %d not found", pageID)
	}
	// Return a copy so mutations don't affect stored data
	cp := make([]byte, len(data))
	copy(cp, data)
	return PageFromBytes(cp), nil
}

// ReadPageForWrite returns a clone of the page, safe for in-place mutation.
func (m *MemPageProvider) ReadPageForWrite(pageID uint64) (*Page, error) {
	// MemPageProvider.ReadPage already returns a copy, but we clone again
	// to match the contract: the returned page must be independently mutable.
	return m.ReadPage(pageID)
}

// ReadPageUncached reads directly without cache.
// MemPageProvider has no cache, so this is identical to ReadPage.
func (m *MemPageProvider) ReadPageUncached(pageID uint64) (*Page, error) {
	return m.ReadPage(pageID)
}

// WritePage writes a page to the given PageID.
// Stores a copy of the page's data.
func (m *MemPageProvider) WritePage(pageID uint64, page *Page) error {
	data := make([]byte, btreeapi.PageSize)
	copy(data, page.Data())
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pages[pageID] = data
	return nil
}

// ReadPageNode reads and deserializes a node from the given PageID.
// This implements btreeapi.PageProvider for backward compatibility
// with vacuum and other external consumers.
func (m *MemPageProvider) ReadPageNode(pageID uint64) (*btreeapi.Node, error) {
	page, err := m.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	return PageToNode(page), nil
}

// ReadPageNodeUncached reads directly without cache (same as ReadPageNode).
func (m *MemPageProvider) ReadPageNodeUncached(pageID uint64) (*btreeapi.Node, error) {
	return m.ReadPageNode(pageID)
}

// WritePageNode converts a *btreeapi.Node to a slotted *Page and writes it.
// This implements btreeapi.PageProvider for backward compatibility.
func (m *MemPageProvider) WritePageNode(pageID uint64, node *btreeapi.Node) error {
	page := NodeToPage(node)
	return m.WritePage(pageID, page)
}

// PageCount returns the number of stored pages (for testing).
func (m *MemPageProvider) PageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pages)
}
