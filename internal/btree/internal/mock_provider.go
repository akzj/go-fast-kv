package internal

import (
	"fmt"
	"sync"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// MemPageProvider is an in-memory PageProvider for testing.
// It stores nodes in a map and validates serialization round-trips.
type MemPageProvider struct {
	mu         sync.Mutex
	pages      map[uint64][]byte // pageID → serialized node
	nextPageID uint64
	serializer btreeapi.NodeSerializer
}

// NewMemPageProvider creates a new in-memory PageProvider.
func NewMemPageProvider() *MemPageProvider {
	return &MemPageProvider{
		pages:      make(map[uint64][]byte),
		nextPageID: 1,
		serializer: NewNodeSerializer(),
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

// ReadPage reads and deserializes a node from the given PageID.
func (m *MemPageProvider) ReadPage(pageID uint64) (*btreeapi.Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.pages[pageID]
	if !ok {
		return nil, fmt.Errorf("mempage: page %d not found", pageID)
	}
	return m.serializer.Deserialize(data)
}

// ReadPageUncached reads directly without cache.
// MemPageProvider has no cache, so this is identical to ReadPage.
func (m *MemPageProvider) ReadPageUncached(pageID uint64) (*btreeapi.Node, error) {
	return m.ReadPage(pageID)
}

// WritePage serializes and writes a node to the given PageID.
func (m *MemPageProvider) WritePage(pageID uint64, node *btreeapi.Node) error {
	data, err := m.serializer.Serialize(node)
	if err != nil {
		return fmt.Errorf("mempage: serialize page %d: %w", pageID, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pages[pageID] = data
	return nil
}

// PageCount returns the number of stored pages (for testing).
func (m *MemPageProvider) PageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pages)
}
