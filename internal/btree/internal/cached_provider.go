package internal

import (
	"fmt"
	"sync"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// CachedMemPageProvider stores deserialized nodes directly in memory.
// It avoids Serialize/Deserialize overhead for pure in-memory benchmarks,
// giving true B-tree algorithm performance without I/O cost.
//
// This is equivalent to a page cache that always hits.
type CachedMemPageProvider struct {
	mu      sync.Mutex
	nodes   map[uint64]*btreeapi.Node
	nextPID uint64
}

// NewCachedMemPageProvider creates a new CachedMemPageProvider.
func NewCachedMemPageProvider() *CachedMemPageProvider {
	return &CachedMemPageProvider{
		nodes:   make(map[uint64]*btreeapi.Node),
		nextPID: 1,
	}
}

// AllocPage allocates a new PageID.
func (p *CachedMemPageProvider) AllocPage() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.nextPID
	p.nextPID++
	return id
}

// ReadPage returns the cached node directly (no Deserialize).
func (p *CachedMemPageProvider) ReadPage(pageID uint64) (*btreeapi.Node, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	node, ok := p.nodes[pageID]
	if !ok {
		return nil, fmt.Errorf("cachedmempage: page %d not found", pageID)
	}
	return node, nil
}

// ReadPageUncached is identical to ReadPage (no cache to bypass).
func (p *CachedMemPageProvider) ReadPageUncached(pageID uint64) (*btreeapi.Node, error) {
	return p.ReadPage(pageID)
}

// WritePage stores the node directly (no Serialize).
func (p *CachedMemPageProvider) WritePage(pageID uint64, node *btreeapi.Node) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodes[pageID] = node
	if pageID >= p.nextPID {
		p.nextPID = pageID + 1
	}
	return nil
}

// PageCount returns the number of stored pages (for testing).
func (p *CachedMemPageProvider) PageCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.nodes)
}