package internal

import (
	"fmt"
	"sync"
)

// CachedMemPageProvider stores pages directly in memory (no serialize/deserialize).
// It avoids disk I/O overhead for pure in-memory benchmarks,
// giving true B-tree algorithm performance.
//
// This is equivalent to a page cache that always hits.
type CachedMemPageProvider struct {
	mu      sync.Mutex
	pages   map[uint64]*Page
	nextPID uint64
}

// NewCachedMemPageProvider creates a new CachedMemPageProvider.
func NewCachedMemPageProvider() *CachedMemPageProvider {
	return &CachedMemPageProvider{
		pages:   make(map[uint64]*Page),
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

// ReadPage returns the page directly (no deserialize).
func (p *CachedMemPageProvider) ReadPage(pageID uint64) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	page, ok := p.pages[pageID]
	if !ok {
		return nil, fmt.Errorf("cachedmempage: page %d not found", pageID)
	}
	return page, nil
}

// ReadPageForWrite returns a clone of the page, safe for in-place mutation.
// Uses pooled buffers to reduce GC pressure.
func (p *CachedMemPageProvider) ReadPageForWrite(pageID uint64) (*Page, error) {
	page, err := p.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	return page.ClonePooled(), nil
}

// ReadPageUncached is identical to ReadPage (no cache to bypass).
func (p *CachedMemPageProvider) ReadPageUncached(pageID uint64) (*Page, error) {
	return p.ReadPage(pageID)
}

// WritePage stores the page directly (no serialize).
// Releases old page's pooled buffer (if any) before replacing.
func (p *CachedMemPageProvider) WritePage(pageID uint64, page *Page) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.pages[pageID]; ok && old != nil {
		old.ReleaseToPool()
	}
	p.pages[pageID] = page
	if pageID >= p.nextPID {
		p.nextPID = pageID + 1
	}
	return nil
}

// PageCount returns the number of stored pages (for testing).
func (p *CachedMemPageProvider) PageCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pages)
}
