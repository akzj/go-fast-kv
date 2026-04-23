package internal

import (
	"sync"
)

// DirectMemPageProvider stores raw page bytes directly in memory.
// This eliminates Serialize/Deserialize overhead - pages are stored
// as []byte and returned directly without any conversion.
//
// This is the "ideal" in-memory case for benchmarking B-tree operations
// without any serialization cost.
type DirectMemPageProvider struct {
	mu      sync.Mutex
	pages   map[uint64][]byte
	nextPID uint64
}

// NewDirectMemPageProvider creates a new DirectMemPageProvider.
func NewDirectMemPageProvider() *DirectMemPageProvider {
	return &DirectMemPageProvider{
		pages:   make(map[uint64][]byte),
		nextPID: 1,
	}
}

// AllocPage allocates a new PageID.
func (p *DirectMemPageProvider) AllocPage() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.nextPID
	p.nextPID++
	return id
}

// ReadPage returns the raw page bytes directly (no Deserialize).
func (p *DirectMemPageProvider) ReadPage(pageID uint64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, ok := p.pages[pageID]
	if !ok {
		return nil, nil // Return nil, nil for non-existent pages (root = 0 case)
	}
	return data, nil
}

// WritePage stores the raw page bytes directly (no Serialize).
func (p *DirectMemPageProvider) WritePage(pageID uint64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pages[pageID] = data
	if pageID >= p.nextPID {
		p.nextPID = pageID + 1
	}
	return nil
}

// PageCount returns the number of stored pages.
func (p *DirectMemPageProvider) PageCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pages)
}
