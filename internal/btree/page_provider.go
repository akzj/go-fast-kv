package btree

import (
	"fmt"
	"sync"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
)

// RealPageProvider adapts a PageStore to the btreeapi.PageProvider interface.
// It serializes/deserializes nodes and delegates storage to PageStore.
//
// WALEntries from Write operations are collected internally and can be
// retrieved via WALEntries() for recovery testing (Phase 6).
// In Phase 7+, the KVStore will handle WAL batching directly.
type RealPageProvider struct {
	mu         sync.Mutex
	store      pagestoreapi.PageStore
	serializer btreeapi.NodeSerializer
	walEntries []pagestoreapi.WALEntry
}

// NewRealPageProvider creates a RealPageProvider backed by the given PageStore.
func NewRealPageProvider(store pagestoreapi.PageStore) *RealPageProvider {
	return &RealPageProvider{
		store:      store,
		serializer: NewNodeSerializer(),
	}
}

// AllocPage allocates a new page via the underlying PageStore.
func (p *RealPageProvider) AllocPage() pagestoreapi.PageID {
	return p.store.Alloc()
}

// ReadPage reads and deserializes a node from the given PageID.
func (p *RealPageProvider) ReadPage(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	data, err := p.store.Read(pageID)
	if err != nil {
		return nil, fmt.Errorf("realpage: read page %d: %w", pageID, err)
	}
	node, err := p.serializer.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("realpage: deserialize page %d: %w", pageID, err)
	}
	return node, nil
}

// WritePage serializes and writes a node to the given PageID.
// The WALEntry from the write is collected internally.
func (p *RealPageProvider) WritePage(pageID pagestoreapi.PageID, node *btreeapi.Node) error {
	data, err := p.serializer.Serialize(node)
	if err != nil {
		return fmt.Errorf("realpage: serialize page %d: %w", pageID, err)
	}
	entry, err := p.store.Write(pageID, data)
	if err != nil {
		return fmt.Errorf("realpage: write page %d: %w", pageID, err)
	}
	p.mu.Lock()
	p.walEntries = append(p.walEntries, entry)
	p.mu.Unlock()
	return nil
}

// WALEntries returns all collected WALEntries from Write operations.
// Used for persistence testing — these entries can be replayed via
// PageStoreRecovery.ApplyPageMap to restore the mapping table.
func (p *RealPageProvider) WALEntries() []pagestoreapi.WALEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]pagestoreapi.WALEntry, len(p.walEntries))
	copy(out, p.walEntries)
	return out
}

// DrainWALEntries returns all collected WALEntries and clears the buffer.
// Used by KVStore to collect WAL entries for a single operation's batch.
func (p *RealPageProvider) DrainWALEntries() []pagestoreapi.WALEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.walEntries
	p.walEntries = nil
	return out
}
