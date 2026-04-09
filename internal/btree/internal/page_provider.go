package internal

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
)

// ─── goroutineID ────────────────────────────────────────────────────

// goroutineID returns the current goroutine's numeric ID.
// Used to route WAL entries to per-operation collectors.
// Cost: ~200ns — acceptable for functions that do disk I/O.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// buf looks like "goroutine 123 [running]:\n..."
	s := buf[:n]
	s = s[len("goroutine "):]
	s = s[:bytes.IndexByte(s, ' ')]
	id, _ := strconv.ParseInt(string(s), 10, 64)
	return id
}

// ─── WALCollector ───────────────────────────────────────────────────

// WALCollector collects WAL entries for a single KVStore operation.
// Each concurrent Put/Delete registers its own collector, so entries
// are never mixed between operations.
type WALCollector struct {
	PageEntries []pagestoreapi.WALEntry
}

// ─── RealPageProvider ───────────────────────────────────────────────

// RealPageProvider adapts a PageStore to the btreeapi.PageProvider interface.
// It serializes/deserializes nodes and delegates storage to PageStore.
//
// WAL entry collection supports two modes:
//   - Per-operation collectors (RegisterCollector): for concurrent KVStore operations.
//     WritePage routes entries to the collector registered for the current goroutine.
//   - Shared buffer (legacy): for tests that don't use collectors.
//     WritePage appends to the shared walEntries slice.
type RealPageProvider struct {
	store      pagestoreapi.PageStore
	serializer btreeapi.NodeSerializer

	// Per-operation WAL entry collectors, keyed by goroutine ID.
	collectors sync.Map // map[int64]*WALCollector

	// Legacy shared buffer (for tests that don't use collectors).
	mu         sync.Mutex
	walEntries []pagestoreapi.WALEntry
}

// NewRealPageProvider creates a RealPageProvider backed by the given PageStore.
func NewRealPageProvider(store pagestoreapi.PageStore) *RealPageProvider {
	return &RealPageProvider{
		store:      store,
		serializer: NewNodeSerializer(),
	}
}

// RegisterCollector creates a per-operation WAL entry collector and registers
// it for the current goroutine. Returns the collector and an unregister function.
// The caller MUST call the unregister function when the operation is complete.
//
// Usage:
//
//	collector, unreg := provider.RegisterCollector()
//	defer unreg()
//	tree.Put(key, value, xid)  // WritePage routes entries to collector
//	entries := collector.PageEntries
func (p *RealPageProvider) RegisterCollector() (*WALCollector, func()) {
	gid := goroutineID()
	c := &WALCollector{}
	p.collectors.Store(gid, c)
	return c, func() { p.collectors.Delete(gid) }
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
// If a collector is registered for the current goroutine (via RegisterCollector),
// the WAL entry is appended to that collector. Otherwise, it falls back to the
// shared walEntries buffer (legacy mode for tests).
func (p *RealPageProvider) WritePage(pageID pagestoreapi.PageID, node *btreeapi.Node) error {
	data, err := p.serializer.Serialize(node)
	if err != nil {
		return fmt.Errorf("realpage: serialize page %d: %w", pageID, err)
	}
	entry, err := p.store.Write(pageID, data)
	if err != nil {
		return fmt.Errorf("realpage: write page %d: %w", pageID, err)
	}

	// Route WAL entry to per-operation collector or shared buffer.
	gid := goroutineID()
	if c, ok := p.collectors.Load(gid); ok {
		collector := c.(*WALCollector)
		collector.PageEntries = append(collector.PageEntries, entry)
	} else {
		// Legacy fallback: shared buffer (for tests).
		p.mu.Lock()
		p.walEntries = append(p.walEntries, entry)
		p.mu.Unlock()
	}
	return nil
}

// WALEntries returns all collected WALEntries from the shared buffer.
// Used for persistence testing — these entries can be replayed via
// PageStoreRecovery.ApplyPageMap to restore the mapping table.
func (p *RealPageProvider) WALEntries() []pagestoreapi.WALEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]pagestoreapi.WALEntry, len(p.walEntries))
	copy(out, p.walEntries)
	return out
}

// DrainWALEntries returns all collected WALEntries from the shared buffer
// and clears it. Used by legacy code paths and tests.
func (p *RealPageProvider) DrainWALEntries() []pagestoreapi.WALEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.walEntries
	p.walEntries = nil
	return out
}
