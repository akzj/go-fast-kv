package internal

import (
	"bytes"
	"container/list"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

// ─── Page Cache (LRU) ──────────────────────────────────────────────

// pageCache is a thread-safe LRU cache for deserialized B-tree nodes.
// It eliminates redundant pread + deserialize calls for hot pages
// (especially internal nodes that are accessed on every operation).
//
// All nodes stored in and returned from the cache are deep-cloned
// (via cloneNode in btree.go) to prevent aliasing with the B-tree's
// in-place mutations.
type pageCache struct {
	mu       sync.Mutex
	capacity int
	items    map[pagestoreapi.PageID]*pageCacheEntry
	order    *list.List // front = most recently used
}

type pageCacheEntry struct {
	pageID pagestoreapi.PageID
	node   *btreeapi.Node
	elem   *list.Element
}

func newPageCache(capacity int) *pageCache {
	return &pageCache{
		capacity: capacity,
		items:    make(map[pagestoreapi.PageID]*pageCacheEntry, capacity),
		order:    list.New(),
	}
}

// Get returns a deep clone of the cached node, or (nil, false) on miss.
func (c *pageCache) Get(pageID pagestoreapi.PageID) (*btreeapi.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[pageID]; ok {
		c.order.MoveToFront(entry.elem)
		return cloneNode(entry.node), true
	}
	return nil, false
}

// Put stores the node directly in the cache (no clone), evicting the LRU
// entry if at capacity.
//
// The stored reference is safe because:
// - hotNodes holds the original reference for zero-copy reads
// - pageCache.Get returns a clone to protect callers from mutations
// - WritePage always replaces hotNodes before calling Put
func (c *pageCache) Put(pageID pagestoreapi.PageID, node *btreeapi.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[pageID]; ok {
		entry.node = node
		c.order.MoveToFront(entry.elem)
		return
	}
	// Evict LRU if at capacity.
	if c.order.Len() >= c.capacity {
		back := c.order.Back()
		if back != nil {
			evicted := back.Value.(*pageCacheEntry)
			delete(c.items, evicted.pageID)
			c.order.Remove(back)
		}
	}
	entry := &pageCacheEntry{pageID: pageID, node: node}
	entry.elem = c.order.PushFront(entry)
	c.items[pageID] = entry
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
// An LRU page cache eliminates redundant pread+deserialize for hot pages.
// All cached nodes are deep-cloned on Get and Put to prevent aliasing
// with the B-tree's in-place mutations.
//
// WAL entry collection supports two modes:
//   - Per-operation collectors (RegisterCollector): for concurrent KVStore operations.
//     WritePage routes entries to the collector registered for the current goroutine.
//   - Shared buffer (legacy): for tests that don't use collectors.
//     WritePage appends to the shared walEntries slice.
type RealPageProvider struct {
	store      pagestoreapi.PageStore
	serializer btreeapi.NodeSerializer
	cache      *pageCache // LRU cache for deserialized nodes

	// Hot node cache: stores original (non-cloned) nodes for zero-copy reads.
	// ReadPage returns directly from hotNodes on hit — no cloneNode allocation.
	// Protected by hotMu to allow concurrent read-only access.
	hotMu    sync.Mutex
	hotNodes map[pagestoreapi.PageID]*btreeapi.Node

	// Per-operation WAL entry collectors, keyed by goroutine ID.
	collectors sync.Map // map[int64]*WALCollector

	// Legacy shared buffer (for tests that don't use collectors).
	mu         sync.Mutex
	walEntries []pagestoreapi.WALEntry

	// Page operation counters for metrics/analysis.
	// Used to detect query path issues (e.g., sequential scan vs hierarchical lookup).
	pageReads      atomic.Uint64 // Total ReadPage calls
	pageWrites     atomic.Uint64 // Total WritePage calls
	pageCacheHits  atomic.Uint64 // Cache hits (no disk I/O)
	pageAlloc      atomic.Uint64 // Total AllocPage calls

	// Latency tracking for page I/O (microseconds).
	// Uses atomic operations for thread safety.
	readLatencyNanos  atomic.Uint64 // Accumulated read latency (ns)
	readLatencyCount  atomic.Uint64 // Number of reads sampled
	writeLatencyNanos atomic.Uint64 // Accumulated write latency (ns)
	writeLatencyCount atomic.Uint64 // Number of writes sampled
}

// NewRealPageProvider creates a RealPageProvider backed by the given PageStore.
func NewRealPageProvider(store pagestoreapi.PageStore, cacheSize int) *RealPageProvider {
	if cacheSize <= 0 {
		cacheSize = 1024
	}
	return &RealPageProvider{
		store:      store,
		serializer: NewNodeSerializer(),
		cache:      newPageCache(cacheSize),
		hotNodes:   make(map[pagestoreapi.PageID]*btreeapi.Node),
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

// CollectAndClear retrieves all WAL entries from the current goroutine's collector,
// clears the collector, and returns the entries. Used by SQL transaction commit
// to gather WAL entries for the transaction's deferred-write operations.
//
// Returns nil if no collector is registered for the current goroutine.
func (p *RealPageProvider) CollectAndClear() []pagestoreapi.WALEntry {
	gid := goroutineID()
	if c, ok := p.collectors.LoadAndDelete(gid); ok {
		collector := c.(*WALCollector)
		entries := make([]pagestoreapi.WALEntry, len(collector.PageEntries))
		copy(entries, collector.PageEntries)
		return entries
	}
	return nil
}

// AllocPage allocates a new page via the underlying PageStore.
func (p *RealPageProvider) AllocPage() pagestoreapi.PageID {
	p.pageAlloc.Add(1)
	return p.store.Alloc()
}

// ReadPage reads and deserializes a node from the given PageID.
//
// Hot node cache optimization: on first access, the original (non-cloned) node
// is stored in hotNodes. Subsequent reads return directly from hotNodes — zero
// allocation, zero deserialize. This eliminates the cloneNode overhead on
// repeated accesses to hot internal nodes.
//
// Falls back to LRU cache for nodes not in hotNodes (e.g., nodes evicted from
// hotNodes to manage memory).
func (p *RealPageProvider) ReadPage(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	startNs := time.Now().UnixNano()
	p.pageReads.Add(1)

	// Hot node cache: zero-copy on repeated accesses.
	p.hotMu.Lock()
	if node, ok := p.hotNodes[pageID]; ok {
		p.hotMu.Unlock()
		p.pageCacheHits.Add(1)
		return node, nil
	}
	p.hotMu.Unlock()

	// Fallback: LRU cache (returns a clone).
	if node, ok := p.cache.Get(pageID); ok {
		p.pageCacheHits.Add(1)
		return node, nil
	}

	// Cache miss — read from disk.
	data, err := p.store.Read(pageID)
	if err != nil {
		return nil, fmt.Errorf("realpage: read page %d: %w", pageID, err)
	}
	node, err := p.serializer.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("realpage: deserialize page %d: %w", pageID, err)
	}

	// Populate both caches.
	p.hotMu.Lock()
	p.hotNodes[pageID] = node
	p.hotMu.Unlock()
	p.cache.Put(pageID, node)

	// Track latency (cache miss includes disk I/O).
	latencyNs := time.Now().UnixNano() - startNs
	p.readLatencyNanos.Add(uint64(latencyNs))
	p.readLatencyCount.Add(1)

	// Return the original — hotNodes holds it without cloning.
	return node, nil
}

// ReadPageUncached reads directly from the underlying PageStore without
// going through the LRU cache. No cloneNode is performed.
//
// This is used by vacuum to scan leaf pages without triggering clone allocations.
// The caller must hold the appropriate page lock (ensured by vacuum's per-page
// write locks acquired before calling this method).
func (p *RealPageProvider) ReadPageUncached(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	data, err := p.store.Read(pageID)
	if err != nil {
		return nil, fmt.Errorf("realpage: uncached read page %d: %w", pageID, err)
	}
	return p.serializer.Deserialize(data)
}

// WritePage serializes and writes a node to the given PageID.
// After a successful write, both caches are updated so subsequent reads
// see the latest version without hitting disk.
//
// Hot node cache is updated directly (no clone) to maintain zero-copy reads.
// LRU cache stores a clone for eviction safety.
//
// If a collector is registered for the current goroutine (via RegisterCollector),
// the WAL entry is appended to that collector. Otherwise, it falls back to the
// shared walEntries buffer (legacy mode for tests).
func (p *RealPageProvider) WritePage(pageID pagestoreapi.PageID, node *btreeapi.Node) error {
	startNs := time.Now().UnixNano()
	p.pageWrites.Add(1)

	data, err := p.serializer.Serialize(node)
	if err != nil {
		return fmt.Errorf("realpage: serialize page %d: %w", pageID, err)
	}
	entry, err := p.store.Write(pageID, data)
	if err != nil {
		return fmt.Errorf("realpage: write page %d: %w", pageID, err)
	}

	// Update both caches with the new version.
	// hotNodes: direct store (zero-copy)
	p.hotMu.Lock()
	p.hotNodes[pageID] = node
	p.hotMu.Unlock()
	// cache: stores a clone for eviction safety.
	p.cache.Put(pageID, node)

	// Track write latency (includes serialization + disk I/O).
	latencyNs := time.Now().UnixNano() - startNs
	p.writeLatencyNanos.Add(uint64(latencyNs))
	p.writeLatencyCount.Add(1)

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

// ─── Metrics Accessors ────────────────────────────────────────────────

// PageStats holds page-level operation statistics for B-tree analysis.
type PageStats struct {
	PageReads           uint64
	PageWrites          uint64
	PageCacheHits       uint64
	PageAlloc           uint64
	ReadLatencyNanos    uint64 // Total read latency (ns)
	ReadLatencyCount    uint64
	WriteLatencyNanos   uint64 // Total write latency (ns)
	WriteLatencyCount   uint64
}

// GetStats returns a snapshot of page operation statistics.
// Used for metrics collection and bottleneck analysis.
func (p *RealPageProvider) GetStats() PageStats {
	return PageStats{
		PageReads:         p.pageReads.Load(),
		PageWrites:        p.pageWrites.Load(),
		PageCacheHits:    p.pageCacheHits.Load(),
		PageAlloc:        p.pageAlloc.Load(),
		ReadLatencyNanos:  p.readLatencyNanos.Load(),
		ReadLatencyCount:  p.readLatencyCount.Load(),
		WriteLatencyNanos: p.writeLatencyNanos.Load(),
		WriteLatencyCount: p.writeLatencyCount.Load(),
	}
}

// ResetStats clears all page operation counters.
// Used between benchmark runs to measure isolated workloads.
func (p *RealPageProvider) ResetStats() {
	p.pageReads.Store(0)
	p.pageWrites.Store(0)
	p.pageCacheHits.Store(0)
	p.pageAlloc.Store(0)
	p.readLatencyNanos.Store(0)
	p.readLatencyCount.Store(0)
	p.writeLatencyNanos.Store(0)
	p.writeLatencyCount.Store(0)
}
