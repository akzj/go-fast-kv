package internal

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/goid"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
)

// walCollectorPool reuses WALCollector objects. Each Put/Delete operation
// registers a collector (~1.9M objects per 1M writes); pooling avoids
// repeated heap allocation.
var walCollectorPool = sync.Pool{
	New: func() interface{} {
		return &WALCollector{}
	},
}

// ─── Page Cache (LRU) ──────────────────────────────────────────────

// pageCache is a thread-safe LRU cache for B-tree pages.
// It eliminates redundant pread calls for hot pages
// (especially internal nodes that are accessed on every operation).
type pageCache struct {
	mu       sync.Mutex
	capacity int
	items    map[pagestoreapi.PageID]*pageCacheEntry
	order    *list.List // front = most recently used
}

type pageCacheEntry struct {
	pageID pagestoreapi.PageID
	page   *Page
	elem   *list.Element
}

func newPageCache(capacity int) *pageCache {
	return &pageCache{
		capacity: capacity,
		items:    make(map[pagestoreapi.PageID]*pageCacheEntry, capacity),
		order:    list.New(),
	}
}

// Get returns a clone of the cached page, or (nil, false) on miss.
// Uses pooled buffers to reduce GC pressure.
func (c *pageCache) Get(pageID pagestoreapi.PageID) (*Page, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[pageID]; ok {
		c.order.MoveToFront(entry.elem)
		return entry.page.ClonePooled(), true
	}
	return nil, false
}

// Put stores the page in the cache, evicting the LRU entry if at capacity.
// Evicted pages have their pooled buffers returned to the pool.
func (c *pageCache) Put(pageID pagestoreapi.PageID, page *Page) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[pageID]; ok {
		old := entry.page
		entry.page = page
		c.order.MoveToFront(entry.elem)
		// Release the old page's pooled buffer (if any).
		if old != nil {
			old.ReleaseToPool()
		}
		return
	}
	// Evict LRU if at capacity.
	if c.order.Len() >= c.capacity {
		back := c.order.Back()
		if back != nil {
			evicted := back.Value.(*pageCacheEntry)
			delete(c.items, evicted.pageID)
			c.order.Remove(back)
			// Return evicted page's buffer to the pool.
			if evicted.page != nil {
				evicted.page.ReleaseToPool()
			}
		}
	}
	entry := &pageCacheEntry{pageID: pageID, page: page}
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

// RealPageProvider adapts a PageStore to the pageStore interface.
// It stores pages as raw []byte buffers (slotted page format) and
// delegates storage to PageStore.
//
// An LRU page cache eliminates redundant pread for hot pages.
//
// WAL entry collection supports two modes:
//   - Per-operation collectors (RegisterCollector): for concurrent KVStore operations.
//     WritePage routes entries to the collector registered for the current goroutine.
//   - Shared buffer (legacy): for tests that don't use collectors.
//     WritePage appends to the shared walEntries slice.
type RealPageProvider struct {
	store pagestoreapi.PageStore
	cache *pageCache // LRU cache for pages

	// Hot page cache: stores pages for zero-copy reads.
	// ReadPage returns directly from hotPages on hit — no clone.
	// Protected by hotMu to allow concurrent read-only access.
	hotMu    sync.Mutex
	hotPages map[pagestoreapi.PageID]*Page

	// Per-operation WAL entry collectors, keyed by goroutine ID.
	collectors sync.Map // map[int64]*WALCollector

	// Legacy shared buffer (for tests that don't use collectors).
	mu         sync.Mutex
	walEntries []pagestoreapi.WALEntry

	// Page operation counters for metrics/analysis.
	pageReads     atomic.Uint64
	pageWrites    atomic.Uint64
	pageCacheHits atomic.Uint64
	pageAlloc     atomic.Uint64

	// Latency tracking for page I/O (nanoseconds).
	readLatencyNanos  atomic.Uint64
	readLatencyCount  atomic.Uint64
	writeLatencyNanos atomic.Uint64
	writeLatencyCount atomic.Uint64
}

// NewRealPageProvider creates a RealPageProvider backed by the given PageStore.
func NewRealPageProvider(store pagestoreapi.PageStore, cacheSize int) *RealPageProvider {
	if cacheSize <= 0 {
		cacheSize = 1024
	}
	return &RealPageProvider{
		store:    store,
		cache:    newPageCache(cacheSize),
		hotPages: make(map[pagestoreapi.PageID]*Page),
	}
}

// RegisterCollector creates a per-operation WAL entry collector and registers
// it for the current goroutine. Returns the collector and an unregister function.
func (p *RealPageProvider) RegisterCollector() (*WALCollector, func()) {
	gid := goid.Get()
	c := walCollectorPool.Get().(*WALCollector)
	c.PageEntries = c.PageEntries[:0]
	p.collectors.Store(gid, c)
	return c, func() {
		p.collectors.Delete(gid)
		walCollectorPool.Put(c)
	}
}

// CollectAndClear retrieves all WAL entries from the current goroutine's collector.
func (p *RealPageProvider) CollectAndClear() []pagestoreapi.WALEntry {
	gid := goid.Get()
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

// ReadPage reads a page from the given PageID.
// Returns a shared pointer from the hot page cache or LRU cache.
func (p *RealPageProvider) ReadPage(pageID pagestoreapi.PageID) (*Page, error) {
	startNs := time.Now().UnixNano()
	p.pageReads.Add(1)

	// Hot page cache: return shared pointer directly (zero-copy).
	p.hotMu.Lock()
	if page, ok := p.hotPages[pageID]; ok {
		p.hotMu.Unlock()
		p.pageCacheHits.Add(1)
		return page, nil
	}
	p.hotMu.Unlock()

	// Fallback: LRU cache (returns a clone).
	if page, ok := p.cache.Get(pageID); ok {
		p.pageCacheHits.Add(1)
		return page, nil
	}

	// Cache miss — read from disk.
	data, err := p.store.Read(pageID)
	if err != nil {
		return nil, fmt.Errorf("realpage: read page %d: %w", pageID, err)
	}
	page := PageFromBytes(data)

	// Populate both caches.
	p.hotMu.Lock()
	p.hotPages[pageID] = page
	p.hotMu.Unlock()
	p.cache.Put(pageID, page)

	// Track latency (cache miss includes disk I/O).
	latencyNs := time.Now().UnixNano() - startNs
	p.readLatencyNanos.Add(uint64(latencyNs))
	p.readLatencyCount.Add(1)

	return page, nil
}

// ReadPageForWrite returns a clone of the page, safe for in-place mutation.
// Must be used when the caller intends to modify the page data (under WLock).
// Uses pooled buffers to reduce GC pressure.
func (p *RealPageProvider) ReadPageForWrite(pageID pagestoreapi.PageID) (*Page, error) {
	page, err := p.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	return page.ClonePooled(), nil
}

// ReadPageUncached reads directly from the underlying PageStore without
// going through the LRU cache.
func (p *RealPageProvider) ReadPageUncached(pageID pagestoreapi.PageID) (*Page, error) {
	data, err := p.store.Read(pageID)
	if err != nil {
		return nil, fmt.Errorf("realpage: uncached read page %d: %w", pageID, err)
	}
	return PageFromBytes(data), nil
}

// WritePage writes a page to the given PageID.
// The page's Data() IS the serialized form — zero serialize!
func (p *RealPageProvider) WritePage(pageID pagestoreapi.PageID, page *Page) error {
	startNs := time.Now().UnixNano()
	p.pageWrites.Add(1)

	// Write directly: copy page data into mmap region.
	entry, err := p.store.WriteDirect(pageID, func(buf []byte) error {
		copy(buf, page.Data())
		return nil
	})
	if err != nil {
		return fmt.Errorf("realpage: write page %d: %w", pageID, err)
	}

	// Update both caches with the new version.
	// Release the old hotPages entry's pooled buffer (if any) before replacing.
	p.hotMu.Lock()
	if old, ok := p.hotPages[pageID]; ok && old != nil {
		old.ReleaseToPool()
	}
	p.hotPages[pageID] = page
	p.hotMu.Unlock()
	p.cache.Put(pageID, page)

	// Track write latency.
	latencyNs := time.Now().UnixNano() - startNs
	p.writeLatencyNanos.Add(uint64(latencyNs))
	p.writeLatencyCount.Add(1)

	// Route WAL entry to per-operation collector or shared buffer.
	gid := goid.Get()
	if c, ok := p.collectors.Load(gid); ok {
		collector := c.(*WALCollector)
		collector.PageEntries = append(collector.PageEntries, entry)
	} else {
		p.mu.Lock()
		p.walEntries = append(p.walEntries, entry)
		p.mu.Unlock()
	}
	return nil
}

// ─── Node-based compatibility methods for vacuum ────────────────────

// ReadPageNode reads and deserializes a node from the given PageID.
// This provides backward compatibility for vacuum and other external
// consumers that still use *btreeapi.Node.
func (p *RealPageProvider) ReadPageNode(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	page, err := p.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("realpage: read page %d: %w", pageID, err)
	}
	return PageToNode(page), nil
}

// ReadPageNodeUncached reads directly from the underlying PageStore
// and converts into a *btreeapi.Node.
func (p *RealPageProvider) ReadPageNodeUncached(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	return p.ReadPageNode(pageID)
}

// WritePageNode converts a *btreeapi.Node to a slotted *Page and writes it.
func (p *RealPageProvider) WritePageNode(pageID pagestoreapi.PageID, node *btreeapi.Node) error {
	page := NodeToPage(node)
	return p.WritePage(pageID, page)
}

// ─── Legacy WAL methods ─────────────────────────────────────────────

// WALEntries returns all collected WALEntries from the shared buffer.
func (p *RealPageProvider) WALEntries() []pagestoreapi.WALEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]pagestoreapi.WALEntry, len(p.walEntries))
	copy(out, p.walEntries)
	return out
}

// DrainWALEntries returns all collected WALEntries and clears the buffer.
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
	PageReads         uint64
	PageWrites        uint64
	PageCacheHits     uint64
	PageAlloc         uint64
	ReadLatencyNanos  uint64
	ReadLatencyCount  uint64
	WriteLatencyNanos uint64
	WriteLatencyCount uint64
}

// GetStats returns a snapshot of page operation statistics.
func (p *RealPageProvider) GetStats() PageStats {
	return PageStats{
		PageReads:         p.pageReads.Load(),
		PageWrites:        p.pageWrites.Load(),
		PageCacheHits:     p.pageCacheHits.Load(),
		PageAlloc:         p.pageAlloc.Load(),
		ReadLatencyNanos:  p.readLatencyNanos.Load(),
		ReadLatencyCount:  p.readLatencyCount.Load(),
		WriteLatencyNanos: p.writeLatencyNanos.Load(),
		WriteLatencyCount: p.writeLatencyCount.Load(),
	}
}

// ResetStats clears all page operation counters.
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
