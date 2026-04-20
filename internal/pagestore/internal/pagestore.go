// Package internal implements the PageStore with LRU page cache.
package internal

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

var (
	_ pagestoreapi.PageStore         = (*pageStore)(nil)
	_ pagestoreapi.PageStoreRecovery = (*pageStore)(nil)
)

type lruCache struct {
	pages   map[uint64]*list.Element
	lru     *list.List
	maxSize int
	mu      sync.Mutex
}

type cacheEntry struct {
	pageID uint64
	data   []byte
}

func newLRUCache(maxSize int) *lruCache {
	if maxSize <= 0 {
		maxSize = 1024
	}
	return &lruCache{
		pages:   make(map[uint64]*list.Element, maxSize),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

func (c *lruCache) Get(pageID uint64) []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.pages[pageID]
	if !ok {
		return nil
	}
	c.lru.MoveToFront(elem)
	return elem.Value.(*cacheEntry).data
}

func (c *lruCache) Put(pageID uint64, data []byte) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.pages[pageID]; ok {
		c.lru.MoveToFront(elem)
		elem.Value.(*cacheEntry).data = data
		return
	}
	for c.lru.Len() >= c.maxSize {
		elem := c.lru.Back()
		if elem != nil {
			c.lru.Remove(elem)
			ent := elem.Value.(*cacheEntry)
			delete(c.pages, ent.pageID)
		}
	}
	ent := &cacheEntry{pageID: pageID, data: data}
	elem := c.lru.PushFront(ent)
	c.pages[pageID] = elem
}

func (c *lruCache) Invalidate(pageID uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.pages[pageID]; ok {
		c.lru.Remove(elem)
		delete(c.pages, pageID)
	}
}

func (c *lruCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

func (c *lruCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages = make(map[uint64]*list.Element)
	c.lru.Init()
}

// ─── pageStore ───────────────────────────────────────────────────────

type pageStore struct {
	mu         sync.Mutex
	segMgr     segmentapi.SegmentManager
	lsm        lsmapi.MappingStore
	nextPageID uint64
	closed     bool
	cache      *lruCache
	statsMgr   interface {
		Increment(segID uint32, count, bytes int64)
		Decrement(segID uint32, count, bytes int64)
	}
}

// statsManagerInterface is the interface for segment stats tracking.
type statsManagerInterface interface {
	Increment(segID uint32, count, bytes int64)
	Decrement(segID uint32, count, bytes int64)
}

func New(cfg pagestoreapi.Config, segMgr segmentapi.SegmentManager, lsmStore lsmapi.MappingStore) pagestoreapi.PageStore {
	ps := &pageStore{
		segMgr:     segMgr,
		lsm:        lsmStore,
		nextPageID: 1,
	}
	if cfg.StatsManager != nil {
		if sm, ok := cfg.StatsManager.(statsManagerInterface); ok {
			ps.statsMgr = sm
		}
	}
	if cfg.PageCacheSize > 0 {
		ps.cache = newLRUCache(cfg.PageCacheSize)
	}
	return ps
}

func (ps *pageStore) Alloc() pagestoreapi.PageID {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	id := ps.nextPageID
	ps.nextPageID++
	return id
}

func (ps *pageStore) Write(pageID pagestoreapi.PageID, data []byte) (pagestoreapi.WALEntry, error) {
	if len(data) != pagestoreapi.PageSize {
		return pagestoreapi.WALEntry{}, pagestoreapi.ErrInvalidPageSize
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return pagestoreapi.WALEntry{}, pagestoreapi.ErrClosed
	}

	// Look up old mapping before overwriting (needed for stats decrement).
	oldPacked, _ := ps.lsm.GetPageMapping(uint64(pageID))

	record := make([]byte, pagestoreapi.PageRecordSize)
	binary.BigEndian.PutUint64(record[:8], pageID)
	copy(record[8:8+pagestoreapi.PageSize], data)
	checksum := crc32.ChecksumIEEE(record[:8+pagestoreapi.PageSize])
	binary.BigEndian.PutUint32(record[8+pagestoreapi.PageSize:], checksum)
	vaddr, err := ps.segMgr.Append(record)
	if err == segmentapi.ErrSegmentFull {
		if rotErr := ps.segMgr.Rotate(); rotErr != nil {
			return pagestoreapi.WALEntry{}, fmt.Errorf("pagestore: rotate on full: %w", rotErr)
		}
		vaddr, err = ps.segMgr.Append(record)
	}
	if err != nil {
		return pagestoreapi.WALEntry{}, err
	}
	packed := vaddr.Pack()
	// LSM handles page mapping persistence (with WAL integration)
	ps.lsm.SetPageMapping(uint64(pageID), packed)
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}

	// Stats update — decrement old segment, increment new.
	// Decrement always goes to old segment (even if same as new, net=0 — correct).
	// Increment always goes to new segment.
	if ps.statsMgr != nil {
		if oldPacked != 0 {
			oldSegID := uint32(oldPacked >> 32)
			ps.statsMgr.Decrement(oldSegID, 1, int64(pagestoreapi.PageRecordSize))
		}
		ps.statsMgr.Increment(vaddr.SegmentID, 1, int64(pagestoreapi.PageRecordSize))
	}

	return pagestoreapi.WALEntry{Type: 1, ID: pageID, VAddr: packed, Size: 0}, nil
}

func (ps *pageStore) Read(pageID pagestoreapi.PageID) ([]byte, error) {
	if ps.cache != nil {
		if data := ps.cache.Get(uint64(pageID)); data != nil {
			result := make([]byte, len(data))
			copy(result, data)
			return result, nil
		}
	}
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil, pagestoreapi.ErrClosed
	}
	vaddr, ok := ps.lsm.GetPageMapping(uint64(pageID))
	ps.mu.Unlock()
	if !ok || vaddr == 0 {
		return nil, pagestoreapi.ErrPageNotFound
	}
	raw, err := ps.segMgr.ReadAt(segmentapi.UnpackVAddr(vaddr), pagestoreapi.PageRecordSize)
	if err != nil {
		return nil, err
	}
	expected := binary.BigEndian.Uint32(raw[8+pagestoreapi.PageSize:])
	actual := crc32.ChecksumIEEE(raw[:8+pagestoreapi.PageSize])
	if expected != actual {
		return nil, fmt.Errorf("%w: pageID=%d expected=0x%08x actual=0x%08x",
			pagestoreapi.ErrChecksumMismatch, pageID, expected, actual)
	}
	result := make([]byte, pagestoreapi.PageSize)
	copy(result, raw[8:8+pagestoreapi.PageSize])
	if ps.cache != nil {
		ps.cache.Put(uint64(pageID), result)
	}
	return result, nil
}

func (ps *pageStore) Free(pageID pagestoreapi.PageID) pagestoreapi.WALEntry {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Look up old mapping before clearing (needed for stats decrement).
	oldPacked, _ := ps.lsm.GetPageMapping(uint64(pageID))

	// Mark page as deleted in LSM (value = 0 signals deleted)
	ps.lsm.SetPageMapping(uint64(pageID), 0)
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}

	// Stats update — decrement old segment.
	if ps.statsMgr != nil && oldPacked != 0 {
		oldSegID := uint32(oldPacked >> 32)
		ps.statsMgr.Decrement(oldSegID, 1, int64(pagestoreapi.PageRecordSize))
	}

	return pagestoreapi.WALEntry{Type: 4, ID: pageID, VAddr: 0, Size: 0}
}

func (ps *pageStore) NextPageID() pagestoreapi.PageID {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.nextPageID
}

func (ps *pageStore) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return nil
	}
	ps.closed = true
	// Flush LSM memtable to SSTable before closing.
	// Without this, the in-memory memtable data is lost on close,
	// causing "page not found" after reopen (LSM has no SSTable data to read).
	if err := ps.lsm.Close(); err != nil {
		return err
	}
	if ps.cache != nil {
		ps.cache.Clear()
	}
	return nil
}

func (ps *pageStore) LoadMapping(entries []pagestoreapi.MappingEntry) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, e := range entries {
		ps.lsm.SetPageMapping(e.PageID, e.VAddr)
	}
	if ps.cache != nil {
		ps.cache.Clear()
	}
}

func (ps *pageStore) ApplyPageMap(pageID pagestoreapi.PageID, vaddr uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.lsm.SetPageMapping(uint64(pageID), vaddr)
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}
}

func (ps *pageStore) ApplyPageFree(pageID pagestoreapi.PageID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.lsm.SetPageMapping(uint64(pageID), 0)
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}
}

func (ps *pageStore) SetNextPageID(nextID pagestoreapi.PageID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nextPageID = nextID
}

// SetLSMSegments sets the LSM segment list from checkpoint (v3+).
// This initializes the LSM manifest with checkpoint-pinned segments,
// skipping rebuild from WAL for pre-checkpoint entries.
// The segments are stored in the underlying *lsm's manifest.
func (ps *pageStore) SetLSMSegments(segments []string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Get the LSM's manifest and populate it with checkpoint segments.
	if lsm, ok := ps.lsm.(interface{ SetSegments([]string) }); ok {
		lsm.SetSegments(segments)
	}
}

// InvalidatePage invalidates any cached entry for the given PageID.
// Used by GC after CAS update to evict the old VAddr from the LRU cache.
func (ps *pageStore) InvalidatePage(pageID pagestoreapi.PageID) {
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}
}

// LSMLifecycle returns the LSM store for WAL replay routing.
// The underlying *lsm implements both MappingStore and LSMLifecycle interfaces.
func (ps *pageStore) LSMLifecycle() pagestoreapi.LSMLifecycle {
	return ps.lsm.(pagestoreapi.LSMLifecycle)
}
