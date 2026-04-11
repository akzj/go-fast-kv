// Package internal implements the PageStore with LRU page cache.
package internal

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

var (
	_ pagestoreapi.PageStore         = (*pageStore)(nil)
	_ pagestoreapi.PageStoreRecovery = (*pageStore)(nil)
)

const defaultInitialCapacity = 1024

// ─── LRU Cache ───────────────────────────────────────────────────────

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
	mapping    []uint64
	nextPageID uint64
	closed     bool
	cache      *lruCache
}

func New(cfg pagestoreapi.Config, segMgr segmentapi.SegmentManager) pagestoreapi.PageStore {
	cap := cfg.InitialCapacity
	if cap <= 0 {
		cap = defaultInitialCapacity
	}
	ps := &pageStore{
		segMgr:     segMgr,
		mapping:    make([]uint64, cap),
		nextPageID: 1,
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
	ps.ensureCapacity(id)
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
	ps.ensureCapacity(pageID)
	ps.mapping[pageID] = packed
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
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
	packed := ps.getMapping(pageID)
	ps.mu.Unlock()
	if packed == 0 {
		return nil, pagestoreapi.ErrPageNotFound
	}
	vaddr := segmentapi.UnpackVAddr(packed)
	raw, err := ps.segMgr.ReadAt(vaddr, pagestoreapi.PageRecordSize)
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
	if pageID < uint64(len(ps.mapping)) {
		ps.mapping[pageID] = 0
	}
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
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
	ps.closed = true
	if ps.cache != nil {
		ps.cache.Clear()
	}
	return nil
}

func (ps *pageStore) LoadMapping(entries []pagestoreapi.MappingEntry) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, e := range entries {
		ps.ensureCapacity(e.PageID)
		ps.mapping[e.PageID] = e.VAddr
	}
	if ps.cache != nil {
		ps.cache.Clear()
	}
}

func (ps *pageStore) ExportMapping() []pagestoreapi.MappingEntry {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var entries []pagestoreapi.MappingEntry
	for i, v := range ps.mapping {
		if v != 0 {
			entries = append(entries, pagestoreapi.MappingEntry{PageID: uint64(i), VAddr: v})
		}
	}
	return entries
}

func (ps *pageStore) ApplyPageMap(pageID pagestoreapi.PageID, vaddr uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.ensureCapacity(pageID)
	ps.mapping[pageID] = vaddr
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}
}

func (ps *pageStore) ApplyPageFree(pageID pagestoreapi.PageID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if pageID < uint64(len(ps.mapping)) {
		ps.mapping[pageID] = 0
	}
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}
}

func (ps *pageStore) SetNextPageID(nextID pagestoreapi.PageID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nextPageID = nextID
}

func (ps *pageStore) ensureCapacity(pageID uint64) {
	for pageID >= uint64(len(ps.mapping)) {
		newCap := len(ps.mapping) * 2
		if newCap == 0 {
			newCap = defaultInitialCapacity
		}
		grown := make([]uint64, newCap)
		copy(grown, ps.mapping)
		ps.mapping = grown
	}
}

func (ps *pageStore) getMapping(pageID uint64) uint64 {
	if pageID >= uint64(len(ps.mapping)) {
		return 0
	}
	return ps.mapping[pageID]
}
