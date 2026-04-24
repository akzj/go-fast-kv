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

// pageRecordPool reuses PageRecordSize (4108-byte) buffers for Write.
// Each Write allocates a record buffer to prepend pageID + append CRC;
// pooling eliminates ~4.8 GB of allocations per 1M writes.
var pageRecordPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, pagestoreapi.PageRecordSize)
		return &b
	},
}

// compactRecordPool reuses MaxPageRecordSize (4110-byte) buffers for
// WriteCompact and ReadCompact. Eliminates a per-call allocation.
var compactRecordPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, pagestoreapi.MaxPageRecordSize)
		return &b
	},
}

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

// ─── Variable-Length Compact Write/Read ──────────────────────────────

// WriteCompact writes variable-length compact page data.
//
// Record format: [PageID:8][DataLen:2][CompactData:N][CRC32:4]
// Packed VAddr uses SegmentID:20 | Offset:30 | RecordLen:14 encoding.
func (ps *pageStore) WriteCompact(pageID pagestoreapi.PageID, compactData []byte) (pagestoreapi.WALEntry, error) {
	dataLen := len(compactData)
	if dataLen > pagestoreapi.PageSize {
		return pagestoreapi.WALEntry{}, pagestoreapi.ErrInvalidPageSize
	}
	recordLen := pagestoreapi.PageRecordOverhead + dataLen // 14 + N

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return pagestoreapi.WALEntry{}, pagestoreapi.ErrClosed
	}

	// Look up old mapping before overwriting (needed for stats decrement).
	oldPacked, _ := ps.lsm.GetPageMapping(uint64(pageID))

	// Build record: [PageID:8][DataLen:2][CompactData:N][CRC32:4]
	bp := compactRecordPool.Get().(*[]byte)
	record := (*bp)[:recordLen]
	binary.BigEndian.PutUint64(record[:8], pageID)
	binary.BigEndian.PutUint16(record[8:10], uint16(dataLen))
	copy(record[10:10+dataLen], compactData)
	checksum := crc32.ChecksumIEEE(record[:10+dataLen])
	binary.BigEndian.PutUint32(record[10+dataLen:], checksum)

	// Append to segment.
	vaddr, err := ps.segMgr.Append(record)
	if err == segmentapi.ErrSegmentFull {
		if rotErr := ps.segMgr.Rotate(); rotErr != nil {
			compactRecordPool.Put(bp)
			return pagestoreapi.WALEntry{}, fmt.Errorf("pagestore: rotate on full: %w", rotErr)
		}
		vaddr, err = ps.segMgr.Append(record)
	}
	// Append has copied the data; safe to return buffer to pool.
	compactRecordPool.Put(bp)
	if err != nil {
		return pagestoreapi.WALEntry{}, err
	}

	// Pack VAddr with record length using new 20:30:14 encoding.
	packed := segmentapi.PackPageVAddr(vaddr.SegmentID, vaddr.Offset, uint16(recordLen))
	ps.lsm.SetPageMapping(uint64(pageID), packed)
	if ps.cache != nil {
		ps.cache.Invalidate(uint64(pageID))
	}

	// Stats update.
	if ps.statsMgr != nil {
		if oldPacked != 0 {
			_, _, oldRecLen := segmentapi.UnpackPageVAddr(oldPacked)
			oldSegID := segmentapi.SegmentIDFromPageVAddr(oldPacked)
			if oldRecLen > 0 {
				ps.statsMgr.Decrement(oldSegID, 1, int64(oldRecLen))
			} else {
				// Legacy fixed-size record
				ps.statsMgr.Decrement(oldSegID, 1, int64(pagestoreapi.PageRecordSize))
			}
		}
		ps.statsMgr.Increment(vaddr.SegmentID, 1, int64(recordLen))
	}

	return pagestoreapi.WALEntry{Type: 1, ID: pageID, VAddr: packed, Size: 0}, nil
}

// ReadCompact reads variable-length compact page data.
//
// Uses RecordLen from the packed VAddr to read the exact bytes in one I/O.
// Returns the compact data (without PageID header or CRC).
func (ps *pageStore) ReadCompact(pageID pagestoreapi.PageID) ([]byte, error) {
	// Check cache first.
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
	packed, ok := ps.lsm.GetPageMapping(uint64(pageID))
	ps.mu.Unlock()
	if !ok || packed == 0 {
		return nil, pagestoreapi.ErrPageNotFound
	}

	// Unpack VAddr to get segment location and record length.
	segID, offset, recordLen := segmentapi.UnpackPageVAddr(packed)
	if recordLen == 0 || recordLen < uint16(pagestoreapi.PageRecordOverhead) {
		// Fallback to legacy fixed-size read if RecordLen is 0 (old format).
		return ps.readLegacy(pageID, packed)
	}

	// Single read: exactly recordLen bytes using pooled buffer.
	addr := segmentapi.VAddr{SegmentID: segID, Offset: offset}
	bp := compactRecordPool.Get().(*[]byte)
	raw := (*bp)[:recordLen]
	err := ps.segMgr.ReadAtInto(addr, raw)
	if err != nil {
		compactRecordPool.Put(bp)
		return nil, err
	}

	// Parse: [PageID:8][DataLen:2][CompactData:N][CRC32:4]
	dataLen := int(binary.BigEndian.Uint16(raw[8:10]))
	if 10+dataLen+4 != int(recordLen) {
		compactRecordPool.Put(bp)
		return nil, fmt.Errorf("pagestore: record length mismatch: header says %d, vaddr says %d",
			10+dataLen+4, recordLen)
	}

	// CRC check on [PageID:8][DataLen:2][CompactData:N].
	expected := binary.BigEndian.Uint32(raw[10+dataLen:])
	actual := crc32.ChecksumIEEE(raw[:10+dataLen])
	if expected != actual {
		compactRecordPool.Put(bp)
		return nil, fmt.Errorf("%w: pageID=%d expected=0x%08x actual=0x%08x",
			pagestoreapi.ErrChecksumMismatch, pageID, expected, actual)
	}

	// Extract compact data (must copy out before returning buffer to pool).
	compactData := make([]byte, dataLen)
	copy(compactData, raw[10:10+dataLen])
	compactRecordPool.Put(bp)

	// Cache the compact data.
	if ps.cache != nil {
		ps.cache.Put(uint64(pageID), compactData)
	}

	return compactData, nil
}

// readLegacy handles reading pages written with the old fixed-size format.
// This provides backward compatibility during migration.
func (ps *pageStore) readLegacy(pageID pagestoreapi.PageID, packed uint64) ([]byte, error) {
	// Old format: VAddr is packed with 32:32 encoding.
	addr := segmentapi.UnpackVAddr(packed)
	bp := pageRecordPool.Get().(*[]byte)
	raw := *bp
	err := ps.segMgr.ReadAtInto(addr, raw)
	if err != nil {
		pageRecordPool.Put(bp)
		return nil, err
	}
	expected := binary.BigEndian.Uint32(raw[8+pagestoreapi.PageSize:])
	actual := crc32.ChecksumIEEE(raw[:8+pagestoreapi.PageSize])
	if expected != actual {
		pageRecordPool.Put(bp)
		return nil, fmt.Errorf("%w: pageID=%d expected=0x%08x actual=0x%08x (legacy)",
			pagestoreapi.ErrChecksumMismatch, pageID, expected, actual)
	}
	result := make([]byte, pagestoreapi.PageSize)
	copy(result, raw[8:8+pagestoreapi.PageSize])
	pageRecordPool.Put(bp)
	if ps.cache != nil {
		ps.cache.Put(uint64(pageID), result)
	}
	return result, nil
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
