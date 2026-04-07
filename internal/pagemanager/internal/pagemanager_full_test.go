package internal

import (
	"sync"
	"testing"
	"time"

	pagemanagerapi "github.com/akzj/go-fast-kv/internal/pagemanager/api"
	storageapi "github.com/akzj/go-fast-kv/internal/storage/api"
	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Mock implementations for testing
// =============================================================================

// mockOSFile implements storageapi.FileOperations for testing.
type mockOSFile struct {
	data  []byte
	path  string
	closed bool
}

func (f *mockOSFile) Open(path string) error { f.path = path; return nil }
func (f *mockOSFile) Close() error { f.closed = true; return nil }
func (f *mockOSFile) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || offset > int64(len(f.data)) {
		return 0, nil
	}
	n := copy(p, f.data[offset:])
	return n, nil
}
func (f *mockOSFile) WriteAt(p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, nil
	}
	if offset+int64(len(p)) > int64(cap(f.data)) {
		newData := make([]byte, offset+int64(len(p)))
		copy(newData, f.data)
		f.data = newData
	}
	copy(f.data[offset:], p)
	return len(p), nil
}
func (f *mockOSFile) Sync() error { return nil }
func (f *mockOSFile) Size() (int64, error) { return int64(len(f.data)), nil }
func (f *mockOSFile) Truncate(size int64) error {
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	} else {
		f.data = append(f.data, make([]byte, size-int64(len(f.data)))...)
	}
	return nil
}
func (f *mockOSFile) Path() string { return f.path }

// mockSegment implements storageapi.Segment for testing.
type mockSegment struct {
	mu          sync.Mutex
	id          vaddr.SegmentID
	data        []byte
	closed      bool
}

func newMockSegment(id vaddr.SegmentID) *mockSegment {
	return &mockSegment{
		id:   id,
		data: make([]byte, 0),
	}
}

func (s *mockSegment) ID() vaddr.SegmentID            { return s.id }
func (s *mockSegment) State() vaddr.SegmentState     { return vaddr.SegmentStateActive }
func (s *mockSegment) File() storageapi.FileOperations { return nil }

func (s *mockSegment) Append(data []byte) (vaddr.VAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return vaddr.VAddr{}, pagemanagerapi.ErrClosed
	}

	offset := int64(len(s.data))
	vaddr_ := vaddr.VAddr{SegmentID: uint64(s.id), Offset: uint64(offset)}
	s.data = append(s.data, data...)
	return vaddr_, nil
}

func (s *mockSegment) ReadAt(offset int64, length int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, pagemanagerapi.ErrClosed
	}

	if offset < 0 || int(offset) > len(s.data) {
		return nil, pagemanagerapi.ErrPageNotFound
	}

	end := int(offset) + length
	if end > len(s.data) {
		end = len(s.data)
	}

	return s.data[offset:end], nil
}

func (s *mockSegment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.data))
}

func (s *mockSegment) PageCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.data) / 4096)
}

func (s *mockSegment) Sync() error {
	return nil
}

func (s *mockSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// mockSegmentManager implements storageapi.SegmentManager for testing.
type mockSegmentManager struct {
	mu       sync.Mutex
	segments []*mockSegment
	nextID   vaddr.SegmentID
	closed   bool
}

func newMockSegmentManager() *mockSegmentManager {
	return &mockSegmentManager{
		segments: make([]*mockSegment, 0),
		nextID:   vaddr.SegmentIDMin,
	}
}

func (sm *mockSegmentManager) ActiveSegment() storageapi.Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed || len(sm.segments) == 0 {
		return nil
	}
	return sm.segments[len(sm.segments)-1]
}

func (sm *mockSegmentManager) GetSegment(id vaddr.SegmentID) storageapi.Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, seg := range sm.segments {
		if seg.id == id {
			return seg
		}
	}
	return nil
}

func (sm *mockSegmentManager) CreateSegment() (storageapi.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil, pagemanagerapi.ErrClosed
	}

	seg := newMockSegment(sm.nextID)
	sm.segments = append(sm.segments, seg)
	sm.nextID++
	return seg, nil
}

func (sm *mockSegmentManager) SealSegment(id vaddr.SegmentID) error {
	return nil
}

func (sm *mockSegmentManager) ArchiveSegment(id vaddr.SegmentID) error {
	return nil
}

func (sm *mockSegmentManager) ListSegments() []storageapi.Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	result := make([]storageapi.Segment, len(sm.segments))
	for i, seg := range sm.segments {
		result[i] = seg
	}
	return result
}

func (sm *mockSegmentManager) ListSegmentsByState(state vaddr.SegmentState) []storageapi.Segment {
	return sm.ListSegments()
}

func (sm *mockSegmentManager) SegmentCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.segments)
}

func (sm *mockSegmentManager) ActiveSegmentCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	count := 0
	for _, seg := range sm.segments {
		if seg.State() == vaddr.SegmentStateActive {
			count++
		}
	}
	return count
}

func (sm *mockSegmentManager) TotalSize() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var total int64
	for _, seg := range sm.segments {
		total += seg.Size()
	}
	return total
}

func (sm *mockSegmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil
	}
	sm.closed = true
	for _, seg := range sm.segments {
		seg.Close()
	}
	sm.segments = nil
	return nil
}

func (m *mockSegmentManager) Directory() string {
	return ""
}

// =============================================================================
// DenseArray Tests
// =============================================================================

// TestDenseArrayOutOfBounds tests Get on out-of-bounds indices.
func TestDenseArrayOutOfBounds(t *testing.T) {
	da := newDenseArray(16)

	// Get on index 0 (invalid)
	vaddr := da.Get(0)
	if !isZeroVAddr(vaddr) {
		t.Errorf("Get(0): expected zero VAddr, got %v", vaddr)
	}

	// Get on very large index
	vaddr = da.Get(1000000)
	if !isZeroVAddr(vaddr) {
		t.Errorf("Get(1000000): expected zero VAddr, got %v", vaddr)
	}

	// Put with key 0 should be ignored
	var testVAddr [16]byte
	testVAddr[0] = 1
	da.Put(0, testVAddr)
	if !isZeroVAddr(da.Get(0)) {
		t.Errorf("Put(0, vaddr): should be ignored, but Get(0) returned non-zero")
	}
}

// TestDenseArrayLiveCountEdgeCases tests live count tracking.
func TestDenseArrayLiveCountEdgeCases(t *testing.T) {
	da := newDenseArray(16)

	// Empty array
	if da.LiveCount() != 0 {
		t.Errorf("LiveCount on empty: got %d, want 0", da.LiveCount())
	}

	// Put a live entry
	var vaddr [16]byte
	vaddr[0] = 1
	da.Put(1, vaddr)

	if da.LiveCount() != 1 {
		t.Errorf("LiveCount after Put: got %d, want 1", da.LiveCount())
	}

	// Update same entry (still live)
	vaddr[1] = 2
	da.Put(1, vaddr)

	if da.LiveCount() != 1 {
		t.Errorf("LiveCount after update: got %d, want 1", da.LiveCount())
	}

	// Tombstone the entry
	da.Put(1, [16]byte{})

	if da.LiveCount() != 0 {
		t.Errorf("LiveCount after tombstone: got %d, want 0", da.LiveCount())
	}
}

// TestDenseArrayRangeQueryEdgeCases tests RangeQuery edge cases.
func TestDenseArrayRangeQueryEdgeCases(t *testing.T) {
	da := newDenseArray(32)

	// Empty range query
	entries := da.RangeQuery(1, 10)
	if len(entries) != 0 {
		t.Errorf("RangeQuery on empty: got %d, want 0", len(entries))
	}

	// Put some entries
	var vaddr [16]byte
	vaddr[0] = 1
	da.Put(5, vaddr)
	da.Put(10, vaddr)
	da.Put(15, vaddr)

	// Range outside all entries
	entries = da.RangeQuery(20, 30)
	if len(entries) != 0 {
		t.Errorf("RangeQuery(20, 30): got %d, want 0", len(entries))
	}

	// Range before all entries
	entries = da.RangeQuery(1, 4)
	if len(entries) != 0 {
		t.Errorf("RangeQuery(1, 4): got %d, want 0", len(entries))
	}

	// Query single entry
	entries = da.RangeQuery(5, 6)
	if len(entries) != 1 {
		t.Errorf("RangeQuery(5, 6): got %d, want 1", len(entries))
	}

	// Query with tombstones
	da.Put(10, [16]byte{}) // Tombstone
	entries = da.RangeQuery(1, 20)
	if len(entries) != 2 {
		t.Errorf("RangeQuery(1, 20) after tombstone: got %d, want 2", len(entries))
	}
}

// TestDenseArrayIterEdgeCases tests Iter edge cases.
func TestDenseArrayIterEdgeCases(t *testing.T) {
	da := newDenseArray(16)

	// Iterate empty array
	count := 0
	da.Iter(func(pageID PageID, vaddr [16]byte) {
		count++
	})
	if count != 0 {
		t.Errorf("Iter on empty: got %d, want 0", count)
	}

	// Iterate with tombstones
	var vaddr [16]byte
	vaddr[0] = 1
	da.Put(1, vaddr)
	da.Put(2, vaddr)
	da.Put(1, [16]byte{}) // Tombstone page 1

	count = 0
	da.Iter(func(pageID PageID, v [16]byte) {
		count++
	})
	if count != 1 {
		t.Errorf("Iter with tombstone: got %d, want 1", count)
	}
}

// =============================================================================
// FreeList Tests
// =============================================================================

// TestFreeListPopEmpty tests Pop on empty list.
func TestFreeListPopEmpty(t *testing.T) {
	fl := newFreeList()

	// Pop on empty list
	pageID, ok := fl.Pop()
	if ok {
		t.Errorf("Pop on empty: got ok=true, want ok=false")
	}
	if pageID != 0 {
		t.Errorf("Pop on empty: got pageID=%d, want 0", pageID)
	}

	// Len on empty list
	if fl.Len() != 0 {
		t.Errorf("Len on empty: got %d, want 0", fl.Len())
	}
}

// TestFreeListPushZero tests that Push(0) is ignored.
func TestFreeListPushZero(t *testing.T) {
	fl := newFreeList()

	fl.Push(0) // Should be ignored
	if fl.Len() != 0 {
		t.Errorf("Push(0): Len() got %d, want 0", fl.Len())
	}

	// Push valid and then 0
	fl.Push(1)
	fl.Push(0) // Should be ignored
	if fl.Len() != 1 {
		t.Errorf("Push(1), Push(0): Len() got %d, want 1", fl.Len())
	}
}

// TestFreeListReuseOrder tests that freed pages are reused correctly.
func TestFreeListReuseOrder(t *testing.T) {
	fl := newFreeList()

	// Push and pop to verify LIFO order
	fl.Push(1)
	fl.Push(2)
	fl.Push(3)

	// Pop should return in LIFO order
	id1, ok := fl.Pop()
	if !ok || id1 != 3 {
		t.Errorf("First pop: got (%d, %v), want (3, true)", id1, ok)
	}

	id2, ok := fl.Pop()
	if !ok || id2 != 2 {
		t.Errorf("Second pop: got (%d, %v), want (2, true)", id2, ok)
	}

	id3, ok := fl.Pop()
	if !ok || id3 != 1 {
		t.Errorf("Third pop: got (%d, %v), want (1, true)", id3, ok)
	}

	// List should be empty now
	if fl.Len() != 0 {
		t.Errorf("Len after all pops: got %d, want 0", fl.Len())
	}
}

// TestFreeListClear tests Clear functionality.
func TestFreeListClear(t *testing.T) {
	fl := newFreeList()

	// Add items
	for i := 1; i <= 100; i++ {
		fl.Push(PageID(i))
	}

	if fl.Len() != 100 {
		t.Errorf("Len after push: got %d, want 100", fl.Len())
	}

	// Clear
	fl.Clear()

	if fl.Len() != 0 {
		t.Errorf("Len after clear: got %d, want 0", fl.Len())
	}

	// Pop after clear should return empty
	pageID, ok := fl.Pop()
	if ok || pageID != 0 {
		t.Errorf("Pop after clear: got (%d, %v), want (0, false)", pageID, ok)
	}
}

// TestFreeListConcurrentPushPop tests concurrent Push/Pop operations.
func TestFreeListConcurrentPushPop(t *testing.T) {
	fl := newFreeList()
	const goroutines = 10
	const itemsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Concurrent pushes
	for i := 0; i < goroutines; i++ {
		go func(base PageID) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				fl.Push(base + PageID(j))
			}
		}(PageID(i * itemsPerGoroutine))
	}

	// Concurrent pops
	done := make(chan bool)
	go func() {
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < itemsPerGoroutine; j++ {
					fl.Pop()
				}
			}()
		}
		close(done)
	}()

	<-done

	wg.Wait()

	// List should be balanced
	// Note: Due to race conditions in concurrent operations, 
	// we only verify that operations completed without panic
}

// =============================================================================
// PageManager Tests
// =============================================================================

// TestPageManagerBasic tests basic PageManager operations.
func TestPageManagerBasic(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate first page
	pageID1, vaddr1 := pm.AllocatePage()
	if pageID1 == 0 {
		t.Errorf("AllocatePage: got pageID=0, want non-zero")
	}
	if isZeroVAddr(vaddr1) {
		t.Errorf("AllocatePage: got zero vaddr, want non-zero")
	}

	// Verify GetVAddr
	gotVAddr := pm.GetVAddr(pageID1)
	if gotVAddr != vaddr1 {
		t.Errorf("GetVAddr(%d): got %v, want %v", pageID1, gotVAddr, vaddr1)
	}

	// Allocate second page
	pageID2, _ := pm.AllocatePage()
	if pageID2 == 0 {
		t.Errorf("AllocatePage: got pageID=0, want non-zero")
	}
	if pageID2 == pageID1 {
		t.Errorf("AllocatePage: got duplicate pageID %d", pageID1)
	}

	// PageCount should be 2
	if pm.PageCount() != 2 {
		t.Errorf("PageCount: got %d, want 2", pm.PageCount())
	}

	// LivePageCount should be 2
	if pm.LivePageCount() != 2 {
		t.Errorf("LivePageCount: got %d, want 2", pm.LivePageCount())
	}
}

// TestPageManagerAllocateAndFree tests AllocatePage and FreePage sequence.
func TestPageManagerAllocateAndFree(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate page
	pageID, _ := pm.AllocatePage()
	if pageID == 0 {
		t.Fatalf("AllocatePage: got pageID=0")
	}

	if pm.LivePageCount() != 1 {
		t.Errorf("LivePageCount after alloc: got %d, want 1", pm.LivePageCount())
	}

	// Free page
	pm.FreePage(pageID)

	// VAddr should be zero after free
	gotVAddr := pm.GetVAddr(pageID)
	if !isZeroVAddr(gotVAddr) {
		t.Errorf("GetVAddr after FreePage: got %v, want zero", gotVAddr)
	}

	// LivePageCount should be 0
	if pm.LivePageCount() != 0 {
		t.Errorf("LivePageCount after free: got %d, want 0", pm.LivePageCount())
	}

	// Allocate again - should reuse freed page
	pageID2, _ := pm.AllocatePage()
	if pageID2 != pageID {
		t.Errorf("Reallocated page: got %d, want %d (reused from free list)", pageID2, pageID)
	}
}

// TestPageManagerFreePageReuse tests that freed pages are reused.
func TestPageManagerFreePageReuse(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate and free multiple pages
	ids := make([]PageID, 5)
	for i := 0; i < 5; i++ {
		ids[i], _ = pm.AllocatePage()
	}

	// Free all pages
	for _, id := range ids {
		pm.FreePage(id)
	}

	// Allocate 5 more pages - should reuse freed IDs
	newIDs := make([]PageID, 5)
	for i := 0; i < 5; i++ {
		newIDs[i], _ = pm.AllocatePage()
	}

	// All new IDs should be from the freed pool
	for i, id := range newIDs {
		found := false
		for _, oldID := range ids {
			if id == oldID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("AllocatePage %d: got %d, want one of %v", i, id, ids)
		}
	}

	// PageCount should still be 5 (reused IDs)
	if pm.PageCount() != 5 {
		t.Errorf("PageCount after reuse: got %d, want 5", pm.PageCount())
	}
}

// TestPageManagerUpdateMapping tests UpdateMapping.
func TestPageManagerUpdateMapping(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate page
	pageID, originalVAddr := pm.AllocatePage()

	// Create new VAddr
	newVAddr := originalVAddr
	newVAddr[0] = 0xFF

	// Update mapping
	pm.UpdateMapping(pageID, newVAddr)

	// GetVAddr should return new VAddr
	gotVAddr := pm.GetVAddr(pageID)
	if gotVAddr != newVAddr {
		t.Errorf("GetVAddr after UpdateMapping: got %v, want %v", gotVAddr, newVAddr)
	}
}

// TestPageManagerUpdateMappingInvalid tests UpdateMapping with invalid inputs.
func TestPageManagerUpdateMappingInvalid(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// UpdateMapping with pageID=0 should be ignored
	var vaddr [16]byte
	vaddr[0] = 1
	pm.UpdateMapping(0, vaddr)

	// UpdateMapping with zero VAddr should be ignored
	pageID, _ := pm.AllocatePage()
	pm.UpdateMapping(pageID, [16]byte{})
	
	// Original VAddr should be unchanged
	gotVAddr := pm.GetVAddr(pageID)
	if isZeroVAddr(gotVAddr) {
		t.Errorf("GetVAddr after invalid UpdateMapping: got zero, want original")
	}
}

// TestPageManagerIter tests Iter functionality.
func TestPageManagerIter(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate pages
	const pageCount = 10
	ids := make(map[PageID]bool)
	for i := 0; i < pageCount; i++ {
		pageID, _ := pm.AllocatePage()
		ids[pageID] = true
	}

	// Iterate and count
	count := 0
	pm.Iter(func(pageID PageID, vaddr [16]byte) {
		count++
		if !ids[pageID] {
			t.Errorf("Iter: unexpected pageID %d", pageID)
		}
		if isZeroVAddr(vaddr) {
			t.Errorf("Iter: zero VAddr for pageID %d", pageID)
		}
	})

	if count != pageCount {
		t.Errorf("Iter count: got %d, want %d", count, pageCount)
	}

	// Free a page and iterate again
	pm.FreePage(1)
	count = 0
	pm.Iter(func(pageID PageID, vaddr [16]byte) {
		count++
	})

	if count != pageCount-1 {
		t.Errorf("Iter count after free: got %d, want %d", count, pageCount-1)
	}
}

// TestPageManagerFlush tests Flush functionality.
func TestPageManagerFlush(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate some pages
	pm.AllocatePage()
	pm.AllocatePage()

	// Flush should succeed
	err = pm.Flush()
	if err != nil {
		t.Errorf("Flush: %v", err)
	}
}

// TestPageManagerClose tests Close functionality.
func TestPageManagerClose(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}

	// Allocate a page
	pm.AllocatePage()

	// Close should succeed
	err = pm.Close()
	if err != nil {
		t.Errorf("Close: %v", err)
	}

	// Operations after close should return zero/invalid values
	pageID, vaddr := pm.AllocatePage()
	if pageID != 0 {
		t.Errorf("AllocatePage after close: got pageID=%d, want 0", pageID)
	}
	if !isZeroVAddr(vaddr) {
		t.Errorf("AllocatePage after close: got vaddr, want zero")
	}

	// Double close should be safe
	err = pm.Close()
	if err != nil {
		t.Errorf("Double close: %v", err)
	}
}

// TestPageManagerGetVAddrNotExist tests GetVAddr for non-existent page.
func TestPageManagerGetVAddrNotExist(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// GetVAddr for non-existent page
	vaddr := pm.GetVAddr(9999)
	if !isZeroVAddr(vaddr) {
		t.Errorf("GetVAddr(9999): got %v, want zero", vaddr)
	}

	// GetVAddr for pageID 0
	vaddr = pm.GetVAddr(0)
	if !isZeroVAddr(vaddr) {
		t.Errorf("GetVAddr(0): got %v, want zero", vaddr)
	}
}

// TestPageManagerFreePageInvalid tests FreePage with invalid inputs.
func TestPageManagerFreePageInvalid(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// FreePage(0) should be safe
	pm.FreePage(0)

	// FreePage on non-existent page should be safe
	pm.FreePage(9999)

	// FreePage twice should be safe (idempotent)
	pageID, _ := pm.AllocatePage()
	pm.FreePage(pageID)
	pm.FreePage(pageID) // Second free should be no-op

	if pm.LivePageCount() != 0 {
		t.Errorf("LivePageCount after double free: got %d, want 0", pm.LivePageCount())
	}
}

// TestPageManagerMultipleSegments tests segment creation during allocation.
func TestPageManagerMultipleSegments(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Allocate many pages to potentially trigger segment creation
	pageCount := 100
	pageIDs := make([]PageID, pageCount)
	for i := 0; i < pageCount; i++ {
		pageIDs[i], _ = pm.AllocatePage()
	}

	// Verify all pages are unique
	seen := make(map[PageID]bool)
	for _, id := range pageIDs {
		if seen[id] {
			t.Errorf("Duplicate pageID %d", id)
		}
		seen[id] = true
	}

	// Verify we can get VAddr for all pages
	for i, id := range pageIDs {
		vaddr := pm.GetVAddr(id)
		if isZeroVAddr(vaddr) {
			t.Errorf("GetVAddr(%d) at index %d: got zero", id, i)
		}
	}
}

// TestPageManagerConfigValidation tests configuration defaults.
func TestPageManagerConfigValidation(t *testing.T) {
	sm := newMockSegmentManager()

	// Zero config should use defaults
	config := PageManagerConfig{}
	pm, err := NewPageManager(sm, config)
	if err != nil {
		t.Fatalf("NewPageManager with zero config: %v", err)
	}
	defer pm.Close()

	// Should be able to allocate
	pageID, _ := pm.AllocatePage()
	if pageID == 0 {
		t.Errorf("AllocatePage with zero config: got pageID=0")
	}
}

// =============================================================================
// Concurrent Safety Tests
// =============================================================================

// TestDenseArrayConcurrentAccess tests concurrent Get/Put operations.
func TestDenseArrayConcurrentAccess(t *testing.T) {
	da := newDenseArray(1024)

	var wg sync.WaitGroup
	const goroutines = 10
	const opsPerGoroutine = 100

	// Concurrent writes
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(base PageID) {
			defer wg.Done()
			var vaddr [16]byte
			vaddr[0] = byte(base)
			for j := 0; j < opsPerGoroutine; j++ {
				da.Put(base+PageID(j), vaddr)
			}
		}(PageID(i * opsPerGoroutine))
	}

	// Concurrent reads
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(base PageID) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				da.Get(base + PageID(j))
			}
		}(PageID(i * opsPerGoroutine))
	}

	wg.Wait()

	// Verify some entries were written correctly
	if da.LiveCount() == 0 {
		t.Errorf("LiveCount: got 0, want > 0")
	}
}

// TestPageManagerConcurrentAllocate tests concurrent allocation.
func TestPageManagerConcurrentAllocate(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	const goroutines = 10
	const pagesPerGoroutine = 50
	totalPages := goroutines * pagesPerGoroutine

	var wg sync.WaitGroup
	pageIDs := make(chan PageID, totalPages)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < pagesPerGoroutine; j++ {
				pageID, _ := pm.AllocatePage()
				pageIDs <- pageID
			}
		}()
	}

	wg.Wait()
	close(pageIDs)

	// Verify we got the right number of pages
	count := 0
	uniqueIDs := make(map[PageID]bool)
	for pageID := range pageIDs {
		count++
		uniqueIDs[pageID] = true
	}

	if count != totalPages {
		t.Errorf("Total allocations: got %d, want %d", count, totalPages)
	}

	if len(uniqueIDs) != totalPages {
		t.Errorf("Unique pageIDs: got %d, want %d", len(uniqueIDs), totalPages)
	}

	if pm.PageCount() != uint64(totalPages) {
		t.Errorf("PageCount: got %d, want %d", pm.PageCount(), totalPages)
	}

	if pm.LivePageCount() != uint64(totalPages) {
		t.Errorf("LivePageCount: got %d, want %d", pm.LivePageCount(), totalPages)
	}
}

// TestPageManagerConcurrentFree tests concurrent free operations.
func TestPageManagerConcurrentFree(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	// Pre-allocate pages
	const pageCount = 100
	pageIDs := make([]PageID, pageCount)
	for i := 0; i < pageCount; i++ {
		pageIDs[i], _ = pm.AllocatePage()
	}

	// Concurrent free
	var wg sync.WaitGroup
	for i := 0; i < pageCount; i++ {
		wg.Add(1)
		go func(id PageID) {
			defer wg.Done()
			pm.FreePage(id)
		}(pageIDs[i])
	}

	wg.Wait()

	// All pages should be freed
	if pm.LivePageCount() != 0 {
		t.Errorf("LivePageCount after concurrent free: got %d, want 0", pm.LivePageCount())
	}

	// Allocate again - should reuse all freed pages
	for i := 0; i < pageCount; i++ {
		newID, _ := pm.AllocatePage()
		found := false
		for _, oldID := range pageIDs {
			if newID == oldID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Page %d: expected to be reused from freed list", i)
		}
	}
}

// TestPageManagerMixedOperations tests mixed concurrent operations.
func TestPageManagerMixedOperations(t *testing.T) {
	sm := newMockSegmentManager()
	pm, err := NewPageManager(sm, DefaultPageManagerConfig())
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}
	defer pm.Close()

	const goroutines = 5
	const iterations = 20

	var wg sync.WaitGroup

	// Allocator goroutines
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				pm.AllocatePage()
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Free goroutines
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations/2; j++ {
				pm.FreePage(PageID(1 + j%10)) // Free pages 1-10
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Reader goroutines
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				pm.GetVAddr(PageID(1))
				pm.LivePageCount()
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify no panic occurred and state is consistent
	if pm.LivePageCount() > uint64(goroutines*iterations) {
		t.Errorf("LivePageCount unexpectedly high: %d", pm.LivePageCount())
	}
}

// =============================================================================
// VAddr Conversion Tests
// =============================================================================

// TestVAddrConversion tests VAddr conversion functions.
func TestVAddrConversion(t *testing.T) {
	// Test round-trip conversion
	original := vaddr.VAddr{SegmentID: 12345, Offset: 67890}
	bytes := convertVAddrToBytes(original)
	converted := convertVAddrToStruct(bytes)

	if converted.SegmentID != original.SegmentID {
		t.Errorf("SegmentID: got %d, want %d", converted.SegmentID, original.SegmentID)
	}
	if converted.Offset != original.Offset {
		t.Errorf("Offset: got %d, want %d", converted.Offset, original.Offset)
	}
}

// TestIsZeroVAddr tests isZeroVAddr function.
func TestIsZeroVAddr(t *testing.T) {
	// Zero VAddr
	if !isZeroVAddr([16]byte{}) {
		t.Errorf("isZeroVAddr([16]byte{}): got false, want true")
	}

	// Non-zero VAddr
	var vaddr [16]byte
	vaddr[0] = 1
	if isZeroVAddr(vaddr) {
		t.Errorf("isZeroVAddr with non-zero: got true, want false")
	}

	// Only last byte non-zero
	vaddr = [16]byte{}
	vaddr[15] = 1
	if isZeroVAddr(vaddr) {
		t.Errorf("isZeroVAddr with last byte non-zero: got true, want false")
	}
}
