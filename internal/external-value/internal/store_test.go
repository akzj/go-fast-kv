package internal

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/akzj/go-fast-kv/internal/storage"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
	api "github.com/akzj/go-fast-kv/internal/external-value/api"
)

// =============================================================================
// Test Setup Helpers
// =============================================================================

// mockSegmentManager implements storage.SegmentManager for testing.
type mockSegmentManager struct {
	dir string
	mu        sync.RWMutex
	segments  map[vaddr.SegmentID]*mockSegment
	activeID  vaddr.SegmentID
	nextID    vaddr.SegmentID
	closed    bool
}

type mockSegment struct {
	mu       sync.RWMutex
	id       vaddr.SegmentID
	state    vaddr.SegmentState
	data     []byte
	dataSize int64
}

func newMockSegmentManager() *mockSegmentManager {
	return &mockSegmentManager{
		segments: make(map[vaddr.SegmentID]*mockSegment),
		nextID:   vaddr.SegmentIDMin,
	}
}

func (m *mockSegmentManager) Directory() string {
	return m.dir
}

func (m *mockSegmentManager) ActiveSegment() storage.Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed || m.activeID == 0 {
		return nil
	}
	return m.segments[m.activeID]
}

func (m *mockSegmentManager) GetSegment(id vaddr.SegmentID) storage.Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.segments[id]
}

func (m *mockSegmentManager) CreateSegment() (storage.Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, nil
	}

	// Seal current active
	if m.activeID != 0 {
		m.segments[m.activeID].state = vaddr.SegmentStateSealed
	}

	segID := m.nextID
	m.nextID++

	seg := &mockSegment{
		id:    segID,
		state: vaddr.SegmentStateActive,
		data:  make([]byte, 0, 1024*1024), // 1MB initial capacity
	}
	m.segments[segID] = seg
	m.activeID = segID
	return seg, nil
}

func (m *mockSegmentManager) SealSegment(id vaddr.SegmentID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.segments[id]; ok {
		s.state = vaddr.SegmentStateSealed
	}
	return nil
}

func (m *mockSegmentManager) ArchiveSegment(id vaddr.SegmentID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.segments[id]; ok {
		s.state = vaddr.SegmentStateArchived
	}
	return nil
}

func (m *mockSegmentManager) ListSegments() []storage.Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]storage.Segment, 0, len(m.segments))
	for _, s := range m.segments {
		result = append(result, s)
	}
	return result
}

func (m *mockSegmentManager) ListSegmentsByState(state vaddr.SegmentState) []storage.Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []storage.Segment
	for _, s := range m.segments {
		if s.state == state {
			result = append(result, s)
		}
	}
	return result
}

func (m *mockSegmentManager) SegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.segments)
}

func (m *mockSegmentManager) ActiveSegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int
	for _, s := range m.segments {
		if s.state == vaddr.SegmentStateActive {
			count++
		}
	}
	return count
}

func (m *mockSegmentManager) TotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int64
	for _, s := range m.segments {
		total += s.dataSize
	}
	return total
}

func (m *mockSegmentManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.segments = nil
	return nil
}

// mockSegment methods for storage.Segment interface
func (s *mockSegment) ID() vaddr.SegmentID                    { return s.id }
func (s *mockSegment) State() vaddr.SegmentState              { return s.state }
func (s *mockSegment) File() storage.FileOperations           { return nil }
func (s *mockSegment) Size() int64                            { return s.dataSize }
func (s *mockSegment) PageCount() uint64                      { return uint64(len(s.data) / vaddr.PageSize) }
func (s *mockSegment) Sync() error                            { return nil }

// segmentHeaderSize matches the real segment's header size
const segmentHeaderSize = 32

func (s *mockSegment) Append(data []byte) (vaddr.VAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Data must be aligned to PageSize (like real segment)
	if len(data)%vaddr.PageSize != 0 {
		return vaddr.VAddr{}, os.ErrInvalid
	}

	// Initialize data with header space if needed (matches real segment layout)
	if len(s.data) < segmentHeaderSize {
		s.data = make([]byte, segmentHeaderSize)
	}

	// Track offset where this data starts (after segment header + previous data)
	// This matches real segment: offset = headerSize + s.dataSize
	offset := segmentHeaderSize + s.dataSize
	
	// Append data to our buffer (no extra padding needed - external-value already aligned)
	s.data = append(s.data, data...)
	s.dataSize += int64(len(data))

	return vaddr.VAddr{
		SegmentID: uint64(s.id),
		Offset:    uint64(offset),
	}, nil
}

func (s *mockSegment) ReadAt(offset int64, length int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// VAddr.Offset points directly into our mock.data buffer (which starts with header)
	// No offset adjustment needed since mock.data layout matches real file layout
	if offset < 0 || offset+int64(length) > int64(len(s.data)) {
		return nil, os.ErrInvalid
	}
	return s.data[offset : offset+int64(length)], nil
}

func (s *mockSegment) Close() error {
	return nil
}

// Ensure mockSegment implements storage.Segment
var _ storage.Segment = (*mockSegment)(nil)

// =============================================================================
// Basic Store/Retrieve Tests
// =============================================================================

func TestStoreRetrieve_SmallValue(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Store a small value
	value := []byte("hello world")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if !addr.IsValid() {
		t.Fatal("VAddr should be valid")
	}

	// Retrieve and verify
	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("Retrieve mismatch: got %q, want %q", retrieved, value)
	}
}

func TestStoreRetrieve_MultipleValues(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	values := [][]byte{
		[]byte("first"),
		[]byte("second value"),
		[]byte("third value with more content"),
	}

	var addrs []vaddr.VAddr
	for _, v := range values {
		addr, err := store.Store(v)
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}
		addrs = append(addrs, addr)
	}

	// Retrieve all and verify
	for i, addr := range addrs {
		retrieved, err := store.Retrieve(addr)
		if err != nil {
			t.Fatalf("Retrieve failed for value %d: %v", i, err)
		}
		if !bytes.Equal(values[i], retrieved) {
			t.Errorf("Value %d mismatch: got %q, want %q", i, retrieved, values[i])
		}
	}
}

// =============================================================================
// Boundary Value Tests
// =============================================================================

func TestStoreRetrieve_Boundary49Bytes(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// 49 bytes - just above 48 threshold
	value := make([]byte, 49)
	for i := range value {
		value[i] = byte(i)
	}

	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("Boundary value mismatch: got %v, want %v", retrieved, value)
	}
}

func TestStoreRetrieve_Boundary48Bytes(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// 48 bytes - at threshold
	value := make([]byte, 48)
	for i := range value {
		value[i] = byte(i)
	}

	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("Boundary value mismatch: got %v, want %v", retrieved, value)
	}
}

// =============================================================================
// Large Value Tests (>4KB, multi-page)
// =============================================================================

func TestStoreRetrieve_MultiPage(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Create a value larger than one page (4064 bytes data per page)
	// PageSize = 4096, Header = 32, DataPerPage = 4064
	value := make([]byte, 5000) // Should span 2 pages
	for i := range value {
		value[i] = byte(i % 256)
	}

	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("Multi-page value mismatch: got len=%d, want len=%d", len(retrieved), len(value))
	}
}

func TestStoreRetrieve_VeryLargeValue(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Create a very large value (>8KB, spanning multiple pages)
	value := make([]byte, 10000)
	for i := range value {
		value[i] = byte((i * 7) % 256) // Non-trivial pattern
	}

	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if len(retrieved) != len(value) {
		t.Errorf("Size mismatch: got %d, want %d", len(retrieved), len(value))
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("Very large value mismatch")
	}
}

// =============================================================================
// GetValueSize Tests
// =============================================================================

func TestGetValueSize(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Store multiple values of different sizes
	values := [][]byte{
		[]byte("small"),
		make([]byte, 100),
		make([]byte, 5000),
	}

	var addrs []vaddr.VAddr
	for _, v := range values {
		addr, err := store.Store(v)
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}
		addrs = append(addrs, addr)
	}

	// Verify sizes
	for i, addr := range addrs {
		size, err := store.GetValueSize(addr)
		if err != nil {
			t.Fatalf("GetValueSize failed for value %d: %v", i, err)
		}
		expectedSize := uint64(len(values[i]))
		if size != expectedSize {
			t.Errorf("Size mismatch for value %d: got %d, want %d", i, size, expectedSize)
		}
	}
}

func TestGetValueSize_NotFound(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Try to get size of non-existent value
	_, err = store.GetValueSize(vaddr.VAddr{SegmentID: 1, Offset: 9999})
	if err != api.ErrValueNotFound {
		t.Errorf("Expected ErrValueNotFound, got: %v", err)
	}
}

// =============================================================================
// Delete Tests
// =============================================================================

func TestDelete(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("to be deleted")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Verify value exists
	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if !bytes.Equal(value, retrieved) {
		t.Errorf("Retrieve mismatch before delete")
	}

	// Delete the value
	err = store.Delete(addr)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// Verify value still accessible (soft delete - data still exists in segment)
	retrieved, err = store.Retrieve(addr)
	if err != nil {
		t.Errorf("Retrieve after delete should still work: %v", err)
	}
	if !bytes.Equal(value, retrieved) {
		t.Errorf("Retrieve mismatch after delete")
	}
}

func TestDelete_Idempotent(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("idempotent delete")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Delete twice - should not error
	err = store.Delete(addr)
	if err != nil {
		t.Errorf("First delete failed: %v", err)
	}

	err = store.Delete(addr)
	if err != nil {
		t.Errorf("Second delete (idempotent) failed: %v", err)
	}
}

func TestDelete_NonExistent(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Delete non-existent value - should not error (idempotent)
	err = store.Delete(vaddr.VAddr{SegmentID: 1, Offset: 9999})
	if err != nil {
		t.Errorf("Delete non-existent should not error, got: %v", err)
	}
}

// =============================================================================
// RetrieveAt Tests
// =============================================================================

func TestRetrieveAt_FullValue(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("retrieve full value")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Retrieve full value using RetrieveAt
	retrieved, err := store.RetrieveAt(addr, 0, uint64(len(value)))
	if err != nil {
		t.Fatalf("RetrieveAt failed: %v", err)
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("RetrieveAt full mismatch: got %q, want %q", retrieved, value)
	}
}

func TestRetrieveAt_PartialRead(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("partial read test data")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Read middle portion
	offset := uint64(8)
	length := uint64(8)
	retrieved, err := store.RetrieveAt(addr, offset, length)
	if err != nil {
		t.Fatalf("RetrieveAt failed: %v", err)
	}

	expected := value[offset : offset+length]
	if !bytes.Equal(expected, retrieved) {
		t.Errorf("RetrieveAt partial mismatch: got %q, want %q", retrieved, expected)
	}
}

func TestRetrieveAt_StartOfValue(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("start of value")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Read first 5 bytes
	retrieved, err := store.RetrieveAt(addr, 0, 5)
	if err != nil {
		t.Fatalf("RetrieveAt failed: %v", err)
	}

	if string(retrieved) != "start" {
		t.Errorf("RetrieveAt start mismatch: got %q, want %q", retrieved, "start")
	}
}

func TestRetrieveAt_EndOfValue(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("read from end")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Read last 4 bytes
	retrieved, err := store.RetrieveAt(addr, uint64(len(value)-4), 4)
	if err != nil {
		t.Fatalf("RetrieveAt failed: %v", err)
	}

	// "read from end" - last 4 chars are " end" (space + end)
	expected := " end"
	if string(retrieved) != expected {
		t.Errorf("RetrieveAt end mismatch: got %q, want %q", string(retrieved), expected)
	}
}

func TestRetrieveAt_PartialReadMultiPage(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Create a large value spanning multiple pages
	value := make([]byte, 5000)
	for i := range value {
		value[i] = byte((i * 13) % 256)
	}

	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Read a portion that crosses page boundary
	offset := uint64(4000)
	length := uint64(500)
	retrieved, err := store.RetrieveAt(addr, offset, length)
	if err != nil {
		t.Fatalf("RetrieveAt multi-page failed: %v", err)
	}

	expected := value[offset : offset+length]
	if !bytes.Equal(expected, retrieved) {
		t.Errorf("RetrieveAt multi-page mismatch")
	}
}

func TestRetrieveAt_PartialReadBeyondBoundary(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("small value")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Try to read beyond boundary
	_, err = store.RetrieveAt(addr, 0, uint64(len(value))+100)
	if err != api.ErrPartialRead {
		t.Errorf("Expected ErrPartialRead, got: %v", err)
	}
}

func TestRetrieveAt_OffsetBeyondBoundary(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("value")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Try to read with offset beyond value size
	_, err = store.RetrieveAt(addr, uint64(len(value))+1, 10)
	if err != api.ErrPartialRead {
		t.Errorf("Expected ErrPartialRead for offset beyond boundary, got: %v", err)
	}
}

// =============================================================================
// Close Tests
// =============================================================================

func TestClose(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}

	value := []byte("before close")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Close the store
	err = store.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// All operations should return ErrStoreClosed
	_, err = store.Store([]byte("after"))
	if err != api.ErrStoreClosed {
		t.Errorf("Store after close should return ErrStoreClosed, got: %v", err)
	}

	_, err = store.Retrieve(addr)
	if err != api.ErrStoreClosed {
		t.Errorf("Retrieve after close should return ErrStoreClosed, got: %v", err)
	}

	_, err = store.RetrieveAt(addr, 0, 10)
	if err != api.ErrStoreClosed {
		t.Errorf("RetrieveAt after close should return ErrStoreClosed, got: %v", err)
	}

	_, err = store.GetValueSize(addr)
	if err != api.ErrStoreClosed {
		t.Errorf("GetValueSize after close should return ErrStoreClosed, got: %v", err)
	}

	err = store.Delete(addr)
	if err != api.ErrStoreClosed {
		t.Errorf("Delete after close should return ErrStoreClosed, got: %v", err)
	}

	// Double close should be safe
	err = store.Close()
	if err != nil {
		t.Errorf("Double close failed: %v", err)
	}
}

// =============================================================================
// Metrics Tests
// =============================================================================

func TestMetrics(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Store some values
	v1 := []byte("first value")
	v2 := []byte("second longer value")
	v3 := make([]byte, 5000)

	addr1, _ := store.Store(v1)
	store.Store(v2)
	store.Store(v3)

	// Retrieve to update metrics
	store.Retrieve(addr1)

	// Check metrics (via type assertion to access unexported methods)
	evStore := store.(*externalValueStore)

	if evStore.storeCount != 3 {
		t.Errorf("StoreCount: got %d, want 3", evStore.storeCount)
	}

	if evStore.retrieveCount != 1 {
		t.Errorf("RetrieveCount: got %d, want 1", evStore.retrieveCount)
	}

	expectedBytes := uint64(len(v1) + len(v2) + len(v3))
	if evStore.totalBytes != expectedBytes {
		t.Errorf("TotalBytes: got %d, want %d", evStore.totalBytes, expectedBytes)
	}

	if evStore.activeCount != 3 {
		t.Errorf("ActiveCount: got %d, want 3", evStore.activeCount)
	}

	// Delete a value
	store.Delete(addr1)

	if evStore.activeCount != 2 {
		t.Errorf("ActiveCount after delete: got %d, want 2", evStore.activeCount)
	}

	if evStore.deletedBytes != uint64(len(v1)) {
		t.Errorf("DeletedBytes: got %d, want %d", evStore.deletedBytes, len(v1))
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestStore_ValueTooLarge(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	config := api.Config{
		MaxValueSize: 100, // Small limit for testing
	}

	store, err := NewExternalValueStore(segMgr, config)
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Try to store value larger than MaxValueSize
	value := make([]byte, 101)
	_, err = store.Store(value)
	if err != api.ErrValueTooLarge {
		t.Errorf("Expected ErrValueTooLarge, got: %v", err)
	}
}

func TestRetrieve_NotFound(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Try to retrieve non-existent value
	_, err = store.Retrieve(vaddr.VAddr{SegmentID: 1, Offset: 9999})
	if err != api.ErrValueNotFound {
		t.Errorf("Expected ErrValueNotFound, got: %v", err)
	}
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func TestConcurrent_StoreRetrieve(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	const goroutines = 10
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Store concurrently
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				value := []byte(string(rune('A'+id)) + string(rune('0'+j%10)))
				addr, err := store.Store(value)
				if err != nil {
					t.Errorf("Concurrent store failed: %v", err)
					return
				}
				// Immediately retrieve
				retrieved, err := store.Retrieve(addr)
				if err != nil {
					t.Errorf("Concurrent retrieve failed: %v", err)
					return
				}
				if !bytes.Equal(value, retrieved) {
					t.Errorf("Concurrent retrieve mismatch")
				}
			}
		}(i)
	}

	// Retrieve concurrently
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// Get size of existing value
				_, err := store.GetValueSize(vaddr.VAddr{SegmentID: 1, Offset: uint64(j)})
				if err != nil {
					// Some sizes may not exist, that's OK
					continue
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrent_Delete(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Store a value
	value := []byte("concurrent delete test")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Delete concurrently (should be safe due to mutex)
	var wg sync.WaitGroup
	const goroutines = 10
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			store.Delete(addr)
		}()
	}

	wg.Wait()

	// Value should still be accessible after concurrent deletes
	_, err = store.Retrieve(addr)
	if err != nil {
		t.Errorf("Retrieve after concurrent delete failed: %v", err)
	}
}

func TestConcurrent_AtomicCounter(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	const goroutines = 20
	const opsPerGoroutine = 100
	var totalStored int64

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				value := []byte{byte(j)}
				_, err := store.Store(value)
				if err != nil {
					t.Errorf("Store failed: %v", err)
					return
				}
				atomic.AddInt64(&totalStored, 1)
			}
		}()
	}

	wg.Wait()

	expected := int64(goroutines * opsPerGoroutine)
	if totalStored != expected {
		t.Errorf("Total stored: got %d, want %d", totalStored, expected)
	}
}

// =============================================================================
// Integration Tests (with real segment files)
// =============================================================================

func TestStoreRetrieve_RealSegments(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "external-value-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create real segment manager
	segMgr, err := storage.OpenSegmentManager(storage.StorageConfig{
		Directory:   tmpDir,
		SegmentSize: 1 << 20, // 1MB segments for testing
	})
	if err != nil {
		t.Fatalf("OpenSegmentManager failed: %v", err)
	}
	defer segMgr.Close()

	// Create external value store
	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Store and retrieve various values
	testCases := [][]byte{
		[]byte("small"),
		make([]byte, 100),
		make([]byte, 4064), // Exactly one page
		make([]byte, 4065), // Just over one page
		make([]byte, 10000),
	}

	for i, tc := range testCases {
		for j := range tc {
			tc[j] = byte((i*100 + j) % 256)
		}

		addr, err := store.Store(tc)
		if err != nil {
			t.Fatalf("Store failed for test case %d: %v", i, err)
		}

		retrieved, err := store.Retrieve(addr)
		if err != nil {
			t.Fatalf("Retrieve failed for test case %d: %v", i, err)
		}

		if !bytes.Equal(tc, retrieved) {
			t.Errorf("Test case %d mismatch: got len=%d, want len=%d", i, len(retrieved), len(tc))
		}
	}
}

func TestStoreRetrieve_AcrossSegments(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "external-value-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create real segment manager with small segment size
	segMgr, err := storage.OpenSegmentManager(storage.StorageConfig{
		Directory:   tmpDir,
		SegmentSize: 1 << 12, // 4KB segments - forces segment rotation
	})
	if err != nil {
		t.Fatalf("OpenSegmentManager failed: %v", err)
	}
	defer segMgr.Close()

	// Create external value store
	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	// Store values that will span multiple segments
	var addrs []vaddr.VAddr
	for i := 0; i < 10; i++ {
		value := make([]byte, 1000)
		for j := range value {
			value[j] = byte((i*100 + j) % 256)
		}

		addr, err := store.Store(value)
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}
		addrs = append(addrs, addr)
	}

	// Verify all values can be retrieved
	for i, addr := range addrs {
		retrieved, err := store.Retrieve(addr)
		if err != nil {
			t.Fatalf("Retrieve failed for value %d: %v", i, err)
		}

		expected := make([]byte, 1000)
		for j := range expected {
			expected[j] = byte((i*100 + j) % 256)
		}

		if !bytes.Equal(expected, retrieved) {
			t.Errorf("Value %d mismatch", i)
		}
	}
}

// =============================================================================
// VAddr Format Tests (known trap: Segment/external-value boundary)
// =============================================================================

func TestVAddrFormat(t *testing.T) {
	segMgr := newMockSegmentManager()
	defer segMgr.Close()

	store, err := NewExternalValueStore(segMgr, api.DefaultConfig())
	if err != nil {
		t.Fatalf("NewExternalValueStore failed: %v", err)
	}
	defer store.Close()

	value := []byte("vaddr format test")
	addr, err := store.Store(value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Verify VAddr format
	if addr.SegmentID == 0 {
		t.Error("VAddr.SegmentID should not be 0")
	}

	if addr.Offset == 0 {
		t.Error("VAddr.Offset should not be 0 for stored value")
	}

	// Verify VAddr is consistent
	retrieved, err := store.Retrieve(addr)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !bytes.Equal(value, retrieved) {
		t.Errorf("Retrieve mismatch")
	}
}

func TestVAddrToBytesRoundTrip(t *testing.T) {
	addr := vaddr.VAddr{
		SegmentID: 12345,
		Offset:    67890,
	}

	// Convert to bytes
	b := addr.ToBytes()

	// Convert back
	addr2 := vaddr.VAddrFromBytes(b)

	if addr.SegmentID != addr2.SegmentID {
		t.Errorf("SegmentID round trip failed: got %d, want %d", addr2.SegmentID, addr.SegmentID)
	}

	if addr.Offset != addr2.Offset {
		t.Errorf("Offset round trip failed: got %d, want %d", addr2.Offset, addr.Offset)
	}
}

// =============================================================================
// Utility Tests
// =============================================================================

func TestThreshold(t *testing.T) {
	if api.Threshold() != 48 {
		t.Errorf("Threshold: got %d, want 48", api.Threshold())
	}
}

func TestShouldStoreExternally(t *testing.T) {
	testCases := []struct {
		size  int
		above bool
	}{
		{48, false},  // At threshold
		{49, true},   // Above threshold
		{47, false},  // Below threshold
		{0, false},   // Zero
		{1000, true}, // Large
	}

	for _, tc := range testCases {
		result := api.ShouldStoreExternally(tc.size)
		if result != tc.above {
			t.Errorf("ShouldStoreExternally(%d): got %v, want %v", tc.size, result, tc.above)
		}
	}
}

func TestInlineCapacity(t *testing.T) {
	if api.InlineCapacity() != 48 {
		t.Errorf("InlineCapacity: got %d, want 48", api.InlineCapacity())
	}
}

// Ensure the test compiles and all interface methods are implemented
var _ api.ExternalValueStore = (*externalValueStore)(nil)
