package internal

import (
	"os"
	"path/filepath"
	"testing"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

func TestSegmentManager_CreateAndClose(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create segment manager
	config := Config{
		Directory:   dir,
		SegmentSize: 1 << 20, // 1MB for testing
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Should start with no segments
	if sm.SegmentCount() != 0 {
		t.Errorf("expected 0 segments, got %d", sm.SegmentCount())
	}

	// Create first segment
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	if seg.ID() != 1 {
		t.Errorf("expected segment ID 1, got %d", seg.ID())
	}
	if seg.State() != vaddr.SegmentStateActive {
		t.Errorf("expected Active state, got %s", seg.State())
	}

	// Active segment should be returned
	if sm.ActiveSegment() == nil {
		t.Error("expected active segment")
	}

	// Segment count should be 1
	if sm.SegmentCount() != 1 {
		t.Errorf("expected 1 segment, got %d", sm.SegmentCount())
	}
}

func TestSegment_Append(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create segment manager
	config := Config{
		Directory:   dir,
		SegmentSize: 1 << 20,
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create segment
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Append page-aligned data
	pageData := make([]byte, vaddr.PageSize)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	addr, err := seg.Append(pageData)
	if err != nil {
		t.Fatal(err)
	}

	if addr.SegmentID != 1 {
		t.Errorf("expected SegmentID 1, got %d", addr.SegmentID)
	}
	if addr.Offset != uint64(headerSize) {
		t.Errorf("expected offset %d, got %d", headerSize, addr.Offset)
	}

	if seg.Size() != int64(vaddr.PageSize) {
		t.Errorf("expected size %d, got %d", vaddr.PageSize, seg.Size())
	}
	if seg.PageCount() != 1 {
		t.Errorf("expected 1 page, got %d", seg.PageCount())
	}
}

func TestSegment_Lifecycle(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create segment manager
	config := Config{
		Directory:   dir,
		SegmentSize: 1 << 20,
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create segment
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	segID := seg.ID()

	// Verify state transitions: Active -> Sealed -> Archived
	if seg.State() != vaddr.SegmentStateActive {
		t.Errorf("expected Active, got %s", seg.State())
	}

	// Seal segment (this also closes the segment from manager's perspective)
	err = sm.SealSegment(segID)
	if err != nil {
		t.Fatal(err)
	}

	// GetSegment returns the segment but it's sealed now
	seg = sm.GetSegment(segID)
	if seg == nil {
		t.Fatal("segment not found")
	}
	if seg.State() != vaddr.SegmentStateSealed {
		t.Errorf("expected Sealed, got %s", seg.State())
	}

	// Archive segment
	err = sm.ArchiveSegment(segID)
	if err != nil {
		t.Fatal(err)
	}

	// GetSegment should show Archived
	seg = sm.GetSegment(segID)
	if seg.State() != vaddr.SegmentStateArchived {
		t.Errorf("expected Archived, got %s", seg.State())
	}
}

func TestSegmentManager_MultipleSegments(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create segment manager
	config := Config{
		Directory:   dir,
		SegmentSize: 1 << 20,
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create multiple segments
	for i := 0; i < 3; i++ {
		_, err := sm.CreateSegment()
		if err != nil {
			t.Fatal(err)
		}
		// Don't close individual segments - manager handles lifecycle
	}

	if sm.SegmentCount() != 3 {
		t.Errorf("expected 3 segments, got %d", sm.SegmentCount())
	}

	// List segments should return in order
	segments := sm.ListSegments()
	if len(segments) != 3 {
		t.Errorf("expected 3 segments, got %d", len(segments))
	}
	for i, seg := range segments {
		if seg.ID() != vaddr.SegmentID(i+1) {
			t.Errorf("expected ID %d, got %d", i+1, seg.ID())
		}
	}

	// List by state: sealed segments 1-2, active segment 3
	sealed := sm.ListSegmentsByState(vaddr.SegmentStateSealed)
	if len(sealed) != 2 {
		t.Errorf("expected 2 sealed segments, got %d", len(sealed))
	}

	active := sm.ListSegmentsByState(vaddr.SegmentStateActive)
	if len(active) != 1 {
		t.Errorf("expected 1 active segment, got %d", len(active))
	}
}

func TestOSFile(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "testfile")
	file := NewOSFile(path)

	// Open file
	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write and read
	data := []byte("hello world")
	n, err := file.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}

	// Read back
	buf := make([]byte, len(data))
	n, err = file.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(buf[:n]))
	}

	// Size
	size, err := file.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}
}

func TestSegment_ReadAt(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create segment manager
	config := Config{
		Directory:   dir,
		SegmentSize: 1 << 20,
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create and populate segment
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Append two pages
	page1 := make([]byte, vaddr.PageSize)
	page2 := make([]byte, vaddr.PageSize)
	for i := range page1 {
		page1[i] = 0xAA
		page2[i] = 0xBB
	}

	_, err = seg.Append(page1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seg.Append(page2)
	if err != nil {
		t.Fatal(err)
	}

	// Read back page 2
	offset := int64(headerSize + vaddr.PageSize)
	data, err := seg.ReadAt(offset, vaddr.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != vaddr.PageSize {
		t.Errorf("expected %d bytes, got %d", vaddr.PageSize, len(data))
	}
	if data[0] != 0xBB {
		t.Errorf("expected 0xBB, got 0x%02X", data[0])
	}
}

func TestSegmentManager_CloseIdempotent(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{
		Directory:   dir,
		SegmentSize: 1 << 20,
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}

	// Close multiple times
	for i := 0; i < 3; i++ {
		if err := sm.Close(); err != nil {
			t.Errorf("close %d: %v", i+1, err)
		}
	}
}
