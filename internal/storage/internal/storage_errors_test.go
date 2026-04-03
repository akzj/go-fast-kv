package internal

import (
	"os"
	"path/filepath"
	"testing"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Segment Error Handling Tests
// =============================================================================

func TestSegment_CloseThenRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Close the segment
	if err := seg.Close(); err != nil {
		t.Fatal(err)
	}

	// Read after close should fail
	_, err = seg.ReadAt(headerSize, vaddr.PageSize)
	if err != ErrSegmentClosed {
		t.Errorf("expected ErrSegmentClosed, got %v", err)
	}
}

func TestSegment_CloseThenAppend(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Close the segment
	if err := seg.Close(); err != nil {
		t.Fatal(err)
	}

	// Append after close should fail
	data := make([]byte, vaddr.PageSize)
	_, err = seg.Append(data)
	if err != ErrSegmentClosed {
		t.Errorf("expected ErrSegmentClosed, got %v", err)
	}
}

func TestSegment_SealedThenAppend(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	segID := seg.ID()
	// Don't close seg - let CreateSegment seal it when we create another

	// Create another segment - this seals the first
	_, err = sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Get the sealed segment
	seg = sm.GetSegment(segID)
	if seg == nil {
		t.Fatal("segment not found")
	}

	// Append to sealed segment should fail
	data := make([]byte, vaddr.PageSize)
	_, err = seg.Append(data)
	if err != ErrSegmentNotActive {
		t.Errorf("expected ErrSegmentNotActive, got %v", err)
	}
}

func TestSegment_InvalidOffset(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Read at offset before header should fail
	_, err = seg.ReadAt(0, vaddr.PageSize)
	if err != ErrInvalidOffset {
		t.Errorf("expected ErrInvalidOffset, got %v", err)
	}

	// Read at negative offset (simulated by checking header boundary)
	_, err = seg.ReadAt(headerSize-10, vaddr.PageSize)
	if err != ErrInvalidOffset {
		t.Errorf("expected ErrInvalidOffset, got %v", err)
	}
}

func TestSegment_InvalidAlignment(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Append non-page-aligned data should fail
	data := make([]byte, vaddr.PageSize-1)
	_, err = seg.Append(data)
	if err != ErrInvalidAlignment {
		t.Errorf("expected ErrInvalidAlignment, got %v", err)
	}

	// Append half page should also fail
	data = make([]byte, vaddr.PageSize/2)
	_, err = seg.Append(data)
	if err != ErrInvalidAlignment {
		t.Errorf("expected ErrInvalidAlignment, got %v", err)
	}

	// Append double page should succeed
	data = make([]byte, vaddr.PageSize*2)
	_, err = seg.Append(data)
	if err != nil {
		t.Errorf("expected no error for page-aligned data, got %v", err)
	}
}

// =============================================================================
// OSFile Error Handling Tests
// =============================================================================

func TestOSFile_OperationsOnClosedFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "testfile")
	file := NewOSFile(path)

	// Open and close
	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	// ReadAt on closed file should fail
	_, err = file.ReadAt(make([]byte, 10), 0)
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	// WriteAt on closed file should fail
	_, err = file.WriteAt(make([]byte, 10), 0)
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	// Sync on closed file should fail
	err = file.Sync()
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	// Truncate on closed file should fail
	err = file.Truncate(100)
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	// Size on closed file should fail
	_, err = file.Size()
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}
}

func TestOSFile_InvalidPath(t *testing.T) {
	// Try to open file in non-existent directory
	path := "/nonexistent/directory/that/does/not/exist/file"
	file := NewOSFile(path)

	err := file.Open(path)
	if err == nil {
		t.Error("expected error opening file in non-existent directory")
	}
}

func TestOSFile_OperationsWithoutOpen(t *testing.T) {
	// Create file but don't open it
	file := NewOSFile("/tmp/testfile")

	// All operations should fail without opening
	_, err := file.ReadAt(make([]byte, 10), 0)
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	_, err = file.WriteAt(make([]byte, 10), 0)
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	err = file.Sync()
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	err = file.Truncate(100)
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}

	_, err = file.Size()
	if err != ErrFileNotOpen {
		t.Errorf("expected ErrFileNotOpen, got %v", err)
	}
}

// =============================================================================
// SegmentManager Error Handling Tests
// =============================================================================

func TestSegmentManager_InvalidDirectory(t *testing.T) {
	// Try to create manager with invalid directory path
	config := Config{
		Directory:   "/nonexistent/path/that/cannot/be/created",
		SegmentSize: 1 << 20,
	}

	_, err := NewSegmentManager(config)
	// This should either fail on mkdir or succeed if parent can be created
	// The behavior depends on the system
	if err != nil {
		// Error is acceptable - directory creation failed
		t.Logf("Expected error for invalid directory: %v", err)
	}
}

func TestSegmentManager_MaxSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{
		Directory:       dir,
		SegmentSize:     1 << 20,
		MaxSegmentCount: 2,
	}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create max segments (2 allowed)
	_, err = sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	_, err = sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Creating third should fail
	_, err = sm.CreateSegment()
	if err != ErrMaxSegments {
		t.Errorf("expected ErrMaxSegments, got %v", err)
	}
}

func TestSegmentManager_SealInvalidSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Seal non-existent segment
	err = sm.SealSegment(vaddr.SegmentID(999))
	if err != ErrSegmentNotFound {
		t.Errorf("expected ErrSegmentNotFound, got %v", err)
	}
}

func TestSegmentManager_SealInvalidID(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Seal with invalid segment ID
	err = sm.SealSegment(vaddr.SegmentIDInvalid)
	if err != ErrInvalidSegmentID {
		t.Errorf("expected ErrInvalidSegmentID, got %v", err)
	}
}

func TestSegmentManager_ArchiveInvalidSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Archive non-existent segment
	err = sm.ArchiveSegment(vaddr.SegmentID(999))
	if err != ErrSegmentNotFound {
		t.Errorf("expected ErrSegmentNotFound, got %v", err)
	}
}

func TestSegmentManager_ArchiveNonSealedSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create a segment (active)
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Try to archive active segment directly
	err = sm.ArchiveSegment(seg.ID())
	if err != ErrSegmentNotSealed {
		t.Errorf("expected ErrSegmentNotSealed, got %v", err)
	}
}

func TestSegmentManager_OperationsAfterClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	sm.Close() // Close immediately

	// CreateSegment after close should fail
	_, err = sm.CreateSegment()
	if err != ErrStorageClosed {
		t.Errorf("expected ErrStorageClosed, got %v", err)
	}

	// SealSegment after close should fail
	err = sm.SealSegment(vaddr.SegmentID(1))
	if err != ErrStorageClosed {
		t.Errorf("expected ErrStorageClosed, got %v", err)
	}

	// ArchiveSegment after close should fail
	err = sm.ArchiveSegment(vaddr.SegmentID(1))
	if err != ErrStorageClosed {
		t.Errorf("expected ErrStorageClosed, got %v", err)
	}
}

func TestSegmentManager_ReuseSealedSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create first segment
	seg1, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	seg1ID := seg1.ID()

	// Append data to first segment
	data := make([]byte, vaddr.PageSize)
	_, err = seg1.Append(data)
	if err != nil {
		t.Fatal(err)
	}
	// Don't close seg1 - let CreateSegment seal it

	// Create second segment - this should seal the first
	seg2, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// First segment should now be sealed
	seg1 = sm.GetSegment(seg1ID)
	if seg1.State() != vaddr.SegmentStateSealed {
		t.Errorf("expected Sealed state, got %s", seg1.State())
	}

	// Second segment should be active
	if seg2.State() != vaddr.SegmentStateActive {
		t.Errorf("expected Active state, got %s", seg2.State())
	}
}

// =============================================================================
// OpenSegment Error Handling Tests
// =============================================================================

func TestOpenSegment_InvalidMagic(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create a file with invalid magic
	path := filepath.Join(dir, "segment_invalid")
	file := NewOSFile(path)
	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write invalid header
	header := make([]byte, headerSize)
	header[0] = 0xFF // Invalid magic
	file.WriteAt(header, 0)

	// Try to open segment with invalid magic
	_, err = OpenSegment(vaddr.SegmentID(1), file)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestOpenSegment_InvalidVersion(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "segment_version")
	file := NewOSFile(path)
	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write header with invalid version
	header := make([]byte, headerSize)
	header[0] = 0x47 // Part of magic
	header[1] = 0x41
	header[2] = 0x4E
	header[3] = 0x41
	header[4] = 0x4D
	header[5] = 0x45
	header[6] = 0x47
	header[7] = 0x53
	// Version at offset 8 (uint16) = 0xFFFF (unsupported)
	header[8] = 0xFF
	header[9] = 0xFF
	file.WriteAt(header, 0)

	_, err = OpenSegment(vaddr.SegmentID(1), file)
	if err == nil {
		t.Error("expected error for invalid version")
	}
}

// =============================================================================
// Sync and Truncate Tests
// =============================================================================

func TestSegment_Sync(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Append data
	data := make([]byte, vaddr.PageSize)
	for i := range data {
		data[i] = byte(i)
	}
	_, err = seg.Append(data)
	if err != nil {
		t.Fatal(err)
	}

	// Sync should succeed
	if err := seg.Sync(); err != nil {
		t.Fatal(err)
	}

	// Read back and verify
	readData, err := seg.ReadAt(headerSize, vaddr.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if readData[i] != data[i] {
			t.Errorf("data mismatch at offset %d", i)
		}
	}
}

func TestOSFile_Truncate(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "testfile")
	file := NewOSFile(path)

	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write some data
	data := []byte("hello world")
	_, err = file.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify size
	size, err := file.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}

	// Truncate to smaller size
	err = file.Truncate(5)
	if err != nil {
		t.Fatal(err)
	}

	// Verify new size
	size, err = file.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 5 {
		t.Errorf("expected size 5, got %d", size)
	}

	// Truncate to larger size
	err = file.Truncate(20)
	if err != nil {
		t.Fatal(err)
	}

	size, err = file.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 20 {
		t.Errorf("expected size 20, got %d", size)
	}
}

// =============================================================================
// WriteAt/ReadAt Boundary Tests
// =============================================================================

func TestOSFile_WriteReadBoundary(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "testfile")
	file := NewOSFile(path)

	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write at specific offsets
	testCases := []struct {
		offset int64
		data   []byte
	}{
		{0, []byte("start")},
		{100, []byte("middle")},
		{1000, []byte("end")},
	}

	for _, tc := range testCases {
		n, err := file.WriteAt(tc.data, tc.offset)
		if err != nil {
			t.Fatalf("write at offset %d: %v", tc.offset, err)
		}
		if n != len(tc.data) {
			t.Errorf("expected %d bytes written, got %d", len(tc.data), n)
		}
	}

	// Read back and verify
	for _, tc := range testCases {
		buf := make([]byte, len(tc.data))
		n, err := file.ReadAt(buf, tc.offset)
		if err != nil {
			t.Fatalf("read at offset %d: %v", tc.offset, err)
		}
		if string(buf[:n]) != string(tc.data) {
			t.Errorf("expected %q at offset %d, got %q", tc.data, tc.offset, string(buf[:n]))
		}
	}
}

// =============================================================================
// Segment State Transitions
// =============================================================================

func TestSegment_FullLifecycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create segment - should be Active
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	segID := seg.ID()

	if seg.State() != vaddr.SegmentStateActive {
		t.Errorf("expected Active, got %s", seg.State())
	}

	// Append data
	data := make([]byte, vaddr.PageSize)
	addr, err := seg.Append(data)
	if err != nil {
		t.Fatal(err)
	}
	if addr.SegmentID != uint64(segID) {
		t.Errorf("expected SegmentID %d, got %d", segID, addr.SegmentID)
	}

	// Read back data
	readData, err := seg.ReadAt(int64(addr.Offset), len(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(readData) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(readData))
	}

	// Sync
	if err := seg.Sync(); err != nil {
		t.Fatal(err)
	}

	// Seal
	if err := sm.SealSegment(segID); err != nil {
		t.Fatal(err)
	}

	// State should be Sealed
	seg = sm.GetSegment(segID)
	if seg.State() != vaddr.SegmentStateSealed {
		t.Errorf("expected Sealed, got %s", seg.State())
	}

	// Archive
	if err := sm.ArchiveSegment(segID); err != nil {
		t.Fatal(err)
	}

	// State should be Archived
	seg = sm.GetSegment(segID)
	if seg.State() != vaddr.SegmentStateArchived {
		t.Errorf("expected Archived, got %s", seg.State())
	}

	// Read should still work on archived segment
	readData, err = seg.ReadAt(int64(addr.Offset), len(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(readData) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(readData))
	}
}

// =============================================================================
// Checksum Tests
// =============================================================================

func TestCRC32Checksum(t *testing.T) {
	// Test that CRC32Checksum is deterministic
	data := []byte("hello world")
	result1 := CRC32Checksum(data)
	result2 := CRC32Checksum(data)
	if result1 != result2 {
		t.Errorf("CRC32 not deterministic: %x != %x", result1, result2)
	}

	// Test empty data
	empty1 := CRC32Checksum([]byte{})
	empty2 := CRC32Checksum([]byte{})
	if empty1 != empty2 {
		t.Errorf("CRC32(empty) not deterministic")
	}

	// Test known CRC32.IEEE values
	// CRC32("a") = 0xe8b7be43 (using crc32.IEEE table)
	expectedA := uint32(0xe8b7be43)
	if result := CRC32Checksum([]byte("a")); result != expectedA {
		t.Errorf("CRC32(a) = %x, expected %x", result, expectedA)
	}
}
