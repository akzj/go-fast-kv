package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segment "github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// openChecksumTestStore creates a fresh PageStore + SegmentManager for testing.
func openChecksumTestStore(t *testing.T) (*pageStore, segmentapi.SegmentManager, string) {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatal(err)
	}
	segMgr, err := segment.New(segmentapi.Config{Dir: segDir, MaxSize: 64 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	ps := New(pagestoreapi.Config{}, segMgr, newMockLSM()).(*pageStore)
	return ps, segMgr, segDir
}

// TestChecksum_WriteReadVerify writes a page and reads it back,
// verifying the CRC32 checksum passes transparently.


func TestChecksum_WriteReadVerify(t *testing.T) {
	ps, segMgr, _ := openChecksumTestStore(t)
	defer segMgr.Close()
	defer ps.Close()

	pageID := ps.Alloc()
	data := make([]byte, pagestoreapi.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err := ps.Write(pageID, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	got, err := ps.Read(pageID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Read data mismatch")
	}
}

// TestChecksum_CorruptData writes a page, then corrupts the page data
// bytes in the segment file. Read should return ErrChecksumMismatch.
func TestChecksum_CorruptData(t *testing.T) {
	ps, segMgr, segDir := openChecksumTestStore(t)

	pageID := ps.Alloc()
	data := make([]byte, pagestoreapi.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err := ps.Write(pageID, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Sync to ensure data is on disk
	if err := segMgr.Sync(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the page data bytes in the segment file
	corruptSegmentFile(t, segDir, 16, 0xFF) // offset 16 = inside page data (after 8-byte pageID header)

	// Read should detect corruption
	_, err = ps.Read(pageID)
	if err == nil {
		t.Fatal("expected error on corrupted data, got nil")
	}
	if !errors.Is(err, pagestoreapi.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}

	segMgr.Close()
	ps.Close()
}

// TestChecksum_CorruptChecksum writes a page, then corrupts only the CRC32
// bytes at the end of the record. Read should return ErrChecksumMismatch.
func TestChecksum_CorruptChecksum(t *testing.T) {
	ps, segMgr, segDir := openChecksumTestStore(t)

	pageID := ps.Alloc()
	data := make([]byte, pagestoreapi.PageSize)
	copy(data, []byte("test data for checksum corruption"))

	_, err := ps.Write(pageID, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := segMgr.Sync(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the CRC32 bytes (last 4 bytes of the record)
	// Record starts at segment header offset. CRC is at offset 8+4096 = 4104 within the record.
	crcOffset := int64(8 + pagestoreapi.PageSize) // 4104 bytes into the record
	corruptSegmentFile(t, segDir, crcOffset, 0xFF)

	_, err = ps.Read(pageID)
	if err == nil {
		t.Fatal("expected error on corrupted CRC, got nil")
	}
	if !errors.Is(err, pagestoreapi.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}

	segMgr.Close()
	ps.Close()
}

// TestChecksum_CorruptPageID writes a page, then corrupts the pageID header.
// The CRC covers the pageID, so this should be detected.
func TestChecksum_CorruptPageID(t *testing.T) {
	ps, segMgr, segDir := openChecksumTestStore(t)

	pageID := ps.Alloc()
	data := make([]byte, pagestoreapi.PageSize)
	copy(data, []byte("test data for pageID corruption"))

	_, err := ps.Write(pageID, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := segMgr.Sync(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the pageID header (first 8 bytes of the record)
	corruptSegmentFile(t, segDir, 2, 0xFF) // corrupt byte 2 of the pageID

	_, err = ps.Read(pageID)
	if err == nil {
		t.Fatal("expected error on corrupted pageID, got nil")
	}
	if !errors.Is(err, pagestoreapi.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}

	segMgr.Close()
	ps.Close()
}

// TestChecksum_MultiplePages writes many pages and verifies all read back correctly.
func TestChecksum_MultiplePages(t *testing.T) {
	ps, segMgr, _ := openChecksumTestStore(t)
	defer segMgr.Close()
	defer ps.Close()

	const numPages = 100
	pages := make(map[pagestoreapi.PageID][]byte)

	for i := 0; i < numPages; i++ {
		pageID := ps.Alloc()
		data := make([]byte, pagestoreapi.PageSize)
		// Fill with unique pattern per page
		binary.BigEndian.PutUint64(data[:8], uint64(i*12345+67890))
		for j := 8; j < len(data); j++ {
			data[j] = byte((i + j) % 256)
		}
		pages[pageID] = data

		_, err := ps.Write(pageID, data)
		if err != nil {
			t.Fatalf("Write page %d failed: %v", pageID, err)
		}
	}

	// Read all back and verify
	for pageID, expected := range pages {
		got, err := ps.Read(pageID)
		if err != nil {
			t.Fatalf("Read page %d failed: %v", pageID, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("Page %d data mismatch", pageID)
		}
	}
}

// TestChecksum_RecordFormat verifies the on-disk record format is
// [pageID:8][data:4096][crc32:4] = 4108 bytes.
func TestChecksum_RecordFormat(t *testing.T) {
	ps, segMgr, _ := openChecksumTestStore(t)
	defer segMgr.Close()
	defer ps.Close()

	pageID := ps.Alloc()
	data := make([]byte, pagestoreapi.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	entry, err := ps.Write(pageID, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	vaddr := segmentapi.UnpackVAddr(entry.VAddr)
	raw, err := segMgr.ReadAt(vaddr, pagestoreapi.PageRecordSize)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	// Verify record size
	if len(raw) != int(pagestoreapi.PageRecordSize) {
		t.Fatalf("record size: got %d, want %d", len(raw), pagestoreapi.PageRecordSize)
	}

	// Verify pageID header
	gotPageID := binary.BigEndian.Uint64(raw[:8])
	if gotPageID != pageID {
		t.Fatalf("pageID: got %d, want %d", gotPageID, pageID)
	}

	// Verify page data
	if !bytes.Equal(raw[8:8+pagestoreapi.PageSize], data) {
		t.Fatal("page data in record doesn't match written data")
	}

	// Verify CRC32
	storedCRC := binary.BigEndian.Uint32(raw[8+pagestoreapi.PageSize:])
	computedCRC := crc32.ChecksumIEEE(raw[:8+pagestoreapi.PageSize])
	if storedCRC != computedCRC {
		t.Fatalf("CRC mismatch: stored=0x%08x computed=0x%08x", storedCRC, computedCRC)
	}
}

// ─── Helpers ────────────────────────────────────────────────────────

// corruptSegmentFile finds the first .seg file in segDir and flips a byte
// at the given offset (relative to the start of the first page record,
// which is after the segment header).
func corruptSegmentFile(t *testing.T, segDir string, recordOffset int64, xorByte byte) {
	t.Helper()

	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatal(err)
	}

	var segFile string
	for _, e := range entries {
		if !e.IsDir() {
			segFile = filepath.Join(segDir, e.Name())
			break
		}
	}
	if segFile == "" {
		t.Fatal("no segment file found")
	}

	f, err := os.OpenFile(segFile, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Detect segment header by checking for known magic bytes.
	// Legacy segments (no magic) have headerSize = 0.
	// New segments start with "PAGESEGM" or "BLOBSEGM" and have a 64-byte header.
	magicBuf := make([]byte, 8)
	var headerSize int64
	if n, err := f.ReadAt(magicBuf, 0); err == nil && n == 8 {
		magic := string(magicBuf)
		if magic == "PAGESEGM" || magic == "BLOBSEGM" {
			headerSize = 64 // SegmentHeaderSize
		}
	}

	absOffset := headerSize + recordOffset

	// Read the byte
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, absOffset); err != nil {
		t.Fatalf("ReadAt offset %d: %v", absOffset, err)
	}

	// XOR to corrupt
	buf[0] ^= xorByte

	// Write back
	if _, err := f.WriteAt(buf, absOffset); err != nil {
		t.Fatalf("WriteAt offset %d: %v", absOffset, err)
	}

	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}
