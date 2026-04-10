package internal

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// ─── SSTable Format ─────────────────────────────────────────────────

// SSTable file format (simplified, no CRC for now):
// [0:4]   uint32  magic ("LSM1")
// [4:8]   uint32  version (1)
// [8:12]  uint32  numPageMappings
// [12:16] uint32  numBlobMappings
// [16:20] uint32  pageMappingsOffset
// [20:24] uint32  blobMappingsOffset
// [24:28] uint32  footerOffset (same as file size, for easy detection)
// [28:32] reserved
// [32:36] uint32  numPageMappingsCopy (for verification)
// [36:]   data area:
//          page mappings: [key:8][value:8] pairs, sorted by key
//          blob mappings: [key:8][value:8][size:4] triples, sorted by key

const (
	sstMagic       = 0x314D534C // "LSM1" in little-endian
	sstVersion     = 1
	sstHeaderSize  = 36
	sstFooterSize  = 4
)

// sstEntry represents a single entry in the SSTable.
type sstEntry struct {
	key   uint64
	value uint64
	size  uint32 // only used for blob mappings
}

// writeSSTable writes a sorted list of entries to an SSTable file.
func writeSSTable(path string, pageMappings []sstEntry, blobMappings []sstEntry) error {
	f, err := createFile(path)
	if err != nil {
		return fmt.Errorf("create sst: %w", err)
	}
	defer f.Close()

	// Sort entries by key
	sort.Slice(pageMappings, func(i, j int) bool {
		return pageMappings[i].key < pageMappings[j].key
	})
	sort.Slice(blobMappings, func(i, j int) bool {
		return blobMappings[i].key < blobMappings[j].key
	})

	// Calculate offsets
	pageOffset := sstHeaderSize
	blobOffset := pageOffset + len(pageMappings)*16 // 8 bytes key + 8 bytes value
	footerOffset := blobOffset + len(blobMappings)*20 // 8 bytes key + 8 bytes value + 4 bytes size

	// Write header
	header := make([]byte, sstHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], sstMagic)
	binary.LittleEndian.PutUint32(header[4:8], sstVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(pageMappings)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(blobMappings)))
	binary.LittleEndian.PutUint32(header[16:20], uint32(pageOffset))
	binary.LittleEndian.PutUint32(header[20:24], uint32(blobOffset))
	binary.LittleEndian.PutUint32(header[24:28], uint32(footerOffset))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(pageMappings))) // copy for verification

	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write page mappings
	for _, e := range pageMappings {
		buf := make([]byte, 16)
		binary.LittleEndian.PutUint64(buf[0:8], e.key)
		binary.LittleEndian.PutUint64(buf[8:16], e.value)
		if _, err := f.Write(buf); err != nil {
			return fmt.Errorf("write page: %w", err)
		}
	}

	// Write blob mappings
	for _, e := range blobMappings {
		buf := make([]byte, 20)
		binary.LittleEndian.PutUint64(buf[0:8], e.key)
		binary.LittleEndian.PutUint64(buf[8:16], e.value)
		binary.LittleEndian.PutUint32(buf[16:20], e.size)
		if _, err := f.Write(buf); err != nil {
			return fmt.Errorf("write blob: %w", err)
		}
	}

	return nil
}

// readSSTable reads an SSTable file and returns the mappings.
func readSSTable(path string) ([]sstEntry, []sstEntry, error) {
	f, err := openFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open sst: %w", err)
	}
	defer f.Close()

	size := getFileSize(f)
	data, err := readAll(f)
	if err != nil {
		return nil, nil, fmt.Errorf("read sst: %w", err)
	}
	_ = size

	if len(data) < sstHeaderSize {
		return nil, nil, fmt.Errorf("file too small: %d bytes", len(data))
	}

	// Verify magic and version
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != sstMagic {
		return nil, nil, fmt.Errorf("bad magic: got %x, want %x", magic, sstMagic)
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != sstVersion {
		return nil, nil, fmt.Errorf("bad version: got %d, want %d", version, sstVersion)
	}

	numPageMappings := binary.LittleEndian.Uint32(data[8:12])
	numBlobMappings := binary.LittleEndian.Uint32(data[12:16])
	pageOffset := int(binary.LittleEndian.Uint32(data[16:20]))
	blobOffset := int(binary.LittleEndian.Uint32(data[20:24]))

	// Verify copy
	numPageMappingsCopy := binary.LittleEndian.Uint32(data[32:36])
	if numPageMappingsCopy != numPageMappings {
		return nil, nil, fmt.Errorf("page count mismatch: %d vs %d", numPageMappingsCopy, numPageMappings)
	}

	// Read page mappings
	pages := make([]sstEntry, numPageMappings)
	for i := uint32(0); i < numPageMappings; i++ {
		off := pageOffset + int(i)*16
		if off+16 > len(data) {
			return nil, nil, fmt.Errorf("page entry out of bounds")
		}
		pages[i] = sstEntry{
			key:   binary.LittleEndian.Uint64(data[off:]),
			value: binary.LittleEndian.Uint64(data[off+8:]),
		}
	}

	// Read blob mappings
	blobs := make([]sstEntry, numBlobMappings)
	for i := uint32(0); i < numBlobMappings; i++ {
		off := blobOffset + int(i)*20
		if off+20 > len(data) {
			return nil, nil, fmt.Errorf("blob entry out of bounds")
		}
		blobs[i] = sstEntry{
			key:   binary.LittleEndian.Uint64(data[off:]),
			value: binary.LittleEndian.Uint64(data[off+8:]),
			size:  binary.LittleEndian.Uint32(data[off+16:]),
		}
	}

	return pages, blobs, nil
}
