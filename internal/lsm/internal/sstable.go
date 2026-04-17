package internal

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// ─── SSTable Format ─────────────────────────────────────────────────

// SSTable file format with Bloom Filter:
// [0:4]   uint32  magic ("LSM1")
// [4:8]   uint32  version (2 - with Bloom Filter)
// [8:12]  uint32  numPageMappings
// [12:16] uint32  numBlobMappings
// [16:20] uint32  pageMappingsOffset
// [20:24] uint32  blobMappingsOffset
// [24:28] uint32  bloomFilterOffset
// [28:32] uint32  bloomFilterSize
// [32:36] uint32  footerOffset (same as file size)
// [36:40] reserved
// [40:44] uint32  numPageMappingsCopy (for verification)
// [44:]   data area:
//          bloom filter data
//          page mappings: [key:8][value:8] pairs, sorted by key
//          blob mappings: [key:8][value:8][size:4] triples, sorted by key

const (
	sstMagic          = 0x314D534C // "LSM1" in little-endian
	sstVersion        = 2          // version 2: includes Bloom Filter
	sstHeaderSize     = 44
	sstFooterSize     = 4
	sstBloomFalseRate = 0.01       // 1% false positive rate
)

// sstEntry represents a single entry in the SSTable.
type sstEntry struct {
	key   uint64
	value uint64
	size  uint32 // only used for blob mappings
}

// sstWithBloom wraps SSTable entries with their Bloom Filter.
type sstWithBloom struct {
	entries     []sstEntry
	bloomFilter *BloomFilter
}

// writeSSTable writes a sorted list of entries to an SSTable file with Bloom Filter.
func writeSSTable(path string, pageMappings []sstEntry, blobMappings []sstEntry) error {
	f, err := createFile(path)
	if err != nil {
		return fmt.Errorf("create sst: %w", err)
	}

	// Sort entries by key
	sort.Slice(pageMappings, func(i, j int) bool {
		return pageMappings[i].key < pageMappings[j].key
	})
	sort.Slice(blobMappings, func(i, j int) bool {
		return blobMappings[i].key < blobMappings[j].key
	})

	// Build Bloom Filter with all keys
	bloom := NewBloomFilter(uint64(len(pageMappings)+len(blobMappings)), sstBloomFalseRate)
	for _, e := range pageMappings {
		bloom.Add(e.key)
	}
	for _, e := range blobMappings {
		bloom.Add(e.key)
	}
	bloomData := bloom.Serialize()

	// Calculate offsets
	bloomOffset := sstHeaderSize
	pageOffset := bloomOffset + len(bloomData)
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
	binary.LittleEndian.PutUint32(header[24:28], uint32(bloomOffset))
	binary.LittleEndian.PutUint32(header[28:32], uint32(len(bloomData)))
	binary.LittleEndian.PutUint32(header[32:36], uint32(footerOffset))
	binary.LittleEndian.PutUint32(header[40:44], uint32(len(pageMappings))) // copy for verification

	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write Bloom Filter
	if _, err := f.Write(bloomData); err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
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

	// Fsync to ensure SSTable data is durable before close.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sst sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("sst close: %w", err)
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

	data, err := readAll(f)
	if err != nil {
		return nil, nil, fmt.Errorf("read sst: %w", err)
	}

	return parseSSTable(data)
}

// parseSSTable parses SSTable data from bytes.
func parseSSTable(data []byte) ([]sstEntry, []sstEntry, error) {
	if len(data) < sstHeaderSize {
		return nil, nil, fmt.Errorf("file too small: %d bytes", len(data))
	}

	// Verify magic
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != sstMagic {
		return nil, nil, fmt.Errorf("bad magic: got %x, want %x", magic, sstMagic)
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	
	numPageMappings := binary.LittleEndian.Uint32(data[8:12])
	numBlobMappings := binary.LittleEndian.Uint32(data[12:16])
	
	var pageOffset, blobOffset uint32
	
	if version >= 2 {
		// Version 2+ includes Bloom Filter
		pageOffset = binary.LittleEndian.Uint32(data[16:20])
		blobOffset = binary.LittleEndian.Uint32(data[20:24])
		// Bloom filter info available via readBloomFilter()
	} else {
		// Version 1: no Bloom Filter
		pageOffset = binary.LittleEndian.Uint32(data[16:20])
		blobOffset = binary.LittleEndian.Uint32(data[20:24])
	}

	// Verify copy
	numPageMappingsCopy := binary.LittleEndian.Uint32(data[40:44])
	if numPageMappingsCopy != numPageMappings {
		return nil, nil, fmt.Errorf("page count mismatch: %d vs %d", numPageMappingsCopy, numPageMappings)
	}

	// Read page mappings
	pages := make([]sstEntry, numPageMappings)
	for i := uint32(0); i < numPageMappings; i++ {
		off := int(pageOffset) + int(i)*16
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
		off := int(blobOffset) + int(i)*20
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

// readBloomFilter reads the Bloom Filter from an SSTable file.
// Returns nil if the SSTable doesn't have a Bloom Filter (version 1).
func readBloomFilter(path string) *BloomFilter {
	f, err := openFile(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	data, err := readAll(f)
	if err != nil {
		return nil
	}

	return parseBloomFilter(data)
}

// parseBloomFilter parses Bloom Filter data from SSTable bytes.
// Returns nil if the SSTable doesn't have a Bloom Filter (version 1).
func parseBloomFilter(data []byte) *BloomFilter {
	if len(data) < sstHeaderSize {
		return nil
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version < 2 {
		return nil // Version 1 doesn't have Bloom Filter
	}

	bloomOffset := binary.LittleEndian.Uint32(data[24:28])
	bloomSize := binary.LittleEndian.Uint32(data[28:32])

	if int(bloomOffset)+int(bloomSize) > len(data) {
		return nil
	}

	bloomData := data[bloomOffset : bloomOffset+bloomSize]
	return DeserializeBloomFilter(bloomData)
}

// MaybeHasKey checks if a key might be in the SSTable using the Bloom Filter.
// Returns:
// - false: key is definitely NOT in the SSTable
// - true: key might be in the SSTable (needs further check)
func (s *sstWithBloom) MaybeHasKey(key uint64) bool {
	if s.bloomFilter == nil {
		return true // Bloom Filter not available, assume possible
	}
	return s.bloomFilter.Contains(key)
}
