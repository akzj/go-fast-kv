package internal

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
	api "github.com/akzj/go-fast-kv/internal/external-value/api"
	storage "github.com/akzj/go-fast-kv/internal/storage/api"
)

// VAddr type alias for convenience
type VAddr = vaddr.VAddr

// =============================================================================
// External Value Store Implementation
// =============================================================================

// externalValueStore implements the ExternalValueStore interface.
// It stores values > 48 bytes in append-only segments, referenced by VAddr.
type externalValueStore struct {
	mu         sync.RWMutex
	segmentMgr storage.SegmentManager
	config     api.Config
	closed     bool

	// Index for tracking value metadata (VAddr -> valueInfo)
	index map[VAddr]*valueInfo

	// Metrics
	storeCount   uint64
	retrieveCount uint64
	totalBytes   uint64
	deletedBytes uint64
	activeCount  uint64
}

// valueInfo stores metadata about a stored value.
type valueInfo struct {
	size      uint64 // Total size of the value
	pageCount int    // Number of pages used
	deleted   bool
}

// NewExternalValueStore creates a new external value store.
func NewExternalValueStore(segmentMgr storage.SegmentManager, config api.Config) (api.ExternalValueStore, error) {
	if config.MaxValueSize == 0 {
		config.MaxValueSize = api.DefaultMaxValueSize
	}
	if config.SegmentSize == 0 {
		config.SegmentSize = 1 << 30 // 1 GB
	}

	store := &externalValueStore{
		segmentMgr: segmentMgr,
		config:     config,
		index:      make(map[VAddr]*valueInfo),
	}

	return store, nil
}

// Store persists a value and returns its VAddr.
func (s *externalValueStore) Store(value []byte) (VAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return VAddr{}, api.ErrStoreClosed
	}

	valueSize := uint64(len(value))
	if valueSize > s.config.MaxValueSize {
		return VAddr{}, api.ErrValueTooLarge
	}

	// Calculate number of pages needed
	// Each page has: 32 bytes header + up to 4064 bytes data
	// Total bytes per page = 4096 (PageSize)
	pageCount := int((valueSize + api.ExternalValueHeaderSize + api.ExternalValueDataPerPage - 1) / api.ExternalValueDataPerPage)

	// Get active segment
	segment := s.segmentMgr.ActiveSegment()
	if segment == nil {
		var err error
		segment, err = s.segmentMgr.CreateSegment()
		if err != nil {
			return VAddr{}, fmt.Errorf("create segment: %w", err)
		}
	}

	// Prepare multi-page data (each page is PageSize bytes, aligned)
	bytesPerPage := vaddr.PageSize
	data := make([]byte, pageCount*bytesPerPage)
	dataOffset := 0

	for page := 0; page < pageCount; page++ {
		// Calculate data size for this page
		pageDataSize := int(api.ExternalValueDataPerPage)
		if int(valueSize)-dataOffset < pageDataSize {
			pageDataSize = int(valueSize) - dataOffset
		}

		// Write header (32 bytes)
		headerOffset := page*bytesPerPage + api.ExternalValueHeaderSize - api.ExternalValueHeaderSize // start of page
		header := data[headerOffset : headerOffset+api.ExternalValueHeaderSize]
		copy(header[0:8], api.ExternalValueMagic)
		binary.LittleEndian.PutUint16(header[8:10], api.ExternalValueVersion)
		binary.LittleEndian.PutUint64(header[10:18], valueSize) // Total value size
		binary.LittleEndian.PutUint32(header[18:22], uint32(page))             // Page index
		binary.LittleEndian.PutUint32(header[22:26], uint32(pageCount))        // Total pages
		binary.LittleEndian.PutUint32(header[26:30], uint32(pageDataSize))    // This page's data size
		// Reserved: header[30:32]

		// Copy data portion (starts after header)
		dataStart := page*bytesPerPage + api.ExternalValueHeaderSize
		copy(data[dataStart:dataStart+pageDataSize], value[dataOffset:dataOffset+pageDataSize])

		dataOffset += pageDataSize
	}

	// Write all pages as a single aligned chunk
	newVAddr, err := segment.Append(data)
	if err != nil {
		return VAddr{}, fmt.Errorf("append to segment: %w", err)
	}

	// Update index
	s.index[newVAddr] = &valueInfo{
		size:      valueSize,
		pageCount: pageCount,
		deleted:   false,
	}

	// Update metrics atomically
	atomic.AddUint64(&s.storeCount, 1)
	atomic.AddUint64(&s.totalBytes, valueSize)
	atomic.AddUint64(&s.activeCount, 1)

	return newVAddr, nil
}

// Retrieve reads the complete value at the given addr.
func (s *externalValueStore) Retrieve(addr VAddr) ([]byte, error) {
	size, err := s.GetValueSize(addr)
	if err != nil {
		return nil, err
	}

	return s.RetrieveAt(addr, 0, size)
}

// RetrieveAt reads a slice of the value.
func (s *externalValueStore) RetrieveAt(addr VAddr, offset, length uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, api.ErrStoreClosed
	}

	info, ok := s.index[addr]
	if !ok {
		return nil, api.ErrValueNotFound
	}

	// Get segment
	segID := vaddr.SegmentID(addr.SegmentID)
	segment := s.segmentMgr.GetSegment(segID)
	if segment == nil {
		return nil, api.ErrValueNotFound
	}

	valueSize := info.size
	if offset+length > valueSize {
		return nil, api.ErrPartialRead
	}

	// Calculate which page(s) to read
	// We store pageCount pages, each of bytesPerPage (aligned)
	// The VAddr.Offset points to the start of the first page's data
	bytesPerPage := vaddr.PageSize
	pageCount := info.pageCount

	// Calculate the actual data range to read
	readOffset := int64(addr.Offset)
	totalReadSize := pageCount * bytesPerPage

	data, err := segment.ReadAt(readOffset, totalReadSize)
	if err != nil {
		return nil, fmt.Errorf("read segment: %w", err)
	}

	// Extract data from pages
	// Skip header (32 bytes) at the start of each page
	headerSize := int(api.ExternalValueHeaderSize)
	pageDataSize := int(api.ExternalValueDataPerPage)

	result := make([]byte, 0, length)
	remaining := int(length)
	skipBytes := int(offset % uint64(pageDataSize))

	for page := 0; page < pageCount && remaining > 0; page++ {
		// Calculate start of this page's data (after its header)
		pageDataStart := page*bytesPerPage + headerSize
		pageDataEnd := pageDataStart + pageDataSize
		if pageDataEnd > len(data) {
			pageDataEnd = len(data)
		}

		pageData := data[pageDataStart:pageDataEnd]

		// For first page, skip to the correct offset within the value data
		if page == 0 && skipBytes > 0 {
			if skipBytes >= len(pageData) {
				skipBytes -= len(pageData)
				continue
			}
			pageData = pageData[skipBytes:]
			skipBytes = 0
		}

		copyLen := len(pageData)
		if copyLen > remaining {
			copyLen = remaining
		}

		result = append(result, pageData[:copyLen]...)
		remaining -= copyLen
	}

	if len(result) != int(length) {
		return nil, api.ErrCorruptedValue
	}

	atomic.AddUint64(&s.retrieveCount, 1)
	return result, nil
}

// Delete marks a value for future reclamation.
func (s *externalValueStore) Delete(addr VAddr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return api.ErrStoreClosed
	}

	info, ok := s.index[addr]
	if !ok {
		// Idempotent: already deleted or never existed
		return nil
	}

	if info.deleted {
		return nil
	}

	info.deleted = true
	atomic.AddUint64(&s.activeCount, ^uint64(0)) // decrement
	atomic.AddUint64(&s.deletedBytes, info.size)

	return nil
}

// GetValueSize returns the size of the value at addr without loading data.
func (s *externalValueStore) GetValueSize(addr VAddr) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, api.ErrStoreClosed
	}

	info, ok := s.index[addr]
	if !ok {
		return 0, api.ErrValueNotFound
	}

	return info.size, nil
}

// Close releases resources held by the store.
func (s *externalValueStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.index = nil

	// Close segment manager to flush trailers
	if s.segmentMgr != nil {
		s.segmentMgr.Close()
	}

	return nil
}

// Metrics interface implementation (optional)
func (s *externalValueStore) StoreCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.storeCount
}

func (s *externalValueStore) RetrieveCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.retrieveCount
}

func (s *externalValueStore) ActiveValueCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeCount
}

func (s *externalValueStore) TotalBytesStored() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalBytes
}

func (s *externalValueStore) DeletedBytes() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deletedBytes
}

// Ensure externalValueStore implements ExternalValueStore
var _ api.ExternalValueStore = (*externalValueStore)(nil)
