package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
	api "github.com/akzj/go-fast-kv/internal/storage/api"
)

// Segment file format:
//   - Header: magic(8) + version(2) + segmentID(8) + createdAt(8) + flags(2) + reserved(6) = 32 bytes
//   - Data pages: aligned to PageSize (4096 bytes)
//   - Trailer: pageCount(8) + dataSize(8) + checksum(8) + reserved(8) = 32 bytes

const (
	// File magic for segment identification.
	segmentMagic = 0x5345474D414E4147 // "SEGMANAG" in little-endian

	// Current segment file version.
	segmentVersion uint16 = 1

	// SegmentHeaderLayout (32 bytes total):
	//   0-7:   Magic (uint64)
	//   8-9:   Version (uint16)
	//   10-17: SegmentID (uint64)
	//   18-25: CreatedAt (uint64, nanoseconds since epoch)
	//   26-27: Flags (uint16)
	//   28-31: Reserved
	headerOffsetMagic     = 0
	headerOffsetVersion  = 8
	headerOffsetSegmentID = 10
	headerOffsetCreatedAt = 18
	headerOffsetFlags     = 26
	headerSize            = 32

	// TrailerLayout (32 bytes total):
	//   0-7:   PageCount (uint64)
	//   8-15:  DataSize (uint64)
	//   16-23: Checksum (uint32) + Reserved(4) at end
	//   24-31: Reserved
	trailerOffsetPageCount = 0
	trailerOffsetDataSize  = 8
	trailerOffsetChecksum  = 16
	trailerSize            = 32
)

// segment implements the api.Segment interface.
type segment struct {
	mu        sync.RWMutex
	id        vaddr.SegmentID
	state     vaddr.SegmentState
	file      api.FileOperations
	dataSize  int64  // Current data size (excluding header/trailer)
	pageCount uint64 // Number of data pages
	closed    bool
}

// newSegment creates a new segment wrapping the given file.
// This is the implementation for storage.NewSegment().
func newSegment(id vaddr.SegmentID, file api.FileOperations) api.Segment {
	return &segment{
		id:    id,
		state: vaddr.SegmentStateActive,
		file:  file,
	}
}

// OpenSegment opens an existing segment file and validates its header.
func OpenSegment(id vaddr.SegmentID, file api.FileOperations) (*segment, error) {
	s := &segment{
		id:   id,
		file: file,
	}

	// Read header to validate
	header := make([]byte, headerSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Validate magic
	magic := binary.LittleEndian.Uint64(header[headerOffsetMagic:])
	if magic != segmentMagic {
		return nil, fmt.Errorf("invalid magic: expected %x, got %x", segmentMagic, magic)
	}

	// Validate version
	version := binary.LittleEndian.Uint16(header[headerOffsetVersion:])
	if version != segmentVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// Validate segment ID
	fileSegID := binary.LittleEndian.Uint64(header[headerOffsetSegmentID:])
	if vaddr.SegmentID(fileSegID) != id {
		return nil, fmt.Errorf("segment ID mismatch: expected %d, got %d", id, fileSegID)
	}

	// Determine state from flags
	flags := binary.LittleEndian.Uint16(header[headerOffsetFlags:])
	if flags&0x01 != 0 {
		s.state = vaddr.SegmentStateActive
	} else if flags&0x02 != 0 {
		s.state = vaddr.SegmentStateSealed
	} else if flags&0x04 != 0 {
		s.state = vaddr.SegmentStateArchived
	} else {
		s.state = vaddr.SegmentStateSealed // Default to sealed for legacy compatibility
	}

	// Read trailer
	fileSize, err := file.Size()
	if err != nil {
		return nil, fmt.Errorf("get file size: %w", err)
	}

	if fileSize > headerSize+trailerSize {
		trailer := make([]byte, trailerSize)
		if _, err := file.ReadAt(trailer, fileSize-trailerSize); err != nil {
			return nil, fmt.Errorf("read trailer: %w", err)
		}

		s.pageCount = binary.LittleEndian.Uint64(trailer[trailerOffsetPageCount:])
		s.dataSize = int64(binary.LittleEndian.Uint64(trailer[trailerOffsetDataSize:]))
	}

	return s, nil
}

// ID returns the segment's unique identifier.
func (s *segment) ID() vaddr.SegmentID {
	return s.id
}

// State returns the current segment state.
func (s *segment) State() vaddr.SegmentState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// File returns the underlying file operations.
func (s *segment) File() api.FileOperations {
	return s.file
}

// Append appends data to the segment.
// Returns the VAddr where data was written.
func (s *segment) Append(data []byte) (vaddr.VAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return vaddr.VAddr{}, ErrSegmentClosed
	}
	if s.state != vaddr.SegmentStateActive {
		return vaddr.VAddr{}, ErrSegmentNotActive
	}

	// Data must be aligned to PageSize
	if len(data)%vaddr.PageSize != 0 {
		return vaddr.VAddr{}, ErrInvalidAlignment
	}

	// Calculate offset (after header)
	offset := int64(headerSize + s.dataSize)

	// Write data
	n, err := s.file.WriteAt(data, offset)
	if err != nil {
		return vaddr.VAddr{}, fmt.Errorf("write data: %w", err)
	}

	// Update metrics
	bytesWritten := int64(n)
	s.dataSize += bytesWritten
	s.pageCount += uint64(len(data) / vaddr.PageSize)

	return vaddr.VAddr{
		SegmentID: uint64(s.id),
		Offset:    uint64(offset),
	}, nil
}

// ReadAt reads data at the given offset.
func (s *segment) ReadAt(offset int64, length int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	// Validate offset is within bounds
	dataStart := int64(headerSize)
	if offset < dataStart {
		return nil, ErrInvalidOffset
	}

	// Validate offset+length doesn't exceed file bounds
	// For external value pages, we read full page-aligned chunks
	// Allow reading up to where trailer would start
	dataEnd := dataStart + s.dataSize + int64(headerSize)
	// Actually, dataSize is the total bytes including page headers
	// So boundary is just dataStart + dataSize
	dataEnd = dataStart + s.dataSize
	if s.dataSize > 0 && offset+int64(length) > dataEnd {
		return nil, ErrInvalidOffset
	}

	buf := make([]byte, length)
	n, err := s.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	return buf[:n], nil
}

// Size returns current data size (excluding header/trailer).
func (s *segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dataSize
}

// PageCount returns the number of pages in this segment.
func (s *segment) PageCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pageCount
}

// Sync ensures all writes to this segment are durable.
func (s *segment) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil
	}

	// Write trailer first
	if err := s.writeTrailer(); err != nil {
		return err
	}

	return s.file.Sync()
}

// Close closes the segment file.
func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	// Write final trailer if active
	if s.state == vaddr.SegmentStateActive {
		if err := s.writeTrailer(); err != nil {
			return err
		}
	}

	return s.file.Close()
}

// writeTrailer writes the trailer to the segment file.
// Caller must hold s.mu.
func (s *segment) writeTrailer() error {
	trailer := make([]byte, trailerSize)

	// Calculate checksum of data
	checksum := s.calculateDataChecksum()

	binary.LittleEndian.PutUint64(trailer[trailerOffsetPageCount:], s.pageCount)
	binary.LittleEndian.PutUint64(trailer[trailerOffsetDataSize:], uint64(s.dataSize))
	binary.LittleEndian.PutUint32(trailer[trailerOffsetChecksum:], checksum)

	// Sync data to disk first
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync file: %w", err)
	}

	// Write trailer after all data (header + dataSize)
	offset := int64(headerSize) + s.dataSize
	_, err := s.file.WriteAt(trailer, offset)
	return err
}

// calculateDataChecksum computes CRC32 of all data pages.
func (s *segment) calculateDataChecksum() uint32 {
	if s.dataSize == 0 {
		return 0
	}

	// Read data in chunks to avoid memory issues
	var checksum uint32 = math.MaxUint32
	chunkSize := 64 * 1024 // 64KB chunks

	for offset := int64(0); offset < s.dataSize; {
		remaining := s.dataSize - offset
		n := int64(chunkSize)
		if n > remaining {
			n = remaining
		}

		buf := make([]byte, n)
		_, err := s.file.ReadAt(buf, headerSize+offset)
		if err != nil {
			return 0
		}

		checksum = crc32.Update(checksum, crc32.MakeTable(crc32.IEEE), buf)
		offset += n
	}

	return checksum
}

// setState changes the segment state.
// Caller must hold s.mu.
func (s *segment) setState(state vaddr.SegmentState) error {
	s.state = state

	// Update header flags
	header := make([]byte, headerSize)
	if _, err := s.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	var flags uint16
	switch state {
	case vaddr.SegmentStateActive:
		flags = 0x01
	case vaddr.SegmentStateSealed:
		flags = 0x02
	case vaddr.SegmentStateArchived:
		flags = 0x04
	}

	binary.LittleEndian.PutUint16(header[headerOffsetFlags:], flags)
	if _, err := s.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Sync to make sure flags are durable
	return s.file.Sync()
}

// WriteHeader writes the segment header.
// This is called during segment creation.
func WriteHeader(file api.FileOperations, id vaddr.SegmentID) error {
	header := make([]byte, headerSize)

	binary.LittleEndian.PutUint64(header[headerOffsetMagic:], segmentMagic)
	binary.LittleEndian.PutUint16(header[headerOffsetVersion:], segmentVersion)
	binary.LittleEndian.PutUint64(header[headerOffsetSegmentID:], uint64(id))
	binary.LittleEndian.PutUint64(header[headerOffsetCreatedAt:], uint64(0)) // Will be set by OS
	binary.LittleEndian.PutUint16(header[headerOffsetFlags:], 0x01)          // Active flag

	_, err := file.WriteAt(header, 0)
	return err
}

// ValidateTrailer validates the trailer checksum.
func ValidateTrailer(file api.FileOperations, dataSize int64) error {
	trailer := make([]byte, trailerSize)
	if _, err := file.ReadAt(trailer, headerSize+dataSize); err != nil {
		return fmt.Errorf("read trailer: %w", err)
	}

	storedChecksum := binary.LittleEndian.Uint32(trailer[trailerOffsetChecksum:])

	// Recalculate checksum
	var checksum uint32 = math.MaxUint32
	chunkSize := 64 * 1024

	for offset := int64(0); offset < dataSize; {
		remaining := dataSize - offset
		n := int64(chunkSize)
		if n > remaining {
			n = remaining
		}

		buf := make([]byte, n)
		_, err := file.ReadAt(buf, headerSize+offset)
		if err != nil {
			return fmt.Errorf("read data: %w", err)
		}

		checksum = crc32.Update(checksum, crc32.MakeTable(crc32.IEEE), buf)
		offset += n
	}

	if checksum != storedChecksum {
		return fmt.Errorf("checksum mismatch: expected %x, got %x", storedChecksum, checksum)
	}

	return nil
}
