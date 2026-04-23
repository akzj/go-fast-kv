// Package segment implements the Segment Manager — the lowest storage
// layer in go-fast-kv. It provides append-only writes and random reads
// over a set of segment files.
//
// Design reference: docs/DESIGN.md §3.1
package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	"golang.org/x/sys/unix"
)

const (
	// SegmentHeaderSize is the fixed size of the segment header (64 bytes).
	SegmentHeaderSize = 64
	// SegmentHeaderVersion is the current version of the segment header format.
	SegmentHeaderVersion = 1
)

// segmentHeader represents the 64-byte header at the start of new segments.
// Layout (little-endian):
// [0:8]    magic number (8 bytes, e.g. "PAGESEGM", "BLOBSEGM")
// [8:12]   version (uint32)
// [12:16]  crc32 checksum of header bytes [0:60] (uint32)
// [16:24]  create timestamp (unix nanos, uint64)
// [24:64]  reserved (40 bytes, 0 for now)
type segmentHeader struct {
	Magic        [8]byte
	Version      uint32
	CRC32        uint32
	CreateTime   uint64
	Reserved     [40]byte
}

// segmentFile represents a single segment file on disk.
type segmentFile struct {
	id         uint32
	file       *os.File
	size       int64
	sealed     bool
	data       []byte // non-nil = mmap'd slice (read-only); nil = fall back to file.ReadAt
	headerSize int64  // 0 for legacy segments, SegmentHeaderSize for new segments
}

// crc32cTable is the Castagnoli CRC32 table for header checksums.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// segmentManager implements segmentapi.SegmentManager.
type segmentManager struct {
	mu            sync.RWMutex
	dir           string
	maxSize       int64
	magic         string // 8-byte magic for new segments, empty = legacy mode
	active        *segmentFile
	sealed        map[uint32]*segmentFile
	nextSegmentID uint32
	closed        bool
}

// Compile-time interface check.
var _ segmentapi.SegmentManager = (*segmentManager)(nil)

// New creates a new SegmentManager. It scans the directory for existing
// segment files and recovers state. If the directory is empty, it creates
// the first segment (ID = 1).
func New(cfg segmentapi.Config) (segmentapi.SegmentManager, error) {
	dir := cfg.Dir
	maxSize := cfg.MaxSize
	if maxSize <= 0 {
		maxSize = segmentapi.MaxSegmentSize
	}
	if maxSize > (1 << 32) {
		return nil, segmentapi.ErrMaxSizeOverflow
	}
	if len(cfg.Magic) > 8 {
		return nil, fmt.Errorf("segment: magic string cannot exceed 8 bytes")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("segment: mkdir %s: %w", dir, err)
	}

	sm := &segmentManager{
		dir:     dir,
		maxSize: maxSize,
		magic:   cfg.Magic,
		sealed:  make(map[uint32]*segmentFile),
	}

	if err := sm.recover(); err != nil {
		return nil, fmt.Errorf("segment: recover: %w", err)
	}

	return sm, nil
}

// ─── Recovery ───────────────────────────────────────────────────────

// recover scans the directory for existing *.seg files and restores state.
func (sm *segmentManager) recover() error {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return err
	}

	var ids []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".seg")
		id, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue // skip non-numeric files
		}
		ids = append(ids, uint32(id))
	}

	if len(ids) == 0 {
		// Fresh start — create first segment.
		return sm.createSegment(1)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// All but the last are sealed; the last is active.
	for i, id := range ids {
		isLast := i == len(ids)-1
		sf, err := sm.openSegmentFile(id, isLast)
		if err != nil {
			return err
		}
		if isLast {
			sm.active = sf
		} else {
			sf.sealed = true
			sm.sealed[id] = sf
		}
	}

	sm.nextSegmentID = ids[len(ids)-1] + 1
	return nil
}

// openSegmentFile opens an existing segment file. If writable is true,
// it opens in append mode; otherwise read-only with mmap.
//
// When writable=false, the file is memory-mapped (MAP_SHARED, PROT_READ)
// for fast zero-syscall reads. If mmap fails, falls back to file.ReadAt.
func (sm *segmentManager) openSegmentFile(id uint32, writable bool) (*segmentFile, error) {
	path := sm.segPath(id)
	var flag int
	if writable {
		flag = os.O_RDWR
	} else {
		flag = os.O_RDONLY
	}

	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("segment: stat %s: %w", path, err)
	}

	var headerSize int64 = 0
	// Check if file has a valid segment header
	if info.Size() >= SegmentHeaderSize {
		// Read first 64 bytes
		headerBuf := make([]byte, SegmentHeaderSize)
		n, err := f.ReadAt(headerBuf, 0)
		if err != nil && err != io.EOF {
			f.Close()
			return nil, fmt.Errorf("segment: read header %s: %w", path, err)
		}
		if n == SegmentHeaderSize {
			// Check if magic matches expected (if set) or is a valid known magic
			magic := string(headerBuf[:8])
			if (sm.magic != "" && magic == sm.magic) || (magic == "PAGESEGM" || magic == "BLOBSEGM") {
				// Valid header, validate CRC32
				version := binary.LittleEndian.Uint32(headerBuf[8:12])
				storedCRC := binary.LittleEndian.Uint32(headerBuf[12:16])
				// Calculate CRC of first 60 bytes
				computedCRC := crc32.Checksum(headerBuf[:60], crc32cTable)
				if storedCRC != computedCRC {
					f.Close()
					return nil, fmt.Errorf("segment: header checksum mismatch for segment %d: stored=0x%08x computed=0x%08x", id, storedCRC, computedCRC)
				}
				if version > SegmentHeaderVersion {
					f.Close()
					return nil, fmt.Errorf("segment: unsupported header version %d for segment %d (max supported %d)", version, id, SegmentHeaderVersion)
				}
				headerSize = SegmentHeaderSize
			}
		}
	}

	sf := &segmentFile{
		id:         id,
		file:       f,
		size:       info.Size(),
		headerSize: headerSize,
	}

	if writable {
		// Active segment on recovery: pre-allocate to maxSize and mmap for writes.
		if err := f.Truncate(sm.maxSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("segment: truncate %s: %w", path, err)
		}
		data, err := unix.Mmap(int(f.Fd()), 0, int(sm.maxSize),
			unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("segment: mmap %s: %w", path, err)
		}
		sf.data = data
		// sf.size stays at info.Size() — the actual written portion.
	}
	// Sealed (read-only) segments: no mmap, use file.ReadAt.

	return sf, nil
}

// createSegment creates a new segment file and sets it as active.
// The file is pre-allocated to maxSize and memory-mapped (PROT_READ|PROT_WRITE,
// MAP_SHARED) so that Append writes go directly to the page cache — no syscall.
func (sm *segmentManager) createSegment(id uint32) error {
	path := sm.segPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("segment: create %s: %w", path, err)
	}

	var headerSize int64 = 0
	if sm.magic != "" {
		// Build the 64-byte segment header.
		var header segmentHeader
		copy(header.Magic[:], []byte(sm.magic))
		header.Version = SegmentHeaderVersion
		header.CreateTime = uint64(time.Now().UnixNano())
		headerBytes := make([]byte, 60)
		copy(headerBytes[0:8], header.Magic[:])
		binary.LittleEndian.PutUint32(headerBytes[8:12], header.Version)
		binary.LittleEndian.PutUint64(headerBytes[16:24], header.CreateTime)
		header.CRC32 = crc32.Checksum(headerBytes, crc32cTable)

		fullHeader := make([]byte, SegmentHeaderSize)
		copy(fullHeader[0:8], header.Magic[:])
		binary.LittleEndian.PutUint32(fullHeader[8:12], header.Version)
		binary.LittleEndian.PutUint32(fullHeader[12:16], header.CRC32)
		binary.LittleEndian.PutUint64(fullHeader[16:24], header.CreateTime)

		// Write header via file I/O before mmap.
		n, err := f.Write(fullHeader)
		if err != nil {
			f.Close()
			return fmt.Errorf("segment: write header %s: %w", path, err)
		}
		if n != SegmentHeaderSize {
			f.Close()
			return fmt.Errorf("segment: short header write %s: %d/%d bytes", path, n, SegmentHeaderSize)
		}
		headerSize = SegmentHeaderSize
	}

	// Pre-allocate the file to maxSize so the mmap covers the full capacity.
	if err := f.Truncate(sm.maxSize); err != nil {
		f.Close()
		return fmt.Errorf("segment: truncate %s: %w", path, err)
	}

	// Memory-map the entire file for read+write (MAP_SHARED).
	data, err := unix.Mmap(int(f.Fd()), 0, int(sm.maxSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return fmt.Errorf("segment: mmap %s: %w", path, err)
	}

	sm.active = &segmentFile{
		id:         id,
		file:       f,
		data:       data,
		size:       headerSize,
		headerSize: headerSize,
	}
	sm.nextSegmentID = id + 1
	return nil
}

// segPath returns the file path for a segment ID.
func (sm *segmentManager) segPath(id uint32) string {
	return filepath.Join(sm.dir, fmt.Sprintf("%08d.seg", id))
}

// ─── Interface Implementation ───────────────────────────────────────

func (sm *segmentManager) Append(data []byte) (segmentapi.VAddr, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return segmentapi.VAddr{}, segmentapi.ErrClosed
	}

	needed := sm.active.size + int64(len(data))
	if needed > sm.maxSize {
		return segmentapi.VAddr{}, segmentapi.ErrSegmentFull
	}

	offset := sm.active.size

	// Fast path: copy into mmap'd region (no syscall).
	if sm.active.data != nil {
		copy(sm.active.data[offset:offset+int64(len(data))], data)
	} else {
		// Fallback: file.Write (shouldn't happen for active segment, but safe).
		n, err := sm.active.file.Write(data)
		if err != nil {
			return segmentapi.VAddr{}, fmt.Errorf("segment: write: %w", err)
		}
		if n != len(data) {
			return segmentapi.VAddr{}, fmt.Errorf("segment: short write: %d/%d", n, len(data))
		}
	}

	sm.active.size += int64(len(data))

	return segmentapi.VAddr{
		SegmentID: sm.active.id,
		Offset:    uint32(offset),
	}, nil
}

func (sm *segmentManager) ReadAt(addr segmentapi.VAddr, size uint32) ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return nil, segmentapi.ErrClosed
	}

	sf := sm.findSegment(addr.SegmentID)
	if sf == nil {
		return nil, fmt.Errorf("segment %d not found: %w", addr.SegmentID, segmentapi.ErrInvalidVAddr)
	}

	// Calculate actual offset in file: logical offset + header size
	fileOffset := sf.headerSize + int64(addr.Offset)
	end := fileOffset + int64(size)
	if end > sf.size {
		return nil, fmt.Errorf("read beyond segment end (off=%d size=%d segSize=%d headerSize=%d): %w",
			addr.Offset, size, sf.size, sf.headerSize, segmentapi.ErrInvalidVAddr)
	}

	// Fast path: read from mmap'd slice (no syscall).
	if sf.data != nil {
		result := make([]byte, size)
		copy(result, sf.data[fileOffset:fileOffset+int64(size)])
		return result, nil
	}

	// Slow path: syscall ReadAt (active segment, or mmap fallback).
	buf := make([]byte, size)
	n, err := sf.file.ReadAt(buf, fileOffset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("segment: readat: %w", err)
	}
	if uint32(n) != size {
		return nil, fmt.Errorf("segment: short read: %d/%d", n, size)
	}

	return buf, nil
}

func (sm *segmentManager) ReadAtInto(addr segmentapi.VAddr, buf []byte) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}

	sf := sm.findSegment(addr.SegmentID)
	if sf == nil {
		return fmt.Errorf("segment %d not found: %w", addr.SegmentID, segmentapi.ErrInvalidVAddr)
	}

	size := len(buf)
	fileOffset := sf.headerSize + int64(addr.Offset)
	end := fileOffset + int64(size)
	if end > sf.size {
		return fmt.Errorf("read beyond segment end (off=%d size=%d segSize=%d headerSize=%d): %w",
			addr.Offset, size, sf.size, sf.headerSize, segmentapi.ErrInvalidVAddr)
	}

	// Fast path: copy from mmap'd slice (no syscall).
	if sf.data != nil {
		copy(buf, sf.data[fileOffset:fileOffset+int64(size)])
		return nil
	}

	// Slow path: syscall ReadAt (active segment, or mmap fallback).
	n, err := sf.file.ReadAt(buf, fileOffset)
	if err != nil && err != io.EOF {
		return fmt.Errorf("segment: readat: %w", err)
	}
	if n != size {
		return fmt.Errorf("segment: short read: %d/%d", n, size)
	}
	return nil
}

func (sm *segmentManager) Sync() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}

	// Flush mmap'd pages to disk, then fsync the file for durability.
	if sm.active.data != nil {
		if err := unix.Msync(sm.active.data, unix.MS_SYNC); err != nil {
			return fmt.Errorf("segment: msync: %w", err)
		}
	}
	return sm.active.file.Sync()
}

func (sm *segmentManager) Rotate() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}

	oldActive := sm.active

	// Munmap the writable mmap before touching the file.
	if oldActive.data != nil {
		if err := unix.Msync(oldActive.data, unix.MS_SYNC); err != nil {
			return fmt.Errorf("segment: msync before rotate: %w", err)
		}
		if err := unix.Munmap(oldActive.data); err != nil {
			return fmt.Errorf("segment: munmap active %d: %w", oldActive.id, err)
		}
		oldActive.data = nil
	}

	// Truncate file to actual written size (remove unused pre-allocated space).
	actualSize := oldActive.size
	if err := oldActive.file.Truncate(actualSize); err != nil {
		return fmt.Errorf("segment: truncate sealed %d: %w", oldActive.id, err)
	}

	// Sync to disk.
	if err := oldActive.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync before rotate: %w", err)
	}

	// Close O_RDWR fd, reopen O_RDONLY for sealed segment.
	if err := oldActive.file.Close(); err != nil {
		return fmt.Errorf("segment: close old fd before rotate: %w", err)
	}
	f, err := os.OpenFile(sm.segPath(oldActive.id), os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("segment: reopen old segment: %w", err)
	}

	// Sealed segments: NO mmap — use ReadAt to save memory.
	oldActive.file = f
	oldActive.data = nil
	oldActive.size = actualSize
	oldActive.sealed = true
	sm.sealed[oldActive.id] = oldActive

	// Create new active segment.
	return sm.createSegment(sm.nextSegmentID)
}

func (sm *segmentManager) RemoveSegment(segID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}

	// Cannot remove active segment.
	if sm.active != nil && sm.active.id == segID {
		return fmt.Errorf("segment: cannot remove active segment %d", segID)
	}

	sf, ok := sm.sealed[segID]
	if !ok {
		return fmt.Errorf("segment %d not found: %w", segID, segmentapi.ErrInvalidVAddr)
	}

	// Munmap before closing the file.
	if sf.data != nil {
		unix.Munmap(sf.data)
		sf.data = nil
	}

	// Close file, delete from disk, remove from map.
	if err := sf.file.Close(); err != nil {
		return fmt.Errorf("segment: close %d: %w", segID, err)
	}

	path := sm.segPath(segID)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("segment: remove %s: %w", path, err)
	}

	delete(sm.sealed, segID)
	return nil
}

func (sm *segmentManager) ActiveSegmentID() uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.active.id
}

func (sm *segmentManager) SegmentSize(segID uint32) (int64, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return 0, segmentapi.ErrClosed
	}

	sf := sm.findSegment(segID)
	if sf == nil {
		return 0, fmt.Errorf("segment %d not found: %w", segID, segmentapi.ErrInvalidVAddr)
	}

	// Return logical size (excluding header) for backward compatibility
	return sf.size - sf.headerSize, nil
}

// StorageDir returns the directory where segment files are stored.
func (sm *segmentManager) StorageDir() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.dir
}

func (sm *segmentManager) SealedSegments() []uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ids := make([]uint32, 0, len(sm.sealed))
	for id := range sm.sealed {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (sm *segmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}
	sm.closed = true

	// Munmap and close all sealed segments.
	var firstErr error
	for _, sf := range sm.sealed {
		if sf.data != nil {
			if err := unix.Munmap(sf.data); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("segment: munmap %d: %w", sf.id, err)
			}
			sf.data = nil
		}
		if err := sf.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: close %d: %w", sf.id, err)
		}
	}

	// Munmap, truncate, sync, and close active.
	if sm.active != nil {
		if sm.active.data != nil {
			if err := unix.Munmap(sm.active.data); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("segment: munmap active %d: %w", sm.active.id, err)
			}
			sm.active.data = nil
		}
		// Truncate to actual written size (remove unused pre-allocated space).
		if err := sm.active.file.Truncate(sm.active.size); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: truncate active %d: %w", sm.active.id, err)
		}
		if err := sm.active.file.Sync(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: sync %d: %w", sm.active.id, err)
		}
		if err := sm.active.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: close %d: %w", sm.active.id, err)
		}
	}

	return firstErr
}

// ─── Helpers ───────────────────────────────────────────────────────

// findSegment returns the segmentFile for the given ID, or nil if not found.
func (sm *segmentManager) findSegment(id uint32) *segmentFile {
	if sm.active != nil && sm.active.id == id {
		return sm.active
	}
	return sm.sealed[id]
}
