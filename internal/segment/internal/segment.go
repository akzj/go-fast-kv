// Package segment implements the Segment Manager — the lowest storage
// layer in go-fast-kv. It provides append-only writes and random reads
// over a set of segment files.
//
// Design reference: docs/DESIGN.md §3.1
package internal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// segmentFile represents a single segment file on disk.
type segmentFile struct {
	id     uint32
	file   *os.File
	size   int64
	sealed bool
}

// segmentManager implements segmentapi.SegmentManager.
type segmentManager struct {
	mu            sync.RWMutex
	dir           string
	maxSize       int64
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

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("segment: mkdir %s: %w", dir, err)
	}

	sm := &segmentManager{
		dir:     dir,
		maxSize: maxSize,
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
// it opens in append mode; otherwise read-only.
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

	// For writable files, seek to end so Write appends correctly.
	if writable {
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			f.Close()
			return nil, fmt.Errorf("segment: seek end %s: %w", path, err)
		}
	}

	return &segmentFile{
		id:   id,
		file: f,
		size: info.Size(),
	}, nil
}

// createSegment creates a new segment file and sets it as active.
func (sm *segmentManager) createSegment(id uint32) error {
	path := sm.segPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("segment: create %s: %w", path, err)
	}

	sm.active = &segmentFile{
		id:   id,
		file: f,
		size: 0,
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

	if sm.active.size+int64(len(data)) > sm.maxSize {
		return segmentapi.VAddr{}, segmentapi.ErrSegmentFull
	}

	offset := sm.active.size

	n, err := sm.active.file.Write(data)
	if err != nil {
		return segmentapi.VAddr{}, fmt.Errorf("segment: write: %w", err)
	}
	if n != len(data) {
		return segmentapi.VAddr{}, fmt.Errorf("segment: short write: %d/%d", n, len(data))
	}

	sm.active.size += int64(n)

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

	end := int64(addr.Offset) + int64(size)
	if end > sf.size {
		return nil, fmt.Errorf("read beyond segment end (off=%d size=%d segSize=%d): %w",
			addr.Offset, size, sf.size, segmentapi.ErrInvalidVAddr)
	}

	buf := make([]byte, size)
	n, err := sf.file.ReadAt(buf, int64(addr.Offset))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("segment: readat: %w", err)
	}
	if uint32(n) != size {
		return nil, fmt.Errorf("segment: short read: %d/%d", n, size)
	}

	return buf, nil
}

func (sm *segmentManager) Sync() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}

	return sm.active.file.Sync()
}

func (sm *segmentManager) Rotate() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return segmentapi.ErrClosed
	}

	// Sync before sealing.
	if err := sm.active.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync before rotate: %w", err)
	}

	// Seal the active segment.
	sm.active.sealed = true
	sm.sealed[sm.active.id] = sm.active

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

	return sf.size, nil
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

	// Sync and close active.
	var firstErr error
	if sm.active != nil {
		if err := sm.active.file.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := sm.active.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close all sealed segments.
	for _, sf := range sm.sealed {
		if err := sf.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// ─── Helpers ────────────────────────────────────────────────────────

// findSegment returns the segmentFile for the given ID, or nil if not found.
func (sm *segmentManager) findSegment(id uint32) *segmentFile {
	if sm.active != nil && sm.active.id == id {
		return sm.active
	}
	return sm.sealed[id]
}
