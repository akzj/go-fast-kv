package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
	api "github.com/akzj/go-fast-kv/internal/storage/api"
)

// segmentManager implements the api.SegmentManager interface.
type segmentManager struct {
	mu           sync.RWMutex
	config       Config
	segments     map[vaddr.SegmentID]*segment
	activeID     vaddr.SegmentID // ID of the active segment (0 if none)
	nextID       vaddr.SegmentID // Next segment ID to allocate
	closed       bool
}

// Config holds segment manager configuration.
type Config struct {
	Directory       string
	SegmentSize     uint64
	MaxSegmentCount int
}

// NewSegmentManager creates a new segment manager.
func NewSegmentManager(config Config) (*segmentManager, error) {
	if config.SegmentSize == 0 {
		config.SegmentSize = 1 << 30 // 1 GB default
	}

	sm := &segmentManager{
		config:   config,
		segments: make(map[vaddr.SegmentID]*segment),
		nextID:   vaddr.SegmentIDMin,
	}

	// Ensure directory exists
	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Scan existing segments
	if err := sm.scanExistingSegments(); err != nil {
		return nil, fmt.Errorf("scan segments: %w", err)
	}

	return sm, nil
}

// scanExistingSegments scans the directory for existing segment files.
func (sm *segmentManager) scanExistingSegments() error {
	entries, err := os.ReadDir(sm.config.Directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No directory yet, that's fine
		}
		return fmt.Errorf("read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Segment files are named "segment_{id}"
		name := entry.Name()
		if len(name) < 8 || name[:7] != "segment" {
			continue
		}

		idStr := name[8:]
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue // Not a valid segment file
		}

		segID := vaddr.SegmentID(id)
		if !segID.IsValid() {
			continue
		}

		// Open and validate the segment
		file := NewOSFile(filepath.Join(sm.config.Directory, name))
		if err := file.Open(file.Path()); err != nil {
			continue // Skip corrupted files
		}

		seg, err := OpenSegment(segID, file)
		if err != nil {
			file.Close()
			continue // Skip corrupted segments
		}

		sm.segments[segID] = seg

		// Track the highest ID
		if segID >= sm.nextID {
			sm.nextID = segID + 1
		}

		// Track active segment
		if seg.state == vaddr.SegmentStateActive {
			if segID > sm.activeID {
				sm.activeID = segID
			}
		}
	}

	return nil
}

// ActiveSegment returns the current active segment for writing.
func (sm *segmentManager) ActiveSegment() api.Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed || sm.activeID == 0 {
		return nil
	}
	return sm.segments[sm.activeID]
}

// GetSegment returns segment by ID.
func (sm *segmentManager) GetSegment(id vaddr.SegmentID) api.Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.segments[id]
}

// CreateSegment creates a new active segment.
func (sm *segmentManager) CreateSegment() (api.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil, ErrStorageClosed
	}

	// Check max segment count
	if sm.config.MaxSegmentCount > 0 && len(sm.segments) >= sm.config.MaxSegmentCount {
		return nil, ErrMaxSegments
	}

	// Seal current active segment if any
	if sm.activeID != 0 {
		oldSeg := sm.segments[sm.activeID]
		if oldSeg != nil {
			if err := oldSeg.setState(vaddr.SegmentStateSealed); err != nil {
				return nil, fmt.Errorf("seal segment: %w", err)
			}
		}
	}

	// Create new segment
	segID := sm.nextID
	sm.nextID++

	// Create file
	filePath := filepath.Join(sm.config.Directory, fmt.Sprintf("segment_%d", segID))
	file := NewOSFile(filePath)
	if err := file.Open(filePath); err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Write header
	if err := WriteHeader(file, segID); err != nil {
		file.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}

	// Create segment
	seg := &segment{
		id:        segID,
		state:     vaddr.SegmentStateActive,
		file:      file,
	}

	sm.segments[segID] = seg
	sm.activeID = segID

	return seg, nil
}

// SealSegment marks a segment as sealed (no new writes).
func (sm *segmentManager) SealSegment(id vaddr.SegmentID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return ErrStorageClosed
	}
	if !id.IsValid() {
		return ErrInvalidSegmentID
	}

	seg, ok := sm.segments[id]
	if !ok {
		return ErrSegmentNotFound
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.state != vaddr.SegmentStateActive {
		return ErrSegmentNotActive
	}

	if err := seg.setState(vaddr.SegmentStateSealed); err != nil {
		return err
	}

	// Update active segment if this was the active one
	if sm.activeID == id {
		sm.activeID = 0
	}

	return nil
}

// ArchiveSegment marks a segment as archived (read-only).
func (sm *segmentManager) ArchiveSegment(id vaddr.SegmentID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return ErrStorageClosed
	}
	if !id.IsValid() {
		return ErrInvalidSegmentID
	}

	seg, ok := sm.segments[id]
	if !ok {
		return ErrSegmentNotFound
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.state != vaddr.SegmentStateSealed {
		return ErrSegmentNotSealed
	}

	return seg.setState(vaddr.SegmentStateArchived)
}

// ListSegments returns all segments in ID order.
func (sm *segmentManager) ListSegments() []api.Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ids := make([]vaddr.SegmentID, 0, len(sm.segments))
	for id := range sm.segments {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	segments := make([]api.Segment, 0, len(ids))
	for _, id := range ids {
		segments = append(segments, sm.segments[id])
	}
	return segments
}

// ListSegmentsByState returns segments filtered by state.
func (sm *segmentManager) ListSegmentsByState(state vaddr.SegmentState) []api.Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var segments []api.Segment
	for _, seg := range sm.segments {
		if seg.state == state {
			segments = append(segments, seg)
		}
	}
	return segments
}

// SegmentCount returns the total number of segments.
func (sm *segmentManager) SegmentCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.segments)
}

// ActiveSegmentCount returns the number of active segments.
func (sm *segmentManager) ActiveSegmentCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var count int
	for _, seg := range sm.segments {
		if seg.state == vaddr.SegmentStateActive {
			count++
		}
	}
	return count
}

// TotalSize returns the total bytes across all segments.
func (sm *segmentManager) TotalSize() int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var total int64
	for _, seg := range sm.segments {
		total += seg.dataSize
	}
	return total
}

// Close releases all resources held by the segment manager.
func (sm *segmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil
	}
	sm.closed = true

	// Close all segments
	for _, seg := range sm.segments {
		seg.Close()
	}

	sm.segments = nil
	return nil
}
