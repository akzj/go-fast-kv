package internal

import (
	"sync"

	api "github.com/akzj/go-fast-kv/internal/storage/api"
	"github.com/akzj/go-fast-kv/internal/vaddr"
)

// memorySegment implements Segment with in-memory storage.
type memorySegment struct {
	id       vaddr.SegmentID
	data     []byte
	offset   int64
	state    vaddr.SegmentState
	pageSize int
}

func (s *memorySegment) ID() vaddr.SegmentID              { return s.id }
func (s *memorySegment) State() vaddr.SegmentState        { return s.state }
func (s *memorySegment) File() api.FileOperations         { return nil }
func (s *memorySegment) Size() int64                      { return s.offset }
func (s *memorySegment) PageCount() uint64                { return uint64(s.offset) / uint64(s.pageSize) }
func (s *memorySegment) Sync() error                      { return nil }
func (s *memorySegment) Close() error                    { return nil }

func (s *memorySegment) Append(data []byte) (vaddr.VAddr, error) {
	addr := vaddr.VAddr{
		SegmentID: uint64(s.id),
		Offset:    uint64(s.offset),
	}
	s.data = append(s.data, data...)
	s.offset += int64(len(data))
	return addr, nil
}

func (s *memorySegment) ReadAt(offset int64, length int) ([]byte, error) {
	if offset < 0 || offset+int64(length) > int64(len(s.data)) {
		return nil, api.ErrInvalidOffset
	}
	result := make([]byte, length)
	copy(result, s.data[offset:offset+int64(length)])
	return result, nil
}

// memorySegmentManager implements SegmentManager with in-memory storage.
type memorySegmentManager struct {
	segments      map[vaddr.SegmentID]*memorySegment
	activeSegment *memorySegment
	nextSegmentID vaddr.SegmentID
	mu            sync.Mutex
}

// NewInMemorySegmentManager creates a SegmentManager backed by memory.
func NewInMemorySegmentManager() *memorySegmentManager {
	sm := &memorySegmentManager{
		segments:      make(map[vaddr.SegmentID]*memorySegment),
		nextSegmentID: 1,
	}
	sm.createSegment()
	return sm
}

func (sm *memorySegmentManager) Directory() string {
	return ""
}

func (sm *memorySegmentManager) ActiveSegment() api.Segment {
	return sm.activeSegment
}

func (sm *memorySegmentManager) GetSegment(id vaddr.SegmentID) api.Segment {
	return sm.segments[id]
}

func (sm *memorySegmentManager) CreateSegment() (api.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.createSegment(), nil
}

func (sm *memorySegmentManager) createSegment() *memorySegment {
	if sm.activeSegment != nil {
		sm.activeSegment.state = vaddr.SegmentStateSealed
	}
	seg := &memorySegment{
		id:       sm.nextSegmentID,
		data:     make([]byte, 0),
		state:    vaddr.SegmentStateActive,
		pageSize: int(vaddr.PageSize),
	}
	sm.nextSegmentID++
	sm.segments[seg.id] = seg
	sm.activeSegment = seg
	return seg
}

func (sm *memorySegmentManager) SealSegment(id vaddr.SegmentID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	seg, ok := sm.segments[id]
	if !ok {
		return api.ErrSegmentNotFound
	}
	if seg.state != vaddr.SegmentStateActive {
		return api.ErrSegmentNotActive
	}
	seg.state = vaddr.SegmentStateSealed
	if sm.activeSegment == seg {
		sm.activeSegment = nil
	}
	return nil
}

func (sm *memorySegmentManager) ArchiveSegment(id vaddr.SegmentID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	seg, ok := sm.segments[id]
	if !ok {
		return api.ErrSegmentNotFound
	}
	if seg.state != vaddr.SegmentStateSealed {
		return api.ErrSegmentNotSealed
	}
	seg.state = vaddr.SegmentStateArchived
	return nil
}

func (sm *memorySegmentManager) ListSegments() []api.Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	result := make([]api.Segment, 0, len(sm.segments))
	for _, seg := range sm.segments {
		result = append(result, seg)
	}
	return result
}

func (sm *memorySegmentManager) ListSegmentsByState(state vaddr.SegmentState) []api.Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var result []api.Segment
	for _, seg := range sm.segments {
		if seg.state == state {
			result = append(result, seg)
		}
	}
	return result
}

func (sm *memorySegmentManager) SegmentCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.segments)
}

func (sm *memorySegmentManager) ActiveSegmentCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	count := 0
	for _, seg := range sm.segments {
		if seg.state == vaddr.SegmentStateActive {
			count++
		}
	}
	return count
}

func (sm *memorySegmentManager) TotalSize() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var total int64
	for _, seg := range sm.segments {
		total += seg.offset
	}
	return total
}

func (sm *memorySegmentManager) Close() error {
	return nil
}
