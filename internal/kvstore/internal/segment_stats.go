package internal

import (
	"sync"
)

// segmentStats holds live record count and bytes for one segment.
type segmentStats struct {
	mu         sync.RWMutex
	aliveCount int64
	aliveBytes int64
}

// Stats returns copies of the counts (safe to read).
func (s *segmentStats) Stats() (count, bytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.aliveCount, s.aliveBytes
}

// add increments both counters.
func (s *segmentStats) add(count, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aliveCount += count
	s.aliveBytes += bytes
}

// segmentStatsManager manages stats for all segments.
type segmentStatsManager struct {
	mu    sync.RWMutex
	stats map[uint32]*segmentStats
}

func newSegmentStatsManager() *segmentStatsManager {
	return &segmentStatsManager{stats: make(map[uint32]*segmentStats)}
}

func (m *segmentStatsManager) getOrCreate(segID uint32) *segmentStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.stats[segID]; ok {
		return s
	}
	s := &segmentStats{}
	m.stats[segID] = s
	return s
}

func (m *segmentStatsManager) Increment(segID uint32, count, bytes int64) {
	m.getOrCreate(segID).add(count, bytes)
}

func (m *segmentStatsManager) Decrement(segID uint32, count, bytes int64) {
	m.getOrCreate(segID).add(-count, -bytes)
}

func (m *segmentStatsManager) Get(segID uint32) (count, bytes int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.stats[segID]; ok {
		return s.Stats()
	}
	return 0, 0
}

func (m *segmentStatsManager) DeadBytes(segID uint32, segSize int64) int64 {
	count, alive := m.Get(segID)
	if count == 0 {
		return segSize // fully dead
	}
	return segSize - alive
}

// ExportAll returns a snapshot of all stats for checkpoint serialization.
func (m *segmentStatsManager) ExportAll() []segmentStatEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := make([]segmentStatEntry, 0, len(m.stats))
	for segID, s := range m.stats {
		count, bytes := s.Stats()
		entries = append(entries, segmentStatEntry{SegID: segID, AliveCount: uint64(count), AliveBytes: uint64(bytes)})
	}
	return entries
}

// LoadAll restores stats from checkpoint.
func (m *segmentStatsManager) LoadAll(entries []segmentStatEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats = make(map[uint32]*segmentStats)
	for _, e := range entries {
		m.stats[e.SegID] = &segmentStats{aliveCount: int64(e.AliveCount), aliveBytes: int64(e.AliveBytes)}
	}
}

// segmentStatEntry is the serializable form for checkpoint/WAL.
type segmentStatEntry struct {
	SegID      uint32
	AliveCount uint64
	AliveBytes uint64
}
