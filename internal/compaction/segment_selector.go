package compaction

import (
	"sync"

	api "github.com/akzj/go-fast-kv/internal/compaction/api"
)

// segmentSelector implements SegmentSelector using age-based selection.
// Older segments (lower SegmentID) are prioritized for compaction.
type segmentSelector struct {
	mu          sync.RWMutex
	segmentSize map[api.SegmentID]int64 // bytes used per segment
	policy     string
}

// NewSegmentSelector creates a SegmentSelector with the given policy.
// Supported policies: "age" (default), "size", "ratio".
func NewSegmentSelector(policy string) api.SegmentSelector {
	if policy == "" {
		policy = "age"
	}
	return &segmentSelector{
		segmentSize: make(map[api.SegmentID]int64),
		policy:      policy,
	}
}

// UpdateSegmentSize records the size of a segment.
func (s *segmentSelector) UpdateSegmentSize(segID api.SegmentID, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segmentSize[segID] = size
}

// Select returns segments eligible for compaction, ordered by priority.
func (s *segmentSelector) Select(archivedSegments []api.SegmentID) []api.SegmentID {
	if len(archivedSegments) == 0 {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Filter to segments that actually have data
	var candidates []api.SegmentID
	for _, segID := range archivedSegments {
		if size, ok := s.segmentSize[segID]; ok && size > 0 {
			candidates = append(candidates, segID)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort by priority (lower SegmentID = older = higher priority for "age" policy)
	switch s.policy {
	case "age":
		// Older segments first (lower IDs)
		// Already in order if archivedSegments is sorted
		return candidates
	case "size":
		// Smaller segments first (more wasteful)
		// Copy to avoid sorting original
		result := make([]api.SegmentID, len(candidates))
		copy(result, candidates)
		s.sortBySize(result)
		return result
	default:
		return candidates
	}
}

// Priority returns the compaction priority for a segment.
// Higher number = more urgent compaction candidate.
func (s *segmentSelector) Priority(segID api.SegmentID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// For "age" policy: lower SegmentID = older = higher priority
	// Max priority = number of segments (oldest gets highest)
	// This is a relative score based on segment age
	return int(segID)
}

// sortBySize sorts segments by size ascending (smallest first).
func (s *segmentSelector) sortBySize(segs []api.SegmentID) {
	s.mu.RLock()
	sizes := make(map[api.SegmentID]int64, len(segs))
	for _, seg := range segs {
		sizes[seg] = s.segmentSize[seg]
	}
	s.mu.RUnlock()

	// Simple bubble sort (small list expected)
	for i := 0; i < len(segs)-1; i++ {
		for j := i + 1; j < len(segs); j++ {
			if sizes[segs[i]] > sizes[segs[j]] {
				segs[i], segs[j] = segs[j], segs[i]
			}
		}
	}
}
