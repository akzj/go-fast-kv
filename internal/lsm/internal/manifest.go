package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// ─── Manifest ───────────────────────────────────────────────────────

// manifest represents the LSM Store manifest file.
// Segments are stored as []segmentEntry with refcount support for
// checkpoint pinning. Checkpoint pins segments (increments refcount)
// before capturing state, ensuring GC doesn't delete them mid-checkpoint.
type manifest struct {
	mu   sync.RWMutex
	path string
	data manifestData
	// segments holds the in-memory segment list with refcount for pinning.
	// Refcount is incremented when checkpoint pins and decremented when
	// checkpoint completes. GC checks refcount==0 before deleting.
	segments []segmentEntry
	wg      sync.WaitGroup // tracks in-flight async saves
}

// manifestData is the on-disk format of the manifest.
type manifestData struct {
	Version       int           `json:"version"`
	Segments      []string     `json:"segments"` // segment filenames
	NextSegmentID uint64        `json:"next_segment_id"`
}

// segmentEntry pairs a segment filename with its reference count.
// Refcount is incremented when checkpoint pins the segment and
// decremented when checkpoint completes or is aborted.
// GC checks refcount==0 before deleting any SSTable.
// bloomFilter stores the deserialized bloom filter for this SSTable.
// level tracks which LSM tree level this SSTable belongs to (0=L0, 1=L1, 2=L2).
// minKey/maxKey track the key range for overlap detection during compaction.
type segmentEntry struct {
	name        string
	level       int
	refcount    atomic.Int64
	bloomFilter *BloomFilter // cached bloom filter, nil if not yet loaded
	minKey      uint64       // minimum key in this SSTable (0 if empty)
	maxKey      uint64       // maximum key in this SSTable (0 if empty)
}

// Level configuration (10x growth per level)
const (
	Level0Capacity = 4  // Max 4 SSTables at L0
	Level1Capacity = 10 // Max 10 SSTables at L1
	Level2Capacity = 100
)

// Pin increments the reference count for a segment.
// Returns true if the segment exists and was pinned.
func (m *manifest) Pin(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.segments {
		if m.segments[i].name == name {
			m.segments[i].refcount.Add(1)
			return true
		}
	}
	return false
}

// Unpin decrements the reference count for a segment.
// Returns true if the segment exists and was unpinned.
func (m *manifest) Unpin(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.segments {
		if m.segments[i].name == name {
			m.segments[i].refcount.Add(-1)
			return true
		}
	}
	return false
}

// PinAll atomically increments refcount for all current segments.
// Returns the list of pinned segment names.
func (m *manifest) PinAll() []string {
	m.mu.Lock()
	names := make([]string, 0, len(m.segments))
	for i := range m.segments {
		m.segments[i].refcount.Add(1)
		names = append(names, m.segments[i].name)
	}
	m.mu.Unlock()
	return names
}

// UnpinAll atomically decrements refcount for all given segments.
func (m *manifest) UnpinAll(names []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, name := range names {
		for i := range m.segments {
			if m.segments[i].name == name {
				m.segments[i].refcount.Add(-1)
				break
			}
		}
	}
}

// Refcount returns the current refcount for a segment, or -1 if not found.
func (m *manifest) Refcount(name string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, seg := range m.segments {
		if seg.name == name {
			return seg.refcount.Load()
		}
	}
	return -1
}

// SetRefcount sets the refcount for a segment (for recovery from checkpoint).
func (m *manifest) SetRefcount(name string, count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.segments {
		if m.segments[i].name == name {
			m.segments[i].refcount.Store(count)
			return
		}
	}
}

// CanDelete returns true if a segment's refcount is 0 (safe to delete).
func (m *manifest) CanDelete(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, seg := range m.segments {
		if seg.name == name {
			return seg.refcount.Load() == 0
		}
	}
	return false // segment doesn't exist
}

// TryDelete atomically checks refcount and removes the segment if 0.
// This prevents the race where checkpoint pins a segment between
// CanDelete() returning true and RemoveSegment() being called.
//
// The race pattern this fixes:
// 1. GC calls CanDelete(seg) → refcount is 0 → returns true
// 2. Concurrently, checkpoint calls Pin(seg) → refcount becomes 1
// 3. GC proceeds to delete segment (using segMgr.RemoveSegment)
// 4. Result: segment deleted while checkpoint holds reference → data loss
//
// By holding the write lock during the entire check+delete sequence,
// concurrent Pin/Unpin calls are blocked, ensuring no deletion while pinned.
//
// Returns true if the segment was deleted, false if refcount > 0.
func (m *manifest) TryDelete(segMgr segmentapi.SegmentManager, segID uint32) bool {
	segName := m.GetSegmentName(uint64(segID))

	// Acquire write lock - blocks concurrent Pin/Unpin
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the segment
	for i, seg := range m.segments {
		if seg.name == segName {
			// Atomically check refcount under write lock
			if seg.refcount.Load() > 0 {
				// Segment is pinned by checkpoint - do NOT delete
				return false
			}
			// Refcount is 0 - safe to delete
			// Remove from manifest
			m.segments = append(m.segments[:i], m.segments[i+1:]...)
			m.data.Segments = append(m.data.Segments[:i], m.data.Segments[i+1:]...)
			// Save manifest to disk
			if err := m.saveLocked(); err != nil {
				// Restore segment list on error
				m.segments = append(m.segments[:i], append([]segmentEntry{seg}, m.segments[i:]...)...)
				m.data.Segments = append(m.data.Segments[:i], append([]string{segName}, m.data.Segments[i:]...)...)
				return false
			}
			// Delete the segment file
			if err := segMgr.RemoveSegment(segID); err != nil {
				// Manifest already updated; segment file deletion failed.
				// This is not a race condition - we successfully prevented
				// deletion while pinned. The file deletion error should be
				// reported separately.
				return false
			}
			return true
		}
	}
	return false // segment not found
}

// GetBloomFilter returns the cached bloom filter for a segment, or nil if not cached.
func (m *manifest) GetBloomFilter(name string) *BloomFilter {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, seg := range m.segments {
		if seg.name == name {
			return seg.bloomFilter
		}
	}
	return nil
}

// SetBloomFilter sets the bloom filter for a segment (called after lazy loading from disk).
func (m *manifest) SetBloomFilter(name string, bf *BloomFilter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.segments {
		if m.segments[i].name == name {
			m.segments[i].bloomFilter = bf
			return
		}
	}
}

// GetSegmentName returns the filename for a given segment ID.
// Used by GC to check CanDelete before removing a segment.
func (m *manifest) GetSegmentName(segID uint64) string {
	return fmt.Sprintf("segment-%03d.sst", segID)
}

// SegmentNames returns a copy of all segment names for parallel iteration.
// This is more efficient than Segments() when you only need names.
func (m *manifest) SegmentNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, len(m.segments))
	for i, seg := range m.segments {
		result[i] = seg.name
	}
	return result
}

// SetSegments sets the segment list from checkpoint (v3+).
// This initializes the manifest with checkpoint-pinned segments,
// skipping rebuild from WAL for pre-checkpoint entries.
func (m *manifest) SetSegments(names []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear existing segments and rebuild from checkpoint
	m.segments = make([]segmentEntry, len(names))
	m.data.Segments = make([]string, len(names))
	for i, name := range names {
		m.segments[i] = segmentEntry{name: name}
		m.data.Segments[i] = name
	}
}

// newManifest creates or opens a manifest file.
func newManifest(dir string) (*manifest, error) {
	path := filepath.Join(dir, "manifest.json")

	m := &manifest{
		path: path,
		data: manifestData{
			Version:       1,
			Segments:      []string{},
			NextSegmentID: 1,
		},
		segments: []segmentEntry{},
	}

	// Try to load existing manifest
	if _, err := os.Stat(path); err == nil {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open manifest: %w", err)
		}
		defer f.Close()

		var data manifestData
		if err := json.NewDecoder(f).Decode(&data); err != nil {
			return nil, fmt.Errorf("decode manifest: %w", err)
		}
		m.data = data
		// Populate segments from loaded data
		for _, seg := range data.Segments {
			m.segments = append(m.segments, segmentEntry{name: seg})
		}
	}

	return m, nil
}

// AddSegment adds a segment to the manifest synchronously with level and key range.
// Unlike NextID (which saves async), this waits for the file system
// to acknowledge the update before returning. Called during LSM close
// when data durability is critical.
func (m *manifest) AddSegmentWithLevel(name string, level int, minKey, maxKey uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add as segmentEntry with refcount=0, level=0, key range
	m.segments = append(m.segments, segmentEntry{
		name:     name,
		level:    level,
		minKey:   minKey,
		maxKey:   maxKey,
	})
	m.data.Segments = append(m.data.Segments, name)
	return m.saveLocked()
}

// RemoveSegment removes a segment from the manifest.
// Refcount must be 0 before removal (checked by caller, e.g., GC).
func (m *manifest) RemoveSegment(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, seg := range m.segments {
		if seg.name == name {
			m.segments = append(m.segments[:i], m.segments[i+1:]...)
			m.data.Segments = append(m.data.Segments[:i], m.data.Segments[i+1:]...)
			return m.saveLocked()
		}
	}
	return nil
}

// NextID returns the next segment ID.
func (m *manifest) NextID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.data.NextSegmentID
	m.data.NextSegmentID++
	// Save asynchronously
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.mu.Lock()
		defer m.mu.Unlock()
		m.saveLocked()
	}()
	return id
}

// Flush waits for all pending async saves to complete.
func (m *manifest) Flush() {
	m.wg.Wait()
}

// Segments returns the list of segment names.
func (m *manifest) Segments() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0, len(m.segments))
	for _, seg := range m.segments {
		result = append(result, seg.name)
	}
	return result
}

// SetLevel sets the level for a segment.
func (m *manifest) SetLevel(name string, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.segments {
		if m.segments[i].name == name {
			m.segments[i].level = level
			return
		}
	}
}

// GetLevel returns the level for a segment, or 0 if not found.
func (m *manifest) GetLevel(name string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, seg := range m.segments {
		if seg.name == name {
			return seg.level
		}
	}
	return 0
}

// SetKeyRange sets the min/max key range for a segment.
func (m *manifest) SetKeyRange(name string, minKey, maxKey uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.segments {
		if m.segments[i].name == name {
			m.segments[i].minKey = minKey
			m.segments[i].maxKey = maxKey
			return
		}
	}
}

// GetKeyRange returns the min/max key range for a segment.
// Returns 0,0 if segment not found.
func (m *manifest) GetKeyRange(name string) (minKey, maxKey uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, seg := range m.segments {
		if seg.name == name {
			return seg.minKey, seg.maxKey
		}
	}
	return 0, 0
}

// GetSegmentsByLevel returns all segments at the given level.
func (m *manifest) GetSegmentsByLevel(level int) []segmentEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]segmentEntry, 0)
	for _, seg := range m.segments {
		if seg.level == level {
			result = append(result, seg)
		}
	}
	return result
}

// GetOverlappingSegments returns all segments at the given level whose
// key ranges overlap with [minKey, maxKey].
func (m *manifest) GetOverlappingSegments(level int, minKey, maxKey uint64) []segmentEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]segmentEntry, 0)
	for _, seg := range m.segments {
		if seg.level != level {
			continue
		}
		// Two ranges [a,b] and [c,d] overlap if a <= d AND c <= b
		if seg.minKey <= maxKey && minKey <= seg.maxKey {
			result = append(result, seg)
		}
	}
	return result
}

// CountLevel returns the number of segments at the given level.
func (m *manifest) CountLevel(level int) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, seg := range m.segments {
		if seg.level == level {
			count++
		}
	}
	return count
}

// saveLocked saves the manifest to disk. Caller must hold m.mu.
func (m *manifest) saveLocked() error {
	tmpPath := m.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(m.data); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("encode: %w", err)
	}
	if err := f.Sync(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("sync: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, m.path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// Sync directory
	dirFile, err := os.Open(filepath.Dir(m.path))
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	dirFile.Sync()
	dirFile.Close()

	return nil
}
