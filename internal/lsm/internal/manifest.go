package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
type segmentEntry struct {
	name     string
	refcount atomic.Int64
}

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

// GetSegmentName returns the filename for a given segment ID.
// Used by GC to check CanDelete before removing a segment.
func (m *manifest) GetSegmentName(segID uint64) string {
	return fmt.Sprintf("segment-%03d.sst", segID)
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

// AddSegment adds a segment to the manifest synchronously.
// Unlike NextID (which saves async), this waits for the file system
// to acknowledge the update before returning. Called during LSM close
// when data durability is critical.
func (m *manifest) AddSegment(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add as segmentEntry with refcount=0
	m.segments = append(m.segments, segmentEntry{name: name})
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
