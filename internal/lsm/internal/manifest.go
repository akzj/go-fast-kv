package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ─── Manifest ───────────────────────────────────────────────────────

// manifest represents the LSM Store manifest file.
type manifest struct {
	mu   sync.RWMutex
	path string
	data manifestData
	wg   sync.WaitGroup // tracks in-flight async saves
}

// manifestData is the on-disk format of the manifest.
type manifestData struct {
	Version       int      `json:"version"`
	Segments      []string `json:"segments"`
	NextSegmentID uint64   `json:"next_segment_id"`
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

	m.data.Segments = append(m.data.Segments, name)
	return m.saveLocked()
}

// RemoveSegment removes a segment from the manifest.
func (m *manifest) RemoveSegment(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, seg := range m.data.Segments {
		if seg == name {
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

// Segments returns the list of segments.
func (m *manifest) Segments() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, len(m.data.Segments))
	copy(result, m.data.Segments)
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
