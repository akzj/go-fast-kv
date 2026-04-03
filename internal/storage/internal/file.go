// Package internal contains private implementation details for storage.
//
// Implementation responsibilities:
//   - Segment file format (header, data pages, trailer)
//   - File I/O (buffered writes, alignment)
//   - Segment rotation (Active → Sealed → Archived)
//   - CRC/checksum validation
package internal

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// OSFile implements FileOperations using OS-level file I/O.
type OSFile struct {
	mu    sync.RWMutex
	file  *os.File
	path  string
}

// NewOSFile creates a new OSFile (does not open the file).
func NewOSFile(path string) *OSFile {
	return &OSFile{path: path}
}

// Open opens or creates a file.
func (f *OSFile) Open(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		return nil // Already open
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Open file (create if not exists, read/write)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	f.file = file
	f.path = path
	return nil
}

// Close closes the file.
func (f *OSFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return nil // Already closed
	}

	err := f.file.Close()
	f.file = nil
	return err
}

// ReadAt reads len(p) bytes into p starting at offset.
func (f *OSFile) ReadAt(p []byte, offset int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.file == nil {
		return 0, ErrFileNotOpen
	}
	return f.file.ReadAt(p, offset)
}

// WriteAt writes len(p) bytes from p starting at offset.
func (f *OSFile) WriteAt(p []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return 0, ErrFileNotOpen
	}
	return f.file.WriteAt(p, offset)
}

// Sync flushes writes to durable storage.
func (f *OSFile) Sync() error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.file == nil {
		return ErrFileNotOpen
	}
	return f.file.Sync()
}

// Size returns the current file size in bytes.
func (f *OSFile) Size() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.file == nil {
		return 0, ErrFileNotOpen
	}
	info, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Truncate changes the file size to exactly size bytes.
func (f *OSFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return ErrFileNotOpen
	}
	return f.file.Truncate(size)
}

// Path returns the file path.
func (f *OSFile) Path() string {
	return f.path
}

// IsOpen returns true if the file is open.
func (f *OSFile) IsOpen() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.file != nil
}

// File returns the underlying *os.File (nil if not open).
// Exposed for testing.
func (f *OSFile) File() *os.File {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.file
}

// Errors.
var (
	ErrFileNotOpen = &os.PathError{Op: "file", Path: "", Err: os.ErrClosed}
)

// =============================================================================
// Checksum utilities
// =============================================================================

// CRC32Checksum computes a CRC32 checksum for the given data.
func CRC32Checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
