package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// SegmentManager handles lifecycle of segment files on disk.
//
// Design:
// - Each SegmentType has its own active segment + sealed segments map
// - Active segment receives all appends until full, then is sealed
// - Sealed segments are read-only (open on demand)
// - File naming: page_seg_{id}.dat, blob_seg_{id}.dat, blob_large_{id}.dat
//
// Thread-safety: segmentMu protects segment creation/lookup; individual
// segment files are append-only (no concurrent writes to same file).
type SegmentManager struct {
	dir string

	// Per-type segment tracking
	pageMu    sync.Mutex
	pageID    atomic.Uint64
	pageAct   *Segment // active page segment
	pageSealed map[api.SegmentID]*Segment

	blobMu    sync.Mutex
	blobID    atomic.Uint64
	blobAct   *Segment
	blobSealed map[api.SegmentID]*Segment

	largeMu   sync.Mutex
	largeID   atomic.Uint64
	largeSealed map[api.SegmentID]*Segment // large blobs: sealed on write, never reused
}

// NewSegmentManager creates a SegmentManager for the given directory.
func NewSegmentManager(dir string) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create segment dir: %w", err)
	}
	return &SegmentManager{
		dir:         dir,
		pageSealed:  make(map[api.SegmentID]*Segment),
		blobSealed:  make(map[api.SegmentID]*Segment),
		largeSealed: make(map[api.SegmentID]*Segment),
	}, nil
}

// Segment represents a single segment file.
//
// Invariant: Either act is appending OR segs[segmentID] is reading,
// never both for the same segment.
//
// Why not embed os.File? We need explicit lifecycle control (close, sync).
type Segment struct {
	ID     api.SegmentID
	Type   api.SegmentType
	Path   string
	file   *os.File
	size   atomic.Int64 // current file size (bytes)
	maxSize int64       // 0 = unlimited (large blob)
	sealed  atomic.Bool
	mu     sync.Mutex   // protects file ops (append writes)
}

// Append writes header + data to the segment.
// Returns the offset where data was written.
//
// Thread-safe: acquires segment lock.
// Returns error if segment is sealed or write fails.
// Why check sealed inside lock? Prevents race between seal and append.
func (s *Segment) Append(ctx context.Context, header *api.ObjectHeader, data []byte) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed.Load() {
		return 0, fmt.Errorf("%w: cannot append to sealed segment", api.ErrSegmentFull)
	}

	// Check size limit (skip for unlimited/large blob)
	if s.maxSize > 0 {
		newSize := s.size.Load() + int64(api.ObjectHeaderSize) + int64(len(data))
		if newSize > s.maxSize {
			return 0, fmt.Errorf("%w: segment at %d, need %d", api.ErrSegmentFull, s.size.Load(), newSize)
		}
	}

	offset := uint32(s.size.Load())

	// Serialize header
	hdrBytes, err := header.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("marshal header: %w", err)
	}

	// Append header + data
	if _, err := s.file.Write(hdrBytes); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}
	if _, err := s.file.Write(data); err != nil {
		return 0, fmt.Errorf("write data: %w", err)
	}

	s.size.Add(int64(len(hdrBytes) + len(data)))
	return offset, nil
}

// Read reads data at the given offset.
// Returns ObjectHeader and data bytes.
//
// Thread-safe: read-only operation, no locking needed for file reads
// (kernel handles concurrent reads; we use pread for stricter safety).
func (s *Segment) Read(ctx context.Context, offset uint32, size uint32) (*api.ObjectHeader, []byte, error) {
	// Read header
	hdrBytes := make([]byte, api.ObjectHeaderSize)
	if _, err := s.file.ReadAt(hdrBytes, int64(offset)); err != nil {
		return nil, nil, fmt.Errorf("read header at %d: %w", offset, err)
	}

	var hdr api.ObjectHeader
	if err := hdr.UnmarshalBinary(hdrBytes); err != nil {
		return nil, nil, fmt.Errorf("unmarshal header: %w", err)
	}

	// Read data
	data := make([]byte, size)
	if _, err := s.file.ReadAt(data, int64(offset)+api.ObjectHeaderSize); err != nil {
		return nil, nil, fmt.Errorf("read data at %d: %w", offset, err)
	}

	return &hdr, data, nil
}

// Sync calls fsync on the segment file.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Sync()
}

// Seal marks the segment as read-only and syncs to disk.
func (s *Segment) Seal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed.Swap(true) {
		return nil // already sealed
	}
	return s.file.Sync()
}

// Close closes the segment file.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

// --- Segment creation helpers ---

// pageSegmentFileName returns the filename for a page segment.
func pageSegmentFileName(id api.SegmentID) string {
	return fmt.Sprintf("page_seg_%d.dat", id)
}

// blobSegmentFileName returns the filename for a blob segment.
func blobSegmentFileName(id api.SegmentID) string {
	return fmt.Sprintf("blob_seg_%d.dat", id)
}

// largeSegmentFileName returns the filename for a large blob segment.
func largeSegmentFileName(id api.SegmentID) string {
	return fmt.Sprintf("blob_large_%d.dat", id)
}

// --- Segment Manager methods ---

// getOrCreateActive returns the active segment for the given type,
// creating one if necessary.
func (sm *SegmentManager) getOrCreateActive(ctx context.Context, segType api.SegmentType) (*Segment, error) {
	switch segType {
	case api.SegmentTypePage:
		return sm.getOrCreateActivePage(ctx)
	case api.SegmentTypeBlob:
		return sm.getOrCreateActiveBlob(ctx)
	default:
		return nil, fmt.Errorf("%w: large blob segments are not pooled", api.ErrInvalidSegment)
	}
}

func (sm *SegmentManager) getOrCreateActivePage(ctx context.Context) (*Segment, error) {
	sm.pageMu.Lock()
	defer sm.pageMu.Unlock()

	if sm.pageAct != nil && !sm.pageAct.sealed.Load() {
		return sm.pageAct, nil
	}

	// Create new page segment
	id := api.SegmentID(sm.pageID.Add(1))
	seg, err := sm.createSegment(id, api.SegmentTypePage, api.PageSegmentMaxSize)
	if err != nil {
		return nil, err
	}
	sm.pageAct = seg
	return seg, nil
}

func (sm *SegmentManager) getOrCreateActiveBlob(ctx context.Context) (*Segment, error) {
	sm.blobMu.Lock()
	defer sm.blobMu.Unlock()

	if sm.blobAct != nil && !sm.blobAct.sealed.Load() {
		return sm.blobAct, nil
	}

	// Create new blob segment
	id := api.SegmentID(sm.blobID.Add(1))
	seg, err := sm.createSegment(id, api.SegmentTypeBlob, api.BlobSegmentMaxSize)
	if err != nil {
		return nil, err
	}
	sm.blobAct = seg
	return seg, nil
}

func (sm *SegmentManager) createSegment(id api.SegmentID, segType api.SegmentType, maxSize int64) (*Segment, error) {
	var filename string
	switch segType {
	case api.SegmentTypePage:
		filename = pageSegmentFileName(id)
	case api.SegmentTypeBlob:
		filename = blobSegmentFileName(id)
	case api.SegmentTypeLarge:
		filename = largeSegmentFileName(id)
	default:
		return nil, fmt.Errorf("%w: unknown segment type %d", api.ErrInvalidSegment, segType)
	}

	path := filepath.Join(sm.dir, filename)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	// Get current size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat segment file: %w", err)
	}

	seg := &Segment{
		ID:       id,
		Type:     segType,
		Path:     path,
		file:     file,
		maxSize:  maxSize,
	}
	seg.size.Store(info.Size())
	return seg, nil
}

// CreateLargeSegment creates a new large blob segment (unlimited size, 1 blob per file).
func (sm *SegmentManager) CreateLargeSegment(ctx context.Context) (*Segment, error) {
	sm.largeMu.Lock()
	id := api.SegmentID(sm.largeID.Add(1))
	sm.largeMu.Unlock()

	seg, err := sm.createSegment(id, api.SegmentTypeLarge, 0) // 0 = unlimited
	if err != nil {
		return nil, err
	}

	sm.largeMu.Lock()
	sm.largeSealed[id] = seg
	sm.largeMu.Unlock()
	return seg, nil
}

// GetSegment returns a sealed segment by ID and type.
func (sm *SegmentManager) GetSegment(ctx context.Context, segID api.SegmentID, segType api.SegmentType) (*Segment, error) {
	switch segType {
	case api.SegmentTypePage:
		sm.pageMu.Lock()
		seg, ok := sm.pageSealed[segID]
		sm.pageMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: page segment %d", api.ErrInvalidSegment, segID)
		}
		return seg, nil
	case api.SegmentTypeBlob:
		sm.blobMu.Lock()
		seg, ok := sm.blobSealed[segID]
		sm.blobMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: blob segment %d", api.ErrInvalidSegment, segID)
		}
		return seg, nil
	case api.SegmentTypeLarge:
		sm.largeMu.Lock()
		seg, ok := sm.largeSealed[segID]
		sm.largeMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: large segment %d", api.ErrInvalidSegment, segID)
		}
		return seg, nil
	default:
		return nil, fmt.Errorf("%w: unknown type %d", api.ErrInvalidSegment, segType)
	}
}

// SealAndRotate seals the active segment and removes it from the active slot.
func (sm *SegmentManager) SealAndRotate(ctx context.Context, segType api.SegmentType) error {
	switch segType {
	case api.SegmentTypePage:
		sm.pageMu.Lock()
		if sm.pageAct != nil {
			if err := sm.pageAct.Seal(); err != nil {
				sm.pageMu.Unlock()
				return err
			}
			sm.pageSealed[sm.pageAct.ID] = sm.pageAct
			sm.pageAct = nil
		}
		sm.pageMu.Unlock()
		return nil
	case api.SegmentTypeBlob:
		sm.blobMu.Lock()
		if sm.blobAct != nil {
			if err := sm.blobAct.Seal(); err != nil {
				sm.blobMu.Unlock()
				return err
			}
			sm.blobSealed[sm.blobAct.ID] = sm.blobAct
			sm.blobAct = nil
		}
		sm.blobMu.Unlock()
		return nil
	default:
		return nil // large blobs are sealed on write, no rotation
	}
}

// SyncAll fsyncs all active segments.
func (sm *SegmentManager) SyncAll() error {
	var errs []error

	// Sync active page
	sm.pageMu.Lock()
	if sm.pageAct != nil {
		if err := sm.pageAct.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync page segment: %w", err))
		}
	}
	sm.pageMu.Unlock()

	// Sync active blob
	sm.blobMu.Lock()
	if sm.blobAct != nil {
		if err := sm.blobAct.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync blob segment: %w", err))
		}
	}
	sm.blobMu.Unlock()

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Close closes all segments.
func (sm *SegmentManager) Close() error {
	var errs []error

	sm.pageMu.Lock()
	if sm.pageAct != nil {
		errs = append(errs, sm.pageAct.Close())
	}
	for _, seg := range sm.pageSealed {
		errs = append(errs, seg.Close())
	}
	sm.pageMu.Unlock()

	sm.blobMu.Lock()
	if sm.blobAct != nil {
		errs = append(errs, sm.blobAct.Close())
	}
	for _, seg := range sm.blobSealed {
		errs = append(errs, seg.Close())
	}
	sm.blobMu.Unlock()

	sm.largeMu.Lock()
	for _, seg := range sm.largeSealed {
		errs = append(errs, seg.Close())
	}
	sm.largeMu.Unlock()

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
