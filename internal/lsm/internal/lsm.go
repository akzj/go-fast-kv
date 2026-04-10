package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
)

// ─── LSM Store ─────────────────────────────────────────────────────

// lsm implements the LSM-based mapping store.
type lsm struct {
	mu     sync.RWMutex
	dir    string
	closed atomic.Bool

	// Active memtable
	active *memtable

	// Immutable memtables waiting to be compacted
	immutables []*memtable

	// Manifest for SSTable tracking
	manifest *manifest

	// Configuration
	memtableSize int64

	// Checkpoint LSN
	checkpointLSN uint64
}

const defaultMemtableSize = 64 * 1024 * 1024 // 64MB

// New creates a new LSM store.
func New(cfg lsmapi.Config) (*lsm, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	memSize := cfg.MemtableSize
	if memSize <= 0 {
		memSize = defaultMemtableSize
	}

	manifest, err := newManifest(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}

	s := &lsm{
		dir:           cfg.Dir,
		active:        newMemtable(),
		manifest:      manifest,
		memtableSize:  memSize,
	}

	return s, nil
}

// ─── Page Mappings ─────────────────────────────────────────────────

// SetPageMapping sets a page mapping.
func (s *lsm) SetPageMapping(pageID uint64, vaddr uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.SetPageMapping(pageID, vaddr)
}

// GetPageMapping gets a page mapping.
func (s *lsm) GetPageMapping(pageID uint64) (vaddr uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check active memtable first
	if vaddr, ok = s.active.GetPageMapping(pageID); ok {
		return vaddr, true
	}

	// Check immutable memtables (newest first)
	for i := len(s.immutables) - 1; i >= 0; i-- {
		if vaddr, ok = s.immutables[i].GetPageMapping(pageID); ok {
			return vaddr, true
		}
	}

	// Check SSTables
	return s.getPageFromSSTables(pageID)
}

// getPageFromSSTables looks up a page mapping in SSTables.
func (s *lsm) getPageFromSSTables(pageID uint64) (uint64, bool) {
	segments := s.manifest.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		path := filepath.Join(s.dir, segments[i])
		pages, _, err := readSSTable(path)
		if err != nil {
			continue
		}

		idx := searchPages(pages, pageID)
		if idx < len(pages) && pages[idx].key == pageID {
			return pages[idx].value, true
		}
	}
	return 0, false
}

func searchPages(pages []sstEntry, key uint64) int {
	lo, hi := 0, len(pages)
	for lo < hi {
		mid := (lo + hi) / 2
		if pages[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// ─── Blob Mappings ─────────────────────────────────────────────────

// SetBlobMapping sets a blob mapping.
func (s *lsm) SetBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.SetBlobMapping(blobID, vaddr, size)
}

// GetBlobMapping gets a blob mapping.
func (s *lsm) GetBlobMapping(blobID uint64) (vaddr uint64, size uint32, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check active memtable first
	if vaddr, size, ok = s.active.GetBlobMapping(blobID); ok {
		return vaddr, size, true
	}

	// Check immutable memtables
	for i := len(s.immutables) - 1; i >= 0; i-- {
		if vaddr, size, ok = s.immutables[i].GetBlobMapping(blobID); ok {
			return vaddr, size, true
		}
	}

	// Check SSTables
	return s.getBlobFromSSTables(blobID)
}

// getBlobFromSSTables looks up a blob mapping in SSTables.
func (s *lsm) getBlobFromSSTables(blobID uint64) (uint64, uint32, bool) {
	segments := s.manifest.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		path := filepath.Join(s.dir, segments[i])
		_, blobs, err := readSSTable(path)
		if err != nil {
			continue
		}

		idx := searchBlobs(blobs, blobID)
		if idx < len(blobs) && blobs[idx].key == blobID {
			return blobs[idx].value, blobs[idx].size, true
		}
	}
	return 0, 0, false
}

func searchBlobs(blobs []sstEntry, key uint64) int {
	lo, hi := 0, len(blobs)
	for lo < hi {
		mid := (lo + hi) / 2
		if blobs[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// DeleteBlobMapping deletes a blob mapping.
func (s *lsm) DeleteBlobMapping(blobID uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.DeleteBlobMapping(blobID)
}

// ─── Checkpoint ───────────────────────────────────────────────────

// Checkpoint records the checkpoint LSN.
func (s *lsm) Checkpoint(lsn uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointLSN = lsn
	return nil
}

// CheckpointLSN returns the last checkpoint LSN.
func (s *lsm) CheckpointLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpointLSN
}

// ─── Compaction ───────────────────────────────────────────────────

// MaybeCompact triggers compaction if the memtable is full.
func (s *lsm) MaybeCompact() error {
	s.mu.RLock()
	needsCompact := s.active.Size() >= s.memtableSize
	s.mu.RUnlock()

	if needsCompact {
		return s.compact()
	}
	return nil
}

// compact flushes the active memtable to SSTable.
func (s *lsm) compact() error {
	s.mu.Lock()

	if s.active.Size() < s.memtableSize {
		s.mu.Unlock()
		return nil
	}

	frozen := s.active
	s.active = newMemtable()
	s.immutables = append(s.immutables, frozen)
	s.mu.Unlock()

	s.runCompaction(frozen)
	return nil
}

// runCompaction writes the frozen memtable to SSTable and updates manifest.
func (s *lsm) runCompaction(frozen *memtable) {
	var pageEntries, blobEntries []sstEntry
	frozen.RangePages(func(pageID uint64, vaddr uint64) bool {
		pageEntries = append(pageEntries, sstEntry{key: pageID, value: vaddr})
		return true
	})
	frozen.RangeBlobs(func(blobID uint64, vaddr uint64, size uint32) bool {
		blobEntries = append(blobEntries, sstEntry{key: blobID, value: vaddr, size: size})
		return true
	})

	segID := s.manifest.NextID()
	segName := fmt.Sprintf("segment-%03d.sst", segID)
	segPath := filepath.Join(s.dir, segName)

	if err := writeSSTable(segPath, pageEntries, blobEntries); err != nil {
		return
	}
	if err := s.manifest.AddSegment(segName); err != nil {
		return
	}

	s.mu.Lock()
	for i, imm := range s.immutables {
		if imm == frozen {
			s.immutables = append(s.immutables[:i], s.immutables[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

// ─── Close ───────────────────────────────────────────────────────

// Close closes the LSM store.
func (s *lsm) Close() error {
	if s.closed.Swap(true) {
		return lsmapi.ErrClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active.Size() > 0 {
		var pageEntries, blobEntries []sstEntry
		s.active.RangePages(func(pageID uint64, vaddr uint64) bool {
			pageEntries = append(pageEntries, sstEntry{key: pageID, value: vaddr})
			return true
		})
		s.active.RangeBlobs(func(blobID uint64, vaddr uint64, size uint32) bool {
			blobEntries = append(blobEntries, sstEntry{key: blobID, value: vaddr, size: size})
			return true
		})

		segID := s.manifest.NextID()
		segName := fmt.Sprintf("segment-%03d.sst", segID)
		segPath := filepath.Join(s.dir, segName)
		writeSSTable(segPath, pageEntries, blobEntries)
		s.manifest.AddSegment(segName)
	}

	return nil
}

// ─── Recovery Support ──────────────────────────────────────────────

// RecoveryStore implements lsmapi.RecoveryStore for rebuilding the LSM store.
type RecoveryStore struct {
	lsm *lsm
}

// NewRecoveryStore creates a recovery store.
func NewRecoveryStore(dir string) (*RecoveryStore, error) {
	cfg := lsmapi.Config{Dir: dir}
	l, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &RecoveryStore{lsm: l}, nil
}

// ApplyPageMapping applies a page mapping update.
func (r *RecoveryStore) ApplyPageMapping(pageID uint64, vaddr uint64) {
	r.lsm.active.SetPageMapping(pageID, vaddr)
}

// ApplyBlobMapping applies a blob mapping update.
func (r *RecoveryStore) ApplyBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	r.lsm.active.SetBlobMapping(blobID, vaddr, size)
}

// ApplyBlobDelete applies a blob deletion.
func (r *RecoveryStore) ApplyBlobDelete(blobID uint64) {
	r.lsm.active.DeleteBlobMapping(blobID)
}

// SetCheckpointLSN sets the checkpoint LSN.
func (r *RecoveryStore) SetCheckpointLSN(lsn uint64) {
	r.lsm.mu.Lock()
	r.lsm.checkpointLSN = lsn
	r.lsm.mu.Unlock()
}

// Build rebuilds the in-memory structures.
func (r *RecoveryStore) Build() error {
	segments := r.lsm.manifest.Segments()
	for _, seg := range segments {
		path := filepath.Join(r.lsm.dir, seg)
		pages, blobs, err := readSSTable(path)
		if err != nil {
			continue
		}
		for _, p := range pages {
			r.lsm.active.SetPageMapping(p.key, p.value)
		}
		for _, b := range blobs {
			r.lsm.active.SetBlobMapping(b.key, b.value, b.size)
		}
	}
	return nil
}
