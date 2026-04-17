package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── LSM Store ─────────────────────────────────────────────────────

// lsm implements the LSM-based mapping store with automatic compaction.
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

	// WAL for durability (LSM entries collected per-goroutine)
	wal walapi.WAL

	// Auto-compaction
	compactInterval time.Duration // interval between compaction checks
	stopCh          chan struct{}  // stop compaction goroutine
	stopped         atomic.Bool
}

// ─── Goroutine-local WAL collectors ─────────────────────────────────
//
// Same pattern as Phase 6 btree. WAL entries are collected per-goroutine
// and flushed at checkpoint/commit time for fsync durability.
var lsmWALCollectors sync.Map // map[int64]*[]walapi.Record

// goroutineID returns the current goroutine's numeric ID.
func goroutineID() int64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	// The format is "goroutine N ..." — extract the number.
	for i := 0; i < n; i++ {
		if buf[i] == 'g' && i+8 < n && string(buf[i:i+8]) == "goroutine" {
			j := i + 9
			for j < n && buf[j] >= '0' && buf[j] <= '9' {
				j++
			}
			if j > i+9 {
				id := int64(0)
				for k := i + 9; k < j; k++ {
					id = id*10 + int64(buf[k]-'0')
				}
				return id
			}
		}
	}
	return 0
}

// registerLSMWALCollector registers a fresh collector for the current goroutine
// only if one is not already registered (idempotent per operation).
func registerLSMWALCollector() {
	gid := goroutineID()
	if _, exists := lsmWALCollectors.Load(gid); exists {
		return // already registered
	}
	var records []walapi.Record
	lsmWALCollectors.Store(gid, &records)
}

// getAndClearLSMWALCollector retrieves and clears the current goroutine's collector.
// Returns nil if no collector is registered.
func getAndClearLSMWALCollector() []walapi.Record {
	gid := goroutineID()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		collector := v.(*[]walapi.Record)
		records := *collector
		*collector = nil // clear
		return records
	}
	return nil
}

const defaultMemtableSize = 64 * 1024 * 1024 // 64MB
const defaultCompactInterval = 1 * time.Second

// New creates a new LSM store.
func New(cfg lsmapi.Config) (*lsm, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	memSize := cfg.MemtableSize
	if memSize <= 0 {
		memSize = defaultMemtableSize
	}

	compactInterval := time.Duration(cfg.CompactInterval) * time.Millisecond
	if compactInterval <= 0 {
		compactInterval = defaultCompactInterval
	}

	manifest, err := newManifest(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}

	s := &lsm{
		dir:              cfg.Dir,
		active:          newMemtable(),
		manifest:        manifest,
		memtableSize:    memSize,
		compactInterval: compactInterval,
		stopCh:          make(chan struct{}),
	}

	// Load existing SSTables into memtable (recovery from previous close).
	// Without this, reopen creates a fresh empty memtable and loses all
	// page/blob mappings written by the previous session's lsm.Close().
	// Load existing SSTables into memtable (recovery from previous close).
	// Without this, reopen creates a fresh empty memtable and loses all
	// page/blob mappings written by the previous session's lsm.Close().
	for _, seg := range manifest.Segments() {
		path := filepath.Join(cfg.Dir, seg)
		pages, blobs, err := readSSTable(path)
		if err != nil {
			continue
		}
		for _, p := range pages {
			s.active.SetPageMapping(p.key, p.value)
		}
		for _, b := range blobs {
			s.active.SetBlobMapping(b.key, b.value, b.size)
		}
	}

	// Start background compaction goroutine
	go s.backgroundCompaction()

	return s, nil
}

// SetWAL sets the WAL for LSM durability. Must be called before any Set*Mapping calls.
func (s *lsm) SetWAL(wal walapi.WAL) {
	s.wal = wal
}

// FlushToWAL collects pending WAL entries and writes them to WAL with fsync.
// Returns the last LSN written, or 0 if no entries.
func (s *lsm) FlushToWAL() (lastLSN uint64, err error) {
	records := getAndClearLSMWALCollector()
	if s.wal == nil || len(records) == 0 {
		return 0, nil
	}

	// Build batch from collected records
	batch := walapi.NewBatch()
	for _, rec := range records {
		batch.Records = append(batch.Records, rec)
	}

	lastLSN, err = s.wal.WriteBatch(batch)
	if err != nil {
		return lastLSN, err
	}
	return lastLSN, nil
}

// LastLSN returns the LSN of the last WAL entry written.
func (s *lsm) LastLSN() uint64 {
	if s.wal == nil {
		return 0
	}
	return s.wal.CurrentLSN()
}

// ─── Page Mappings ─────────────────────────────────────────────────

// SetPageMapping sets a page mapping and records it to WAL for durability.
func (s *lsm) SetPageMapping(pageID uint64, vaddr uint64) {
	// Register collector for this goroutine (idempotent per call)
	registerLSMWALCollector()

	// Append WAL entry to per-goroutine collector
	gid := goroutineID()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		records := v.(*[]walapi.Record)
		*records = append(*records, walapi.Record{
			ModuleType: walapi.ModuleLSM,
			Type:       walapi.RecordPageMap,
			ID:         pageID,
			VAddr:      vaddr,
		})
	}

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

// SetBlobMapping sets a blob mapping and records it to WAL for durability.
func (s *lsm) SetBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	// Register collector for this goroutine
	registerLSMWALCollector()

	// Append WAL entry to per-goroutine collector
	gid := goroutineID()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		records := v.(*[]walapi.Record)
		*records = append(*records, walapi.Record{
			ModuleType: walapi.ModuleLSM,
			Type:       walapi.RecordBlobMap,
			ID:         blobID,
			VAddr:      vaddr,
			Size:       size,
		})
	}

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

// DeleteBlobMapping deletes a blob mapping and records the deletion to WAL for durability.
func (s *lsm) DeleteBlobMapping(blobID uint64) {
	// Register collector for this goroutine
	registerLSMWALCollector()

	// Append WAL entry to per-goroutine collector
	gid := goroutineID()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		records := v.(*[]walapi.Record)
		*records = append(*records, walapi.Record{
			ModuleType: walapi.ModuleLSM,
			Type:       walapi.RecordBlobFree,
			ID:         blobID,
		})
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.DeleteBlobMapping(blobID)
}

// ─── Checkpoint ───────────────────────────────────────────────────

// Checkpoint records the checkpoint LSN after flushing pending WAL entries.
func (s *lsm) Checkpoint(lsn uint64) error {
	// Flush any pending WAL entries first (durability guarantee)
	if _, err := s.FlushToWAL(); err != nil {
		return err
	}

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

// backgroundCompaction runs compaction checks in the background.
func (s *lsm) backgroundCompaction() {
	ticker := time.NewTicker(s.compactInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if s.closed.Load() {
				return
			}
			s.MaybeCompact()
		case <-s.stopCh:
			return
		}
	}
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

	// Stop background compaction
	close(s.stopCh)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect all entries from active memtable.
	// NOTE: Do NOT use s.active.Size() > 0 as a guard — the size counter
	// can go negative due to update/delete accounting, even when entries exist.
	var pageEntries, blobEntries []sstEntry
	s.active.RangePages(func(pageID uint64, vaddr uint64) bool {
		pageEntries = append(pageEntries, sstEntry{key: pageID, value: vaddr})
		return true
	})
	s.active.RangeBlobs(func(blobID uint64, vaddr uint64, size uint32) bool {
		blobEntries = append(blobEntries, sstEntry{key: blobID, value: vaddr, size: size})
		return true
	})

	if len(pageEntries) > 0 || len(blobEntries) > 0 {
		segID := s.manifest.NextID()
		segName := fmt.Sprintf("segment-%03d.sst", segID)
		segPath := filepath.Join(s.dir, segName)
		if err := writeSSTable(segPath, pageEntries, blobEntries); err != nil {
			return err
		}
		if err := s.manifest.AddSegment(segName); err != nil {
			return err
		}
	}

	// Ensure all async manifest saves are complete before returning.
	s.manifest.Flush()
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

// Build rebuilds the in-memory structures from SSTables.
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

// ApplyPageMapping applies a page mapping update.
func (r *RecoveryStore) ApplyPageMapping(pageID uint64, vaddr uint64) {
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm.active.SetPageMapping(pageID, vaddr)
}

// ApplyBlobMapping applies a blob mapping update.
func (r *RecoveryStore) ApplyBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm.active.SetBlobMapping(blobID, vaddr, size)
}

// ApplyBlobDelete applies a blob deletion.
func (r *RecoveryStore) ApplyBlobDelete(blobID uint64) {
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm.active.DeleteBlobMapping(blobID)
}

// ApplyPageDelete applies a page deletion.
func (r *RecoveryStore) ApplyPageDelete(pageID uint64) {
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm.active.DeletePageMapping(pageID)
}

// SetCheckpointLSN sets the checkpoint LSN.
func (r *RecoveryStore) SetCheckpointLSN(lsn uint64) {
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm.checkpointLSN = lsn
}

// SetNextSegmentID sets the next segment ID for SSTable naming.
func (r *RecoveryStore) SetNextSegmentID(id uint64) {
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm.manifest.data.NextSegmentID = id
}

// ─── LSMLifecycle: Recovery surface for WAL replay ───────────────────
//
// These methods are on *lsm (not RecoveryStore) so that recovery.go
// can get the LSM via PageStore.LSMLifecycle() and replay ModuleLSM records.
func (s *lsm) ApplyPageMapping(pageID uint64, vaddr uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active.SetPageMapping(pageID, vaddr)
}

func (s *lsm) ApplyPageDelete(pageID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active.DeletePageMapping(pageID)
}

func (s *lsm) ApplyBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active.SetBlobMapping(blobID, vaddr, size)
}

func (s *lsm) ApplyBlobDelete(blobID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active.DeleteBlobMapping(blobID)
}

func (s *lsm) SetCheckpointLSN(lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointLSN = lsn
}

// DrainCollector retrieves and clears the current goroutine's WAL collector.
// Used by kvstore.assembleBatchFromCollectors to add LSM entries to the WAL batch
// with ModuleLSM module type.
func (s *lsm) DrainCollector() []walapi.Record {
	return getAndClearLSMWALCollector()
}

// NewRecoveryStore creates a recovery store.
