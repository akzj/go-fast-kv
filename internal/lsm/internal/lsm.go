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
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// Compile-time check: lsm implements LSMLifecycle interface used by GC.
var _ pagestoreapi.LSMLifecycle = (*lsm)(nil)

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
	compactWG       sync.WaitGroup // tracks pending background compaction goroutines

	// Per-key sharded locks for fine-grained CAS operations.
	// Reduces contention vs global s.mu — concurrent CAS on different keys
	// can proceed in parallel.
	casLocks    []sync.Mutex
	casLockMask uint64 // mask for fast modulo: lockCount must be power of 2

	// Worker pool for parallel SSTable reads during lookups.
	readPool *workerPool
}

const defaultReadPoolWorkers = 4

const defaultCASLockCount = 256 // must be power of 2

// ─── Goroutine-local WAL collectors ─────────────────────────────────
//
// Same pattern as Phase 6 btree. WAL entries are collected per-goroutine
// and flushed at checkpoint/commit time for fsync durability.
var lsmWALCollectors sync.Map // map[int64]*[]walapi.Record
var lsmWALCollectorMu sync.Map // map[int64]*sync.Mutex — one mutex per goroutine for collector access

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

// getOrCreateCollectorMu returns the mutex for the given goroutine ID,
// creating one if it doesn't exist. Thread-safe.
func getOrCreateCollectorMu(gid int64) *sync.Mutex {
	if v, ok := lsmWALCollectorMu.Load(gid); ok {
		return v.(*sync.Mutex)
	}
	mu := &sync.Mutex{}
	actual, loaded := lsmWALCollectorMu.LoadOrStore(gid, mu)
	if loaded {
		return actual.(*sync.Mutex)
	}
	return mu
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
	// Also register the mutex for this goroutine
	getOrCreateCollectorMu(gid)
}

// getAndClearLSMWALCollector retrieves and clears the current goroutine's collector.
// Returns nil if no collector is registered. Thread-safe with SetPageMapping.
func getAndClearLSMWALCollector() []walapi.Record {
	gid := goroutineID()
	mu := getOrCreateCollectorMu(gid)
	mu.Lock()
	defer mu.Unlock()

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

	// Initialize per-key sharded CAS locks.
	// Using 256 locks reduces contention vs global s.mu — concurrent CAS on different
	// keys proceeds in parallel. lockCount must be power of 2 for fast bitmask modulo.
	lockCount := uint64(defaultCASLockCount)
	s.casLocks = make([]sync.Mutex, lockCount)
	s.casLockMask = lockCount - 1

	// Initialize worker pool for parallel SSTable reads.
	s.readPool = newWorkerPool(defaultReadPoolWorkers)

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

	// Append WAL entry to per-goroutine collector (protected by per-goroutine mutex)
	gid := goroutineID()
	mu := getOrCreateCollectorMu(gid)
	mu.Lock()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		records := v.(*[]walapi.Record)
		*records = append(*records, walapi.Record{
			ModuleType: walapi.ModuleLSM,
			Type:       walapi.RecordPageMap,
			ID:         pageID,
			VAddr:      vaddr,
		})
	}
	mu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.SetPageMapping(pageID, vaddr)
}

// CompareAndSetPageMapping atomically sets a page mapping only if the current
// value equals expectedVAddr. Returns true if the update was applied,
// false if the current value was not expected (concurrent modification).
// Records the new value to WAL for durability.
//
// Uses per-key sharded lock (not s.mu) to allow concurrent CAS on different keys.
func (s *lsm) CompareAndSetPageMapping(pageID uint64, expectedVAddr uint64, newVAddr uint64) bool {
	// Register collector for this goroutine (idempotent per call)
	registerLSMWALCollector()

	// Append WAL entry to per-goroutine collector (protected by per-goroutine mutex)
	gid := goroutineID()
	colMu := getOrCreateCollectorMu(gid)
	colMu.Lock()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		records := v.(*[]walapi.Record)
		*records = append(*records, walapi.Record{
			ModuleType: walapi.ModuleLSM,
			Type:       walapi.RecordPageMap,
			ID:         pageID,
			VAddr:      newVAddr,
		})
	}
	colMu.Unlock()

	// Per-key sharded lock — serializes CAS on this specific pageID.
	lockIdx := pageID & s.casLockMask
	s.casLocks[lockIdx].Lock()
	defer s.casLocks[lockIdx].Unlock()

	// Read current value under lock, then compare-and-set.
	current, ok := s.active.GetPageMapping(pageID)
	if !ok || current != expectedVAddr {
		return false
	}
	s.active.SetPageMapping(pageID, newVAddr)
	return true
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

// pageResult holds the result of a parallel SSTable page lookup.
type pageResult struct {
	value   uint64
	found   bool
}

// getPageFromSSTables looks up a page mapping in SSTables using parallel reads.
// Uses bloom filters to skip SSTables that definitely don't contain the key.
// Releases lock during SSTable file I/O for better concurrency.
func (s *lsm) getPageFromSSTables(pageID uint64) (uint64, bool) {
	segments := s.manifest.SegmentNames()

	// Phase 1: Check bloom filters to find candidate SSTables (lock-free, read-only)
	type candidate struct {
		name string
		path string
	}
	candidates := make([]candidate, 0, len(segments))
	for i := len(segments) - 1; i >= 0; i-- {
		segName := segments[i]
		segPath := filepath.Join(s.dir, segName)

		bloom := s.manifest.GetBloomFilter(segName)
		if bloom == nil {
			// No bloom filter cached, try to read it lazily from disk
			bloom = readBloomFilter(segPath)
			if bloom != nil {
				s.manifest.SetBloomFilter(segName, bloom)
			}
		}
		if bloom != nil && !bloom.Contains(pageID) {
			// Bloom filter says key definitely not in this SSTable, skip it
			continue
		}
		candidates = append(candidates, candidate{name: segName, path: segPath})
	}

	// No candidates after bloom filter check
	if len(candidates) == 0 {
		return 0, false
	}

	// Phase 2: Fan out SSTable reads in parallel using worker pool
	type result struct {
		segIdx int
		value  uint64
		found  bool
	}
	results := newResultCollector[result](len(candidates))

	for idx, cand := range candidates {
		segIdx := idx
		s.readPool.Submit(func() {
			pages, _, err := readSSTable(cand.path)
			if err != nil {
				return
			}
			pos := searchPages(pages, pageID)
			if pos < len(pages) && pages[pos].key == pageID {
				results.Add(result{segIdx: segIdx, value: pages[pos].value, found: true})
			}
		})
	}

	// Wait for all parallel reads to complete
	// We need to wait for pool completion - this requires synchronization
	// The pool's Wait() closes the task channel, but we need a way to know when done
	// Solution: track in-flight reads with a waitgroup
	type pendingResult struct {
		result result
		ready  chan struct{}
	}
	pending := make([]pendingResult, len(candidates))
	var readWG sync.WaitGroup

	for idx, cand := range candidates {
		segIdx := idx
		pending[idx].ready = make(chan struct{}, 1)
		readWG.Add(1)
		s.readPool.Submit(func() {
			defer readWG.Done()
			pages, _, err := readSSTable(cand.path)
			if err != nil {
				close(pending[segIdx].ready)
				return
			}
			pos := searchPages(pages, pageID)
			if pos < len(pages) && pages[pos].key == pageID {
				pending[segIdx].result = result{segIdx: segIdx, value: pages[pos].value, found: true}
			}
			close(pending[segIdx].ready)
		})
	}

	// Wait for all reads to complete
	readWG.Wait()

	// Phase 3: Find the newest (lowest index) matching segment
	// Candidates are newest-to-oldest, so lower index = newer
	for i := 0; i < len(pending); i++ {
		if pending[i].result.found {
			return pending[i].result.value, true
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

	// Append WAL entry to per-goroutine collector (protected by per-goroutine mutex)
	gid := goroutineID()
	mu := getOrCreateCollectorMu(gid)
	mu.Lock()
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
	mu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.SetBlobMapping(blobID, vaddr, size)
}

// CompareAndSetBlobMapping atomically sets a blob mapping only if the current
// value equals expectedVAddr and expectedSize. Returns true if the update was applied,
// false if the current value was not expected (concurrent modification).
// Records the new value to WAL for durability.
//
// Uses per-key sharded lock (not s.mu) to allow concurrent CAS on different keys.
func (s *lsm) CompareAndSetBlobMapping(blobID uint64, expectedVAddr uint64, expectedSize uint32, newVAddr uint64, newSize uint32) bool {
	// Register collector for this goroutine
	registerLSMWALCollector()

	// Append WAL entry to per-goroutine collector (always record new value, protected by per-goroutine mutex)
	gid := goroutineID()
	colMu := getOrCreateCollectorMu(gid)
	colMu.Lock()
	if v, ok := lsmWALCollectors.Load(gid); ok {
		records := v.(*[]walapi.Record)
		*records = append(*records, walapi.Record{
			ModuleType: walapi.ModuleLSM,
			Type:       walapi.RecordBlobMap,
			ID:         blobID,
			VAddr:      newVAddr,
			Size:       newSize,
		})
	}
	colMu.Unlock()

	// Per-key sharded lock — serializes CAS on this specific blobID.
	lockIdx := blobID & s.casLockMask
	s.casLocks[lockIdx].Lock()
	defer s.casLocks[lockIdx].Unlock()

	// Read current value under lock, then compare-and-set.
	current, size, ok := s.active.GetBlobMapping(blobID)
	if !ok || current != expectedVAddr || size != expectedSize {
		return false
	}
	s.active.SetBlobMapping(blobID, newVAddr, newSize)
	return true
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
// Uses bloom filters to skip SSTables that definitely don't contain the key.
func (s *lsm) getBlobFromSSTables(blobID uint64) (uint64, uint32, bool) {
	segments := s.manifest.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		segName := segments[i]
		segPath := filepath.Join(s.dir, segName)

		// Check bloom filter first to skip SSTable if key is definitely not present
		bloom := s.manifest.GetBloomFilter(segName)
		if bloom == nil {
			// No bloom filter available, try to read it lazily from disk
			bloom = readBloomFilter(segPath)
			if bloom != nil {
				s.manifest.SetBloomFilter(segName, bloom)
			}
		}
		if bloom != nil && !bloom.Contains(blobID) {
			// Bloom filter says key definitely not in this SSTable, skip it
			continue
		}

		_, blobs, err := readSSTable(segPath)
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
// Non-blocking: uses atomic swap and background goroutine for I/O.
func (s *lsm) compact() error {
	s.mu.Lock()

	if s.active.Size() < s.memtableSize {
		s.mu.Unlock()
		return nil
	}

	// Atomic swap: new active, old becomes immutable
	frozen := s.active
	s.active = newMemtable()
	s.immutables = append(s.immutables, frozen)
	s.mu.Unlock()

	// Background flush (non-blocking) — writers continue immediately
	s.compactWG.Add(1)
	go func() {
		defer s.compactWG.Done()
		s.runCompaction(frozen)
	}()
	return nil
}

// WaitForCompaction waits for all pending background compaction goroutines to complete.
// Used by tests to ensure compaction finishes before assertions.
func (s *lsm) WaitForCompaction() {
	s.compactWG.Wait()
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

	// Wait for any pending background compaction goroutines
	s.compactWG.Wait()

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

// Manifest returns the LSM manifest for checkpoint pinning.
// Caller can call PinAll/UnpinAll to manage segment refcounts.
func (s *lsm) Manifest() lsmapi.Manifest {
	return s.manifest
}

// SetSegments sets the segment list from checkpoint (v3+).
// This initializes the LSM manifest with checkpoint-pinned segments,
// skipping rebuild from WAL for pre-checkpoint entries.
func (s *lsm) SetSegments(names []string) {
	s.manifest.SetSegments(names)
}

// DrainCollector retrieves and clears the current goroutine's WAL collector.
// Used by kvstore.assembleBatchFromCollectors to add LSM entries to the WAL batch
// with ModuleLSM module type.
func (s *lsm) DrainCollector() []walapi.Record {
	return getAndClearLSMWALCollector()
}

// NewRecoveryStore creates a recovery store.
