// Package kvstore implements the KVStore, the top-level integration layer.
//
// It wires together: SegmentManager×2, WAL, PageStore, BlobStore, BTree, TxnManager.
// Every Put/Get/Delete/Scan operates in auto-commit mode.
//
// Concurrency model:
//   - Put/Delete use s.mu.RLock (shared) — multiple writers run concurrently.
//   - Get/Scan use s.mu.RLock (shared) — readers run concurrently with writers.
//   - Checkpoint/Close use s.mu.Lock (exclusive) — block all other operations.
//   - Per-operation WAL entry isolation via goroutine-keyed collectors:
//     each Put/Delete registers a collector before tree.Put/Delete, and
//     WritePage/WriteBlob route entries to the caller's collector via goroutine ID.
//   - B-tree is concurrent-safe (per-page RwLocks).
//   - PageStore, BlobStore, SegmentManager, TxnManager are all internally locked.
//
// Design reference: docs/DESIGN.md §1, §3.6, §3.9.10
package internal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/akzj/go-fast-kv/internal/blobstore"
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	"github.com/akzj/go-fast-kv/internal/btree"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/lock"
	"github.com/akzj/go-fast-kv/internal/pagestore"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	"github.com/akzj/go-fast-kv/internal/txn"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	"github.com/akzj/go-fast-kv/internal/vacuum"
	vacuumapi "github.com/akzj/go-fast-kv/internal/vacuum/api"
	"github.com/akzj/go-fast-kv/internal/wal"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// snapshotTxnID returns the current NextXID from the TxnManager,
// used as a snapshot boundary for reads. Entries from transactions
// that started at or after this point are invisible to the reader.

const (
	defaultMaxSegmentSize  = 64 * 1024 * 1024 // 64 MB
	defaultInlineThreshold = 256
)

// ─── goroutineID ────────────────────────────────────────────────────

// goroutineID returns the current goroutine's numeric ID.
// Used to route WAL entries to per-operation collectors in blobWriterAdapter.
// Cost: ~200ns — acceptable for functions that do disk I/O.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// buf looks like "goroutine 123 [running]:\n..."
	s := buf[:n]
	s = s[len("goroutine "):]
	s = s[:bytes.IndexByte(s, ' ')]
	id, _ := strconv.ParseInt(string(s), 10, 64)
	return id
}

// store implements kvstoreapi.Store.
type store struct {
	mu sync.RWMutex

	dir string
	cfg kvstoreapi.Config

	// Storage layers
	pageSegMgr segmentapi.SegmentManager
	blobSegMgr segmentapi.SegmentManager
	wal        walapi.WAL
	pageStore  pagestoreapi.PageStore
	blobStore  blobstoreapi.BlobStore

	// Index
	tree     btreeapi.BTree
	provider *btree.RealPageProvider

	// Blob adapter (collects WAL entries per operation)
	blobAdapter *blobWriterAdapter

	// Transaction manager — always available
	txnMgr txnapi.TxnManager

	// Vacuum — MVCC old version cleanup

	// Vacuum — MVCC old version cleanup
	vacuum vacuumapi.Vacuum

	// Vacuum trigger — lazy async goroutine model.
	// No persistent background goroutine. A goroutine spawns on-demand
	// when threshold is crossed, cleans up, and exits. Self-triggers
	// if more garbage accumulated during the pass.
	vacuumTrigger struct {
		mu        sync.Mutex   // protects 'running' flag
		running   bool         // a vacuum goroutine is currently cleaning
		dirty     atomic.Bool  // opsCount was incremented while running
		opsCount  atomic.Int64 // total Put+Delete ops since last vacuum
		totalKeys atomic.Int64 // approximate live entry count
	}
	vacuumWg sync.WaitGroup // tracks in-flight vacuum goroutines for Close()
	closing  atomic.Bool   // set early in Close() to signal vacuum goroutines to exit

	// Read snapshots: maps readTxnID → Snapshot for MVCC visibility.
	// Get/Scan register a snapshot before reading, unregister after.
	readSnaps sync.Map // map[uint64]*txnapi.Snapshot

	closed bool
}

// ─── Open ───────────────────────────────────────────────────────────

// Open opens or creates a KVStore at the given directory.
// If a checkpoint exists, crash recovery is performed automatically.
func Open(cfg kvstoreapi.Config) (kvstoreapi.Store, error) {
	if cfg.MaxSegmentSize <= 0 {
		cfg.MaxSegmentSize = defaultMaxSegmentSize
	}
	if cfg.InlineThreshold <= 0 {
		cfg.InlineThreshold = defaultInlineThreshold
	}
	if cfg.AutoVacuumThreshold <= 0 {
		cfg.AutoVacuumThreshold = 1000
	}
	if cfg.AutoVacuumRatio <= 0 {
		cfg.AutoVacuumRatio = 0.1
	}

	// Create subdirectories
	pageSeg := filepath.Join(cfg.Dir, "page_segments")
	blobSeg := filepath.Join(cfg.Dir, "blob_segments")
	walDir := filepath.Join(cfg.Dir, "wal")
	for _, d := range []string{pageSeg, blobSeg, walDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, err
		}
	}

	// Create segment managers
	pageSegMgr, err := segment.New(segmentapi.Config{Dir: pageSeg, MaxSize: cfg.MaxSegmentSize})
	if err != nil {
		return nil, err
	}
	blobSegMgr, err := segment.New(segmentapi.Config{Dir: blobSeg, MaxSize: cfg.MaxSegmentSize})
	if err != nil {
		pageSegMgr.Close()
		return nil, err
	}

	// Create WAL
	w, err := wal.New(walapi.Config{Dir: walDir, SyncMode: int(cfg.SyncMode)})
	if err != nil {
		pageSegMgr.Close()
		blobSegMgr.Close()
		return nil, err
	}

	// Create PageStore and BlobStore
	ps := pagestore.New(pagestoreapi.Config{}, pageSegMgr)
	bs := blobstore.New(blobstoreapi.Config{}, blobSegMgr)

	// Create TxnManager — use SSI mode if configured
	// When SSI is configured, BeginSSITxn() tracks RWSet/WWSet for conflict detection.
	var tm txnapi.TxnManager
	if cfg.IsolationLevel == kvstoreapi.IsolationSerializable {
		tm = txn.NewWithSSI()
	} else {
		tm = txn.New()
	}

	// Create page provider and blob adapter
	cacheSize := cfg.PageCacheSize
	if cacheSize <= 0 {
		cacheSize = 8192
	}
	provider := btree.NewRealPageProvider(ps, cacheSize)
	ba := &blobWriterAdapter{store: bs}

	// We need a pointer to the store for the VisibilityChecker closure,
	// but the store doesn't exist yet. Use a pointer variable.
	var storeRef *store

	// Create BTree with snapshot-based MVCC visibility checker.
	// Readers register a Snapshot in readSnaps before calling Get/Scan.
	// The checker looks up the snapshot and uses Snapshot.IsVisible()
	// for true point-in-time consistency (immune to CLOG mutations mid-scan).
	// Writers (Delete's internal visibility) have no snapshot registered;
	// they fall back to a CLOG-only check.
	tree := btree.New(btreeapi.Config{
		InlineThreshold: cfg.InlineThreshold,
		VisibilityChecker: func(txnMin, txnMax, readTxnID uint64) bool {
			clog := tm.CLOG()

			// Try to find a registered read snapshot for this readTxnID.
			if storeRef != nil {
				if snapVal, ok := storeRef.readSnaps.Load(readTxnID); ok {
					snap := snapVal.(*txnapi.Snapshot)
					return snap.IsVisible(txnMin, txnMax, clog)
				}
			}

			// Fallback: writer path (Delete's internal visibility check).
			// The writer uses its own xid as readTxnID. No snapshot registered.
			// Own writes: visible if not yet deleted
			if txnMin == readTxnID {
				return txnMax == txnapi.TxnMaxInfinity
			}
			// Creator must be committed
			if clog.Get(txnMin) != txnapi.TxnCommitted {
				return false
			}
			// If deleted/superseded by a committed transaction, not visible
			if txnMax != txnapi.TxnMaxInfinity {
				if clog.Get(txnMax) == txnapi.TxnCommitted {
					return false
				}
			}
			return true
		},
	}, provider, ba)

	s := &store{
		dir:         cfg.Dir,
		cfg:         cfg,
		pageSegMgr:  pageSegMgr,
		blobSegMgr:  blobSegMgr,
		wal:         w,
		pageStore:   ps,
		blobStore:   bs,
		tree:        tree,
		provider:    provider,
		blobAdapter: ba,
		txnMgr:      tm,
	}
	// Wire up the storeRef so the VisibilityChecker closure can access readSnaps.
	storeRef = s

	// Wire vacuum — extract the B-tree's page locks via type assertion
	// so vacuum acquires the same per-page locks as Put/Delete/Get/Scan.
	type pageLockerProvider interface {
		PageLocks() *lock.PageRWLocks
	}
	if plp, ok := tree.(pageLockerProvider); ok {
		s.vacuum = vacuum.New(
			tree.RootPageID,
			provider,
			tm,
			bs,
			w,
			pageSegMgr.Sync,
			// Wrap RegisterCollector to return a pointer to the entries slice.
			// Vacuum's goroutine gets its own collector (keyed by goroutine ID),
			// so WritePage routes entries there instead of the shared buffer.
			// This prevents the shared-buffer stealing bug.
			func() (*[]pagestoreapi.WALEntry, func()) {
				collector, unreg := provider.RegisterCollector()
				return &collector.PageEntries, unreg
			},
			plp.PageLocks(),
		)
	}

	// Attempt crash recovery
	if err := s.recover(); err != nil {
		s.closeAll()
		return nil, err
	}

	return s, nil
}

// ─── Put ────────────────────────────────────────────────────────────

func (s *store) Put(key, value []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}
	if len(key) > btreeapi.MaxKeySize {
		return kvstoreapi.ErrKeyTooLarge
	}

	// Register per-operation WAL collectors (goroutine-keyed).
	// WritePage/WriteBlob route entries here via goroutine ID.
	pageCollector, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	blobCollector, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// Start SSI-aware transaction if configured
	var xid uint64
	var ssiTxn txnapi.Transaction

	if s.cfg.IsolationLevel == kvstoreapi.IsolationSerializable {
		// Use SSI transaction for write skew detection
		ssiTxn = s.txnMgr.BeginSSITxn()
		xid = ssiTxn.XID()
	} else {
		// Use regular auto-commit transaction
		xid, _ = s.txnMgr.BeginTxn()
	}

	// B-tree Put (concurrent-safe via per-page locks).
	// Entry is in the tree but NOT yet visible — VisibilityChecker
	// checks CLOG, and xid is still InProgress.
	if err := s.tree.Put(key, value, xid); err != nil {
		if ssiTxn != nil {
			ssiTxn.Abort()
		} else {
			s.txnMgr.Abort(xid)
		}
		return err
	}

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Build WAL commit entry manually (Type 7 = RecordTxnCommit).
	// We write the commit record to WAL BEFORE updating CLOG in memory,
	// so the entry remains invisible until WAL succeeds.
	commitWALEntry := txnapi.WALEntry{Type: 7, ID: xid}
	batch := assembleBatchFromCollectors(pageCollector, blobCollector, rootPageID, commitWALEntry)

	// WAL fsync provides durability. If this fails, abort the transaction
	// so the entry never becomes visible.
	if _, err := s.wal.WriteBatch(batch); err != nil {
		if ssiTxn != nil {
			ssiTxn.Abort()
		} else {
			s.txnMgr.Abort(xid)
		}
		return err
	}

	// WAL succeeded — now validate SSI conflicts (if enabled) and commit.
	if ssiTxn != nil {
		// SSI validation: checks RWSet/WWSet for write skew conflicts.
		// Returns ErrSerializationFailure if conflict detected.
		if err := ssiTxn.Commit(); err != nil {
			// SSI conflict detected — transaction already aborted internally.
			// Entry is still in tree but invisible (xid was aborted in CLOG).
			return err
		}
		// SSI validation passed — entry is committed and visible.
	} else {
		// Regular commit: update CLOG to make entry visible.
		s.txnMgr.Commit(xid)
	}

	// Trigger auto-vacuum check (lazy async goroutine).
	s.vacuumTrigger.opsCount.Add(1)
	s.vacuumTrigger.dirty.Store(true)
	s.checkAutoVacuum()

	return nil
}

// ─── Get ────────────────────────────────────────────────────────────

func (s *store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, kvstoreapi.ErrClosed
	}

	// Snapshot read: create a read-only snapshot WITHOUT allocating a XID.
	// This avoids inflating the CLOG and active set under high read load.
	// The snapshot captures ActiveXIDs at this moment, making visibility
	// immune to concurrent commits (true point-in-time isolation).
	readXID, snap := s.txnMgr.ReadSnapshot()
	s.readSnaps.Store(readXID, snap)
	defer func() {
		s.readSnaps.Delete(readXID)
		// No Abort() needed — ReadSnapshot doesn't allocate a real XID
	}()

	val, err := s.tree.Get(key, readXID)
	if err != nil {
		if err == btreeapi.ErrKeyNotFound {
			return nil, kvstoreapi.ErrKeyNotFound
		}
		return nil, err
	}
	return val, nil
}

// ─── Delete ─────────────────────────────────────────────────────────

func (s *store) Delete(key []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Register per-operation WAL collectors (goroutine-keyed).
	pageCollector, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	blobCollector, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// Start SSI-aware transaction if configured
	var xid uint64
	var ssiTxn txnapi.Transaction

	if s.cfg.IsolationLevel == kvstoreapi.IsolationSerializable {
		// Use SSI transaction for write skew detection
		ssiTxn = s.txnMgr.BeginSSITxn()
		xid = ssiTxn.XID()
	} else {
		// Use regular auto-commit transaction
		xid, _ = s.txnMgr.BeginTxn()
	}

	// B-tree Delete (concurrent-safe via per-page locks).
	// Deletion mark is in the tree but NOT yet visible — VisibilityChecker
	// checks CLOG, and xid is still InProgress.
	if err := s.tree.Delete(key, xid); err != nil {
		if ssiTxn != nil {
			ssiTxn.Abort()
		} else {
			s.txnMgr.Abort(xid)
		}
		if err == btreeapi.ErrKeyNotFound {
			return kvstoreapi.ErrKeyNotFound
		}
		return err
	}

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Build WAL commit entry manually. Write WAL BEFORE committing in CLOG.
	commitWALEntry := txnapi.WALEntry{Type: 7, ID: xid}
	batch := assembleBatchFromCollectors(pageCollector, blobCollector, rootPageID, commitWALEntry)

	if _, err := s.wal.WriteBatch(batch); err != nil {
		if ssiTxn != nil {
			ssiTxn.Abort()
		} else {
			s.txnMgr.Abort(xid)
		}
		return err
	}

	// WAL succeeded — now validate SSI conflicts (if enabled) and commit.
	if ssiTxn != nil {
		if err := ssiTxn.Commit(); err != nil {
			return err
		}
	} else {
		s.txnMgr.Commit(xid)
	}

	// Trigger auto-vacuum check (lazy async goroutine).
	s.vacuumTrigger.opsCount.Add(1)
	s.vacuumTrigger.dirty.Store(true)
	s.checkAutoVacuum()

	return nil
}

// ─── Scan ───────────────────────────────────────────────────────────

func (s *store) Scan(start, end []byte) kvstoreapi.Iterator {
	s.mu.RLock()

	if s.closed {
		s.mu.RUnlock()
		return &errIterator{err: kvstoreapi.ErrClosed}
	}

	// Snapshot read: create a read-only snapshot WITHOUT allocating a XID.
	// This avoids inflating the CLOG and active set under high read load.
	// The snapshot captures ActiveXIDs at this moment, providing true
	// point-in-time isolation for the entire scan (immune to concurrent commits).
	readXID, snap := s.txnMgr.ReadSnapshot()
	s.readSnaps.Store(readXID, snap)

	// B-tree Scan is concurrent-safe (per-page RwLocks).
	// RLock held during iterator creation to prevent Close() race.
	btreeIter := s.tree.Scan(start, end, readXID)
	s.mu.RUnlock()

	// Snapshot cleanup happens in snapshotIterator.Close()
	return &snapshotIterator{
		inner:   btreeIter,
		store:   s,
		readXID: readXID,
	}
}

// ─── Close ──────────────────────────────────────────────────────────

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Signal vacuum goroutines to exit early. This is an atomic.Bool
	// checked by vacuum goroutines WITHOUT holding s.mu, avoiding the
	// deadlock where Close holds s.mu.Lock and vacuumWg.Wait blocks
	// while the vacuum goroutine tries s.mu.RLock.
	s.closing.Store(true)

	// Wait for any in-flight auto-vacuum goroutine to finish.
	// This ensures vacuum isn't holding page locks when we close
	// the tree and page store below.
	s.vacuumWg.Wait()

	// Checkpoint before closing to persist all in-memory state.
	// This ensures the next Open can recover quickly from the checkpoint
	// rather than replaying the entire WAL. Even if Checkpoint fails,
	// we still close — WAL replay will recover on next Open.
	_ = s.checkpointLocked()

	s.closed = true

	// Wait for any in-flight vacuum goroutines to finish.
	// This prevents vacuum from running against a closed store.
	s.vacuumWg.Wait()

	return s.closeAll()
}

func (s *store) closeAll() error {
	var firstErr error
	save := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	save(s.tree.Close())
	save(s.pageStore.Close())
	save(s.blobStore.Close())
	save(s.wal.Close())
	save(s.pageSegMgr.Close())
	save(s.blobSegMgr.Close())
	return firstErr
}

// ─── RunVacuum ──────────────────────────────────────────────────────

// RunVacuum performs a single vacuum pass, cleaning up old MVCC versions
// from B-tree leaf nodes that are no longer visible to any active transaction.
//
// Thread safety: Vacuum acquires per-page write locks individually (the same
// locks used by Put/Delete/Get/Scan), so it can run concurrently with normal
// operations without blocking the entire store.
func (s *store) RunVacuum() (*kvstoreapi.VacuumStats, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, kvstoreapi.ErrClosed
	}
	s.mu.RUnlock()

	if s.vacuum == nil {
		return nil, errors.New("kvstore: vacuum not initialized")
	}

	return s.vacuum.Run()
}

// setVacuumDirty sets the dirty flag if a vacuum goroutine is currently running.
// This signals that the goroutine should re-trigger after it finishes.
func (s *store) setVacuumDirty() {
	s.vacuumTrigger.mu.Lock()
	if s.vacuumTrigger.running {
		s.vacuumTrigger.dirty.Store(true)
	}
	s.vacuumTrigger.mu.Unlock()
}

// ─── Auto-vacuum trigger ───────────────────────────────────────────

const defaultAutoVacuumThreshold = 1000
const defaultAutoVacuumRatio = 0.1

// checkAutoVacuum decides whether to spawn a lazy vacuum goroutine.
// Called after every Put/Delete. Returns immediately — vacuum runs async.
//
// Thread safety: a mutex ensures only one vacuum goroutine runs at a time.
// If garbage accumulates while vacuum is running, the 'dirty' flag causes
// a self-trigger after completion.
func (s *store) checkAutoVacuum() {
	if s.vacuum == nil {
		return
	}

	threshold := int64(s.cfg.AutoVacuumThreshold)
	if s.cfg.AutoVacuumRatio > 0 {
		total := s.vacuumTrigger.totalKeys.Load()
		if adaptive := int64(float64(total) * s.cfg.AutoVacuumRatio); adaptive > threshold {
			threshold = adaptive
		}
	}

	if s.vacuumTrigger.opsCount.Load() < threshold {
		return
	}

	// Try to claim the vacuum slot.
	s.vacuumTrigger.mu.Lock()
	if s.vacuumTrigger.running {
		s.vacuumTrigger.mu.Unlock()
		return
	}
	// Double-check threshold under lock (another goroutine may have vacuumed).
	if s.vacuumTrigger.opsCount.Load() < threshold {
		s.vacuumTrigger.mu.Unlock()
		return
	}
	s.vacuumTrigger.running = true
	s.vacuumTrigger.mu.Unlock()

	s.vacuumWg.Add(1)
	go func() {
		defer s.vacuumWg.Done()
		defer func() {
			s.vacuumTrigger.mu.Lock()
			s.vacuumTrigger.running = false
			if s.vacuumTrigger.dirty.Swap(false) {
				// More garbage accumulated while we ran — self-trigger.
				s.vacuumTrigger.mu.Unlock()
				s.checkAutoVacuum()
				return
			}
			s.vacuumTrigger.mu.Unlock()
		}()

		// Guard against TOCTOU race: Close() may have run between the
		// threshold check and this goroutine starting. Check s.closing
		// (atomic.Bool set early in Close) to avoid running vacuum on a
		// closing store. We do NOT use s.mu.RLock here because Close()
		// holds s.mu.Lock and waits on vacuumWg — using RLock would deadlock.
		if s.closing.Load() {
			return
		}

		// Run vacuum (thread-safe via per-page locks).
		stats, err := s.vacuum.Run()
		if err != nil || stats == nil {
			return
		}
		// Decrement opsCount by the number of removes we actually performed.
		// This keeps opsCount proportional to garbage accumulated, not total ops.
		removed := int64(stats.EntriesRemoved)
		for {
			current := s.vacuumTrigger.opsCount.Load()
			// Cap subtraction: don't go negative.
			newVal := current - removed
			if newVal < 0 {
				newVal = 0
			}
			if s.vacuumTrigger.opsCount.CompareAndSwap(current, newVal) {
				break
			}
		}
	}()
}

// ─── assembleBatchFromCollectors ────────────────────────────────────

// assembleBatchFromCollectors builds a WAL batch from per-operation collectors.
// This replaces the old assembleBatch that drained shared buffers.
func assembleBatchFromCollectors(
	pageCollector *btree.WALCollector,
	blobCollector *blobCollector,
	rootPageID uint64,
	commitEntry txnapi.WALEntry,
) *walapi.Batch {
	batch := walapi.NewBatch()

	// Page WAL entries
	for _, e := range pageCollector.PageEntries {
		batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Blob WAL entries
	for _, e := range blobCollector.Entries {
		batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Root pointer change
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, rootPageID, 0, 0)

	// Transaction commit/abort
	batch.Add(walapi.ModuleTree, walapi.RecordType(commitEntry.Type), commitEntry.ID, 0, 0)

	return batch
}

// ─── blobWriterAdapter ──────────────────────────────────────────────

// blobCollector collects blob WAL entries for a single KVStore operation.
type blobCollector struct {
	Entries []blobstoreapi.WALEntry
}

// blobWriterAdapter adapts BlobStore to btreeapi.BlobWriter,
// collecting WAL entries per-operation via goroutine-keyed collectors.
type blobWriterAdapter struct {
	store      blobstoreapi.BlobStore
	collectors sync.Map // map[int64]*blobCollector — keyed by goroutine ID
}

// registerCollector creates a per-operation blob WAL entry collector
// for the current goroutine. Returns the collector and an unregister function.
func (a *blobWriterAdapter) registerCollector() (*blobCollector, func()) {
	gid := goroutineID()
	c := &blobCollector{}
	a.collectors.Store(gid, c)
	return c, func() { a.collectors.Delete(gid) }
}

func (a *blobWriterAdapter) WriteBlob(data []byte) (uint64, error) {
	blobID, entry, err := a.store.Write(data)
	if err != nil {
		return 0, err
	}

	// Route WAL entry to per-operation collector or drop (should not happen
	// in normal operation — registerCollector is always called before WriteBlob).
	gid := goroutineID()
	if c, ok := a.collectors.Load(gid); ok {
		collector := c.(*blobCollector)
		collector.Entries = append(collector.Entries, entry)
	}
	// No fallback buffer — if no collector is registered, the entry is lost.
	// This is intentional: all KVStore operations register collectors.
	return blobID, nil
}

func (a *blobWriterAdapter) ReadBlob(blobID uint64) ([]byte, error) {
	return a.store.Read(blobID)
}

// ─── iteratorAdapter ────────────────────────────────────────────────

// iteratorAdapter wraps btreeapi.Iterator as kvstoreapi.Iterator.
type iteratorAdapter struct {
	inner btreeapi.Iterator
}

func (it *iteratorAdapter) Next() bool    { return it.inner.Next() }
func (it *iteratorAdapter) Key() []byte   { return it.inner.Key() }
func (it *iteratorAdapter) Value() []byte { return it.inner.Value() }
func (it *iteratorAdapter) Err() error    { return it.inner.Err() }
func (it *iteratorAdapter) Close()        { it.inner.Close() }

// ─── snapshotIterator ───────────────────────────────────────────────

// snapshotIterator wraps btreeapi.Iterator and manages the read snapshot lifetime.
// The snapshot is registered in store.readSnaps at Scan() time and cleaned up
// when Close() is called. This ensures the VisibilityChecker uses a consistent
// point-in-time snapshot for the entire scan duration.
type snapshotIterator struct {
	inner   btreeapi.Iterator
	store   *store
	readXID uint64
}

func (it *snapshotIterator) Next() bool    { return it.inner.Next() }
func (it *snapshotIterator) Key() []byte   { return it.inner.Key() }
func (it *snapshotIterator) Value() []byte { return it.inner.Value() }
func (it *snapshotIterator) Err() error    { return it.inner.Err() }
func (it *snapshotIterator) Close() {
	it.inner.Close()
	// Clean up the read snapshot so the VisibilityChecker no longer references it.
	// No Abort() needed — ReadSnapshot doesn't allocate a real XID.
	it.store.readSnaps.Delete(it.readXID)
}

// errIterator is returned when the store is closed.
type errIterator struct{ err error }

func (it *errIterator) Next() bool    { return false }
func (it *errIterator) Key() []byte   { return nil }
func (it *errIterator) Value() []byte { return nil }
func (it *errIterator) Err() error    { return it.err }
func (it *errIterator) Close()        {}

// ─── WriteBatch ─────────────────────────────────────────────────────

// batchOp represents a single staged operation in a WriteBatch.
type batchOp struct {
	opType byte // 0 = put, 1 = delete
	key    []byte
	value  []byte
}

// writeBatch implements kvstoreapi.WriteBatch.
// It stages Put/Delete operations and applies them atomically on Commit,
// sharing a single transaction and a single WAL fsync.
//
// NOT thread-safe — create one per goroutine.
type writeBatch struct {
	store    *store
	ops      []batchOp
	finished bool // true after Commit or Discard
}

// NewWriteBatch creates a new write batch for grouping operations.
func (s *store) NewWriteBatch() kvstoreapi.WriteBatch {
	return &writeBatch{store: s}
}

// Put stages a key-value pair for writing.
func (wb *writeBatch) Put(key, value []byte) error {
	if wb.finished {
		return kvstoreapi.ErrBatchCommitted
	}
	if len(key) > btreeapi.MaxKeySize {
		return kvstoreapi.ErrKeyTooLarge
	}
	wb.ops = append(wb.ops, batchOp{opType: 0, key: key, value: value})
	return nil
}

// Delete stages a key for deletion.
func (wb *writeBatch) Delete(key []byte) error {
	if wb.finished {
		return kvstoreapi.ErrBatchCommitted
	}
	wb.ops = append(wb.ops, batchOp{opType: 1, key: key})
	return nil
}

// Commit atomically applies all staged operations.
// All operations share one transaction and one WAL fsync.
// On error, the transaction is aborted — no partial writes are visible.
func (wb *writeBatch) Commit() error {
	if wb.finished {
		return kvstoreapi.ErrBatchCommitted
	}
	wb.finished = true

	s := wb.store
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Empty batch — nothing to do
	if len(wb.ops) == 0 {
		return nil
	}

	// Register per-operation WAL collectors (goroutine-keyed).
	pageCollector, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	blobCollector, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// Start transaction (SSI or regular)
	var xid uint64
	var ssiTxn txnapi.Transaction

	if s.cfg.IsolationLevel == kvstoreapi.IsolationSerializable {
		ssiTxn = s.txnMgr.BeginSSITxn()
		xid = ssiTxn.XID()
	} else {
		xid, _ = s.txnMgr.BeginTxn()
	}

	// Execute all operations in the same transaction
	for _, op := range wb.ops {
		var err error
		switch op.opType {
		case 0: // Put
			err = s.tree.Put(op.key, op.value, xid)
		case 1: // Delete
			err = s.tree.Delete(op.key, xid)
			if err == btreeapi.ErrKeyNotFound {
				err = kvstoreapi.ErrKeyNotFound
			}
		}
		if err != nil {
			if ssiTxn != nil {
				ssiTxn.Abort()
			} else {
				s.txnMgr.Abort(xid)
			}
			return err
		}
	}

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Build WAL commit entry manually. Write WAL BEFORE committing in CLOG.
	// All batch entries are in the tree but NOT visible — VisibilityChecker
	// checks CLOG, and xid is still InProgress. This guarantees atomicity:
	// readers see either all entries (after Commit) or none (before Commit).
	commitWALEntry := txnapi.WALEntry{Type: 7, ID: xid}
	batch := assembleBatchFromCollectors(pageCollector, blobCollector, rootPageID, commitWALEntry)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		if ssiTxn != nil {
			ssiTxn.Abort()
		} else {
			s.txnMgr.Abort(xid)
		}
		return err
	}

	// WAL succeeded — now validate SSI conflicts (if enabled) and commit.
	if ssiTxn != nil {
		if err := ssiTxn.Commit(); err != nil {
			return err
		}
	} else {
		s.txnMgr.Commit(xid)
	}

	// Trigger auto-vacuum check for each operation in the batch.
	if len(wb.ops) > 0 {
		s.vacuumTrigger.opsCount.Add(int64(len(wb.ops)))
		s.vacuumTrigger.dirty.Store(true)
		s.checkAutoVacuum()
	}

	return nil
}

// Discard releases resources without committing.
// Safe to call multiple times.
func (wb *writeBatch) Discard() {
	wb.ops = nil
	wb.finished = true
}
