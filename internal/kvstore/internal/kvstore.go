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
	"time"
	"sync/atomic"

	"github.com/akzj/go-fast-kv/internal/blobstore"
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	"github.com/akzj/go-fast-kv/internal/btree"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	gcapi "github.com/akzj/go-fast-kv/internal/gc/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/lock"
	lsm "github.com/akzj/go-fast-kv/internal/lsm"
	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
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

	// GC — segment garbage collection (Phase 2).
	gcMu    sync.RWMutex
	gcStats *segmentStatsManager
	pageGC  gcapi.PageGC
	blobGC  gcapi.BlobGC
	pageGCTrigger struct {
		mu      sync.Mutex
		running bool
		dirty   atomic.Bool
	}
	blobGCTrigger struct {
		mu      sync.Mutex
		running bool
		dirty   atomic.Bool
	}
	pageGCWg  sync.WaitGroup
	blobGCWg sync.WaitGroup

	// Read snapshots: maps readTxnID → Snapshot for MVCC visibility.
	// Get/Scan register a snapshot before reading, unregister after.
	readSnaps sync.Map // map[uint64]*txnapi.Snapshot

	// Goroutine-local active transaction context.
	// When set (via SetActiveTxnContext), Get/Scan use this snapshot
	// instead of creating a fresh one via ReadSnapshot().
	// This enables deferred-write transactions: writes use txnCtx.XID(),
	// reads use the same txnCtx snapshot for own-write visibility.
	activeTxnCtx sync.Map // map[int64]txnapi.TxnContext

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
	lsmDir := filepath.Join(cfg.Dir, "lsm")
	for _, d := range []string{pageSeg, blobSeg, walDir, lsmDir} {
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

	// Create LSM for PageStore mapping, attach WAL for durability
	lsmStore, err := lsm.New(lsmapi.Config{Dir: lsmDir})
	if err != nil {
		pageSegMgr.Close()
		blobSegMgr.Close()
		w.Close()
		return nil, err
	}
	lsmStore.SetWAL(w)

	// Init GC stats manager (local var, assigned to store below)
	gcStats := newSegmentStatsManager()

	// Create PageStore and BlobStore (with stats tracking for GC)
	ps := pagestore.New(pagestoreapi.Config{PageCacheSize: cfg.PageCacheSize, StatsManager: gcStats}, pageSegMgr, lsmStore)
	bs := blobstore.New(blobstoreapi.Config{StatsManager: gcStats}, blobSegMgr)

	// Create TxnManager
	var tm txnapi.TxnManager = txn.New()

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

			// BulkLoad entries have txnMin=0, treated as "always committed".
			// This allows Delete to find entries loaded via BulkLoad.
			if txnMin == 0 {
				return txnMax == txnapi.TxnMaxInfinity
			}

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
		gcStats:     gcStats,
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

	// Init GC instances (page GC and blob GC)
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	bsRecovery := s.blobStore.(blobstoreapi.BlobStoreRecovery)
	s.pageGC = gcapi.NewPageGC(s.pageSegMgr, s.pageStore, psRecovery, s.wal)
	s.blobGC = gcapi.NewBlobGC(s.blobSegMgr, s.blobStore, bsRecovery, s.wal)

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
	// Note: LSM's registerLSMWALCollector is idempotent — SetPageMapping inside
	// tree.Put will use the same collector, not create a new one.
	_, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	_, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// Start regular auto-commit transaction
	xid, _ := s.txnMgr.BeginTxn()

	// B-tree Put (concurrent-safe via per-page locks).
	// Entry is in the tree but NOT yet visible — VisibilityChecker
	// checks CLOG, and xid is still InProgress.
	if err := s.tree.Put(key, value, xid); err != nil {
		s.txnMgr.Abort(xid)
		return err
	}

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Build WAL commit entry manually (Type 7 = RecordTxnCommit).
	// We write the commit record to WAL BEFORE updating CLOG in memory,
	// so the entry remains invisible until WAL succeeds.
	commitWALEntry := txnapi.WALEntry{Type: 7, ID: xid}
	batch := assembleBatchFromCollectors(s.provider, s.blobAdapter, s.pageStore.(pagestoreapi.PageStoreRecovery).LSMLifecycle(), rootPageID, commitWALEntry)

	// WAL fsync provides durability. If this fails, abort the transaction
	// so the entry never becomes visible.
	if _, err := s.wal.WriteBatch(batch); err != nil {
		s.txnMgr.Abort(xid)
		return err
	}

	// WAL succeeded — commit the transaction to make entry visible.
	s.txnMgr.Commit(xid)

	// Trigger auto-vacuum check (lazy async goroutine).
	s.vacuumTrigger.opsCount.Add(1)
	s.vacuumTrigger.dirty.Store(true)
	s.checkAutoVacuum()

	return nil
}

// ─── BulkLoad ───────────────────────────────────────────────────────

// BulkLoad performs a fast bulk import of pre-sorted key-value pairs.
// All entries are loaded with TxnMin=0, TxnMax=MaxUint64 (visible to all readers).
//
// This bypasses the normal O(log n) insert path, achieving O(n) complexity.
// Individual page writes are NOT logged to WAL for performance.
func (s *store) BulkLoad(pairs []btreeapi.KVPair) error {
	return s.bulkLoad(pairs, btreeapi.BulkModeFast, 0)
}

// BulkLoadMVCC performs a bulk import with MVCC versioning.
// Entries are committed to CLOG, making them immediately visible to all readers.
// The startTxnID parameter is used only for CLOG tracking.
func (s *store) BulkLoadMVCC(pairs []btreeapi.KVPair, startTxnID uint64) error {
	// Use BulkModeFast (txnMin=0) so entries are visible to all readers immediately.
	// Then commit startTxnID to CLOG for proper transaction accounting.
	// The startTxnID is committed but not used as the visibility boundary
	// (since readTxnID from ReadSnapshot will be larger than startTxnID).
	return s.bulkLoad(pairs, btreeapi.BulkModeFast, startTxnID)
}

func (s *store) bulkLoad(pairs []btreeapi.KVPair, mode btreeapi.BulkMode, txnID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	if len(pairs) == 0 {
		return nil
	}

	// Create bulk loader
	var loader btreeapi.BulkLoader
	if mode == btreeapi.BulkModeFast || txnID == 0 {
		// Fast mode: txnMin=0, visible to all readers immediately
		loader = s.tree.NewBulkLoader(mode)
	} else {
		// MVCC mode: txnMin=txnID
		loader = s.tree.NewBulkLoaderWithTxn(mode, txnID)
	}

	// Add all pairs
	if err := loader.AddSorted(pairs); err != nil {
		loader.Close()
		return err
	}

	// Build the tree
	newRootPID, err := loader.Build()
	if err != nil {
		loader.Close()
		return err
	}

	// Atomic root swap
	s.tree.SetRootPageID(newRootPID)

	// Write WAL entry for crash recovery
	// We write a root-change entry so recovery knows to use the new root
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, newRootPID, 0, 0)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	// For MVCC mode: commit txnID in CLOG so entries become visible
	if txnID > 0 {
		s.txnMgr.Commit(txnID)
	}

	return nil
}

// ─── Get ────────────────────────────────────────────────────────────

func (s *store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, kvstoreapi.ErrClosed
	}

	// Check for a goroutine-local active transaction context.
	// If set, use the transaction's snapshot for reads so that own writes
	// (PutWithXID using the same txnCtx.XID) are visible.
	gid := goroutineID()
	if txnCtxRaw, ok := s.activeTxnCtx.Load(gid); ok {
		txnCtx := txnCtxRaw.(txnapi.TxnContext)
		snap := txnCtx.Snapshot()
		if snap != nil {
			s.readSnaps.Store(txnCtx.XID(), snap)
			defer s.readSnaps.Delete(txnCtx.XID())
			val, err := s.tree.Get(key, txnCtx.XID())
			if err == btreeapi.ErrKeyNotFound {
				return nil, kvstoreapi.ErrKeyNotFound
			}
			return val, err
		}
	}

	// Snapshot read: create a read-only snapshot WITHOUT allocating a XID.
	readXID, snap := s.txnMgr.ReadSnapshot()
	s.readSnaps.Store(readXID, snap)
	defer s.readSnaps.Delete(readXID)

	val, err := s.tree.Get(key, readXID)
	if err == btreeapi.ErrKeyNotFound {
		return nil, kvstoreapi.ErrKeyNotFound
	}
	return val, err
}

// ─── Delete ─────────────────────────────────────────────────────────

func (s *store) Delete(key []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Register per-operation WAL collectors (goroutine-keyed).
	_, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	_, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// Start regular auto-commit transaction
	xid, _ := s.txnMgr.BeginTxn()

	// B-tree Delete (concurrent-safe via per-page locks).
	// Deletion mark is in the tree but NOT yet visible — VisibilityChecker
	// checks CLOG, and xid is still InProgress.
	if err := s.tree.Delete(key, xid); err != nil {
		s.txnMgr.Abort(xid)
		if err == btreeapi.ErrKeyNotFound {
			return kvstoreapi.ErrKeyNotFound
		}
		return err
	}

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Build WAL commit entry manually. Write WAL BEFORE committing in CLOG.
	commitWALEntry := txnapi.WALEntry{Type: 7, ID: xid}
	batch := assembleBatchFromCollectors(s.provider, s.blobAdapter, s.pageStore.(pagestoreapi.PageStoreRecovery).LSMLifecycle(), rootPageID, commitWALEntry)

	if _, err := s.wal.WriteBatch(batch); err != nil {
		s.txnMgr.Abort(xid)
		return err
	}

	// WAL succeeded — commit the transaction to make entry visible.
	s.txnMgr.Commit(xid)

	// Trigger auto-vacuum check (lazy async goroutine).
	s.vacuumTrigger.opsCount.Add(1)
	s.vacuumTrigger.dirty.Store(true)
	s.checkAutoVacuum()

	return nil
}

// PutWithXID writes a key-value pair directly with a specific transaction ID.
// Unlike Put which allocates a fresh XID and auto-commits, this writes to the
// btree with the given txnID and does NOT commit or abort. The caller is
// responsible for managing the transaction lifecycle (via txnMgr.Commit/Abort).
//
// This is used by the SQL executor for deferred-write transactions:
// - All writes share the transaction's XID (own-write visibility via txnMin==s.XID)
// - Rollback marks entries as deleted with the same XID (txnMax==txnXID → invisible)
//
// WARNING: Caller must call txnMgr.Commit/Abort on txnID to update CLOG.
// WARNING: This method registers WAL collectors for page/blob changes so that
// SQL transaction commit can flush these entries to WAL for crash durability.
func (s *store) PutWithXID(key, value []byte, txnID uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}
	if len(key) > btreeapi.MaxKeySize {
		return kvstoreapi.ErrKeyTooLarge
	}

	// Register collectors for WAL tracking. The collector persists for the
	// entire transaction (NOT unregistered here). CommitWithXID/AbortWithXID
	// calls CollectAndClear to retrieve and delete the collector entries.
	// Multiple PutWithXID calls on the same goroutine share one collector.
	s.provider.RegisterCollector()
	s.blobAdapter.registerCollector()

	// Write directly to btree with given txnID — no new XID allocation.
	// WAL entries are captured in the collectors above for later flush.
	return s.tree.Put(key, value, txnID)
}

// DeleteWithXID marks a key as deleted directly with a specific transaction ID.
// Unlike Delete which allocates a fresh XID and auto-commits, this marks
// txnMax=txnID in the btree and does NOT commit or abort. The caller is
// responsible for managing the transaction lifecycle.
//
// For SQL rollback: a self-delete (txnMax==txnXID) makes the entry invisible
// without needing to restore the original value (fundamental MVCC limitation).
//
// WARNING: Caller must call txnMgr.Commit/Abort on txnID to update CLOG.
// WARNING: This method registers WAL collectors for crash durability.
func (s *store) DeleteWithXID(key []byte, txnID uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Register collectors for WAL tracking (shared across transaction).
	s.provider.RegisterCollector()
	s.blobAdapter.registerCollector()

	// Mark txnMax=txnID directly in btree — no new XID allocation, no WAL entry.
	// The self-delete (txnMax==txnXID) rule makes the entry invisible.
	// WAL entries captured in collectors above for later flush.
	err := s.tree.Delete(key, txnID)
	if err == btreeapi.ErrKeyNotFound {
		return kvstoreapi.ErrKeyNotFound
	}
	return err
}

// CommitWithXID finalizes a SQL transaction by flushing pending WAL entries to disk
// and updating CLOG. This is called by the SQL layer at COMMIT time.
//
// The flow: PutWithXID/DeleteWithXID register collectors (goroutine-keyed),
// tree.Put/Delete writes pages → WritePage routes entries to the same collectors.
// At commit, we collect those entries, write them to WAL, and update CLOG.
//
// This ensures SQL transaction writes survive crashes (WAL durability).
func (s *store) CommitWithXID(xid uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Collect WAL entries from this goroutine's collectors (registered by PutWithXID/DeleteWithXID).
	pageEntries := s.provider.CollectAndClear()
	blobEntries := s.blobAdapter.CollectAndClear()
	rootPageID := s.tree.RootPageID()

	// Build WAL batch with page mappings + transaction commit record.
	batch := walapi.NewBatch()
	// Page entries → ModuleLSM (LSM handles page→vaddr mapping persistence)
	for _, e := range pageEntries {
		batch.Add(walapi.ModuleLSM, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}
	// Blob entries → ModuleLSM (LSM handles blob→vaddr mapping persistence)
	for _, e := range blobEntries {
		batch.Add(walapi.ModuleLSM, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}
	// LSM collector: entries from SetPageMapping/SetBlobMapping during this transaction
	// Drain via LSMLifecycle interface and add as ModuleLSM records.
	psRecovery := s.pageStore.(pagestoreapi.PageStoreRecovery)
	if lsm := psRecovery.LSMLifecycle(); lsm != nil {
		for _, rec := range lsm.DrainCollector() {
			batch.Records = append(batch.Records, rec)
		}
	}
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, rootPageID, 0, 0)
	batch.Add(walapi.ModuleTree, walapi.RecordTxnCommit, xid, 0, 0)

	// WAL fsync — provides durability. If this fails, the transaction is NOT committed.
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	// WAL succeeded — now update CLOG to make the transaction visible.
	s.txnMgr.Commit(xid)
	return nil
}

// AbortWithXID rolls back a SQL transaction by writing a TxnAbort WAL record.
// This is called by the SQL layer at ROLLBACK time.
//
// Rollback marks entries as self-deleted (txnMax=txnXID) already happened
// during the DML operations. This method writes the abort record to WAL
// for crash-consistency: on recovery, aborted transactions are ignored.
func (s *store) AbortWithXID(xid uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Collect any remaining WAL entries from this goroutine's collectors.
	pageEntries := s.provider.CollectAndClear()
	blobEntries := s.blobAdapter.CollectAndClear()
	rootPageID := s.tree.RootPageID()

	// Build WAL batch with abort record.
	batch := walapi.NewBatch()
	for _, e := range pageEntries {
		batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}
	for _, e := range blobEntries {
		batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, rootPageID, 0, 0)
	// RecordTxnAbort = 8 (from txn/api/api.go WALEntry definition)
	batch.Add(walapi.ModuleTree, 8, xid, 0, 0)

	// Write WAL batch (fsync).
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	// Update CLOG to mark transaction as aborted.
	s.txnMgr.Abort(xid)
	return nil
}

// DeleteRange removes all keys in [start, end).
// Uses WriteBatch internally for efficiency.
// Returns the number of keys deleted.
func (s *store) DeleteRange(start, end []byte) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, kvstoreapi.ErrClosed
	}

	// Use WriteBatch for atomicity and efficiency
	batch := s.NewWriteBatch()
	count := 0

	// Scan the range and delete each key
	iter := s.Scan(start, end)
	defer iter.Close()

	for iter.Next() {
		key := iter.Key()
		if err := batch.Delete(key); err != nil {
			batch.Discard()
			return count, err
		}
		count++
	}

	if err := iter.Err(); err != nil {
		batch.Discard()
		return count, err
	}

	if count == 0 {
		return 0, nil
	}

	if err := batch.Commit(); err != nil {
		return count, err
	}

	return count, nil
}

// ─── Scan ───────────────────────────────────────────────────────────

func (s *store) Scan(start, end []byte) kvstoreapi.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return &errIterator{err: kvstoreapi.ErrClosed}
	}

	// Check for a goroutine-local active transaction context.
	gid := goroutineID()
	if txnCtxRaw, ok := s.activeTxnCtx.Load(gid); ok {
		txnCtx := txnCtxRaw.(txnapi.TxnContext)
		snap := txnCtx.Snapshot()
		if snap != nil {
			s.readSnaps.Store(txnCtx.XID(), snap)
			btreeIter := s.tree.Scan(start, end, txnCtx.XID())
			return &snapshotIterator{
				inner:   btreeIter,
				store:   s,
				readXID: txnCtx.XID(),
				cleanup: func() { s.readSnaps.Delete(txnCtx.XID()) },
			}
		}
	}

	// Snapshot read: create a read-only snapshot WITHOUT allocating a XID.
	readXID, snap := s.txnMgr.ReadSnapshot()
	s.readSnaps.Store(readXID, snap)

	btreeIter := s.tree.Scan(start, end, readXID)

	return &snapshotIterator{
		inner:   btreeIter,
		store:   s,
		readXID: readXID,
	}
}


// SetTTL sets a key with expiration time.
// Currently a stub - TTL is stored in value metadata.
func (s *store) SetTTL(key []byte, ttl time.Duration) error {
	return kvstoreapi.ErrNotImplemented
}

// TTL returns the remaining time for a key.
// Currently a stub.
func (s *store) TTL(key []byte) (time.Duration, error) {
	return 0, kvstoreapi.ErrNotImplemented
}

// TxnManager returns the underlying transaction manager.
// Used by the SQL layer to create TxnContext for BEGIN...COMMIT transactions.
func (s *store) TxnManager() txnapi.TxnContextFactory {
	return s.txnMgr
}

// RegisterSnapshot registers a transaction's read snapshot.
// Used by SQL layer to provide snapshot isolation within transactions.
func (s *store) RegisterSnapshot(txnXID uint64, snap *txnapi.Snapshot) {
	s.readSnaps.Store(txnXID, snap)
}

// UnregisterSnapshot removes a transaction's snapshot from readSnaps.
func (s *store) UnregisterSnapshot(txnXID uint64) {
	s.readSnaps.Delete(txnXID)
}

// SetActiveTxnContext registers a goroutine-local active transaction context.
// Called by the SQL layer at the start of a transaction (BEGIN).
// Get/Scan check this to use the transaction's snapshot for own-write visibility.
func (s *store) SetActiveTxnContext(txnCtx txnapi.TxnContext) {
	s.activeTxnCtx.Store(goroutineID(), txnCtx)
}

// ClearActiveTxnContext removes the goroutine-local active transaction context.
// Called by the SQL layer after Commit or Rollback.
func (s *store) ClearActiveTxnContext() {
	s.activeTxnCtx.Delete(goroutineID())
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

// ─── Auto-GC trigger ────────────────────────────────────────────────

// checkAutoGC decides whether to spawn a lazy GC goroutine for page or blob segments.
// Called after every Put/Delete. Returns immediately — GC runs async.
//
// Thread safety: a mutex per GC type ensures only one goroutine runs at a time.
// If more sealed segments accumulate while running, the 'dirty' flag self-triggers.
func (s *store) checkAutoGC() {
	if s.gcStats == nil {
		return
	}

	// Page GC trigger
	pageThreshold := 5 // sealed page segments
	pageSealed := s.pageSegMgr.SealedSegments()
	if len(pageSealed) >= pageThreshold {
		s.pageGCTrigger.mu.Lock()
		if !s.pageGCTrigger.running {
			s.pageGCTrigger.running = true
			s.pageGCTrigger.mu.Unlock()
			s.pageGCWg.Add(1)
			go func() {
				defer s.pageGCWg.Done()
				defer func() {
					s.pageGCTrigger.mu.Lock()
					s.pageGCTrigger.running = false
					s.pageGCTrigger.mu.Unlock()
				}()
				if s.closing.Load() {
					return
				}
				s.runPageGC()
			}()
		} else {
			s.pageGCTrigger.dirty.Store(true)
			s.pageGCTrigger.mu.Unlock()
		}
	}

	// Blob GC trigger
	blobThreshold := 5 // sealed blob segments
	blobSealed := s.blobSegMgr.SealedSegments()
	if len(blobSealed) >= blobThreshold {
		s.blobGCTrigger.mu.Lock()
		if !s.blobGCTrigger.running {
			s.blobGCTrigger.running = true
			s.blobGCTrigger.mu.Unlock()
			s.blobGCWg.Add(1)
			go func() {
				defer s.blobGCWg.Done()
				defer func() {
					s.blobGCTrigger.mu.Lock()
					s.blobGCTrigger.running = false
					s.blobGCTrigger.mu.Unlock()
				}()
				if s.closing.Load() {
					return
				}
				s.runBlobGC()
			}()
		} else {
			s.blobGCTrigger.dirty.Store(true)
			s.blobGCTrigger.mu.Unlock()
		}
	}
}

// runPageGC collects page segments until no more have dead bytes.
func (s *store) runPageGC() {
	for {
		if !s.pageGCTrigger.dirty.Load() {
			return
		}
		s.pageGCTrigger.dirty.Store(false)

		sealed := s.pageSegMgr.SealedSegments()
		if len(sealed) == 0 {
			return
		}

		// Collect one segment at a time (CollectOne picks internally).
		stats, err := s.pageGC.CollectOne()
		if err != nil {
			return
		}
		if stats == nil {
			return
		}

		// After CollectOne: old sealed segment's stats are now 0 (all records moved or dead).
		// Active segment gained LiveRecords. Update stats accordingly.
		activeSegID := s.pageSegMgr.ActiveSegmentID()
		s.gcStats.Decrement(stats.SegmentID, int64(stats.TotalRecords), 0)
		s.gcStats.Increment(activeSegID, int64(stats.LiveRecords), 0)
	}
}

// runBlobGC collects blob segments until no more have dead bytes.
func (s *store) runBlobGC() {
	for {
		if !s.blobGCTrigger.dirty.Load() {
			return
		}
		s.blobGCTrigger.dirty.Store(false)

		sealed := s.blobSegMgr.SealedSegments()
		if len(sealed) == 0 {
			return
		}

		// Collect one segment at a time.
		stats, err := s.blobGC.CollectOne()
		if err != nil {
			return
		}
		if stats == nil {
			return
		}

		// After CollectOne: old sealed segment's stats are now 0 (all records moved or dead).
		// Active segment gained LiveRecords.
		activeSegID := s.blobSegMgr.ActiveSegmentID()
		s.gcStats.Decrement(stats.SegmentID, int64(stats.TotalRecords), 0)
		s.gcStats.Increment(activeSegID, int64(stats.LiveRecords), 0)
	}
}

// ─── assembleBatchFromCollectors ────────────────────────────────────

// assembleBatchFromCollectors builds a WAL batch from per-operation collectors.
// This replaces the old assembleBatch that drained shared buffers.
//
// lsmRecovery is the PageStore's LSM lifecycle interface. It provides access to
// the goroutine-local WAL collector (populated by SetPageMapping/SetBlobMapping
// calls) via DrainCollector(), and we add those entries as ModuleLSM records.
func assembleBatchFromCollectors(
	provider *btree.RealPageProvider,
	blobAdapter *blobWriterAdapter,
	lsmRecovery pagestoreapi.LSMLifecycle,
	rootPageID uint64,
	commitEntry txnapi.WALEntry,
) *walapi.Batch {
	batch := walapi.NewBatch()

	// Drain collectors AFTER tree.Put completes (inside this function, not before).
	// The btree collector was populated during tree.Put → WritePage.
	pageEntries := provider.CollectAndClear()
	blobEntries := blobAdapter.CollectAndClear()

	// Page WAL entries → ModuleLSM (LSM handles page→vaddr mapping persistence)
	for _, e := range pageEntries {
		batch.Add(walapi.ModuleLSM, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Blob WAL entries → ModuleLSM (LSM handles blob→vaddr mapping persistence)
	for _, e := range blobEntries {
		batch.Add(walapi.ModuleLSM, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// LSM collector: entries collected by SetPageMapping/SetBlobMapping during this operation
	// Drain and add as ModuleLSM so recovery can replay them.
	if lsmRecovery != nil {
		for _, rec := range lsmRecovery.DrainCollector() {
			batch.Records = append(batch.Records, rec)
		}
	}

	// Root pointer change → ModuleTree (btree root)
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, rootPageID, 0, 0)

	// Transaction commit/abort → ModuleTree
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

// CollectAndClear retrieves all blob WAL entries from the current goroutine's collector,
// clears the collector, and returns the entries. Used by SQL transaction commit.
func (a *blobWriterAdapter) CollectAndClear() []blobstoreapi.WALEntry {
	gid := goroutineID()
	if c, ok := a.collectors.LoadAndDelete(gid); ok {
		collector := c.(*blobCollector)
		entries := make([]blobstoreapi.WALEntry, len(collector.Entries))
		copy(entries, collector.Entries)
		return entries
	}
	return nil
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
	cleanup func() // optional cleanup func called on Close()
}

func (it *snapshotIterator) Next() bool    { return it.inner.Next() }
func (it *snapshotIterator) Key() []byte   { return it.inner.Key() }
func (it *snapshotIterator) Value() []byte { return it.inner.Value() }
func (it *snapshotIterator) Err() error    { return it.inner.Err() }
func (it *snapshotIterator) Close() {
	it.inner.Close()
	// Clean up the read snapshot so the VisibilityChecker no longer references it.
	if it.cleanup != nil {
		it.cleanup()
	} else {
		it.store.readSnaps.Delete(it.readXID)
	}
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
	// LSM's registerLSMWALCollector is idempotent — SetPageMapping inside
	// tree.Put uses the same collector, not a new one.
	_, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	_, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// Start regular auto-commit transaction
	xid, _ := s.txnMgr.BeginTxn()

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
			s.txnMgr.Abort(xid)
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
	batch := assembleBatchFromCollectors(s.provider, s.blobAdapter, s.pageStore.(pagestoreapi.PageStoreRecovery).LSMLifecycle(), rootPageID, commitWALEntry)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		s.txnMgr.Abort(xid)
		return err
	}

	// WAL succeeded — commit the transaction to make entry visible.
	s.txnMgr.Commit(xid)

	// Trigger auto-vacuum check for each operation in the batch.
	if len(wb.ops) > 0 {
		s.vacuumTrigger.opsCount.Add(int64(len(wb.ops)))
		s.vacuumTrigger.dirty.Store(true)
		s.checkAutoVacuum()
		s.checkAutoGC()
	}

	return nil
}

// Discard releases resources without committing.
// Safe to call multiple times.
func (wb *writeBatch) Discard() {
	wb.ops = nil
	wb.finished = true
}

// PutWithXID writes a key-value pair with a specific transaction ID.
// Unlike Put which allocates a fresh XID and auto-commits, this writes
// directly to the btree with the given txnID without allocating a new XID.
// The write is NOT committed in CLOG — caller manages transaction lifecycle.
// Used by SQL executor for deferred-write transactions.
func (wb *writeBatch) PutWithXID(key, value []byte, txnID uint64) error {
	if wb.finished {
		return kvstoreapi.ErrBatchCommitted
	}
	if len(key) > btreeapi.MaxKeySize {
		return kvstoreapi.ErrKeyTooLarge
	}

	// Write directly to btree without new XID allocation or WAL entry.
	// Caller handles WAL durability and CLOG commit/abort.
	return wb.store.tree.Put(key, value, txnID)
}

// DeleteWithXID marks a key as deleted with a specific transaction ID.
// Unlike Delete which allocates a fresh XID and auto-commits, this marks
// txnMax=txnID directly in the btree without allocating a new XID.
// The delete is NOT committed in CLOG — caller manages transaction lifecycle.
// For SQL rollback: a self-delete (txnMax==txnXID) makes entry invisible.
func (wb *writeBatch) DeleteWithXID(key []byte, txnID uint64) error {
	if wb.finished {
		return kvstoreapi.ErrBatchCommitted
	}

	err := wb.store.tree.Delete(key, txnID)
	if err == btreeapi.ErrKeyNotFound {
		return kvstoreapi.ErrKeyNotFound
	}
	return err
}
