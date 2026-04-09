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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

	"github.com/akzj/go-fast-kv/internal/blobstore"
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	"github.com/akzj/go-fast-kv/internal/btree"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/pagestore"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	txnmod "github.com/akzj/go-fast-kv/internal/txn"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	"github.com/akzj/go-fast-kv/internal/wal"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// readTxnID is used for auto-commit reads (Get/Scan).
// MaxXID = MaxUint64-1, so TxnMaxInfinity (MaxUint64) > readTxnID → visible.
const readTxnID = txnapi.MaxXID

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

	// Transaction
	txnMgr txnapi.TxnManager

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

	// Create TxnManager
	tm := txnmod.New()

	// Create page provider and blob adapter
	provider := btree.NewRealPageProvider(ps)
	ba := &blobWriterAdapter{store: bs}

	// Create BTree
	tree := btree.New(btreeapi.Config{InlineThreshold: cfg.InlineThreshold}, provider, ba)

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

	// Begin transaction
	xid, _ := s.txnMgr.BeginTxn()

	// Register per-operation WAL collectors (goroutine-keyed).
	// WritePage/WriteBlob route entries here via goroutine ID.
	pageCollector, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	blobCollector, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// B-tree Put (concurrent-safe via per-page locks).
	// WritePage → pageCollector, WriteBlob → blobCollector.
	if err := s.tree.Put(key, value, xid); err != nil {
		s.txnMgr.Abort(xid)
		return err
	}

	// Commit transaction (returns WAL entry)
	commitEntry := s.txnMgr.Commit(xid)

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Assemble WAL batch from per-operation collectors
	batch := assembleBatchFromCollectors(pageCollector, blobCollector, rootPageID, commitEntry)

	// WAL fsync provides durability. Segment data is fsynced at checkpoint time.
	// On crash recovery, WAL replay reconstructs any un-fsynced segment pages.
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
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

	// Auto-commit read: use readTxnID to see all committed versions.
	// B-tree is concurrent-safe (per-page RwLocks).
	// RLock held for entire read to prevent Close() from shutting down
	// segment files while a read is in flight.
	val, err := s.tree.Get(key, readTxnID)
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

	// Begin transaction
	xid, _ := s.txnMgr.BeginTxn()

	// Register per-operation WAL collectors (goroutine-keyed).
	pageCollector, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	blobCollector, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

	// B-tree Delete (concurrent-safe via per-page locks).
	if err := s.tree.Delete(key, xid); err != nil {
		s.txnMgr.Abort(xid)
		if err == btreeapi.ErrKeyNotFound {
			return kvstoreapi.ErrKeyNotFound
		}
		return err
	}

	// Commit
	commitEntry := s.txnMgr.Commit(xid)
	rootPageID := s.tree.RootPageID()

	batch := assembleBatchFromCollectors(pageCollector, blobCollector, rootPageID, commitEntry)

	// WAL fsync provides durability. Segment data is fsynced at checkpoint time.
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	return nil
}

// ─── Scan ───────────────────────────────────────────────────────────

func (s *store) Scan(start, end []byte) kvstoreapi.Iterator {
	s.mu.RLock()

	if s.closed {
		s.mu.RUnlock()
		return &errIterator{err: kvstoreapi.ErrClosed}
	}

	// B-tree Scan is concurrent-safe (per-page RwLocks).
	// RLock held during iterator creation to prevent Close() race.
	// The B-tree iterator clones leaf data under its own per-page locks,
	// so we can release RLock after creating the iterator.
	btreeIter := s.tree.Scan(start, end, readTxnID)
	s.mu.RUnlock()
	return &iteratorAdapter{inner: btreeIter}
}

// ─── Close ──────────────────────────────────────────────────────────

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}
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
		batch.Add(walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Blob WAL entries
	for _, e := range blobCollector.Entries {
		batch.Add(walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Root pointer change
	batch.Add(walapi.RecordSetRoot, rootPageID, 0, 0)

	// Transaction commit/abort
	batch.Add(walapi.RecordType(commitEntry.Type), commitEntry.ID, 0, 0)

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

	// One transaction for the entire batch
	xid, _ := s.txnMgr.BeginTxn()

	// Register per-operation WAL collectors (goroutine-keyed).
	pageCollector, unregPage := s.provider.RegisterCollector()
	defer unregPage()
	blobCollector, unregBlob := s.blobAdapter.registerCollector()
	defer unregBlob()

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

	// Commit transaction
	commitEntry := s.txnMgr.Commit(xid)
	rootPageID := s.tree.RootPageID()

	// ONE WAL batch for ALL operations → ONE fsync
	batch := assembleBatchFromCollectors(pageCollector, blobCollector, rootPageID, commitEntry)
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	return nil
}

// Discard releases resources without committing.
// Safe to call multiple times.
func (wb *writeBatch) Discard() {
	wb.ops = nil
	wb.finished = true
}
