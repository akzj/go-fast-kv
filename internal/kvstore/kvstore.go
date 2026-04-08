// Package kvstore implements the KVStore, the top-level integration layer.
//
// It wires together: SegmentManager×2, WAL, PageStore, BlobStore, BTree, TxnManager.
// Every Put/Get/Delete/Scan operates in auto-commit mode.
//
// Design reference: docs/DESIGN.md §1, §3.6, §3.9.10
package kvstore

import (
	"os"
	"path/filepath"
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

// store implements kvstoreapi.Store.
type store struct {
	mu sync.Mutex

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
	w, err := wal.New(walapi.Config{Dir: walDir})
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}
	if len(key) > btreeapi.MaxKeySize {
		return kvstoreapi.ErrKeyTooLarge
	}

	// Begin transaction
	xid, _ := s.txnMgr.BeginTxn()

	// Clear WAL entry collectors
	s.provider.DrainWALEntries()
	s.blobAdapter.drain()

	// B-tree Put (internally calls WritePage → collects page WAL entries;
	// large values call WriteBlob → collects blob WAL entries)
	if err := s.tree.Put(key, value, xid); err != nil {
		s.txnMgr.Abort(xid)
		return err
	}

	// Commit transaction (returns WAL entry)
	commitEntry := s.txnMgr.Commit(xid)

	// Collect root change
	rootPageID := s.tree.RootPageID()

	// Assemble WAL batch
	batch := s.assembleBatch(rootPageID, commitEntry)

	// Fsync ordering: segments → WAL
	if err := s.pageSegMgr.Sync(); err != nil {
		return err
	}
	if len(s.blobAdapter.entries) > 0 {
		if err := s.blobSegMgr.Sync(); err != nil {
			return err
		}
	}
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	return nil
}

// ─── Get ────────────────────────────────────────────────────────────

func (s *store) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, kvstoreapi.ErrClosed
	}

	// Auto-commit read: use readTxnID to see all committed versions
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstoreapi.ErrClosed
	}

	// Begin transaction
	xid, _ := s.txnMgr.BeginTxn()

	// Clear WAL entry collectors
	s.provider.DrainWALEntries()
	s.blobAdapter.drain()

	// B-tree Delete
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

	batch := s.assembleBatch(rootPageID, commitEntry)

	if err := s.pageSegMgr.Sync(); err != nil {
		return err
	}
	if _, err := s.wal.WriteBatch(batch); err != nil {
		return err
	}

	return nil
}

// ─── Scan ───────────────────────────────────────────────────────────

func (s *store) Scan(start, end []byte) kvstoreapi.Iterator {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return &errIterator{err: kvstoreapi.ErrClosed}
	}

	btreeIter := s.tree.Scan(start, end, readTxnID)
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

// ─── assembleBatch ──────────────────────────────────────────────────

// assembleBatch collects all WAL entries from the current operation
// into a single atomic WAL batch.
func (s *store) assembleBatch(rootPageID uint64, commitEntry txnapi.WALEntry) *walapi.Batch {
	batch := walapi.NewBatch()

	// Page WAL entries
	for _, e := range s.provider.DrainWALEntries() {
		batch.Add(walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Blob WAL entries
	for _, e := range s.blobAdapter.drain() {
		batch.Add(walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}

	// Root pointer change
	batch.Add(walapi.RecordSetRoot, rootPageID, 0, 0)

	// Transaction commit/abort
	batch.Add(walapi.RecordType(commitEntry.Type), commitEntry.ID, 0, 0)

	return batch
}

// ─── blobWriterAdapter ──────────────────────────────────────────────

// blobWriterAdapter adapts BlobStore to btreeapi.BlobWriter,
// collecting WAL entries for each write operation.
type blobWriterAdapter struct {
	store   blobstoreapi.BlobStore
	entries []blobstoreapi.WALEntry
}

func (a *blobWriterAdapter) WriteBlob(data []byte) (uint64, error) {
	blobID, entry, err := a.store.Write(data)
	if err != nil {
		return 0, err
	}
	a.entries = append(a.entries, entry)
	return blobID, nil
}

func (a *blobWriterAdapter) ReadBlob(blobID uint64) ([]byte, error) {
	return a.store.Read(blobID)
}

func (a *blobWriterAdapter) drain() []blobstoreapi.WALEntry {
	out := a.entries
	a.entries = nil
	return out
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
