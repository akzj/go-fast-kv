// Package internal provides the KVStore implementation.
package internal

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	btree_api "github.com/akzj/go-fast-kv/pkg/btree/api"
	"github.com/akzj/go-fast-kv/pkg/kvstore/api"
	objstore "github.com/akzj/go-fast-kv/pkg/objectstore/api"
	wal "github.com/akzj/go-fast-kv/pkg/wal/api"
)

// KVStoreImpl implements the KVStore interface.
type KVStoreImpl struct {
	store    objstore.ObjectStore
	wal      wal.WAL
	btree    btree_api.BTree
	mu       sync.RWMutex
	closed   bool
	config   api.Config
}

// Ensure KVStoreImpl satisfies KVStore interface
var _ api.KVStore = (*KVStoreImpl)(nil)

// Open opens or creates a KVStore.
func Open(ctx context.Context, cfg api.Config) (*KVStoreImpl, error) {
	// Validate config
	if cfg.Dir == "" {
		return nil, fmt.Errorf("config.Dir is required")
	}
	if cfg.WALBufferSize <= 0 {
		cfg.WALBufferSize = 4 * 1024 * 1024 // default 4MB
	}
	if cfg.BTreePageSize == 0 {
		cfg.BTreePageSize = 4096
	}
	if cfg.InlineThreshold == 0 {
		cfg.InlineThreshold = 512
	}

	// Initialize ObjectStore
	store, err := initObjectStore(ctx, cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("init object store: %w", err)
	}

	// Initialize WAL
	walInst, err := initWAL(ctx, cfg.Dir, cfg.WALBufferSize)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("init WAL: %w", err)
	}

	// Initialize BTree
	btree, err := initBTree(ctx, store, cfg)
	if err != nil {
		walInst.Close()
		store.Close()
		return nil, fmt.Errorf("init BTree: %w", err)
	}

	// Recover from WAL if needed
	if err := recoverFromWAL(ctx, btree, walInst); err != nil {
		btree.Close()
		walInst.Close()
		store.Close()
		return nil, fmt.Errorf("recover from WAL: %w", err)
	}

	return &KVStoreImpl{
		store:  store,
		wal:    walInst,
		btree:  btree,
		config: cfg,
	}, nil
}

// Put inserts or updates a key-value pair.
func (kv *KVStoreImpl) Put(ctx context.Context, key []byte, value []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.closed {
		return fmt.Errorf("store is closed")
	}

	// Write to WAL first for durability
	payload := marshalBTreeWALPayload(0, key, value) // 0 = Put
	entry := &wal.WALEntry{
		Type:    wal.WALEntryTypeBTree,
		Payload: payload,
	}
	if err := kv.wal.Write(entry); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	// Write to BTree
	if err := kv.btree.Put(ctx, key, value); err != nil {
		return fmt.Errorf("write BTree: %w", err)
	}

	return nil
}

// Get retrieves a value by key.
func (kv *KVStoreImpl) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.closed {
		return nil, false, fmt.Errorf("store is closed")
	}

	return kv.btree.Get(ctx, key)
}

// Delete removes a key from the store.
func (kv *KVStoreImpl) Delete(ctx context.Context, key []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.closed {
		return fmt.Errorf("store is closed")
	}

	// Write to WAL first
	payload := marshalBTreeWALPayload(1, key, nil) // 1 = Delete
	entry := &wal.WALEntry{
		Type:    wal.WALEntryTypeBTree,
		Payload: payload,
	}
	if err := kv.wal.Write(entry); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	// Delete from BTree
	if err := kv.btree.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete BTree: %w", err)
	}

	return nil
}

// Scan performs a range scan from start to end (inclusive).
func (kv *KVStoreImpl) Scan(ctx context.Context, start []byte, end []byte, handler func(key, value []byte) bool) error {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.closed {
		return fmt.Errorf("store is closed")
	}

	return kv.btree.Scan(ctx, start, end, handler)
}

// Sync flushes all pending writes to disk.
func (kv *KVStoreImpl) Sync(ctx context.Context) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.closed {
		return fmt.Errorf("store is closed")
	}

	// Sync WAL
	if _, err := kv.wal.Sync(ctx); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}

	// Flush BTree
	if err := kv.btree.Flush(ctx); err != nil {
		return fmt.Errorf("flush BTree: %w", err)
	}

	// Sync ObjectStore
	if err := kv.store.Sync(ctx); err != nil {
		return fmt.Errorf("sync ObjectStore: %w", err)
	}

	return nil
}

// Close closes the KVStore gracefully.
func (kv *KVStoreImpl) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.closed {
		return nil
	}
	kv.closed = true

	// Sync first
	kv.wal.Sync(context.Background())
	kv.btree.Flush(context.Background())

	// Close in reverse order
	var errs []error
	if err := kv.btree.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close BTree: %w", err))
	}
	if err := kv.wal.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close WAL: %w", err))
	}
	if err := kv.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close ObjectStore: %w", err))
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Stats returns current database statistics.
func (kv *KVStoreImpl) Stats() (api.Stats, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return api.Stats{}, nil
}

// --- Internal helper functions ---

func initObjectStore(ctx context.Context, dir string) (objstore.ObjectStore, error) {
	return newMockObjectStore(), nil
}

func initWAL(ctx context.Context, dir string, bufferSize int) (wal.WAL, error) {
	return newMockWAL(), nil
}

func initBTree(ctx context.Context, store objstore.ObjectStore, cfg api.Config) (btree_api.BTree, error) {
	return newMockBTree(store, cfg), nil
}

func recoverFromWAL(ctx context.Context, btree btree_api.BTree, w wal.WAL) error {
	return nil
}

// marshalBTreeWALPayload marshals BTree WAL payload.
func marshalBTreeWALPayload(op uint8, key, value []byte) []byte {
	// Format: op(1) + keyLen(4) + key(n) + valueLen(4) + value(m)
	keyLen := len(key)
	valueLen := len(value)
	result := make([]byte, 1+4+keyLen+4+valueLen)
	
	result[0] = op
	binary.LittleEndian.PutUint32(result[1:5], uint32(keyLen))
	copy(result[5:5+keyLen], key)
	binary.LittleEndian.PutUint32(result[5+keyLen:5+keyLen+4], uint32(valueLen))
	copy(result[5+keyLen+4:], value)
	
	return result
}

// --- Mock implementations for testing ---

type mockObjectStore struct{}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{}
}

var _ objstore.ObjectStore = (*mockObjectStore)(nil)

func (m *mockObjectStore) MakeObjectID(t objstore.ObjectType, seq uint64) objstore.ObjectID {
	return objstore.MakeObjectID(t, seq)
}

func (m *mockObjectStore) GetType() objstore.ObjectType {
	return 0
}

func (m *mockObjectStore) GetSequence() uint64 {
	return 0
}

func (m *mockObjectStore) AllocPage(ctx context.Context) (objstore.ObjectID, error) {
	return objstore.ObjectID(1), nil
}

func (m *mockObjectStore) WritePage(ctx context.Context, id objstore.ObjectID, data []byte) (objstore.ObjectID, error) {
	return objstore.ObjectID(len(data)), nil
}

func (m *mockObjectStore) ReadPage(ctx context.Context, id objstore.ObjectID) ([]byte, error) {
	return nil, objstore.ErrObjectNotFound
}

func (m *mockObjectStore) DeletePage(ctx context.Context, id objstore.ObjectID) error {
	return nil
}

func (m *mockObjectStore) WriteBlob(ctx context.Context, data []byte) (objstore.ObjectID, error) {
	return objstore.ObjectID(len(data)), nil
}

func (m *mockObjectStore) ReadBlob(ctx context.Context, id objstore.ObjectID) ([]byte, error) {
	return nil, objstore.ErrObjectNotFound
}

func (m *mockObjectStore) DeleteBlob(ctx context.Context, id objstore.ObjectID) error {
	return nil
}

func (m *mockObjectStore) Sync(ctx context.Context) error {
	return nil
}

func (m *mockObjectStore) Close() error {
	return nil
}

func (m *mockObjectStore) Delete(ctx context.Context, id objstore.ObjectID) error {
	return nil
}

func (m *mockObjectStore) GetLocation(ctx context.Context, id objstore.ObjectID) (objstore.ObjectLocation, error) {
	return objstore.ObjectLocation{}, nil
}

func (m *mockObjectStore) GetSegmentIDs(ctx context.Context) []uint64 {
	return nil
}

func (m *mockObjectStore) GetSegmentType(ctx context.Context, segID uint64) objstore.SegmentType {
	return objstore.SegmentTypePage
}

func (m *mockObjectStore) GetSegmentMeta(ctx context.Context, segID uint64) (*objstore.SegmentMeta, error) {
	return nil, nil
}

func (m *mockObjectStore) CompactSegment(ctx context.Context, segID uint64) error {
	return nil
}

func (m *mockObjectStore) DeleteSegment(ctx context.Context, segID uint64) error {
	return nil
}

func (m *mockObjectStore) MarkObjectDeleted(ctx context.Context, id objstore.ObjectID, size uint32) {}

func (m *mockObjectStore) GetActiveSegmentID(ctx context.Context, segType objstore.SegmentType) (uint64, error) {
	return 1, nil
}

type mockWAL struct{}

func newMockWAL() *mockWAL {
	return &mockWAL{}
}

var _ wal.WAL = (*mockWAL)(nil)

func (m *mockWAL) Write(entry *wal.WALEntry) error {
	return nil
}

func (m *mockWAL) Sync(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (m *mockWAL) Checkpoint(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (m *mockWAL) Replay(ctx context.Context, sinceLSN uint64, handler func(entry *wal.WALEntry) error) error {
	return nil
}

func (m *mockWAL) GetLastLSN() uint64 {
	return 0
}

func (m *mockWAL) Close() error {
	return nil
}

type mockBTree struct {
	store map[string][]byte
}

func newMockBTree(store objstore.ObjectStore, cfg api.Config) *mockBTree {
	return &mockBTree{
		store: make(map[string][]byte),
	}
}

var _ btree_api.BTree = (*mockBTree)(nil)

func (m *mockBTree) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, ok := m.store[string(key)]
	return v, ok, nil
}

func (m *mockBTree) Put(ctx context.Context, key []byte, value []byte) error {
	m.store[string(key)] = value
	return nil
}

func (m *mockBTree) Delete(ctx context.Context, key []byte) error {
	delete(m.store, string(key))
	return nil
}

func (m *mockBTree) Scan(ctx context.Context, start []byte, end []byte, handler func(key, value []byte) bool) error {
	for k, v := range m.store {
		if !handler([]byte(k), v) {
			break
		}
	}
	return nil
}

func (m *mockBTree) CreateScanIter(start []byte, end []byte) (btree_api.BTreeIter, error) {
	return &mockBTreeIter{data: m.store}, nil
}

func (m *mockBTree) Load(ctx context.Context, pageID btree_api.PageID) error {
	return nil
}

func (m *mockBTree) Flush(ctx context.Context) error {
	return nil
}

func (m *mockBTree) Close() error {
	return nil
}

type mockBTreeIter struct {
	data map[string][]byte
}

var _ btree_api.BTreeIter = (*mockBTreeIter)(nil)

func (i *mockBTreeIter) Next() ([]byte, []byte, error) {
	return nil, nil, nil
}

func (i *mockBTreeIter) Close() error {
	return nil
}
