// Package internal contains private implementation details for kvstore.
package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
	"github.com/akzj/go-fast-kv/internal/blinktree"
	"github.com/akzj/go-fast-kv/internal/external-value"
	"github.com/akzj/go-fast-kv/internal/wal"
	"github.com/akzj/go-fast-kv/internal/storage"
)

// =============================================================================
// Constants
// =============================================================================

const (
	DefaultMaxKeySize        = 1024
	DefaultMaxValueSize      = 64 * 1024 * 1024
	DefaultNodeSize          = 64 * 1024
	DefaultSyncWrites        = true
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrKeyNotFound        = errors.New("kvstore: key not found")
	ErrStoreClosed        = errors.New("kvstore: store is closed")
	ErrTransactionAborted = errors.New("kvstore: transaction aborted")
	ErrStoreFull          = errors.New("kvstore: store is full")
	ErrKeyTooLarge        = errors.New("kvstore: key too large")
	ErrValueTooLarge      = errors.New("kvstore: value too large")
	ErrWriteLocked        = errors.New("kvstore: write operation in progress")
	ErrReadOnly           = errors.New("kvstore: store is read-only")
	ErrTransactionFull    = errors.New("kvstore: too many transactions")
	ErrBatchCommitted     = errors.New("kvstore: batch already committed")
)

// =============================================================================
// Types
// =============================================================================

type IsolationLevel int

type Config struct {
	Directory         string
	MaxKeySize        uint32
	MaxValueSize      uint64
	ReadOnly          bool
	SyncWrites        bool
	BLinkTreeNodeSize uint32
}

func DefaultConfig() Config {
	return Config{
		MaxKeySize:        DefaultMaxKeySize,
		MaxValueSize:      DefaultMaxValueSize,
		SyncWrites:        DefaultSyncWrites,
		BLinkTreeNodeSize: DefaultNodeSize,
	}
}

type TransactionOptions struct {
	Timeout        int
	ReadOnly       bool
	IsolationLevel int
}

// =============================================================================
// Interfaces
// =============================================================================

type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(start, end []byte) (Iterator, error)
	Close() error
}

type KVStoreWithTransactions interface {
	KVStore
	Begin() (Transaction, error)
	BeginWithOptions(opts TransactionOptions) (Transaction, error)
}

type Transaction interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(start, end []byte) (Iterator, error)
	Commit() error
	Rollback()
	TxID() uint64
}

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
	Close()
}

type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Commit() error
	Reset()
}

type BatchCreator interface {
	NewBatch() Batch
}

// =============================================================================
// WAL Payload Helpers
// =============================================================================

// parseWALPayload extracts key and raw value from WAL payload.
// Format: [len(key)][key bytes][len(value)][value bytes]
func parseWALPayload(payload []byte) (key, value []byte, err error) {
	if len(payload) < 2 {
		return nil, nil, errors.New("invalid WAL payload")
	}
	keyLen := int(payload[0])
	if len(payload) < 1+keyLen+1 {
		return nil, nil, errors.New("invalid WAL payload: key truncated")
	}
	key = payload[1 : 1+keyLen]
	valueLen := int(payload[1+keyLen])
	if len(payload) < 1+keyLen+1+valueLen {
		return nil, nil, errors.New("invalid WAL payload: value truncated")
	}
	value = payload[1+keyLen+1 : 1+keyLen+1+valueLen]
	return key, value, nil
}

// parseWALPayloadForReplay extracts key and InlineValue from WAL payload.
// Format: [len(key)][key bytes][len(inlineValue)=64][inlineValue bytes]
func parseWALPayloadForReplay(payload []byte) (key []byte, iv blinktree.InlineValue, err error) {
	if len(payload) < 2 {
		return nil, iv, errors.New("invalid WAL payload")
	}
	keyLen := int(payload[0])
	if len(payload) < 1+keyLen+1 {
		return nil, iv, errors.New("invalid WAL payload: key truncated")
	}
	key = make([]byte, keyLen)
	copy(key, payload[1:1+keyLen])
	
	inlineLen := int(payload[1+keyLen])
	expectedLen := 1 + keyLen + 1 + 64
	if inlineLen != 64 || len(payload) < expectedLen {
		return nil, iv, errors.New("invalid WAL payload: invalid InlineValue length")
	}
	
	// Extract InlineValue bytes
	offset := 1 + keyLen + 1
	copy(iv.Length[:], payload[offset:offset+8])
	copy(iv.Data[:], payload[offset+8:offset+64])
	
	return key, iv, nil
}

func inlineValueFromBytes(store *kvStore, value []byte) (blinktree.InlineValue, error) {
	var iv blinktree.InlineValue
	if len(value) > blinktree.ExternalThreshold {
		extVAddr, err := store.extStore.Store(value)
		if err != nil {
			return iv, fmt.Errorf("store external value: %w", err)
		}
		// Store actual length in Data[48:56] to avoid MSB flag corruption
		binary.BigEndian.PutUint64(iv.Length[:], uint64(len(value)))
		iv.Length[0] |= 0x80 // Set external flag
		binary.BigEndian.PutUint64(iv.Data[0:8], extVAddr.SegmentID)
		binary.BigEndian.PutUint64(iv.Data[8:16], extVAddr.Offset)
		// Store actual external value length for retrieval
		binary.BigEndian.PutUint64(iv.Data[48:56], uint64(len(value)))
	} else {
		binary.BigEndian.PutUint64(iv.Length[:], uint64(len(value)))
		copy(iv.Data[:], value)
	}
	return iv, nil
}

func inlineValueToBytes(store *kvStore, iv blinktree.InlineValue) ([]byte, error) {
	if !iv.IsValid() {
		return nil, nil
	}
	if iv.IsExternal() {
		segID := binary.BigEndian.Uint64(iv.Data[0:8])
		offset := binary.BigEndian.Uint64(iv.Data[8:16])
		extVAddr := vaddr.VAddr{
			SegmentID: segID,
			Offset:    offset,
		}
		if !extVAddr.IsValid() {
			return nil, errors.New("invalid external address")
		}
		return store.extStore.Retrieve(extVAddr)
	}
	length := binary.BigEndian.Uint64(iv.Length[:])
	return iv.Data[:length], nil
}

// =============================================================================
// KVStore Implementation
// =============================================================================

type kvStore struct {
	mu           sync.Mutex
	tree         blinktree.TreeMutator
	segMgr       storage.SegmentManager
	extStore     externalvalue.ExternalValueStore
	wal          wal.WAL
	config       Config
	closed       bool
	closedOnce   sync.Once
	txCounter    uint64
	readOnly     bool
	metadataFile string
}

func NewKVStore(config Config) (*kvStore, error) {
	if config.MaxKeySize == 0 {
		config.MaxKeySize = DefaultMaxKeySize
	}
	if config.MaxValueSize == 0 {
		config.MaxValueSize = DefaultMaxValueSize
	}
	if config.BLinkTreeNodeSize == 0 {
		config.BLinkTreeNodeSize = DefaultNodeSize
	}

	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	walDir := filepath.Join(config.Directory, "wal")
	walConfig := wal.WALConfig{
		Directory:   walDir,
		SyncWrites:  config.SyncWrites,
		SegmentSize: 64 * 1024 * 1024,
	}
	walInstance, err := wal.OpenWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	segDir := filepath.Join(config.Directory, "segments")
	segConfig := storage.StorageConfig{
		Directory:   segDir,
		SegmentSize: 1 << 30,
	}
	segMgr, err := storage.OpenSegmentManager(segConfig)
	if err != nil {
		walInstance.Close()
		return nil, fmt.Errorf("open segment manager: %w", err)
	}

	extStore, err := externalvalue.NewExternalValueStore(segMgr, externalvalue.Config{
		MaxValueSize: config.MaxValueSize,
		SegmentSize:  1 << 30,
	})
	if err != nil {
		segMgr.Close()
		walInstance.Close()
		return nil, fmt.Errorf("create external value store: %w", err)
	}

	// Initialize B-link tree with persistent node manager
	nodeOps := blinktree.NewNodeOperations()
	nodeMgr := blinktree.NewNodeManager(segMgr, nodeOps)
	tree := blinktree.NewTreeMutator(nodeOps, nodeMgr)

	// Ensure we have an active segment for writes BEFORE opening tree
	if segMgr.ActiveSegment() == nil {
		if _, err := segMgr.CreateSegment(); err != nil {
			segMgr.Close()
			walInstance.Close()
			return nil, fmt.Errorf("create segment: %w", err)
		}
	}
	

	// Load tree root from metadata file if exists
	metadataFile := filepath.Join(config.Directory, "metadata.json")
	if data, err := os.ReadFile(metadataFile); err == nil && len(data) >= 8 {
		// Metadata exists, try to restore root PageID
		rootPageID := blinktree.PageID(binary.LittleEndian.Uint64(data[0:8]))
		if rootPageID != 0 {
			tree.RestoreRootPageID(rootPageID)
		}
	}

	if err := tree.Open(""); err != nil {
		extStore.Close()
		segMgr.Close()
		walInstance.Close()
		return nil, fmt.Errorf("open tree: %w", err)
	}

	// If metadata file existed, tree was restored from segments.
	// Skip WAL replay because tree nodes are already in segments.
	// WAL replay would corrupt external values (extracts garbage from raw value bytes).
	if _, err := os.Stat(metadataFile); err == nil {
		// Tree restored from metadata - don't replay WAL
		store := &kvStore{
			tree:         tree,
			segMgr:       segMgr,
			extStore:     extStore,
			wal:          walInstance,
			config:       config,
			metadataFile: metadataFile,
			readOnly:     config.ReadOnly,
		}
		store.syncRoot()
		return store, nil
	}

	// No metadata - replay WAL for crash recovery
	// Keys are now stored directly as []byte in the B-tree (no hash conversion)
	iter, err := walInstance.ReadFrom(1)
	if err == nil {
		for iter.Next() {
			rec := iter.Record()
			if rec.RecordType == wal.WALNodeWrite {
				key, iv, parseErr := parseWALPayloadForReplay(rec.Payload)
				if parseErr == nil {
					if iv.IsValid() {
						tree.Put(key, iv)
					} else {
						tree.Delete(key)
					}
				}
			}
		}
		iter.Close()
	}

	store := &kvStore{
		tree:        tree,
		segMgr:      segMgr,
		extStore:    extStore,
		wal:         walInstance,
		config:      config,
		metadataFile: metadataFile,
		readOnly:    config.ReadOnly,
	}

	// Don't sync root here - segments may not be synced yet.
	// metadata.json will be written in Close() after segment sync.
	return store, nil
}

func (s *kvStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	if s.readOnly {
		return nil, ErrReadOnly
	}
	if len(key) > int(s.config.MaxKeySize) {
		return nil, ErrKeyTooLarge
	}
	// Pass key directly to B-tree — no hash conversion needed
	iv, err := s.tree.Get(key)
	if err != nil {
		if errors.Is(err, blinktree.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("tree get: %w", err)
	}
	return inlineValueToBytes(s, iv)
}

func (s *kvStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if s.readOnly {
		return ErrReadOnly
	}
	if len(key) > int(s.config.MaxKeySize) {
		return ErrKeyTooLarge
	}
	if uint64(len(value)) > s.config.MaxValueSize {
		return ErrValueTooLarge
	}
	iv, err := inlineValueFromBytes(s, value)
	if err != nil {
		return fmt.Errorf("convert value: %w", err)
	}
	if err := s.logWAL(wal.WALNodeWrite, key, iv); err != nil {
		return fmt.Errorf("log WAL: %w", err)
	}
	// Pass key directly to B-tree — no hash conversion needed
	if err := s.tree.Put(key, iv); err != nil {
		return fmt.Errorf("tree put: %w", err)
	}
	if s.config.SyncWrites {
		if err := s.wal.Flush(); err != nil {
			return fmt.Errorf("flush WAL: %w", err)
		}
	}
	return nil
}

func (s *kvStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if s.readOnly {
		return ErrReadOnly
	}
	iv, err := s.tree.Get(key)
	if err != nil {
		if errors.Is(err, blinktree.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		return fmt.Errorf("tree get: %w", err)
	}
	if iv.IsExternal() {
		extVAddr := vaddr.VAddr{
			SegmentID: binary.BigEndian.Uint64(iv.Data[0:8]),
			Offset:    binary.BigEndian.Uint64(iv.Data[8:16]),
		}
		if extVAddr.IsValid() {
			if err := s.extStore.Delete(extVAddr); err != nil {
				return fmt.Errorf("delete external value: %w", err)
			}
		}
	}
	// Pass zero-value InlineValue to indicate deletion in WAL
	var emptyIV blinktree.InlineValue
	if err := s.logWAL(wal.WALNodeWrite, key, emptyIV); err != nil {
		return fmt.Errorf("log WAL: %w", err)
	}
	if err := s.tree.Delete(key); err != nil {
		return fmt.Errorf("tree delete: %w", err)
	}
	if s.config.SyncWrites {
		if err := s.wal.Flush(); err != nil {
			return fmt.Errorf("flush WAL: %w", err)
		}
	}
	return nil
}

func (s *kvStore) Scan(start, end []byte) (Iterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	// Pass start/end directly to B-tree — no hash conversion needed
	// nil/empty start means scan from beginning; nil/empty end means scan to end
	treeIter, err := s.tree.Scan(start, end)
	if err != nil {
		return nil, fmt.Errorf("tree scan: %w", err)
	}
	return &kvIterator{store: s, treeIter: treeIter}, nil
}

func (s *kvStore) Close() error {
	var err error
	s.closedOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		// Sync root to metadata before closing
		s.syncRoot()
		if err = s.tree.Close(); err != nil {
			err = fmt.Errorf("close tree: %w", err)
			return
		}
		// Sync segments
		if s.segMgr != nil {
			for _, seg := range s.segMgr.ListSegments() {
				if err = seg.Sync(); err != nil {
					err = fmt.Errorf("sync segment: %w", err)
					return
				}
			}
		}
		if err = s.extStore.Close(); err != nil {
			err = fmt.Errorf("close external store: %w", err)
			return
		}
		if err = s.wal.Close(); err != nil {
			err = fmt.Errorf("close WAL: %w", err)
			return
		}
	})
	return err
}

// syncRoot persists tree root PageID to metadata file
func (s *kvStore) syncRoot() {
	if s.metadataFile == "" {
		return
	}
	rootPageID := s.tree.GetRootPageID()
	if rootPageID == 0 {
		return
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data[0:8], uint64(rootPageID))
	_ = os.WriteFile(s.metadataFile, data, 0644)
}

func (s *kvStore) logWAL(opType wal.WALRecordType, key []byte, iv blinktree.InlineValue) error {
	payload := make([]byte, 0, len(key)+64+8)
	payload = append(payload, byte(len(key)))
	payload = append(payload, key...)
	// Write InlineValue bytes directly (64 bytes)
	payload = append(payload, byte(64)) // InlineValue length
	payload = append(payload, iv.Length[:]...)
	payload = append(payload, iv.Data[:]...)
	_, err := s.wal.Append(&wal.WALRecord{RecordType: opType, Payload: payload})
	return err
}

func (s *kvStore) Begin() (Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	s.txCounter++
	return &kvTransaction{
		store:   s,
		txID:    s.txCounter,
		pending: make(map[string][]byte),
		writes:  make(map[string][]byte),
	}, nil
}

func (s *kvStore) BeginWithOptions(opts TransactionOptions) (Transaction, error) {
	return s.Begin()
}

func (s *kvStore) NewBatch() Batch {
	return &kvBatch{
		store: s,
		puts:  make([]kvBatchOp, 0),
		dels:  make([]kvBatchOp, 0),
	}
}

// =============================================================================
// Iterator
// =============================================================================

type kvIterator struct {
	store    *kvStore
	treeIter blinktree.TreeIterator
	current  []byte
	value    []byte
	err      error
	closed   bool
	mu       sync.Mutex
}

func (it *kvIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed {
		return false
	}
	if !it.treeIter.Next() {
		it.err = it.treeIter.Error()
		return false
	}
	// TreeIterator.Key() now returns []byte directly — no conversion needed
	it.current = it.treeIter.Key()
	val, err := inlineValueToBytes(it.store, it.treeIter.Value())
	if err != nil {
		it.err = err
		return false
	}
	it.value = val
	return true
}

func (it *kvIterator) Key() []byte        { return it.current }
func (it *kvIterator) Value() []byte     { return it.value }
func (it *kvIterator) Error() error      { return it.err }
func (it *kvIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()
	if !it.closed {
		it.closed = true
		it.treeIter.Close()
	}
}

// txScanIterator merges store iterator with uncommitted transaction writes
type txScanIterator struct {
	storeIter Iterator
	writes    map[string][]byte  // committed but uncommitted writes
	deleted   map[string]bool     // keys deleted in tx
	pending   []string            // sorted pending keys
	index     int
	current   []byte
	value     []byte
	err       error
	closed    bool
	mu        sync.Mutex
}

func (it *txScanIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed {
		return false
	}
	// First yield pending uncommitted writes in sorted order
	if it.index < len(it.pending) {
		k := it.pending[it.index]
		it.index++
		it.current = []byte(k)
		it.value = it.writes[k]
		return true
	}
	// Then advance store iterator, skipping deleted keys
	for it.storeIter.Next() {
		k := string(it.storeIter.Key())
		if it.deleted[k] {
			continue
		}
		// Skip if overwritten by uncommitted write
		if _, ok := it.writes[k]; ok {
			continue
		}
		it.current = it.storeIter.Key()
		it.value = it.storeIter.Value()
		return true
	}
	return false
}

func (it *txScanIterator) Key() []byte      { return it.current }
func (it *txScanIterator) Value() []byte   { return it.value }
func (it *txScanIterator) Error() error     { return it.err }

func (it *txScanIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()
	if !it.closed {
		it.closed = true
		it.storeIter.Close()
	}
}

// =============================================================================
// Transaction
// =============================================================================

type kvTransaction struct {
	store     *kvStore
	txID      uint64
	pending   map[string][]byte
	writes    map[string][]byte
	aborted   bool
	committed bool
	closed    bool
	mu        sync.Mutex
}

func (tx *kvTransaction) Get(key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return nil, ErrStoreClosed
	}
	if tx.aborted {
		return nil, ErrTransactionAborted
	}
	if val, ok := tx.writes[string(key)]; ok {
		if val == nil {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}
	if _, ok := tx.pending[string(key)]; ok {
		return nil, ErrKeyNotFound
	}
	return tx.store.Get(key)
}

func (tx *kvTransaction) Put(key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return ErrStoreClosed
	}
	if tx.aborted {
		return ErrTransactionAborted
	}
	if tx.committed {
		return ErrTransactionAborted
	}
	if _, ok := tx.pending[string(key)]; !ok {
		val, err := tx.store.Get(key)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if errors.Is(err, ErrKeyNotFound) {
			tx.pending[string(key)] = nil
		} else {
			tx.pending[string(key)] = val
		}
	}
	tx.writes[string(key)] = value
	return nil
}

func (tx *kvTransaction) Delete(key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return ErrStoreClosed
	}
	if tx.aborted {
		return ErrTransactionAborted
	}
	if tx.committed {
		return ErrTransactionAborted
	}
	if _, ok := tx.pending[string(key)]; !ok {
		val, err := tx.store.Get(key)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if errors.Is(err, ErrKeyNotFound) {
			tx.pending[string(key)] = nil
		} else {
			tx.pending[string(key)] = val
		}
	}
	tx.writes[string(key)] = nil
	return nil
}

func (tx *kvTransaction) Scan(start, end []byte) (Iterator, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return nil, ErrStoreClosed
	}
	if tx.aborted {
		return nil, ErrTransactionAborted
	}
	storeIter, err := tx.store.Scan(start, end)
	if err != nil {
		return nil, err
	}
	// Build pending keys list from writes
	pending := make([]string, 0, len(tx.writes))
	deleted := make(map[string]bool)
	for k, v := range tx.writes {
		if v == nil {
			deleted[k] = true
		} else {
			pending = append(pending, k)
		}
	}
	sort.Strings(pending)
	return &txScanIterator{
		storeIter: storeIter,
		writes:    tx.writes,
		deleted:   deleted,
		pending:   pending,
	}, nil
}

func (tx *kvTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed {
		return nil
	}
	if tx.closed {
		return ErrTransactionAborted
	}
	if tx.aborted {
		return ErrTransactionAborted
	}
	for key, value := range tx.writes {
		if value == nil {
			if err := tx.store.Delete([]byte(key)); err != nil {
				return fmt.Errorf("commit delete: %w", err)
			}
		} else {
			if err := tx.store.Put([]byte(key), value); err != nil {
				return fmt.Errorf("commit put: %w", err)
			}
		}
	}
	tx.committed = true
	tx.closed = true
	tx.pending = nil
	tx.writes = nil
	return nil
}

func (tx *kvTransaction) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return
	}
	tx.aborted = true
	tx.closed = true
	tx.pending = nil
	tx.writes = nil
}

func (tx *kvTransaction) TxID() uint64 {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.txID
}

// =============================================================================
// Batch
// =============================================================================

type kvBatch struct {
	store *kvStore
	puts  []kvBatchOp
	dels  []kvBatchOp
	err   error
}

type kvBatchOp struct {
	key   []byte
	value []byte
}

func (b *kvBatch) Put(key, value []byte) {
	if b.err != nil {
		return
	}
	b.puts = append(b.puts, kvBatchOp{key: key, value: value})
}

func (b *kvBatch) Delete(key []byte) {
	if b.err != nil {
		return
	}
	b.dels = append(b.dels, kvBatchOp{key: key})
}

func (b *kvBatch) Commit() error {
	if b.err != nil {
		return b.err
	}
	for _, op := range b.dels {
		if err := b.store.Delete(op.key); err != nil && !errors.Is(err, ErrKeyNotFound) {
			b.err = err
			return err
		}
	}
	for _, op := range b.puts {
		if err := b.store.Put(op.key, op.value); err != nil {
			b.err = err
			return err
		}
	}
	b.puts = nil
	b.dels = nil
	return nil
}

func (b *kvBatch) Reset() {
	b.puts = b.puts[:0]
	b.dels = b.dels[:0]
	b.err = nil
}

// =============================================================================
// Interface Assertions
// =============================================================================

var (
	_ KVStore                 = (*kvStore)(nil)
	_ Iterator                = (*kvIterator)(nil)
	_ Transaction             = (*kvTransaction)(nil)
	_ Batch                   = (*kvBatch)(nil)
	_ KVStoreWithTransactions = (*kvStore)(nil)
	_ BatchCreator            = (*kvStore)(nil)
)
