// Package internal contains private implementation details for WALImpl.
// This package is not importable by other modules.
package internal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// WALRecordType - Record Classification
// =============================================================================

// WALRecordType identifies the type of WAL record.
type WALRecordType uint8

const (
	WALPageAlloc WALRecordType = iota
	WALPageFree
	WALNodeWrite
	WALExternalValue
	WALRootUpdate
	WALCheckpoint
	WALIndexUpdate
	WALIndexRootUpdate
)

// =============================================================================
// WALRecord - Single Log Entry
// =============================================================================

// WALRecord is a single record in the write-ahead log.
type WALRecord struct {
	LSN        uint64
	RecordType WALRecordType
	Length     uint32
	Checksum   uint32
	Payload    []byte
}

// =============================================================================
// WAL - Write-Ahead Log Interface
// =============================================================================

// WAL provides write-ahead logging for crash recovery.
type WAL interface {
	Append(record *WALRecord) (uint64, error)
	ReadAt(lsn uint64) (*WALRecord, error)
	ReadFrom(startLSN uint64) (WALIterator, error)
	Truncate(truncateLSN uint64) error
	LastLSN() uint64
	Flush() error
	Close() error
}

// =============================================================================
// WALIterator - Sequential WAL Access
// =============================================================================

type WALIterator interface {
	Next() bool
	Record() *WALRecord
	Error() error
	Close()
}

// =============================================================================
// CheckpointManager - Snapshot Management
// =============================================================================

type CheckpointManager interface {
	CreateCheckpoint() (uint64, error)
	LatestCheckpoint() (*Checkpoint, error)
	Recover() error
	Checkpoint(lsn uint64) (*Checkpoint, error)
	ListCheckpoints() []*Checkpoint
	DeleteCheckpoint(lsn uint64) error
}

// =============================================================================
// Checkpoint - Consistent Snapshot
// =============================================================================

type Checkpoint struct {
	ID      uint64
	LSN     uint64
	TreeRoot vaddr.VAddr
	PageManager PageManagerSnapshot
	ExternalStore ExternalValueSnapshot
	Timestamp uint64
}

type PageManagerSnapshot struct {
	RootVAddr     vaddr.VAddr
	LivePageCount uint64
	CheckpointLSN uint64
}

type ExternalValueSnapshot struct {
	ActiveVAddrs  []vaddr.VAddr
	CheckpointLSN uint64
}

// =============================================================================
// Configuration
// =============================================================================

type WALConfig struct {
	Directory   string
	SegmentSize uint64
	SyncWrites  bool
	BufferSize  uint64
}

type CheckpointConfig struct {
	Interval             time.Duration
	WALSizeLimit         uint64
	DirtyPageLimit       int
	MinCheckpointInterval time.Duration
}

// =============================================================================
// Record Format
// =============================================================================

const (
	RecordHeaderSize = 17
)

type recordHeader struct {
	LSN    uint64
	Type   WALRecordType
	Length uint32
	CRC    uint32
}

func WalSegmentFileName(segmentID uint64) string {
	return fmt.Sprintf("WALImpl-%06d.WALImpl", segmentID)
}

func CheckpointFileName(id uint64) string {
	return fmt.Sprintf("checkpoint-%06d.json", id)
}

// =============================================================================
// WAL Implementation
// =============================================================================

// WALImpl implements the WAL interface.
type WALImpl struct {
	mu        sync.Mutex
	directory string
	segmentSize uint64
	syncWrites bool
	bufferSize uint64

	currentSegmentID   uint64
	currentSegmentFile *os.File
	currentOffset      int64

	nextLSN uint64

	closed     bool
	closedOnce sync.Once

	checkpointManager *checkpointManager
}

func NewWAL(config WALConfig) (*WALImpl, error) {
	if config.SegmentSize == 0 {
		config.SegmentSize = 64 * 1024 * 1024
	}
	if config.BufferSize == 0 {
		config.BufferSize = 1 * 1024 * 1024
	}

	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}

	w := &WALImpl{
		directory:       config.Directory,
		segmentSize:     config.SegmentSize,
		syncWrites:      config.SyncWrites,
		bufferSize:      config.BufferSize,
		nextLSN:         1,
		currentSegmentID: 0,
	}

	if err := w.recover(); err != nil {
		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	if err := w.openNewSegment(); err != nil {
		return nil, fmt.Errorf("open WAL segment: %w", err)
	}

	return w, nil
}

func (w *WALImpl) recover() error {
	entries, err := os.ReadDir(w.directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var maxSegmentID uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var segID uint64
		if _, err := fmt.Sscanf(entry.Name(), "WALImpl-%d.WALImpl", &segID); err == nil {
			if segID > maxSegmentID {
				maxSegmentID = segID
			}
		}
	}

	if maxSegmentID > 0 {
		segmentPath := filepath.Join(w.directory, WalSegmentFileName(maxSegmentID))
		info, err := os.Stat(segmentPath)
		if err == nil && info.Size() > RecordHeaderSize {
			f, err := os.Open(segmentPath)
			if err == nil {
				defer f.Close()

				var lastLSN uint64
				offset := info.Size() - RecordHeaderSize
				header := make([]byte, RecordHeaderSize)

				for offset >= 0 {
					if _, err := f.ReadAt(header, offset); err != nil {
						break
					}
					h := decodeHeader(header)
					if h.LSN > 0 {
						lastLSN = h.LSN
						break
					}
					if offset < RecordHeaderSize {
						break
					}
					offset -= RecordHeaderSize + int64(h.Length)
				}

				w.nextLSN = lastLSN + 1
				w.currentSegmentID = maxSegmentID
			}
		}
	}

	return nil
}

func (w *WALImpl) openNewSegment() error {
	if w.currentSegmentFile != nil {
		w.currentSegmentFile.Close()
	}

	segmentID := w.currentSegmentID + 1
	segmentPath := filepath.Join(w.directory, WalSegmentFileName(segmentID))

	f, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			f, err = os.OpenFile(segmentPath, os.O_RDWR, 0644)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	w.currentSegmentID = segmentID
	w.currentSegmentFile = f
	w.currentOffset = 0

	return nil
}

func (w *WALImpl) Append(record *WALRecord) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrWALClosed
	}

	lsn := atomic.SwapUint64(&w.nextLSN, w.nextLSN+1)
	if lsn == 0 {
		return 0, ErrInvalidLSN
	}

	record.LSN = lsn
	record.Checksum = crc32.ChecksumIEEE(record.Payload)
	record.Length = uint32(len(record.Payload))

	header := encodeHeader(record)
	recordSize := int64(RecordHeaderSize + len(record.Payload))

	if w.currentOffset+recordSize > int64(w.segmentSize) {
		if err := w.openNewSegment(); err != nil {
			atomic.StoreUint64(&w.nextLSN, lsn)
			return 0, fmt.Errorf("rotate segment: %w", err)
		}
	}

	if _, err := w.currentSegmentFile.WriteAt(header, w.currentOffset); err != nil {
		atomic.StoreUint64(&w.nextLSN, lsn)
		return 0, fmt.Errorf("write header: %w", err)
	}

	if len(record.Payload) > 0 {
		if _, err := w.currentSegmentFile.WriteAt(record.Payload, w.currentOffset+RecordHeaderSize); err != nil {
			atomic.StoreUint64(&w.nextLSN, lsn)
			return 0, fmt.Errorf("write payload: %w", err)
		}
	}

	w.currentOffset += recordSize

	if w.syncWrites {
		if err := w.currentSegmentFile.Sync(); err != nil {
			atomic.StoreUint64(&w.nextLSN, lsn)
			return 0, fmt.Errorf("sync: %w", err)
		}
	}

	return lsn, nil
}

func (w *WALImpl) ReadAt(lsn uint64) (*WALRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.directory)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var segID uint64
		if _, err := fmt.Sscanf(entry.Name(), "WALImpl-%d.WALImpl", &segID); err != nil {
			continue
		}

		segmentPath := filepath.Join(w.directory, entry.Name())
		f, err := os.Open(segmentPath)
		if err != nil {
			continue
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			continue
		}

		offset := int64(0)
		for offset+RecordHeaderSize <= info.Size() {
			header := make([]byte, RecordHeaderSize)
			if _, err := f.ReadAt(header, offset); err != nil {
				break
			}

			h := decodeHeader(header)
			if h.LSN == lsn {
				f.Close()
				payload := make([]byte, h.Length)
				if h.Length > 0 {
					if _, err := f.ReadAt(payload, offset+RecordHeaderSize); err != nil {
						return nil, fmt.Errorf("read payload: %w", err)
					}
				}
				checksum := crc32.ChecksumIEEE(payload)
				if checksum != h.CRC {
					return nil, ErrWALCorrupted
				}
				return &WALRecord{
					LSN:        h.LSN,
					RecordType: h.Type,
					Length:     h.Length,
					Checksum:   h.CRC,
					Payload:    payload,
				}, nil
			}

			offset += RecordHeaderSize + int64(h.Length)
		}
		f.Close()
	}

	return nil, nil
}

func (w *WALImpl) ReadFrom(startLSN uint64) (WALIterator, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.directory)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}

	var segmentPaths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var segID uint64
		if _, err := fmt.Sscanf(entry.Name(), "WALImpl-%d.WALImpl", &segID); err != nil {
			continue
		}
		segmentPaths = append(segmentPaths, filepath.Join(w.directory, entry.Name()))
	}

	return newWALIterator(segmentPaths, startLSN), nil
}

func (w *WALImpl) Truncate(truncateLSN uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if truncateLSN > w.LastLSN() {
		return ErrTruncateLSNTooLarge
	}

	return nil
}

func (w *WALImpl) LastLSN() uint64 {
	return atomic.LoadUint64(&w.nextLSN) - 1
}

func (w *WALImpl) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentSegmentFile == nil {
		return nil
	}

	return w.currentSegmentFile.Sync()
}

func (w *WALImpl) Close() error {
	var err error
	w.closedOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.closed = true

		if w.currentSegmentFile != nil {
			err = w.currentSegmentFile.Sync()
			if err != nil {
				return
			}
			err = w.currentSegmentFile.Close()
			w.currentSegmentFile = nil
		}
	})
	return err
}

// =============================================================================
// WALIterator Implementation
// =============================================================================

type walIterator struct {
	files       []string
	currentFile *os.File
	currentIdx  int
	offset      int64
	startLSN    uint64
	current     *WALRecord
	err         error
	closed      bool
	mu          sync.Mutex
}

func newWALIterator(segmentPaths []string, startLSN uint64) *walIterator {
	return &walIterator{
		files:      segmentPaths,
		startLSN:   startLSN,
		currentIdx: -1,
	}
}

func (it *walIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	for {
		if it.currentFile == nil {
			it.currentIdx++
			if it.currentIdx >= len(it.files) {
				return false
			}

			f, err := os.Open(it.files[it.currentIdx])
			if err != nil {
				it.err = err
				return false
			}
			it.currentFile = f
			it.offset = 0
		}

		info, err := it.currentFile.Stat()
		if err != nil {
			it.err = err
			it.closeFile()
			continue
		}

		if it.offset+RecordHeaderSize > info.Size() {
			it.closeFile()
			continue
		}

		header := make([]byte, RecordHeaderSize)
		n, err := it.currentFile.ReadAt(header, it.offset)
		if err != nil || n < RecordHeaderSize {
			it.closeFile()
			continue
		}

		h := decodeHeader(header)

		recordEnd := it.offset + RecordHeaderSize + int64(h.Length)
		if recordEnd > info.Size() {
			it.closeFile()
			return false
		}

		if h.LSN < it.startLSN {
			it.offset = recordEnd
			continue
		}

		var payload []byte
		if h.Length > 0 {
			payload = make([]byte, h.Length)
			_, err = it.currentFile.ReadAt(payload, it.offset+RecordHeaderSize)
			if err != nil {
				it.err = err
				it.closeFile()
				return false
			}
		}

		checksum := crc32.ChecksumIEEE(payload)
		if checksum != h.CRC {
			it.err = ErrWALCorrupted
			it.closeFile()
			return false
		}

		it.current = &WALRecord{
			LSN:        h.LSN,
			RecordType: h.Type,
			Length:     h.Length,
			Checksum:   h.CRC,
			Payload:    payload,
		}
		it.offset = recordEnd
		return true
	}
}

func (it *walIterator) closeFile() {
	if it.currentFile != nil {
		it.currentFile.Close()
		it.currentFile = nil
	}
}

func (it *walIterator) Record() *WALRecord {
	return it.current
}

func (it *walIterator) Error() error {
	return it.err
}

func (it *walIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()
	if !it.closed {
		it.closed = true
		it.closeFile()
	}
}

// =============================================================================
// Header Encoding/Decoding
// =============================================================================

func encodeHeader(r *WALRecord) []byte {
	b := make([]byte, RecordHeaderSize)
	binary.LittleEndian.PutUint64(b[0:8], r.LSN)
	b[8] = byte(r.RecordType)
	binary.LittleEndian.PutUint32(b[9:13], r.Length)
	binary.LittleEndian.PutUint32(b[13:17], r.Checksum)
	return b
}

func decodeHeader(b []byte) recordHeader {
	return recordHeader{
		LSN:    binary.LittleEndian.Uint64(b[0:8]),
		Type:   WALRecordType(b[8]),
		Length: binary.LittleEndian.Uint32(b[9:13]),
		CRC:    binary.LittleEndian.Uint32(b[13:17]),
	}
}

// =============================================================================
// Errors
// =============================================================================

var (
	ErrWALClosed           = errors.New("WALImpl: WAL is closed")
	ErrWALCorrupted        = errors.New("WALImpl: WAL record corrupted")
	ErrInvalidLSN          = errors.New("WALImpl: invalid LSN")
	ErrTruncateLSNTooLarge  = errors.New("WALImpl: truncate LSN too large")
	ErrCheckpointInProgress = errors.New("WALImpl: checkpoint in progress")
	ErrNoCheckpoint        = errors.New("WALImpl: no checkpoint found")
	ErrRecoveryFailed      = errors.New("WALImpl: recovery failed")
)

// =============================================================================
// CheckpointManager Implementation
// =============================================================================

type checkpointManager struct {
	mu              sync.Mutex
	WALImpl             *WALImpl
	config          WALConfig
	checkpointConfig CheckpointConfig
	directory       string
	checkpoints     []*Checkpoint
	nextID          uint64
	treeRoot        vaddr.VAddr
	pmSnapshot      PageManagerSnapshot
	extSnapshot     ExternalValueSnapshot
	inRecovery      bool
}

func NewCheckpointManager(WALImpl *WALImpl, config WALConfig, checkpointConfig CheckpointConfig) (*checkpointManager, error) {
	cm := &checkpointManager{
		WALImpl:             WALImpl,
		config:          config,
		checkpointConfig: checkpointConfig,
		directory:       config.Directory,
		nextID:          1,
	}

	if err := os.MkdirAll(cm.directory, 0755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory: %w", err)
	}

	if err := cm.loadCheckpoints(); err != nil {
		return nil, fmt.Errorf("load checkpoints: %w", err)
	}

	cm.pmSnapshot = PageManagerSnapshot{
		CheckpointLSN: 0,
	}
	cm.extSnapshot = ExternalValueSnapshot{
		CheckpointLSN: 0,
	}

	return cm, nil
}

func (cm *checkpointManager) loadCheckpoints() error {
	entries, err := os.ReadDir(cm.directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var checkpointFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if len(entry.Name()) > 11 && entry.Name()[:11] == "checkpoint-" {
			checkpointFiles = append(checkpointFiles, filepath.Join(cm.directory, entry.Name()))
		}
	}

	for _, path := range checkpointFiles {
		cp, err := cm.loadCheckpointFile(path)
		if err != nil {
			continue
		}
		cm.checkpoints = append(cm.checkpoints, cp)
		if cp.ID >= cm.nextID {
			cm.nextID = cp.ID + 1
		}
	}

	sort.Slice(cm.checkpoints, func(i, j int) bool {
		return cm.checkpoints[i].LSN < cm.checkpoints[j].LSN
	})

	return nil
}

func (cm *checkpointManager) loadCheckpointFile(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}

	return &cp, nil
}

func (cm *checkpointManager) CreateCheckpoint() (uint64, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if err := cm.WALImpl.Flush(); err != nil {
		return 0, fmt.Errorf("flush WAL: %w", err)
	}

	checkpointRecord := &WALRecord{
		RecordType: WALCheckpoint,
		Payload:    nil,
	}

	checkpointLSN, err := cm.WALImpl.Append(checkpointRecord)
	if err != nil {
		return 0, fmt.Errorf("append checkpoint record: %w", err)
	}

	if err := cm.WALImpl.Flush(); err != nil {
		return 0, fmt.Errorf("flush checkpoint: %w", err)
	}

	cp := &Checkpoint{
		ID:      cm.nextID,
		LSN:     checkpointLSN,
		TreeRoot: cm.treeRoot,
		PageManager: PageManagerSnapshot{
			RootVAddr:     cm.pmSnapshot.RootVAddr,
			LivePageCount: cm.pmSnapshot.LivePageCount,
			CheckpointLSN: checkpointLSN,
		},
		ExternalStore: ExternalValueSnapshot{
			ActiveVAddrs:  cm.extSnapshot.ActiveVAddrs,
			CheckpointLSN: checkpointLSN,
		},
		Timestamp: uint64(time.Now().UnixNano()),
	}

	if err := cm.saveCheckpoint(cp); err != nil {
		return 0, fmt.Errorf("save checkpoint: %w", err)
	}

	if err := cm.rotateCheckpoints(cp); err != nil {
		return 0, fmt.Errorf("rotate checkpoints: %w", err)
	}

	cm.checkpoints = append(cm.checkpoints, cp)
	cm.nextID++

	return checkpointLSN, nil
}

func (cm *checkpointManager) saveCheckpoint(cp *Checkpoint) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	filename := CheckpointFileName(cp.ID)
	path := filepath.Join(cm.directory, filename)

	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write checkpoint file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("rename checkpoint file: %w", err)
	}

	return nil
}

func (cm *checkpointManager) rotateCheckpoints(newCP *Checkpoint) error {
	const maxCheckpoints = 3

	if len(cm.checkpoints) <= maxCheckpoints {
		return nil
	}

	sorted := make([]*Checkpoint, len(cm.checkpoints))
	copy(sorted, cm.checkpoints)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LSN > sorted[j].LSN
	})

	for i := maxCheckpoints; i < len(sorted); i++ {
		oldCP := sorted[i]
		filename := CheckpointFileName(oldCP.ID)
		path := filepath.Join(cm.directory, filename)

		if _, err := os.Stat(path); err == nil {
			if err := os.Remove(path); err != nil {
				continue
			}
		}

		for j := 0; j < len(cm.checkpoints); j++ {
			if cm.checkpoints[j].ID == oldCP.ID {
				cm.checkpoints = append(cm.checkpoints[:j], cm.checkpoints[j+1:]...)
				break
			}
		}
	}

	return nil
}

func (cm *checkpointManager) LatestCheckpoint() (*Checkpoint, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(cm.checkpoints) == 0 {
		return nil, nil
	}

	var latest *Checkpoint
	for _, cp := range cm.checkpoints {
		if latest == nil || cp.LSN > latest.LSN {
			latest = cp
		}
	}

	return latest, nil
}

func (cm *checkpointManager) Recover() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.inRecovery = true
	defer func() { cm.inRecovery = false }()

	latest, err := cm.LatestCheckpoint()
	if err != nil {
		return fmt.Errorf("find latest checkpoint: %w", err)
	}

	if latest == nil {
		return nil
	}

	cm.treeRoot = latest.TreeRoot
	cm.pmSnapshot = latest.PageManager
	cm.extSnapshot = latest.ExternalStore

	if err := cm.replayWAL(latest.LSN); err != nil {
		return fmt.Errorf("replay WAL: %w", err)
	}

	return nil
}

func (cm *checkpointManager) replayWAL(startLSN uint64) error {
	iter, err := cm.WALImpl.ReadFrom(startLSN)
	if err != nil {
		return fmt.Errorf("open WAL iterator: %w", err)
	}
	defer iter.Close()

	for iter.Next() {
		record := iter.Record()
		if record == nil {
			continue
		}

		switch record.RecordType {
		case WALPageAlloc:
		case WALPageFree:
		case WALNodeWrite:
		case WALExternalValue:
		case WALRootUpdate:
			if len(record.Payload) >= 16 {
				cm.treeRoot = vaddr.VAddr{
					SegmentID: binary.LittleEndian.Uint64(record.Payload[0:8]),
					Offset:    binary.LittleEndian.Uint64(record.Payload[8:16]),
				}
			}
		case WALCheckpoint:
		case WALIndexUpdate:
		case WALIndexRootUpdate:
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}

func (cm *checkpointManager) Checkpoint(lsn uint64) (*Checkpoint, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, cp := range cm.checkpoints {
		if cp.LSN == lsn {
			return cp, nil
		}
	}

	return nil, nil
}

func (cm *checkpointManager) ListCheckpoints() []*Checkpoint {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	result := make([]*Checkpoint, len(cm.checkpoints))
	copy(result, cm.checkpoints)
	return result
}

func (cm *checkpointManager) DeleteCheckpoint(lsn uint64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var cp *Checkpoint
	var idx int
	for i, c := range cm.checkpoints {
		if c.LSN == lsn {
			cp = c
			idx = i
			break
		}
	}

	if cp == nil {
		return nil
	}

	filename := CheckpointFileName(cp.ID)
	path := filepath.Join(cm.directory, filename)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete checkpoint file: %w", err)
	}

	cm.checkpoints = append(cm.checkpoints[:idx], cm.checkpoints[idx+1:]...)

	return nil
}

func (cm *checkpointManager) SetTreeRoot(root vaddr.VAddr) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.treeRoot = root
}

func (cm *checkpointManager) SetPageManagerSnapshot(snapshot PageManagerSnapshot) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.pmSnapshot = snapshot
}

func (cm *checkpointManager) SetExternalValueSnapshot(snapshot ExternalValueSnapshot) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.extSnapshot = snapshot
}

func (cm *checkpointManager) GetTreeRoot() vaddr.VAddr {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.treeRoot
}
