// Package wal implements the shared Write-Ahead Log with group commit
// and segmented file storage.
package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// Default segment size: 64MB
const defaultSegmentSize = 64 * 1024 * 1024

// oldWALFileName is the legacy single WAL file name.
const oldWALFileName = "wal.log"

// segmentNameRE parses segment file names.
var segmentNameRE = regexp.MustCompile(`^wal\.(\d+)\.(?:(\d+)|active)\.log$`)

// ─── Group Commit Types ─────────────────────────────────────────────

type walRequest struct {
	batch  *walapi.Batch
	result chan walResult
}

type walResult struct {
	lsn uint64
	err error
}

// batchResult holds per-request outcome in processBatch.
type batchResult struct {
	lsn uint64
	err error
}

// ─── WAL ────────────────────────────────────────────────────────────

// wal implements walapi.WAL with segmented storage and group commit.
type wal struct {
	mu       sync.Mutex // protects file, segments, currentLSN
	dir      string
	syncMode int // 0=SyncAlways, 1=SyncNone
	closed   atomic.Bool

	// Segment management
	segmentSize int64
	activeFile  *os.File
	activeBegin uint64 // LSN at start of active segment
	currentLSN  uint64

	// Group commit channels
	reqCh  chan walRequest
	stopCh chan struct{} // closed by Close()
	doneCh chan struct{} // closed when consumer exits

	// Batch size limits
	maxBatchSize int // max requests to drain per batch (from config, default 1024)

	// Reusable buffers for processBatch (single consumer goroutine — no lock needed).
	combinedBuf []byte        // accumulates serialized batches across pending requests
	serBuf      []byte        // scratch buffer for serializeBatchLocked
	results     []batchResult // per-request results, reused across processBatch calls
}

type segmentInfo struct {
	name     string
	beginLSN uint64
	endLSN   uint64 // 0 for active segment
	isActive bool
	path     string
}

// New creates or opens a WAL in the given directory.
func New(cfg walapi.Config) (walapi.WAL, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", cfg.Dir, err)
	}

	segmentSize := cfg.SegmentSize
	if segmentSize <= 0 {
		segmentSize = defaultSegmentSize
	}

	maxBatchSize := cfg.MaxWALBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 1024 // default for backward compatibility
	}

	w := &wal{
		dir:         cfg.Dir,
		syncMode:    cfg.SyncMode,
		segmentSize: segmentSize,
		reqCh:       make(chan walRequest, maxBatchSize),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		maxBatchSize: maxBatchSize,
	}

	// Check for old wal.log file (backward compatibility)
	oldPath := filepath.Join(cfg.Dir, oldWALFileName)
	if _, err := os.Stat(oldPath); err == nil {
		if err := w.convertOldWAL(oldPath); err != nil {
			return nil, fmt.Errorf("wal: convert old WAL: %w", err)
		}
	}

	// Find or create active segment
	if err := w.findOrCreateActiveSegment(); err != nil {
		return nil, err
	}

	// Start consumer goroutine
	go w.consumeLoop()

	return w, nil
}

// convertOldWAL replays an old wal.log file to find currentLSN.
func (w *wal) convertOldWAL(oldPath string) error {
	f, err := os.Open(oldPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var maxLSN uint64
	headerBuf := make([]byte, walapi.BatchHeaderSize)

	for {
		_, err := io.ReadFull(f, headerBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		count := binary.LittleEndian.Uint32(headerBuf[0:4])
		totalSize := binary.LittleEndian.Uint32(headerBuf[4:8])
		expectedSize := uint32(walapi.BatchHeaderSize) + count*walapi.RecordSize
		if totalSize != expectedSize || count == 0 {
			break
		}

		recordSize := int(totalSize) - walapi.BatchHeaderSize
		data := make([]byte, recordSize)
		if _, err := io.ReadFull(f, data); err != nil {
			break
		}

		for i := uint32(0); i < count; i++ {
			off := int(i) * walapi.RecordSize
			lsn := binary.LittleEndian.Uint64(data[off+0:])
			if lsn > maxLSN {
				maxLSN = lsn
			}
		}
	}

	w.currentLSN = maxLSN
	f.Close()
	return os.Remove(oldPath)
}

// findOrCreateActiveSegment finds an existing active segment or creates a new one.
// For closed segments, endLSN from the filename is used.
// For the active segment, we replay it (fn=nil) to discover the actual currentLSN,
// since the active segment's endLSN is 0 (unknown until read).
// This replay is safe: fn=nil means no records are delivered to the caller;
// s.recover() will later call Replay() which resets currentLSN and replays again.
func (w *wal) findOrCreateActiveSegment() error {
	segments := w.listSegmentsInternal()
	if len(segments) == 0 {
		return w.createNewSegment()
	}

	// Track currentLSN from closed segment metadata.
	for _, seg := range segments {
		if !seg.isActive && seg.endLSN > w.currentLSN {
			w.currentLSN = seg.endLSN
		}
	}

	for _, seg := range segments {
		if seg.isActive {
			// Replay active segment to find actual currentLSN.
			rf, err := os.Open(seg.path)
			if err != nil {
				return fmt.Errorf("wal: open active segment for replay: %w", err)
			}
			if err := w.replaySegment(rf, 0, nil); err != nil {
				rf.Close()
				return fmt.Errorf("wal: replay active segment: %w", err)
			}
			rf.Close()

			// Open for append writes.
			f, err := os.OpenFile(seg.path, os.O_APPEND|os.O_RDWR, 0644)
			if err != nil {
				return fmt.Errorf("wal: open active segment: %w", err)
			}
			w.activeFile = f
			w.activeBegin = seg.beginLSN
			return nil
		}
	}
	return w.createNewSegment()
}

// createNewSegment creates a new active segment file.
func (w *wal) createNewSegment() error {
	beginLSN := w.currentLSN + 1
	name := fmt.Sprintf("wal.%020d.active.log", beginLSN)
	path := filepath.Join(w.dir, name)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}

	w.activeFile = f
	w.activeBegin = beginLSN
	return nil
}

// listSegmentsInternal returns all segment info, sorted by begin LSN.
func (w *wal) listSegmentsInternal() []segmentInfo {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil
	}

	var segments []segmentInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		if name == oldWALFileName || name == oldWALFileName+".tmp" {
			continue
		}

		matches := segmentNameRE.FindStringSubmatch(name)
		if matches == nil {
			continue
		}

		beginLSN, _ := strconv.ParseUint(matches[1], 10, 64)
		var endLSN uint64
		isActive := matches[2] == ""

		if !isActive {
			endLSN, _ = strconv.ParseUint(matches[2], 10, 64)
		}

		segments = append(segments, segmentInfo{
			name:     name,
			beginLSN: beginLSN,
			endLSN:   endLSN,
			isActive: isActive,
			path:     filepath.Join(w.dir, name),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].beginLSN < segments[j].beginLSN
	})

	return segments
}

// ─── WriteBatch (Producer) ──────────────────────────────────────────

func (w *wal) WriteBatch(batch *walapi.Batch) (uint64, error) {
	if w.closed.Load() {
		return 0, walapi.ErrClosed
	}
	if batch.Len() == 0 {
		return atomic.LoadUint64(&w.currentLSN), nil
	}

	req := walRequest{
		batch:  batch,
		result: make(chan walResult, 1),
	}

	select {
	case w.reqCh <- req:
	case <-w.doneCh:
		return 0, walapi.ErrClosed
	default:
		select {
		case w.reqCh <- req:
		case <-w.doneCh:
			return 0, walapi.ErrClosed
		}
	}

	res := <-req.result
	return res.lsn, res.err
}

// ─── Consumer Loop (Group Commit) ────────────────────────────────────

func (w *wal) consumeLoop() {
	defer close(w.doneCh)

	for {
		var req walRequest
		var ok bool
		select {
		case req, ok = <-w.reqCh:
			if !ok {
				return
			}
		case <-w.stopCh:
			w.drainAndProcess()
			return
		}

		pending := []walRequest{req}
		drain:
		for len(pending) < w.maxBatchSize {
			select {
			case r := <-w.reqCh:
				pending = append(pending, r)
			case <-w.stopCh:
				w.safeProcessBatch(pending)
				w.drainAndProcess()
				return
			default:
				break drain
			}
		}

		w.safeProcessBatch(pending)
	}
}

func (w *wal) drainAndProcess() {
	var remaining []walRequest
	for {
		select {
		case req := <-w.reqCh:
			remaining = append(remaining, req)
		default:
			if len(remaining) > 0 {
				w.safeProcessBatch(remaining)
			}
			return
		}
	}
}

func (w *wal) safeProcessBatch(pending []walRequest) {
	defer func() {
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("wal: consumer panic: %v", r)
			for _, p := range pending {
				select {
				case p.result <- walResult{err: panicErr}:
				default:
				}
			}
		}
	}()
	w.processBatch(pending)
}

func (w *wal) processBatch(pending []walRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()

	savedLSN := w.currentLSN

	// Reuse results slice (single consumer goroutine — no contention).
	if cap(w.results) < len(pending) {
		w.results = make([]batchResult, len(pending))
	} else {
		w.results = w.results[:len(pending)]
	}
	results := w.results

	// Reuse combinedBuf (reset length, keep capacity).
	w.combinedBuf = w.combinedBuf[:0]

	for i, p := range pending {
		if w.closed.Load() {
			results[i] = batchResult{err: walapi.ErrClosed}
			continue
		}
		w.combinedBuf = w.serializeBatchAppend(p.batch, w.combinedBuf)
		results[i] = batchResult{lsn: w.currentLSN}
	}

	var writeErr, syncErr error
	if len(w.combinedBuf) > 0 {
		info, err := w.activeFile.Stat()
		if err == nil && info.Size()+int64(len(w.combinedBuf)) > w.segmentSize {
			if err := w.rotateLocked(); err != nil {
				writeErr = err
				w.currentLSN = savedLSN
			}
		}

		if writeErr == nil {
			if _, writeErr = w.activeFile.Write(w.combinedBuf); writeErr != nil {
				writeErr = fmt.Errorf("wal: write: %w", writeErr)
				w.currentLSN = savedLSN
			} else if w.syncMode == 0 {
				if syncErr = w.activeFile.Sync(); syncErr != nil {
					syncErr = fmt.Errorf("wal: sync: %w", syncErr)
				}
			}
		}
	}

	for i, p := range pending {
		res := results[i]
		if res.err == nil && writeErr != nil {
			res.err = writeErr
		}
		if res.err == nil && syncErr != nil {
			res.err = syncErr
		}
		p.result <- walResult{lsn: res.lsn, err: res.err}
	}
}

// serializeBatchAppend assigns LSNs, computes CRCs, and appends the
// serialized batch to dst. It reuses w.serBuf as scratch space to avoid
// allocating a new buffer per batch.
func (w *wal) serializeBatchAppend(batch *walapi.Batch, dst []byte) []byte {
	count := uint32(batch.Len())
	totalSize := int(walapi.BatchHeaderSize + count*walapi.RecordSize)

	// Grow scratch buffer if needed (single goroutine — safe).
	if cap(w.serBuf) < totalSize {
		w.serBuf = make([]byte, totalSize)
	}
	buf := w.serBuf[:totalSize]

	binary.LittleEndian.PutUint32(buf[0:4], count)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(totalSize))
	binary.LittleEndian.PutUint32(buf[8:12], 0) // zero CRC field before computing

	for i := range batch.Records {
		w.currentLSN++
		batch.Records[i].LSN = w.currentLSN

		off := int(walapi.BatchHeaderSize) + i*int(walapi.RecordSize)
		serializeRecordLocked(buf[off:off+int(walapi.RecordSize)], &batch.Records[i])
	}

	batchCRC := crc32c(buf)
	binary.LittleEndian.PutUint32(buf[8:12], batchCRC)

	return append(dst, buf...)
}

func serializeRecordLocked(buf []byte, r *walapi.Record) {
	binary.LittleEndian.PutUint64(buf[0:8], r.LSN)
	buf[8] = byte(r.ModuleType)
	buf[9] = byte(r.Type)
	binary.LittleEndian.PutUint64(buf[10:18], r.ID)
	binary.LittleEndian.PutUint64(buf[18:26], r.VAddr)
	binary.LittleEndian.PutUint32(buf[26:30], r.Size)

	r.CRC = crc32c(buf[0:30])
	binary.LittleEndian.PutUint32(buf[30:34], r.CRC)
}

func deserializeRecordLocked(buf []byte) walapi.Record {
	return walapi.Record{
		LSN:        binary.LittleEndian.Uint64(buf[0:8]),
		ModuleType: walapi.ModuleType(buf[8]),
		Type:       walapi.RecordType(buf[9]),
		ID:         binary.LittleEndian.Uint64(buf[10:18]),
		VAddr:      binary.LittleEndian.Uint64(buf[18:26]),
		Size:       binary.LittleEndian.Uint32(buf[26:30]),
		CRC:        binary.LittleEndian.Uint32(buf[30:34]),
	}
}

// rotateLocked closes the current active segment and starts a new one.
func (w *wal) rotateLocked() error {
	if w.activeFile == nil {
		return nil
	}

	endLSN := w.currentLSN

	if err := w.activeFile.Close(); err != nil {
		return err
	}

	oldName := fmt.Sprintf("wal.%020d.active.log", w.activeBegin)
	newName := fmt.Sprintf("wal.%020d.%020d.log", w.activeBegin, endLSN)

	if err := os.Rename(filepath.Join(w.dir, oldName), filepath.Join(w.dir, newName)); err != nil {
		return fmt.Errorf("wal: rename segment: %w", err)
	}

	return w.createNewSegment()
}

// ─── Replay ─────────────────────────────────────────────────────────

func (w *wal) Replay(afterLSN uint64, fn func(walapi.Record) error) error {
	if w.closed.Load() {
		return walapi.ErrClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Reset currentLSN so ALL records with LSN > afterLSN are replayed.
	// Without this, records from the current segment are skipped if a previous
	// Replay call already set currentLSN to the segment's end.
	w.currentLSN = 0

	segments := w.listSegmentsInternal()
	if len(segments) == 0 {
		return nil
	}

	for _, seg := range segments {
		f, err := os.Open(seg.path)
		if err != nil {
			return fmt.Errorf("wal: open segment %s: %w", seg.name, err)
		}

		if err := w.replaySegment(f, afterLSN, fn); err != nil {
			f.Close()
			return fmt.Errorf("wal: replay segment %s: %w", seg.name, err)
		}
		f.Close()
	}

	return nil
}

func (w *wal) replaySegment(f *os.File, afterLSN uint64, fn func(walapi.Record) error) error {
	headerBuf := make([]byte, walapi.BatchHeaderSize)

	for {
		_, err := io.ReadFull(f, headerBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		count := binary.LittleEndian.Uint32(headerBuf[0:4])
		totalSize := binary.LittleEndian.Uint32(headerBuf[4:8])
		storedBatchCRC := binary.LittleEndian.Uint32(headerBuf[8:12])

		expectedSize := uint32(walapi.BatchHeaderSize) + count*walapi.RecordSize
		if totalSize != expectedSize || count == 0 {
			break
		}

		batchBuf := make([]byte, totalSize)
		copy(batchBuf[0:walapi.BatchHeaderSize], headerBuf)
		recordsData := batchBuf[walapi.BatchHeaderSize:]
		if _, err := io.ReadFull(f, recordsData); err != nil {
			break
		}

		binary.LittleEndian.PutUint32(batchBuf[8:12], 0)
		computedCRC := crc32c(batchBuf)
		if computedCRC != storedBatchCRC {
			break
		}
		binary.LittleEndian.PutUint32(batchBuf[8:12], storedBatchCRC)

		for i := uint32(0); i < count; i++ {
			off := walapi.BatchHeaderSize + i*walapi.RecordSize
			rec := deserializeRecordLocked(batchBuf[off : off+walapi.RecordSize])

			expectedRecCRC := crc32c(batchBuf[off : off+30])
			if rec.CRC != expectedRecCRC {
				return walapi.ErrCorruptBatch
			}

			if rec.LSN > w.currentLSN {
				w.currentLSN = rec.LSN
			}

			if fn != nil && rec.LSN > afterLSN {
				if err := fn(rec); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ─── CurrentLSN ─────────────────────────────────────────────────────

func (w *wal) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// ─── Rotate ──────────────────────────────────────────────────────────

func (w *wal) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

// ─── DeleteSegmentsBefore ───────────────────────────────────────────

func (w *wal) DeleteSegmentsBefore(lsn uint64) error {
	if w.closed.Load() {
		return walapi.ErrClosed
	}

	segments := w.listSegmentsInternal()
	var errs []error

	for _, seg := range segments {
		if seg.isActive {
			continue
		}
		if seg.endLSN <= lsn {
			if err := os.Remove(seg.path); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("wal: delete segments: %v", errs)
	}
	return nil
}

// ─── ListSegments ────────────────────────────────────────────────────

func (w *wal) ListSegments() []string {
	segments := w.listSegmentsInternal()
	names := make([]string, len(segments))
	for i, seg := range segments {
		names[i] = seg.name
	}
	return names
}

// ─── Truncate ────────────────────────────────────────────────────────

func (w *wal) Truncate(upToLSN uint64) error {
	return w.DeleteSegmentsBefore(upToLSN)
}

// ─── Close ──────────────────────────────────────────────────────────

func (w *wal) Close() error {
	if w.closed.Swap(true) {
		return walapi.ErrClosed
	}

	close(w.stopCh)
	<-w.doneCh

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile != nil {
		if err := w.activeFile.Sync(); err != nil {
			w.activeFile.Close()
			return err
		}
		if err := w.activeFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// ─── Errors ─────────────────────────────────────────────────────────

var ErrCorruptBatch = errors.New("wal: corrupt batch")
