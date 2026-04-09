// Package wal implements the shared Write-Ahead Log with group commit.
//
// WriteBatch uses a channel-based producer-consumer pattern:
// multiple goroutines submit batches to a channel, a single consumer
// goroutine drains pending requests, writes them all, and performs
// a single fsync. The fsync latency is the natural batching window —
// no artificial sleep is needed.
//
// Design reference: docs/DESIGN.md §3.6
package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

const walFileName = "wal.log"

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// ─── Group Commit Types ─────────────────────────────────────────────

// walRequest is submitted by WriteBatch producers to the consumer goroutine.
type walRequest struct {
	batch  *walapi.Batch
	result chan walResult
}

// walResult is the response from the consumer goroutine back to a producer.
type walResult struct {
	lsn uint64
	err error
}

// ─── WAL ────────────────────────────────────────────────────────────

// wal implements walapi.WAL with channel-based group commit.
type wal struct {
	mu         sync.Mutex // protects file, currentLSN for Replay/Truncate
	dir        string
	file       *os.File
	currentLSN uint64
	closed     atomic.Bool

	// Group commit channel and consumer lifecycle.
	reqCh  chan walRequest
	doneCh chan struct{} // closed when consumer goroutine exits
}

const reqChBufferSize = 1024

// New creates or opens a WAL in the given directory.
// If the WAL file already exists, it replays to recover currentLSN
// and truncates any trailing corrupt batches.
// Starts the background consumer goroutine for group commit.
func New(cfg walapi.Config) (walapi.WAL, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", cfg.Dir, err)
	}

	path := filepath.Join(cfg.Dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	w := &wal{
		dir:    cfg.Dir,
		file:   f,
		reqCh:  make(chan walRequest, reqChBufferSize),
		doneCh: make(chan struct{}),
	}

	// Recover: replay to find currentLSN and truncate corrupt tail.
	if err := w.recover(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: recover: %w", err)
	}

	// Start the consumer goroutine.
	go w.consumeLoop()

	return w, nil
}

// recover replays the WAL file to restore currentLSN.
// If a corrupt batch is found, the file is truncated to the last valid position.
func (w *wal) recover() error {
	validEnd, err := w.replayInternal(0, nil)
	if err != nil {
		return err
	}

	// Truncate any trailing corrupt data.
	fileSize, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if validEnd < fileSize {
		if err := w.file.Truncate(validEnd); err != nil {
			return err
		}
	}

	// Seek to end for future appends.
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// ─── WriteBatch (Producer) ──────────────────────────────────────────

// WriteBatch atomically writes a batch of records to the WAL.
// It submits the batch to the consumer goroutine via channel and
// waits for the result. Multiple concurrent WriteBatch calls share
// a single fsync (group commit).
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

	// Submit to consumer. If channel is closed (shutdown), recover from panic.
	select {
	case w.reqCh <- req:
	default:
		// Channel full or closed — check closed flag.
		if w.closed.Load() {
			return 0, walapi.ErrClosed
		}
		// Channel full — block.
		w.reqCh <- req
	}

	res := <-req.result
	return res.lsn, res.err
}

// ─── Consumer Loop (Group Commit) ───────────────────────────────────

// consumeLoop is the single consumer goroutine. It:
// 1. Blocks waiting for the first request
// 2. Drains all pending requests from the channel (non-blocking)
// 3. Serializes all batches + writes to file + single fsync
// 4. Notifies all producers
// 5. Repeats
//
// The fsync latency (~50μs SSD, ~5ms HDD) is the natural batching window.
// During fsync, new producers fill the channel buffer. When fsync returns,
// the consumer immediately drains them all — zero artificial delay.
func (w *wal) consumeLoop() {
	defer close(w.doneCh)

	for {
		// 1. Block wait for the first request.
		req, ok := <-w.reqCh
		if !ok {
			return // channel closed → shutdown
		}

		// 2. Drain: non-blocking read all pending requests.
		pending := []walRequest{req}
	drain:
		for {
			select {
			case r, ok := <-w.reqCh:
				if !ok {
					// Channel closed during drain. Process what we have, then exit.
					w.processBatch(pending)
					return
				}
				pending = append(pending, r)
			default:
				break drain // channel empty → stop collecting
			}
		}

		// 3. Process the batch: serialize + write + fsync + notify.
		w.processBatch(pending)
	}
}

// processBatch serializes all pending batches, writes them to the file,
// performs a single fsync, and notifies all waiting producers.
func (w *wal) processBatch(pending []walRequest) {
	w.mu.Lock()

	// Serialize all batches and collect results.
	type batchResult struct {
		lsn uint64
		err error
	}
	results := make([]batchResult, len(pending))

	// Accumulate all serialized data into a single buffer for one write call.
	var combinedBuf []byte

	for i, p := range pending {
		if w.closed.Load() {
			results[i] = batchResult{err: walapi.ErrClosed}
			continue
		}
		buf := w.serializeBatch(p.batch)
		combinedBuf = append(combinedBuf, buf...)
		results[i] = batchResult{lsn: w.currentLSN}
	}

	// Single write + single fsync for the entire group.
	var writeErr, syncErr error
	if len(combinedBuf) > 0 {
		if _, writeErr = w.file.Write(combinedBuf); writeErr != nil {
			writeErr = fmt.Errorf("wal: write: %w", writeErr)
		} else {
			if syncErr = w.file.Sync(); syncErr != nil {
				syncErr = fmt.Errorf("wal: sync: %w", syncErr)
			}
		}
	}

	w.mu.Unlock()

	// Notify all producers.
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

// ─── Serialization ──────────────────────────────────────────────────

// serializeBatch assigns LSNs, computes CRCs, and returns the serialized bytes.
// Must be called with w.mu held.
func (w *wal) serializeBatch(batch *walapi.Batch) []byte {
	count := uint32(batch.Len())
	totalSize := uint32(walapi.BatchHeaderSize + count*walapi.RecordSize)
	buf := make([]byte, totalSize)

	// Write batch header (batchCRC = 0 for now).
	binary.LittleEndian.PutUint32(buf[0:4], count)
	binary.LittleEndian.PutUint32(buf[4:8], totalSize)
	// buf[8:12] = 0 (batchCRC placeholder)

	// Write records.
	for i := range batch.Records {
		w.currentLSN++
		batch.Records[i].LSN = w.currentLSN

		off := walapi.BatchHeaderSize + uint32(i)*walapi.RecordSize
		serializeRecord(buf[off:off+walapi.RecordSize], &batch.Records[i])
	}

	// Compute and fill batchCRC.
	batchCRC := crc32c(buf)
	binary.LittleEndian.PutUint32(buf[8:12], batchCRC)

	return buf
}

// serializeRecord writes a Record into a 33-byte buffer and computes its CRC.
func serializeRecord(buf []byte, r *walapi.Record) {
	binary.LittleEndian.PutUint64(buf[0:8], r.LSN)
	buf[8] = byte(r.Type)
	binary.LittleEndian.PutUint64(buf[9:17], r.ID)
	binary.LittleEndian.PutUint64(buf[17:25], r.VAddr)
	binary.LittleEndian.PutUint32(buf[25:29], r.Size)

	r.CRC = crc32c(buf[0:29])
	binary.LittleEndian.PutUint32(buf[29:33], r.CRC)
}

// deserializeRecord reads a Record from a 33-byte buffer.
func deserializeRecord(buf []byte) walapi.Record {
	return walapi.Record{
		LSN:   binary.LittleEndian.Uint64(buf[0:8]),
		Type:  walapi.RecordType(buf[8]),
		ID:    binary.LittleEndian.Uint64(buf[9:17]),
		VAddr: binary.LittleEndian.Uint64(buf[17:25]),
		Size:  binary.LittleEndian.Uint32(buf[25:29]),
		CRC:   binary.LittleEndian.Uint32(buf[29:33]),
	}
}

// ─── Replay ─────────────────────────────────────────────────────────

// Replay reads all valid batches after afterLSN, calling fn for each record.
func (w *wal) Replay(afterLSN uint64, fn func(walapi.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return walapi.ErrClosed
	}

	validEnd, err := w.replayInternal(afterLSN, fn)
	if err != nil {
		return err
	}

	// Truncate corrupt tail if any.
	fileSize, seekErr := w.file.Seek(0, io.SeekEnd)
	if seekErr != nil {
		return seekErr
	}
	if validEnd < fileSize {
		if err := w.file.Truncate(validEnd); err != nil {
			return err
		}
		if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}

	return nil
}

// replayInternal reads batches from the file, validates CRCs, calls fn for
// records with LSN > afterLSN, updates w.currentLSN, and returns the file
// offset of the end of the last valid batch.
// fn may be nil (used during recovery to just find currentLSN).
func (w *wal) replayInternal(afterLSN uint64, fn func(walapi.Record) error) (int64, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var validEnd int64
	headerBuf := make([]byte, walapi.BatchHeaderSize)

	for {
		// Read batch header.
		n, err := io.ReadFull(w.file, headerBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // end of file or incomplete header → stop
			}
			return validEnd, err
		}
		if n < walapi.BatchHeaderSize {
			break
		}

		count := binary.LittleEndian.Uint32(headerBuf[0:4])
		totalSize := binary.LittleEndian.Uint32(headerBuf[4:8])
		storedBatchCRC := binary.LittleEndian.Uint32(headerBuf[8:12])

		expectedSize := uint32(walapi.BatchHeaderSize) + count*walapi.RecordSize
		if totalSize != expectedSize || count == 0 {
			break // invalid batch header → stop
		}

		// Read full batch (header + records) for CRC validation.
		batchBuf := make([]byte, totalSize)
		copy(batchBuf[0:walapi.BatchHeaderSize], headerBuf)

		recordsData := batchBuf[walapi.BatchHeaderSize:]
		if _, err := io.ReadFull(w.file, recordsData); err != nil {
			break // incomplete batch → stop
		}

		// Validate batch CRC: zero out batchCRC field, compute, compare.
		binary.LittleEndian.PutUint32(batchBuf[8:12], 0)
		computedCRC := crc32c(batchBuf)
		if computedCRC != storedBatchCRC {
			break // corrupt batch → stop
		}

		// Restore batchCRC (not strictly needed, but clean).
		binary.LittleEndian.PutUint32(batchBuf[8:12], storedBatchCRC)

		// Process records.
		for i := uint32(0); i < count; i++ {
			off := walapi.BatchHeaderSize + i*walapi.RecordSize
			rec := deserializeRecord(batchBuf[off : off+walapi.RecordSize])

			// Validate per-record CRC.
			expectedRecCRC := crc32c(batchBuf[off : off+29])
			if rec.CRC != expectedRecCRC {
				// Record CRC mismatch within a valid batch — treat batch as corrupt.
				return validEnd, nil
			}

			if rec.LSN > w.currentLSN {
				w.currentLSN = rec.LSN
			}

			if fn != nil && rec.LSN > afterLSN {
				if err := fn(rec); err != nil {
					return validEnd, err
				}
			}
		}

		validEnd += int64(totalSize)
	}

	return validEnd, nil
}

// ─── CurrentLSN ─────────────────────────────────────────────────────

// CurrentLSN returns the LSN of the last successfully written record.
func (w *wal) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// ─── Truncate ───────────────────────────────────────────────────────

// Truncate removes all WAL data at or before upToLSN.
func (w *wal) Truncate(upToLSN uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return walapi.ErrClosed
	}

	// Read all batches, keep only those with records having LSN > upToLSN.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var keepBatches [][]byte
	headerBuf := make([]byte, walapi.BatchHeaderSize)

	for {
		n, err := io.ReadFull(w.file, headerBuf)
		if err != nil || n < walapi.BatchHeaderSize {
			break
		}

		totalSize := binary.LittleEndian.Uint32(headerBuf[4:8])
		count := binary.LittleEndian.Uint32(headerBuf[0:4])
		expectedSize := uint32(walapi.BatchHeaderSize) + count*walapi.RecordSize
		if totalSize != expectedSize || count == 0 {
			break
		}

		batchBuf := make([]byte, totalSize)
		copy(batchBuf[0:walapi.BatchHeaderSize], headerBuf)
		if _, err := io.ReadFull(w.file, batchBuf[walapi.BatchHeaderSize:]); err != nil {
			break
		}

		// Check if any record in this batch has LSN > upToLSN.
		// A batch is atomic — we keep or discard the whole batch.
		// We keep the batch if ANY record has LSN > upToLSN.
		keep := false
		for i := uint32(0); i < count; i++ {
			off := walapi.BatchHeaderSize + i*walapi.RecordSize
			lsn := binary.LittleEndian.Uint64(batchBuf[off : off+8])
			if lsn > upToLSN {
				keep = true
				break
			}
		}
		if keep {
			keepBatches = append(keepBatches, batchBuf)
		}
	}

	// Write kept batches to a temp file, then rename.
	path := filepath.Join(w.dir, walFileName)
	tmpPath := path + ".tmp"

	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal: create tmp: %w", err)
	}

	for _, b := range keepBatches {
		if _, err := tmpFile.Write(b); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("wal: write tmp: %w", err)
		}
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("wal: sync tmp: %w", err)
	}
	tmpFile.Close()

	// Close old file, rename, reopen.
	w.file.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("wal: rename: %w", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("wal: reopen: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return fmt.Errorf("wal: seek: %w", err)
	}

	w.file = f
	return nil
}

// ─── Close ──────────────────────────────────────────────────────────

// Close shuts down the WAL:
// 1. Sets closed flag (rejects new WriteBatch calls)
// 2. Closes the request channel (consumer drains remaining, then exits)
// 3. Waits for consumer goroutine to finish
// 4. Final fsync + close file
func (w *wal) Close() error {
	if w.closed.Swap(true) {
		return walapi.ErrClosed // already closed
	}

	// Close the channel to signal consumer to stop.
	close(w.reqCh)

	// Wait for consumer goroutine to finish processing remaining requests.
	<-w.doneCh

	// Final fsync + close.
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}
