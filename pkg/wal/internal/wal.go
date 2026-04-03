package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/akzj/go-fast-kv/pkg/wal/api"
)

// WALImpl implements the WAL interface using a three-layer architecture:
// entryChan -> buffer -> writerRun() goroutine
// This ensures file I/O never blocks the caller.
type WALImpl struct {
	dir        string
	bufferSize int
	file       *os.File
	fileNum    atomic.Uint64

	// Three-layer architecture
	entryChan chan *marshalEntry // Layer 1: caller writes here
	buffer    *bytes.Buffer      // Layer 2: consumer marshals here

	// Writer goroutine
	writerDone chan struct{}
	syncDone   chan struct{}
	writerWG   sync.WaitGroup

	// LSN tracking
	lsn     atomic.Uint64
	lastSyncedLSN atomic.Uint64

	// Close signaling
	closeOnce sync.Once
	closed    atomic.Bool
}

// Ensure WALImpl implements api.WAL
var _ api.WAL = (*WALImpl)(nil)

// marshalEntry holds a marshaled entry ready for writing.
type marshalEntry struct {
	data []byte
	lsn  uint64
}

// NewWAL creates a new WAL instance.
func NewWAL(dir string, bufferSize int) (api.WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	if bufferSize <= 0 {
		bufferSize = api.DefaultBufferSize
	}

	w := &WALImpl{
		dir:        dir,
		bufferSize: bufferSize,
		entryChan:  make(chan *marshalEntry, bufferSize/64),
		buffer:     bytes.NewBuffer(make([]byte, 0, bufferSize)),
		writerDone: make(chan struct{}),
		syncDone:   make(chan struct{}),
	}

	// Open first WAL file
	if err := w.openNewFile(); err != nil {
		return nil, err
	}

	// Recover LSN from existing files
	w.recoverLSN()

	// Start writer goroutine
	w.writerWG.Add(1)
	go w.writerRun()

	return w, nil
}

// openNewFile opens a new WAL file.
func (w *WALImpl) openNewFile() error {
	num := w.fileNum.Add(1)
	filename := fmt.Sprintf("%s_%06d%s", api.WALFilePrefix, num, api.WALFileExt)
	path := filepath.Join(w.dir, filename)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open wal file: %w", err)
	}

	if w.file != nil {
		w.file.Close()
	}
	w.file = file
	return nil
}

// recoverLSN recovers the last LSN from existing WAL files.
func (w *WALImpl) recoverLSN() {
	entries, err := filepath.Glob(filepath.Join(w.dir, fmt.Sprintf("%s_*.wal", api.WALFilePrefix)))
	if err != nil || len(entries) == 0 {
		return
	}

	// Sort files by number
	sort.Strings(entries)

	// Sum entries across ALL files
	var totalEntries uint64
	for _, path := range entries {
		totalEntries += w.countEntries(path)
	}
	w.lastSyncedLSN.Store(totalEntries)
	w.lsn.Store(totalEntries)
}

// countEntries counts entries in a WAL file.
func (w *WALImpl) countEntries(path string) uint64 {
	file, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer file.Close()

	var count uint64
	for {
		header := make([]byte, 5)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		length := binary.BigEndian.Uint32(header[1:5])
		// Entry format: [1:type][4:length][N:payload][4:checksum]
		_, err = file.Seek(int64(length)+4, io.SeekCurrent) // +4 for checksum
		if err != nil {
			break
		}
		count++
	}
	return count
}

// writerRun is the writer goroutine that batches writes.
// When an entry arrives, it drains all available entries then flushes.
func (w *WALImpl) writerRun() {
	defer w.writerWG.Done()

	var batch []*marshalEntry
	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Write all entries in batch
		for _, entry := range batch {
			// Check if we need to rotate file
			info, _ := w.file.Stat()
			if info.Size() >= api.MaxWALFileSize {
				w.openNewFile()
			}

			_, err := w.file.Write(entry.data)
			if err != nil {
				// Log error but continue
			}
		}

		// Fsync
		w.file.Sync()

		// Update last synced LSN
		if len(batch) > 0 {
			w.lastSyncedLSN.Store(batch[len(batch)-1].lsn)
		}

		batch = batch[:0]
	}

	for {
		// Wait for first entry or signal (blocking)
		select {
		case <-w.writerDone:
			flush()
			return
		case <-w.syncDone:
			// Sync requested, drain and signal completion
			for {
				select {
				case entry, ok := <-w.entryChan:
					if !ok {
						flush()
						return
					}
					batch = append(batch, entry)
				default:
					flush()
					return
				}
			}
		case entry, ok := <-w.entryChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, entry)

			// Drain all available entries (non-blocking)
			for {
				select {
				case entry, ok := <-w.entryChan:
					if !ok {
						flush()
						return
					}
					batch = append(batch, entry)
				default:
					// No more entries, flush and loop
					flush()
					goto done
				}
			}
		done:
		}
	}
}

// Write appends a WAL entry to the buffer.
// Entry is marshaled and sent to writer goroutine via channel.
func (w *WALImpl) Write(entry *api.WALEntry) error {
	if w.closed.Load() {
		return fmt.Errorf("wal closed")
	}

	lsn := w.lsn.Add(1)

	// Marshal entry
	data, err := w.marshalEntry(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	// Send to writer goroutine via channel (non-blocking)
	select {
	case w.entryChan <- &marshalEntry{data: data, lsn: lsn}:
		return nil
	default:
		// Channel full, block until it can be sent
		w.entryChan <- &marshalEntry{data: data, lsn: lsn}
		return nil
	}
}

// marshalEntry serializes a WAL entry.
// Format: [Type:1][Length:4][Payload:n][Checksum:4]
func (w *WALImpl) marshalEntry(entry *api.WALEntry) ([]byte, error) {
	length := len(entry.Payload)
	totalLen := 1 + 4 + length + 4 // type + length + payload + checksum

	data := make([]byte, totalLen)
	data[0] = byte(entry.Type)
	binary.BigEndian.PutUint32(data[1:5], uint32(length))
	copy(data[5:5+length], entry.Payload)

	// Calculate checksum (SHA256 of type + length + payload)
	checksumData := data[:5+length]
	hash := sha256.Sum256(checksumData)
	copy(data[5+length:totalLen], hash[:4])

	return data, nil
}

// unmarshalEntry deserializes a WAL entry.
func (w *WALImpl) unmarshalEntry(data []byte) (*api.WALEntry, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("entry too short")
	}

	typeByte := data[0]
	length := binary.BigEndian.Uint32(data[1:5])

	if len(data) < 9+int(length) {
		return nil, fmt.Errorf("entry truncated")
	}

	payload := make([]byte, length)
	copy(payload, data[5:5+length])

	// Verify checksum
	checksumData := data[:5+length]
	expectedHash := sha256.Sum256(checksumData)
	expectedChecksum := binary.BigEndian.Uint32(data[5+length:9+length])
	actualChecksum := binary.BigEndian.Uint32(expectedHash[:4])

	if expectedChecksum != actualChecksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return &api.WALEntry{
		Type:    api.WALEntryType(typeByte),
		Payload: payload,
	}, nil
}

// Sync flushes all entries to disk and returns the LSN.
func (w *WALImpl) Sync(ctx context.Context) (uint64, error) {
	if w.closed.Load() {
		return 0, fmt.Errorf("wal closed")
	}

	// Signal current writer to flush and exit
	w.syncDone <- struct{}{}

	// Wait for current writer to finish
	w.writerWG.Wait()

	// Restart writer with fresh channels
	w.syncDone = make(chan struct{})
	w.writerDone = make(chan struct{})
	w.entryChan = make(chan *marshalEntry, w.bufferSize/64)
	w.writerWG.Add(1)
	go w.writerRun()

	return w.lastSyncedLSN.Load(), nil
}

// Checkpoint writes a checkpoint marker.
func (w *WALImpl) Checkpoint(ctx context.Context) (uint64, error) {
	entry := &api.WALEntry{
		Type:    api.WALEntryTypeCheckpoint,
		Payload: nil,
	}

	if err := w.Write(entry); err != nil {
		return 0, err
	}

	return w.Sync(ctx)
}

// Replay replays WAL entries from the specified LSN.
func (w *WALImpl) Replay(ctx context.Context, sinceLSN uint64, handler func(entry *api.WALEntry) error) error {
	entries, err := filepath.Glob(filepath.Join(w.dir, fmt.Sprintf("%s_*.wal", api.WALFilePrefix)))
	if err != nil {
		return fmt.Errorf("glob wal files: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	// Sort files by number
	sort.Strings(entries)

	var lsn uint64
	for _, path := range entries {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open wal file: %w", err)
		}

		for {
			header := make([]byte, 5)
			_, err := io.ReadFull(file, header)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			length := binary.BigEndian.Uint32(header[1:5])
			// Entry format: [1:type][4:length][N:payload][4:checksum]
			data := make([]byte, 5+length+4) // +4 for checksum
			copy(data, header)
			_, err = io.ReadFull(file, data[5:5+length+4])
			if err != nil {
				break
			}

			lsn++
			if lsn <= sinceLSN {
				continue
			}

			entry, err := w.unmarshalEntry(data)
			if err != nil {
				continue
			}

			if err := handler(entry); err != nil {
				file.Close()
				return err
			}
		}

		file.Close()
	}

	return nil
}

// GetLastLSN returns the LSN of the last entry.
func (w *WALImpl) GetLastLSN() uint64 {
	return w.lsn.Load()
}

// Close closes the WAL.
func (w *WALImpl) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)

		// Signal writer to stop
		close(w.writerDone)
		w.writerWG.Wait()

		// Close file
		if w.file != nil {
			w.file.Close()
		}
	})

	return nil
}
