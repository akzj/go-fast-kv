package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

func newTestWAL(t *testing.T) walapi.WAL {
	t.Helper()
	dir := t.TempDir()
	w, err := New(walapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

func newTestWALWithDir(t *testing.T, dir string) walapi.WAL {
	t.Helper()
	w, err := New(walapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

// Test 1: WriteBatch + Replay basic read/write
func TestWriteBatchAndReplay(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	batch := walapi.NewBatch()
	batch.Add(walapi.RecordPageMap, 1, 0x0001_00000010, 0)
	batch.Add(walapi.RecordBlobMap, 2, 0x0001_00001000, 4096)
	batch.Add(walapi.RecordSetRoot, 1, 0, 0)

	lastLSN, err := w.WriteBatch(batch)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if lastLSN != 3 {
		t.Fatalf("expected lastLSN=3, got %d", lastLSN)
	}

	var records []walapi.Record
	err = w.Replay(0, func(r walapi.Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if records[0].Type != walapi.RecordPageMap || records[0].ID != 1 {
		t.Errorf("record[0] mismatch: %+v", records[0])
	}
	if records[1].Type != walapi.RecordBlobMap || records[1].ID != 2 || records[1].Size != 4096 {
		t.Errorf("record[1] mismatch: %+v", records[1])
	}
	if records[2].Type != walapi.RecordSetRoot || records[2].ID != 1 {
		t.Errorf("record[2] mismatch: %+v", records[2])
	}
}

// Test 2: Multiple batches sequential write + replay all
func TestMultipleBatches(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	for i := uint64(1); i <= 5; i++ {
		batch := walapi.NewBatch()
		batch.Add(walapi.RecordPageMap, i, i*100, 0)
		batch.Add(walapi.RecordPageMap, i+100, i*200, 0)
		if _, err := w.WriteBatch(batch); err != nil {
			t.Fatalf("WriteBatch %d: %v", i, err)
		}
	}

	var records []walapi.Record
	err := w.Replay(0, func(r walapi.Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(records) != 10 {
		t.Fatalf("expected 10 records, got %d", len(records))
	}
}

// Test 3: LSN monotonically increasing
func TestLSNMonotonic(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	for i := 0; i < 5; i++ {
		batch := walapi.NewBatch()
		batch.Add(walapi.RecordPageMap, uint64(i), 0, 0)
		if _, err := w.WriteBatch(batch); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
	}

	var prevLSN uint64
	err := w.Replay(0, func(r walapi.Record) error {
		if r.LSN <= prevLSN {
			t.Errorf("LSN not monotonic: prev=%d, current=%d", prevLSN, r.LSN)
		}
		prevLSN = r.LSN
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if prevLSN != 5 {
		t.Errorf("expected last LSN=5, got %d", prevLSN)
	}
}

// Test 4: Replay afterLSN filtering
func TestReplayAfterLSN(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	// Write 3 batches, 1 record each → LSN 1, 2, 3
	for i := uint64(1); i <= 3; i++ {
		batch := walapi.NewBatch()
		batch.Add(walapi.RecordPageMap, i, 0, 0)
		if _, err := w.WriteBatch(batch); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
	}

	// Replay only after LSN 2 → should get LSN 3
	var records []walapi.Record
	err := w.Replay(2, func(r walapi.Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].LSN != 3 {
		t.Errorf("expected LSN=3, got %d", records[0].LSN)
	}
}

// Test 5: Corrupt batch detection
func TestCorruptBatch(t *testing.T) {
	dir := t.TempDir()
	w := newTestWALWithDir(t, dir)

	// Write 2 batches.
	batch1 := walapi.NewBatch()
	batch1.Add(walapi.RecordPageMap, 1, 100, 0)
	if _, err := w.WriteBatch(batch1); err != nil {
		t.Fatalf("WriteBatch 1: %v", err)
	}

	batch2 := walapi.NewBatch()
	batch2.Add(walapi.RecordPageMap, 2, 200, 0)
	if _, err := w.WriteBatch(batch2); err != nil {
		t.Fatalf("WriteBatch 2: %v", err)
	}
	w.Close()

	// Corrupt the last byte of the file.
	path := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Reopen — should recover with only batch 1.
	w2 := newTestWALWithDir(t, dir)
	defer w2.Close()

	var records []walapi.Record
	err = w2.Replay(0, func(r walapi.Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record after corruption, got %d", len(records))
	}
	if records[0].ID != 1 {
		t.Errorf("expected record ID=1, got %d", records[0].ID)
	}
}

// Test 6: Truncate then replay
func TestTruncate(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	// Write 3 batches → LSN 1, 2, 3
	for i := uint64(1); i <= 3; i++ {
		batch := walapi.NewBatch()
		batch.Add(walapi.RecordPageMap, i, i*100, 0)
		if _, err := w.WriteBatch(batch); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
	}

	// Truncate LSN <= 2
	if err := w.Truncate(2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Replay should only return LSN 3
	var records []walapi.Record
	err := w.Replay(0, func(r walapi.Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record after truncate, got %d", len(records))
	}
	if records[0].LSN != 3 {
		t.Errorf("expected LSN=3, got %d", records[0].LSN)
	}
}

// Test 7: Close then operations return ErrClosed
func TestCloseReturnsErrClosed(t *testing.T) {
	w := newTestWAL(t)
	w.Close()

	batch := walapi.NewBatch()
	batch.Add(walapi.RecordPageMap, 1, 0, 0)
	_, err := w.WriteBatch(batch)
	if err != walapi.ErrClosed {
		t.Errorf("WriteBatch after close: expected ErrClosed, got %v", err)
	}

	err = w.Replay(0, func(r walapi.Record) error { return nil })
	if err != walapi.ErrClosed {
		t.Errorf("Replay after close: expected ErrClosed, got %v", err)
	}

	err = w.Truncate(0)
	if err != walapi.ErrClosed {
		t.Errorf("Truncate after close: expected ErrClosed, got %v", err)
	}

	err = w.Close()
	if err != walapi.ErrClosed {
		t.Errorf("double Close: expected ErrClosed, got %v", err)
	}
}

// Test 8: Restart recovery
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	w := newTestWALWithDir(t, dir)

	batch := walapi.NewBatch()
	batch.Add(walapi.RecordPageMap, 42, 0x0002_00000100, 0)
	batch.Add(walapi.RecordTxnCommit, 7, 0, 0)
	if _, err := w.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if w.CurrentLSN() != 2 {
		t.Fatalf("expected CurrentLSN=2 before close, got %d", w.CurrentLSN())
	}
	w.Close()

	// Reopen.
	w2 := newTestWALWithDir(t, dir)
	defer w2.Close()

	if w2.CurrentLSN() != 2 {
		t.Fatalf("expected CurrentLSN=2 after reopen, got %d", w2.CurrentLSN())
	}

	// Write another batch — LSN should continue from 3.
	batch2 := walapi.NewBatch()
	batch2.Add(walapi.RecordPageFree, 99, 0, 0)
	lastLSN, err := w2.WriteBatch(batch2)
	if err != nil {
		t.Fatalf("WriteBatch after reopen: %v", err)
	}
	if lastLSN != 3 {
		t.Errorf("expected lastLSN=3 after reopen+write, got %d", lastLSN)
	}
}

// Test 9: Empty WAL replay
func TestEmptyReplay(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	called := false
	err := w.Replay(0, func(r walapi.Record) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Replay on empty WAL: %v", err)
	}
	if called {
		t.Error("fn should not be called on empty WAL")
	}
}

// Test 10: Batch CRC and Record CRC correctness
func TestCRCCorrectness(t *testing.T) {
	dir := t.TempDir()
	w := newTestWALWithDir(t, dir)

	batch := walapi.NewBatch()
	batch.Add(walapi.RecordPageMap, 1, 100, 0)
	batch.Add(walapi.RecordBlobMap, 2, 200, 512)
	if _, err := w.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	w.Close()

	// Read raw file and manually verify CRCs.
	path := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Verify batch CRC.
	storedBatchCRC := binary.LittleEndian.Uint32(data[8:12])
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	binary.LittleEndian.PutUint32(dataCopy[8:12], 0)
	computedBatchCRC := crc32c(dataCopy)
	if storedBatchCRC != computedBatchCRC {
		t.Errorf("batch CRC mismatch: stored=%d, computed=%d", storedBatchCRC, computedBatchCRC)
	}

	// Verify each record CRC.
	for i := 0; i < 2; i++ {
		off := walapi.BatchHeaderSize + uint32(i)*walapi.RecordSize
		recBytes := data[off : off+walapi.RecordSize]
		storedRecCRC := binary.LittleEndian.Uint32(recBytes[29:33])
		computedRecCRC := crc32c(recBytes[0:29])
		if storedRecCRC != computedRecCRC {
			t.Errorf("record[%d] CRC mismatch: stored=%d, computed=%d", i, storedRecCRC, computedRecCRC)
		}
	}
}

// Test 11: WriteBatch with empty batch is a no-op
func TestEmptyBatch(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	batch := walapi.NewBatch()
	lsn, err := w.WriteBatch(batch)
	if err != nil {
		t.Fatalf("WriteBatch empty: %v", err)
	}
	if lsn != 0 {
		t.Errorf("expected LSN=0 for empty batch, got %d", lsn)
	}
}

// Test 12: Write after Truncate continues LSN correctly
func TestWriteAfterTruncate(t *testing.T) {
	w := newTestWAL(t)
	defer w.Close()

	// Write batch → LSN 1, 2
	batch1 := walapi.NewBatch()
	batch1.Add(walapi.RecordPageMap, 1, 100, 0)
	batch1.Add(walapi.RecordPageMap, 2, 200, 0)
	if _, err := w.WriteBatch(batch1); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Truncate all
	if err := w.Truncate(2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Write more — LSN should continue from 3
	batch2 := walapi.NewBatch()
	batch2.Add(walapi.RecordPageMap, 3, 300, 0)
	lastLSN, err := w.WriteBatch(batch2)
	if err != nil {
		t.Fatalf("WriteBatch after truncate: %v", err)
	}
	if lastLSN != 3 {
		t.Errorf("expected LSN=3, got %d", lastLSN)
	}
}
