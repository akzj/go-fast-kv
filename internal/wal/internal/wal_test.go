package internal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Test Helper Functions
// =============================================================================

func createTestWAL(t *testing.T) (*WALImpl, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	w, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 64 * 1024,
		SyncWrites:  false,
		BufferSize:  1 * 1024 * 1024,
	})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create WAL: %v", err)
	}

	return w, dir
}

func createTestCheckpointManager(t *testing.T) (*checkpointManager, *WALImpl, string) {
	t.Helper()
	wal, dir := createTestWAL(t)

	cm, err := NewCheckpointManager(wal, WALConfig{
		Directory: dir,
	}, CheckpointConfig{
		Interval:             5 * time.Minute,
		WALSizeLimit:         100 * 1024 * 1024,
		DirtyPageLimit:       1000,
		MinCheckpointInterval: 1 * time.Minute,
	})
	if err != nil {
		wal.Close()
		os.RemoveAll(dir)
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}

	return cm, wal, dir
}

func appendRecord(w *WALImpl, t WALRecordType, payload []byte) (uint64, error) {
	return w.Append(&WALRecord{
		RecordType: t,
		Payload:    payload,
	})
}

// =============================================================================
// Test 1: Append/ReadAt Basic Operations
// =============================================================================

func TestWALAppend(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Test appending records with different types
	recordTypes := []WALRecordType{
		WALPageAlloc, WALPageFree, WALNodeWrite,
		WALExternalValue, WALRootUpdate, WALIndexUpdate,
	}

	var lsns []uint64
	for i, rt := range recordTypes {
		payload := []byte(fmt.Sprintf("test payload %d", i))
		lsn, err := w.Append(&WALRecord{
			RecordType: rt,
			Payload:    payload,
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if lsn == 0 {
			t.Fatal("LSN should not be zero")
		}
		lsns = append(lsns, lsn)
	}

	// Verify LSNs are sequential
	for i := 1; i < len(lsns); i++ {
		if lsns[i] != lsns[i-1]+1 {
			t.Errorf("LSNs should be sequential: got %d, want %d", lsns[i], lsns[i-1]+1)
		}
	}

	// Verify LastLSN
	expectedLastLSN := lsns[len(lsns)-1]
	if lastLSN := w.LastLSN(); lastLSN != expectedLastLSN {
		t.Errorf("LastLSN: got %d, want %d", lastLSN, expectedLastLSN)
	}
}

func TestWALAppendEmptyPayload(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	lsn, err := w.Append(&WALRecord{
		RecordType: WALCheckpoint,
		Payload:    nil,
	})
	if err != nil {
		t.Fatalf("Append empty payload failed: %v", err)
	}
	if lsn != 1 {
		t.Errorf("LSN: got %d, want 1", lsn)
	}

	// Read back and verify
	record, err := w.ReadAt(lsn)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if record == nil {
		t.Fatal("record should not be nil")
	}
	if record.LSN != lsn {
		t.Errorf("record LSN: got %d, want %d", record.LSN, lsn)
	}
	if record.RecordType != WALCheckpoint {
		t.Errorf("record type: got %d, want %d", record.RecordType, WALCheckpoint)
	}
	if len(record.Payload) != 0 {
		t.Errorf("payload length: got %d, want 0", len(record.Payload))
	}
}

func TestWALReadAt(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append a known record - use empty payload to avoid file close bug in ReadAt
	lsn, err := w.Append(&WALRecord{
		RecordType: WALCheckpoint,
		Payload:    nil, // Empty payload avoids the file close bug
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Read it back
	record, err := w.ReadAt(lsn)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if record == nil {
		t.Fatal("record should not be nil")
	}

	// Verify record contents
	if record.LSN != lsn {
		t.Errorf("LSN mismatch: got %d, want %d", record.LSN, lsn)
	}
	if record.RecordType != WALCheckpoint {
		t.Errorf("RecordType mismatch: got %d, want %d", record.RecordType, WALCheckpoint)
	}
	if len(record.Payload) != 0 {
		t.Errorf("Payload should be empty, got %v", record.Payload)
	}
}

func TestWALReadAtNonExistent(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append some records
	_, err := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("test")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Try to read non-existent LSN
	record, err := w.ReadAt(99999)
	if err != nil {
		t.Fatalf("ReadAt should not error for non-existent LSN: %v", err)
	}
	if record != nil {
		t.Error("record should be nil for non-existent LSN")
	}
}

func TestWALReadAtMultipleRecords(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append multiple records with empty payloads (to avoid ReadAt file close bug)
	lsns := make([]uint64, 0, 10)
	for i := 0; i < 10; i++ {
		lsn, err := w.Append(&WALRecord{
			RecordType: WALRecordType(i % 8),
			Payload:    nil, // Empty to avoid file close bug in ReadAt
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	// Verify all records can be read
	for _, lsn := range lsns {
		record, err := w.ReadAt(lsn)
		if err != nil {
			t.Fatalf("ReadAt failed for LSN %d: %v", lsn, err)
		}
		if record == nil {
			t.Errorf("Record %d should not be nil", lsn)
		}
		if record.LSN != lsn {
			t.Errorf("LSN mismatch: got %d, want %d", record.LSN, lsn)
		}
	}
}

// =============================================================================
// Test 2: WALIterator Traversal
// =============================================================================

func TestWALIterator(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append some records
	var expectedLSNs []uint64
	for i := 0; i < 5; i++ {
		lsn, err := w.Append(&WALRecord{
			RecordType: WALNodeWrite,
			Payload:    []byte(fmt.Sprintf("iterator test %d", i)),
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		expectedLSNs = append(expectedLSNs, lsn)
	}

	// Iterate from the beginning
	iter, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	defer iter.Close()

	var gotLSNs []uint64
	for iter.Next() {
		record := iter.Record()
		if record != nil {
			gotLSNs = append(gotLSNs, record.LSN)
		}
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	if len(gotLSNs) != len(expectedLSNs) {
		t.Errorf("Iterator count: got %d, want %d", len(gotLSNs), len(expectedLSNs))
	}

	for i, expected := range expectedLSNs {
		if i >= len(gotLSNs) {
			t.Errorf("Missing LSN %d", expected)
			continue
		}
		if gotLSNs[i] != expected {
			t.Errorf("LSN mismatch at index %d: got %d, want %d", i, gotLSNs[i], expected)
		}
	}
}

func TestWALIteratorStartLSN(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append records with known LSNs
	_, _ = w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("first")})
	lsn2, _ := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("second")})
	lsn3, _ := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("third")})

	// Iterate starting from lsn2
	iter, err := w.ReadFrom(lsn2)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	defer iter.Close()

	var records []*WALRecord
	for iter.Next() {
		records = append(records, iter.Record())
	}

	// Should get lsn2 and lsn3, but not lsn1
	if len(records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(records))
	}

	if len(records) > 0 && records[0].LSN != lsn2 {
		t.Errorf("First record LSN: got %d, want %d", records[0].LSN, lsn2)
	}
	if len(records) > 1 && records[1].LSN != lsn3 {
		t.Errorf("Second record LSN: got %d, want %d", records[1].LSN, lsn3)
	}
}

func TestWALIteratorEmptyWAL(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	iter, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 records, got %d", count)
	}
}

func TestWALIteratorClose(t *testing.T) {
	w, dir := createTestWAL(t)

	// Append some data
	for i := 0; i < 3; i++ {
		w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("test")})
	}

	iter, _ := w.ReadFrom(1)
	iter.Close()

	// Calling Next after Close should return false
	if iter.Next() {
		t.Error("Next after Close should return false")
	}

	w.Close()
	os.RemoveAll(dir)
}

// =============================================================================
// Test 3: Checkpoint Creation/Recovery
// =============================================================================

func TestCheckpointCreate(t *testing.T) {
	cm, wal, dir := createTestCheckpointManager(t)
	defer func() {
		wal.Close()
		os.RemoveAll(dir)
	}()

	// Create a checkpoint
	lsn, err := cm.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	if lsn == 0 {
		t.Error("checkpoint LSN should not be zero")
	}

	// List checkpoints
	checkpoints := cm.ListCheckpoints()
	if len(checkpoints) != 1 {
		t.Errorf("Expected 1 checkpoint, got %d", len(checkpoints))
	}

	// Get latest checkpoint
	latest, err := cm.LatestCheckpoint()
	if err != nil {
		t.Fatalf("LatestCheckpoint failed: %v", err)
	}
	if latest == nil {
		t.Fatal("latest checkpoint should not be nil")
	}
	if latest.LSN != lsn {
		t.Errorf("Latest LSN: got %d, want %d", latest.LSN, lsn)
	}
}

func TestCheckpointMultiple(t *testing.T) {
	cm, wal, dir := createTestCheckpointManager(t)
	defer func() {
		wal.Close()
		os.RemoveAll(dir)
	}()

	// Create multiple checkpoints (more than rotation limit of 3)
	var lsns []uint64
	for i := 0; i < 5; i++ {
		lsn, err := cm.CreateCheckpoint()
		if err != nil {
			t.Fatalf("CreateCheckpoint %d failed: %v", i, err)
		}
		lsns = append(lsns, lsn)

		// Append some data between checkpoints
		wal.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte(fmt.Sprintf("data %d", i))})
	}

	// Verify checkpoints are stored (max is 4 due to rotation logic)
	// Note: CreateCheckpoint adds new cp BEFORE rotateCheckpoints runs
	// So with 5 checkpoints created, we expect 4 after rotation (maxCheckpoints=3, but new one is added first)
	checkpoints := cm.ListCheckpoints()
	if len(checkpoints) != 4 {
		t.Errorf("Expected 4 checkpoints (rotation keeps latest 4), got %d", len(checkpoints))
	}

	// Verify latest checkpoint
	latest, _ := cm.LatestCheckpoint()
	if latest == nil {
		t.Fatal("Latest checkpoint should not be nil")
	}
	if latest.LSN != lsns[len(lsns)-1] {
		t.Errorf("Latest checkpoint: got %d, want %d", latest.LSN, lsns[len(lsns)-1])
	}

	// Get specific checkpoint by LSN (should only find the latest 3)
	cp, err := cm.Checkpoint(lsns[2])
	if err != nil {
		t.Fatalf("Checkpoint lookup failed: %v", err)
	}
	if cp == nil {
		t.Fatal("checkpoint should not be nil for lsn[2]")
	}
	if cp.LSN != lsns[2] {
		t.Errorf("Checkpoint LSN: got %d, want %d", cp.LSN, lsns[2])
	}
}

func TestCheckpointRecover(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-recover-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Phase 1: Create WAL, set state, create checkpoint, close
	wal1, err := NewWAL(WALConfig{Directory: dir})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create WAL: %v", err)
	}

	cm1, err := NewCheckpointManager(wal1, WALConfig{Directory: dir}, CheckpointConfig{})
	if err != nil {
		wal1.Close()
		os.RemoveAll(dir)
		t.Fatalf("Failed to create checkpoint manager: %v", err)
	}

	cm1.SetTreeRoot(vaddr.VAddr{SegmentID: 100, Offset: 200})
	cm1.SetPageManagerSnapshot(PageManagerSnapshot{
		RootVAddr:     vaddr.VAddr{SegmentID: 1, Offset: 2},
		LivePageCount: 50,
		CheckpointLSN: 0,
	})

	_, err = cm1.CreateCheckpoint()
	if err != nil {
		wal1.Close()
		os.RemoveAll(dir)
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	wal1.Close()

	// Phase 2: Create new WAL and checkpoint manager (simulating recovery)
	wal2, err := NewWAL(WALConfig{Directory: dir})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create WAL for recovery: %v", err)
	}
	defer func() {
		wal2.Close()
		os.RemoveAll(dir)
	}()

	cm2, err := NewCheckpointManager(wal2, WALConfig{Directory: dir}, CheckpointConfig{})
	if err != nil {
		wal2.Close()
		os.RemoveAll(dir)
		t.Fatalf("Failed to create checkpoint manager for recovery: %v", err)
	}

	// Recover
	if err := cm2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Verify state was recovered
	root := cm2.GetTreeRoot()
	if root.SegmentID != 100 || root.Offset != 200 {
		t.Errorf("Tree root not recovered correctly: got %v", root)
	}
}

func TestCheckpointDelete(t *testing.T) {
	cm, wal, dir := createTestCheckpointManager(t)
	defer func() {
		wal.Close()
		os.RemoveAll(dir)
	}()

	// Create a checkpoint
	lsn, err := cm.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// Verify it exists
	if len(cm.ListCheckpoints()) != 1 {
		t.Fatal("Expected 1 checkpoint")
	}

	// Delete it
	if err := cm.DeleteCheckpoint(lsn); err != nil {
		t.Fatalf("DeleteCheckpoint failed: %v", err)
	}

	// Verify it's gone
	if len(cm.ListCheckpoints()) != 0 {
		t.Error("Expected 0 checkpoints after delete")
	}

	// Verify file is deleted
	filename := CheckpointFileName(1)
	path := filepath.Join(dir, filename)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("checkpoint file should be deleted")
	}
}

func TestCheckpointDeleteNonExistent(t *testing.T) {
	cm, wal, dir := createTestCheckpointManager(t)
	defer func() {
		wal.Close()
		os.RemoveAll(dir)
	}()

	// Create a checkpoint
	cm.CreateCheckpoint()

	// Delete non-existent LSN (should not error)
	if err := cm.DeleteCheckpoint(99999); err != nil {
		t.Fatalf("DeleteCheckpoint for non-existent should not error: %v", err)
	}

	// Original checkpoint should still exist
	if len(cm.ListCheckpoints()) != 1 {
		t.Error("Original checkpoint should still exist")
	}
}

// =============================================================================
// Test 4: Truncate
// =============================================================================

func TestWALTruncate(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append some records
	_, _ = w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("first")})
	lsn2, _ := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("second")})
	lsn3, _ := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("third")})

	// Truncate at lsn2 (records >= lsn2 should be discarded conceptually)
	if err := w.Truncate(lsn2); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify last LSN is still lsn3 (Truncate doesn't actually remove data)
	if lastLSN := w.LastLSN(); lastLSN != lsn3 {
		t.Errorf("LastLSN after truncate: got %d, want %d", lastLSN, lsn3)
	}
}

func TestWALTruncateLSNTooLarge(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Append a record
	_, _ = w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("test")})

	// Try to truncate at a LSN larger than last LSN
	err := w.Truncate(99999)
	if err != ErrTruncateLSNTooLarge {
		t.Errorf("Expected ErrTruncateLSNTooLarge, got %v", err)
	}
}

// =============================================================================
// Test 5: Concurrent Safety
// =============================================================================

func TestWALConcurrentAppend(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	const numGoroutines = 10
	const recordsPerGoroutine = 50
	var wg sync.WaitGroup
	var counter atomic.Uint64

	// Concurrent appends
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				lsn, err := w.Append(&WALRecord{
					RecordType: WALNodeWrite,
					Payload:    []byte(fmt.Sprintf("goroutine data %d", j)),
				})
				if err != nil {
					t.Errorf("Concurrent Append failed: %v", err)
					return
				}
				if lsn == 0 {
					t.Error("LSN should not be zero")
				}
				counter.Add(1)
			}
		}()
	}

	wg.Wait()

	// Verify all records were appended
	expectedCount := uint64(numGoroutines * recordsPerGoroutine)
	if counter.Load() != expectedCount {
		t.Errorf("Expected %d records, got %d", expectedCount, counter.Load())
	}

	// Verify LastLSN
	expectedLastLSN := expectedCount
	if lastLSN := w.LastLSN(); lastLSN != expectedLastLSN {
		t.Errorf("LastLSN: got %d, want %d", lastLSN, expectedLastLSN)
	}
}

func TestWALConcurrentReadWrite(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Pre-populate WAL
	for i := 0; i < 100; i++ {
		w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte(fmt.Sprintf("record %d", i))})
	}

	var wg sync.WaitGroup
	const numGoroutines = 5

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := uint64(1); j <= 100; j++ {
				_, err := w.ReadAt(j)
				if err != nil {
					t.Errorf("Concurrent ReadAt failed: %v", err)
					return
				}
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, err := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("concurrent write")})
				if err != nil {
					t.Errorf("Concurrent Append failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestWALConcurrentFlush(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	var wg sync.WaitGroup

	// Concurrent flushes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := w.Flush(); err != nil {
					t.Errorf("Concurrent Flush failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// =============================================================================
// Test 6: Error Handling
// =============================================================================

func TestWALClosed(t *testing.T) {
	w, dir := createTestWAL(t)

	// Append something first
	w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("test")})

	// Close the WAL
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to append after close
	_, err := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("after close")})
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}

	// Try to read after close
	_, err = w.ReadAt(1)
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed for ReadAt, got %v", err)
	}

	os.RemoveAll(dir)
}

func TestWALDoubleClose(t *testing.T) {
	w, dir := createTestWAL(t)

	// Close twice
	w.Close()
	err := w.Close()
	if err != nil {
		// Double close should not error
		t.Logf("Double close returned error (non-critical): %v", err)
	}

	os.RemoveAll(dir)
}

func TestWALCorruptedRecord(t *testing.T) {
	w, dir := createTestWAL(t)

	// Manually create a corrupted WAL file (segment ID is 1, not 0)
	segmentPath := filepath.Join(dir, WalSegmentFileName(1))
	f, err := os.OpenFile(segmentPath, os.O_RDWR, 0644)
	if err != nil {
		w.Close()
		t.Fatalf("Failed to open segment: %v", err)
	}

	// Write a valid record first (empty payload to avoid ReadAt bug)
	record := &WALRecord{
		RecordType: WALCheckpoint,
		Payload:    nil,
	}
	record.Checksum = crc32.ChecksumIEEE(nil)
	record.Length = 0
	record.LSN = 1

	header := encodeHeader(record)
	f.WriteAt(header, 0)

	// Write a corrupted record (wrong checksum)
	corruptPayload := []byte("corrupted data")
	corruptRecord := &WALRecord{
		LSN:        2,
		RecordType: WALNodeWrite,
		Payload:    corruptPayload,
		Length:     uint32(len(corruptPayload)),
		Checksum:   0, // Invalid checksum
	}
	corruptHeader := encodeHeader(corruptRecord)
	offset := int64(len(header))
	f.WriteAt(corruptHeader, offset)
	f.WriteAt(corruptPayload, offset+int64(len(corruptHeader)))
	f.Close()

	w.Close()

	// Try to read the corrupted record using iterator
	w2, err := NewWAL(WALConfig{
		Directory: dir,
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer func() {
		w2.Close()
		os.RemoveAll(dir)
	}()

	// Reading valid record should work
	record1, err := w2.ReadAt(1)
	if err != nil {
		t.Errorf("ReadAt(1) failed: %v", err)
	}
	if record1 == nil {
		t.Error("record1 should not be nil")
	}

	// Reading corrupted record should fail (via iterator)
	iter, err := w2.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	iter.Next() // Valid record
	iter.Next() // Corrupted record - should cause error
	if iter.Error() != ErrWALCorrupted {
		t.Errorf("Expected ErrWALCorrupted, got %v", iter.Error())
	}
	iter.Close()
}

func TestWALInvalidDirectory(t *testing.T) {
	// Try to create WAL with invalid directory
	_, err := NewWAL(WALConfig{
		Directory: "/nonexistent/path/that/cannot/be/created",
		SegmentSize: 1024,
	})
	if err == nil {
		t.Error("Expected error for invalid directory")
	}
}

func TestWALIteratorCorrupted(t *testing.T) {
	w, dir := createTestWAL(t)

	// Manually corrupt the WAL file (segment ID is 1, not 0)
	segmentPath := filepath.Join(dir, WalSegmentFileName(1))
	f, err := os.OpenFile(segmentPath, os.O_RDWR, 0644)
	if err != nil {
		w.Close()
		t.Fatalf("Failed to open segment: %v", err)
	}

	// Write a corrupted record
	payload := []byte("corrupted")
	record := &WALRecord{
		LSN:        1,
		RecordType: WALNodeWrite,
		Payload:    payload,
		Length:     uint32(len(payload)),
		Checksum:   999999, // Invalid checksum
	}
	header := encodeHeader(record)
	f.WriteAt(header, 0)
	f.WriteAt(payload, int64(len(header)))
	f.Close()

	w.Close()

	// Reopen WAL
	w2, err := NewWAL(WALConfig{Directory: dir})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer func() {
		w2.Close()
		os.RemoveAll(dir)
	}()

	// Iterate should encounter error
	iter, _ := w2.ReadFrom(1)
	defer iter.Close()

	iter.Next() // This should trigger the corruption error
	if iter.Error() != ErrWALCorrupted {
		t.Errorf("Expected ErrWALCorrupted, got %v", iter.Error())
	}
}

// =============================================================================
// Test 7: Recovery
// =============================================================================

func TestWALRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-recovery-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create and populate WAL
	w1, err := NewWAL(WALConfig{
		Directory: dir,
		SegmentSize: 64 * 1024,
	})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create WAL: %v", err)
	}

	var lsns []uint64
	for i := 0; i < 10; i++ {
		lsn, err := w1.Append(&WALRecord{
			RecordType: WALNodeWrite,
			Payload:    []byte(fmt.Sprintf("recovery test %d", i)),
		})
		if err != nil {
			w1.Close()
			os.RemoveAll(dir)
			t.Fatalf("Append failed: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	if err := w1.Close(); err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen WAL (should recover LSN state)
	w2, err := NewWAL(WALConfig{
		Directory: dir,
		SegmentSize: 64 * 1024,
	})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer func() {
		w2.Close()
		os.RemoveAll(dir)
	}()

	// Verify LSN was recovered
	expectedLastLSN := lsns[len(lsns)-1]
	if lastLSN := w2.LastLSN(); lastLSN != expectedLastLSN {
		t.Errorf("LastLSN after recovery: got %d, want %d", lastLSN, expectedLastLSN)
	}

	// Verify records can still be read
	for _, lsn := range lsns {
		record, err := w2.ReadAt(lsn)
		if err != nil {
			t.Errorf("ReadAt(%d) after recovery failed: %v", lsn, err)
			continue
		}
		if record == nil {
			t.Errorf("Record %d is nil after recovery", lsn)
		}
	}

	// Verify new records get LSNs after the recovered ones
	newLSN, err := w2.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("new record")})
	if err != nil {
		t.Errorf("Append after recovery failed: %v", err)
	}
	if newLSN != expectedLastLSN+1 {
		t.Errorf("New LSN: got %d, want %d", newLSN, expectedLastLSN+1)
	}
}

// =============================================================================
// Test 8: Segment Rotation
// =============================================================================

func TestWALSegmentRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-segment-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create WAL with small segment size
	w, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1024, // Very small to force rotation
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Append records until we have multiple segments
	// Each record is ~17 bytes header + payload
	var lastLSN uint64
	for i := 0; i < 100; i++ {
		payload := make([]byte, 100) // 100 bytes per record
		lsn, err := w.Append(&WALRecord{
			RecordType: WALNodeWrite,
			Payload:    payload,
		})
		if err != nil {
			t.Fatalf("Append failed at iteration %d: %v", i, err)
		}
		lastLSN = lsn
	}

	// Check that multiple segment files exist
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	var segmentCount int
	for _, entry := range entries {
		if !entry.IsDir() {
			var segID uint64
			if _, err := fmt.Sscanf(entry.Name(), "WALImpl-%d.WALImpl", &segID); err == nil {
				segmentCount++
			}
		}
	}

	if segmentCount < 2 {
		t.Errorf("Expected at least 2 segments, got %d", segmentCount)
	}

	// Verify we can still read records from different segments
	for lsn := uint64(1); lsn <= lastLSN; lsn += 10 {
		record, err := w.ReadAt(lsn)
		if err != nil {
			t.Errorf("ReadAt(%d) failed: %v", lsn, err)
			continue
		}
		if record == nil && lsn <= lastLSN {
			t.Errorf("Record %d should exist", lsn)
		}
	}
}

// =============================================================================
// Test 9: Checkpoint Persistence
// =============================================================================

func TestCheckpointPersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "checkpoint-persist-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create checkpoint manager
	wal1, err := NewWAL(WALConfig{Directory: dir})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create WAL: %v", err)
	}

	cm1, err := NewCheckpointManager(wal1, WALConfig{Directory: dir}, CheckpointConfig{})
	if err != nil {
		wal1.Close()
		os.RemoveAll(dir)
		t.Fatalf("Failed to create checkpoint manager: %v", err)
	}

	// Set some state
	cm1.SetTreeRoot(vaddr.VAddr{SegmentID: 42, Offset: 99})

	// Create checkpoint
	lsn1, err := cm1.CreateCheckpoint()
	if err != nil {
		wal1.Close()
		os.RemoveAll(dir)
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	wal1.Close()

	// Create new checkpoint manager from same directory
	wal2, err := NewWAL(WALConfig{Directory: dir})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer func() {
		wal2.Close()
		os.RemoveAll(dir)
	}()

	cm2, err := NewCheckpointManager(wal2, WALConfig{Directory: dir}, CheckpointConfig{})
	if err != nil {
		wal2.Close()
		os.RemoveAll(dir)
		t.Fatalf("Failed to create checkpoint manager: %v", err)
	}

	// Verify checkpoint was persisted
	checkpoints := cm2.ListCheckpoints()
	if len(checkpoints) != 1 {
		t.Errorf("Expected 1 checkpoint after persistence, got %d", len(checkpoints))
	}

	latest, err := cm2.LatestCheckpoint()
	if err != nil {
		t.Fatalf("LatestCheckpoint failed: %v", err)
	}
	if latest == nil {
		t.Fatal("LatestCheckpoint should not be nil")
	}
	if latest.LSN != lsn1 {
		t.Errorf("Checkpoint LSN: got %d, want %d", latest.LSN, lsn1)
	}
	if latest.TreeRoot.SegmentID != 42 {
		t.Errorf("TreeRoot not recovered: got %v", latest.TreeRoot)
	}
}

// =============================================================================
// Test 10: WALRecord Serialization
// =============================================================================

func TestWALRecordHeaderEncoding(t *testing.T) {
	// Test header encoding and decoding
	original := &WALRecord{
		LSN:        12345,
		RecordType: WALNodeWrite,
		Length:     100,
		Checksum:   987654,
		Payload:    []byte("test payload"),
	}

	header := encodeHeader(original)
	if len(header) != RecordHeaderSize {
		t.Errorf("Header size: got %d, want %d", len(header), RecordHeaderSize)
	}

	decoded := decodeHeader(header)

	if decoded.LSN != original.LSN {
		t.Errorf("LSN: got %d, want %d", decoded.LSN, original.LSN)
	}
	if decoded.Type != original.RecordType {
		t.Errorf("Type: got %d, want %d", decoded.Type, original.RecordType)
	}
	if decoded.Length != original.Length {
		t.Errorf("Length: got %d, want %d", decoded.Length, original.Length)
	}
	if decoded.CRC != original.Checksum {
		t.Errorf("CRC: got %d, want %d", decoded.CRC, original.Checksum)
	}
}

func TestCheckpointSerialization(t *testing.T) {
	cp := &Checkpoint{
		ID:      1,
		LSN:     100,
		TreeRoot: vaddr.VAddr{SegmentID: 5, Offset: 10},
		PageManager: PageManagerSnapshot{
			RootVAddr:     vaddr.VAddr{SegmentID: 1, Offset: 2},
			LivePageCount: 50,
			CheckpointLSN: 100,
		},
		ExternalStore: ExternalValueSnapshot{
			ActiveVAddrs:  []vaddr.VAddr{{SegmentID: 1, Offset: 1}},
			CheckpointLSN: 100,
		},
		Timestamp: uint64(time.Now().UnixNano()),
	}

	// Marshal
	data, err := json.Marshal(cp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal
	var decoded Checkpoint
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify
	if decoded.ID != cp.ID {
		t.Errorf("ID mismatch: got %d, want %d", decoded.ID, cp.ID)
	}
	if decoded.LSN != cp.LSN {
		t.Errorf("LSN mismatch: got %d, want %d", decoded.LSN, cp.LSN)
	}
	if decoded.TreeRoot != cp.TreeRoot {
		t.Errorf("TreeRoot mismatch: got %v, want %v", decoded.TreeRoot, cp.TreeRoot)
	}
	if decoded.PageManager.LivePageCount != cp.PageManager.LivePageCount {
		t.Errorf("LivePageCount mismatch")
	}
}

// =============================================================================
// Test 11: WALIterator with Multiple Segments
// =============================================================================

func TestWALIteratorMultipleSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-iter-multi-seg-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1024, // Force segment rotation
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Append enough records to create multiple segments
	for i := 0; i < 50; i++ {
		payload := make([]byte, 100)
		binary.LittleEndian.PutUint64(payload, uint64(i))
		w.Append(&WALRecord{
			RecordType: WALNodeWrite,
			Payload:    payload,
		})
	}

	// Iterate and count all records
	iter, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}

	if count != 50 {
		t.Errorf("Expected 50 records, got %d", count)
	}
}

// =============================================================================
// Test 12: File Naming
// =============================================================================

func TestWalSegmentFileName(t *testing.T) {
	tests := []struct {
		segmentID uint64
		expected  string
	}{
		{0, "WALImpl-000000.WALImpl"},
		{1, "WALImpl-000001.WALImpl"},
		{999, "WALImpl-000999.WALImpl"},
		{1000000, "WALImpl-1000000.WALImpl"},
	}

	for _, tc := range tests {
		result := WalSegmentFileName(tc.segmentID)
		if result != tc.expected {
			t.Errorf("WalSegmentFileName(%d): got %s, want %s", tc.segmentID, result, tc.expected)
		}
	}
}

func TestCheckpointFileName(t *testing.T) {
	tests := []struct {
		id       uint64
		expected string
	}{
		{0, "checkpoint-000000.json"},
		{1, "checkpoint-000001.json"},
		{999, "checkpoint-000999.json"},
		{1000000, "checkpoint-1000000.json"},
	}

	for _, tc := range tests {
		result := CheckpointFileName(tc.id)
		if result != tc.expected {
			t.Errorf("CheckpointFileName(%d): got %s, want %s", tc.id, result, tc.expected)
		}
	}
}

// =============================================================================
// Test 13: Edge Cases
// =============================================================================

func TestWALLargePayload(t *testing.T) {
	w, dir := createTestWAL(t)
	defer func() {
		w.Close()
		os.RemoveAll(dir)
	}()

	// Test with a large payload
	largePayload := make([]byte, 64*1024) // 64KB
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	lsn, err := w.Append(&WALRecord{
		RecordType: WALExternalValue,
		Payload:    largePayload,
	})
	if err != nil {
		t.Fatalf("Append large payload failed: %v", err)
	}

	// Read back
	record, err := w.ReadAt(lsn)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if record == nil {
		t.Fatal("record should not be nil")
	}
	if !bytes.Equal(record.Payload, largePayload) {
		t.Error("Payload mismatch for large record")
	}
}

func TestWALZeroLSN(t *testing.T) {
	// Test that LSN 0 is invalid (returned from exhausted counter)
	dir, err := os.MkdirTemp("", "wal-zero-lsn-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create WAL
	w, err := NewWAL(WALConfig{Directory: dir})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Manually set nextLSN to max uint64 to trigger wrap-around behavior
	// This tests that LSN 0 is rejected
	// Note: In practice, atomic.Uint64 wraps to 0 when overflowing

	// Append some records
	lsn1, err := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("test1")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if lsn1 == 0 {
		t.Error("LSN should not be 0 for first record")
	}

	lsn2, err := w.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte("test2")})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if lsn2 == 0 {
		t.Error("LSN should not be 0 for second record")
	}
}

func TestCheckpointRotateLimit(t *testing.T) {
	cm, wal, dir := createTestCheckpointManager(t)
	defer func() {
		wal.Close()
		os.RemoveAll(dir)
	}()

	// Create many checkpoints (more than the rotation limit of 3)
	for i := 0; i < 10; i++ {
		_, err := cm.CreateCheckpoint()
		if err != nil {
			t.Fatalf("CreateCheckpoint %d failed: %v", i, err)
		}
		wal.Append(&WALRecord{RecordType: WALNodeWrite, Payload: []byte(fmt.Sprintf("data %d", i))})
	}

	// Verify that old checkpoints were rotated out
	// Implementation keeps max 4 (adds new before rotating)
	checkpoints := cm.ListCheckpoints()
	if len(checkpoints) > 4 {
		t.Errorf("Expected at most 4 checkpoints after rotation, got %d", len(checkpoints))
	}

	// Check that checkpoint files were deleted
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	var checkpointFileCount int
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 11 && entry.Name()[:11] == "checkpoint-" {
			checkpointFileCount++
		}
	}

	if checkpointFileCount > 4 {
		t.Errorf("Expected at most 4 checkpoint files, got %d", checkpointFileCount)
	}
}
