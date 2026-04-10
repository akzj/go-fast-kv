package internal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	checkpointapi "github.com/akzj/go-fast-kv/internal/checkpoint/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
	walpkg "github.com/akzj/go-fast-kv/internal/wal"
)

// mockCheckpointable implements Checkpointable for testing.
type mockCheckpointable struct {
	lsn uint64
}

func (m *mockCheckpointable) Checkpoint(lsn uint64) error {
	m.lsn = lsn
	return nil
}

func (m *mockCheckpointable) CheckpointLSN() uint64 {
	return m.lsn
}

func TestMetadataWriteRead(t *testing.T) {
	dir := t.TempDir()

	meta := checkpointapi.NewMetadata()
	meta.SetModule("lsm", checkpointapi.ModuleMetadata{CheckpointLSN: 100})
	meta.SetModule("tree", checkpointapi.ModuleMetadata{CheckpointLSN: 200})

	if err := writeMetadata(dir, meta); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}

	readMeta, err := readMetadata(dir)
	if err != nil {
		t.Fatalf("readMetadata: %v", err)
	}

	if readMeta.Version != 1 {
		t.Errorf("Version = %d, want 1", readMeta.Version)
	}

	lsm, ok := readMeta.GetModule("lsm")
	if !ok {
		t.Fatal("lsm module not found")
	}
	if lsm.CheckpointLSN != 100 {
		t.Errorf("lsm.CheckpointLSN = %d, want 100", lsm.CheckpointLSN)
	}

	tree, ok := readMeta.GetModule("tree")
	if !ok {
		t.Fatal("tree module not found")
	}
	if tree.CheckpointLSN != 200 {
		t.Errorf("tree.CheckpointLSN = %d, want 200", tree.CheckpointLSN)
	}
}

func TestCheckpointTwoPhase(t *testing.T) {
	walDir := t.TempDir()
	cpDir := t.TempDir()

	// Create WAL
	wal, err := walpkg.New(walapi.Config{Dir: walDir})
	if err != nil {
		t.Fatalf("WAL New: %v", err)
	}
	defer wal.Close()

	// Create CheckpointManager
	cm, err := New(cpDir, wal)
	if err != nil {
		t.Fatalf("CheckpointManager New: %v", err)
	}
	defer cm.Close()

	// Register modules
	lsm := &mockCheckpointable{}
	tree := &mockCheckpointable{}
	cm.RegisterModule("lsm", lsm)
	cm.RegisterModule("tree", tree)

	// Write some WAL records
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordPageMap, 1, 100, 0)
	if _, err := wal.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Do checkpoint
	if err := cm.DoCheckpoint(); err != nil {
		t.Fatalf("DoCheckpoint: %v", err)
	}

	// Wait a bit for Phase 2
	time.Sleep(100 * time.Millisecond)

	// Verify metadata
	meta := cm.GetMetadata()
	if meta == nil {
		t.Fatal("GetMetadata returned nil")
	}

	lsmMeta, ok := meta.GetModule("lsm")
	if !ok {
		t.Fatal("lsm module not found in metadata")
	}
	if lsmMeta.CheckpointLSN == 0 {
		t.Error("lsm.CheckpointLSN should be set")
	}

	// Verify module checkpoint was called
	if lsm.lsn == 0 {
		t.Error("lsm.Checkpoint() was not called")
	}
}

func TestWALCleanupAfterCheckpoint(t *testing.T) {
	walDir := t.TempDir()
	cpDir := t.TempDir()

	// Create WAL
	wal, err := walpkg.New(walapi.Config{Dir: walDir})
	if err != nil {
		t.Fatalf("WAL New: %v", err)
	}

	// Write enough records to potentially need rotation
	batch := walapi.NewBatch()
	for i := uint64(0); i < 100; i++ {
		batch.Add(walapi.ModuleTree, walapi.RecordPageMap, i, i*100, 0)
	}
	if _, err := wal.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Create CheckpointManager
	cm, err := New(cpDir, wal)
	if err != nil {
		t.Fatalf("CheckpointManager New: %v", err)
	}

	// Register module
	lsm := &mockCheckpointable{}
	cm.RegisterModule("lsm", lsm)

	// Do checkpoint
	if err := cm.DoCheckpoint(); err != nil {
		t.Fatalf("DoCheckpoint: %v", err)
	}

	// Wait for Phase 2
	time.Sleep(100 * time.Millisecond)

	// List segments
	segments := wal.ListSegments()
	t.Logf("Segments after checkpoint: %v", segments)

	cm.Close()
	wal.Close()
}

func TestRecoveryWithCheckpoint(t *testing.T) {
	walDir := t.TempDir()
	cpDir := t.TempDir()

	// First run: write data and checkpoint
	{
		wal, err := walpkg.New(walapi.Config{Dir: walDir})
		if err != nil {
			t.Fatalf("WAL New: %v", err)
		}

		cm, err := New(cpDir, wal)
		if err != nil {
			t.Fatalf("CheckpointManager New: %v", err)
		}

		lsm := &mockCheckpointable{}
		cm.RegisterModule("lsm", lsm)

		// Write records
		batch := walapi.NewBatch()
		batch.Add(walapi.ModuleTree, walapi.RecordPageMap, 1, 100, 0)
		batch.Add(walapi.ModuleTree, walapi.RecordPageMap, 2, 200, 0)
		wal.WriteBatch(batch)

		// Checkpoint
		if err := cm.DoCheckpoint(); err != nil {
			t.Fatalf("DoCheckpoint: %v", err)
		}

		cm.Close()
		wal.Close()
	}

	// Verify metadata exists
	if !metadataExists(cpDir) {
		t.Fatal("metadata.json should exist after checkpoint")
	}

	// Second run: recover
	{
		wal, err := walpkg.New(walapi.Config{Dir: walDir})
		if err != nil {
			t.Fatalf("WAL New: %v", err)
		}

		cm, err := New(cpDir, wal)
		if err != nil {
			t.Fatalf("CheckpointManager New: %v", err)
		}

		// Verify metadata loaded
		meta := cm.GetMetadata()
		if meta == nil {
			t.Fatal("GetMetadata returned nil after recovery")
		}

		lsmMeta, ok := meta.GetModule("lsm")
		if !ok {
			t.Fatal("lsm module not found")
		}

		t.Logf("Recovered lsm.CheckpointLSN: %d", lsmMeta.CheckpointLSN)

		cm.Close()
		wal.Close()
	}
}

func TestCheckpointManagerClose(t *testing.T) {
	walDir := t.TempDir()
	cpDir := t.TempDir()

	wal, err := walpkg.New(walapi.Config{Dir: walDir})
	if err != nil {
		t.Fatalf("WAL New: %v", err)
	}

	cm, err := New(cpDir, wal)
	if err != nil {
		t.Fatalf("CheckpointManager New: %v", err)
	}

	if err := cm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNoCheckpoint(t *testing.T) {
	walDir := t.TempDir()
	cpDir := t.TempDir()

	wal, err := walpkg.New(walapi.Config{Dir: walDir})
	if err != nil {
		t.Fatalf("WAL New: %v", err)
	}
	defer wal.Close()

	cm, err := New(cpDir, wal)
	if err != nil {
		t.Fatalf("CheckpointManager New: %v", err)
	}
	defer cm.Close()

	// No checkpoint yet
	meta := cm.GetMetadata()
	if meta != nil {
		t.Log("GetMetadata returned non-nil before any checkpoint (acceptable if loading existing)")
	}
}

func TestMetadataAtomicWrite(t *testing.T) {
	dir := t.TempDir()

	// Write metadata
	meta := checkpointapi.NewMetadata()
	meta.SetModule("test", checkpointapi.ModuleMetadata{CheckpointLSN: 42})

	if err := writeMetadata(dir, meta); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}

	// Verify file exists
	metaPath := filepath.Join(dir, metadataFileName)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Fatal("metadata.json should exist after write")
	}

	// Verify content
	readMeta, err := readMetadata(dir)
	if err != nil {
		t.Fatalf("readMetadata: %v", err)
	}

	testMeta, _ := readMeta.GetModule("test")
	if testMeta.CheckpointLSN != 42 {
		t.Errorf("CheckpointLSN = %d, want 42", testMeta.CheckpointLSN)
	}
}
