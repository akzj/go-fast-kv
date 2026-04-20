package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// TestBackupRestore tests the Backup/Restore functionality.
func TestBackupRestore(t *testing.T) {
	storeDir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: storeDir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// Write more data to ensure checkpoint has real work.
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := s.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}

	// Checkpoint (async - wait for it to complete).
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // Wait for async checkpoint to complete

	// Verify checkpoint file exists.
	cpPath := filepath.Join(storeDir, "checkpoint")
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Fatal("checkpoint file not created")
	}

	// Backup.
	backupDir := filepath.Join(t.TempDir(), "backup")
	internalStore := s.(*store)
	if err := internalStore.Backup(backupDir); err != nil {
		t.Fatalf("failed to backup: %v", err)
	}

	// Verify manifest.
	manifestPath := filepath.Join(backupDir, "backup.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}

	var manifest backupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("failed to parse manifest: %v", err)
	}

	if manifest.Version != 1 {
		t.Errorf("expected manifest version 1, got %d", manifest.Version)
	}
	if manifest.CheckpointLSN == 0 {
		t.Error("expected non-zero checkpoint LSN")
	}

	// Check all files in manifest exist.
	for _, entry := range manifest.Files {
		fullPath := filepath.Join(backupDir, entry.Name)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Errorf("manifest references missing file: %s", entry.Name)
		}
	}

	// Close store.
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// Restore.
	restoreDir := filepath.Join(t.TempDir(), "restore")
	restoredStore, err := Restore(backupDir, restoreDir)
	if err != nil {
		t.Fatalf("failed to restore: %v", err)
	}
	defer restoredStore.Close()

	// Verify data restored.
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val, err := restoredStore.Get(key)
		if err != nil {
			t.Errorf("failed to get key-%d after restore: %v", i, err)
			continue
		}
		expected := []byte(fmt.Sprintf("value-%d", i))
		if string(val) != string(expected) {
			t.Errorf("key-%d: expected %s, got %s", i, expected, val)
		}
	}
}

// TestBackupEmptyStore tests backup on an empty store.
func TestBackupEmptyStore(t *testing.T) {
	storeDir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: storeDir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	backupDir := filepath.Join(t.TempDir(), "backup")

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}
	internalStore := s.(*store)
	if err := internalStore.Backup(backupDir); err != nil {
		t.Fatalf("backup of empty store failed: %v", err)
	}
}

// TestBackupChecksumMismatch verifies restore detects corrupted files.
func TestBackupChecksumMismatch(t *testing.T) {
	storeDir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: storeDir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if err := s.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // Wait for async checkpoint to complete

	backupDir := filepath.Join(t.TempDir(), "backup-corrupt")
	internalStore := s.(*store)
	if err := internalStore.Backup(backupDir); err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	// Corrupt a file in the backup.
	corruptPath := filepath.Join(backupDir, "checkpoint")
	if err := os.WriteFile(corruptPath, []byte("corrupted data"), 0644); err != nil {
		t.Fatalf("failed to corrupt file: %v", err)
	}

	// Restore should fail.
	restoreDir := filepath.Join(t.TempDir(), "restore-corrupt")
	_, err = Restore(backupDir, restoreDir)
	if err == nil {
		t.Error("expected error for corrupted backup, got nil")
	}
}