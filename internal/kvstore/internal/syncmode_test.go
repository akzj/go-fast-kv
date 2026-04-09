package internal

import (
	"fmt"
	"os"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// openSyncModeStore opens a store with the given SyncMode.
func openSyncModeStore(t *testing.T, dir string, mode kvstoreapi.SyncMode) kvstoreapi.Store {
	t.Helper()
	cfg := kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 64 * 1024 * 1024,
		SyncMode:       mode,
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open(%v): %v", mode, err)
	}
	return s
}

func TestSyncMode_Default(t *testing.T) {
	dir, err := os.MkdirTemp("", "syncmode-default-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Default config — SyncAlways (zero value)
	s := openSyncModeStore(t, dir, kvstoreapi.SyncAlways)

	// Put some keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	// Verify all readable
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("val-%05d", i)
		if string(val) != expected {
			t.Fatalf("Get(%s) = %q, want %q", key, val, expected)
		}
	}

	// Checkpoint + Close for persistence
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify persistence
	s2 := openSyncModeStore(t, dir, kvstoreapi.SyncAlways)
	defer s2.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val, err := s2.Get(key)
		if err != nil {
			t.Fatalf("Reopen Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("val-%05d", i)
		if string(val) != expected {
			t.Fatalf("Reopen Get(%s) = %q, want %q", key, val, expected)
		}
	}
}

func TestSyncMode_None(t *testing.T) {
	dir, err := os.MkdirTemp("", "syncmode-none-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := openSyncModeStore(t, dir, kvstoreapi.SyncNone)

	// Put 1000 keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	// Verify all readable in current session
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("val-%06d", i)
		if string(val) != expected {
			t.Fatalf("Get(%s) = %q, want %q", key, val, expected)
		}
	}

	// Checkpoint + Close (checkpoint fsyncs segments, close fsyncs WAL)
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify all data persisted
	s2 := openSyncModeStore(t, dir, kvstoreapi.SyncNone)
	defer s2.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val, err := s2.Get(key)
		if err != nil {
			t.Fatalf("Reopen Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("val-%06d", i)
		if string(val) != expected {
			t.Fatalf("Reopen Get(%s) = %q, want %q", key, val, expected)
		}
	}
}

func TestSyncMode_None_WriteBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "syncmode-none-batch-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := openSyncModeStore(t, dir, kvstoreapi.SyncNone)

	// Use WriteBatch with SyncNone
	batch := s.NewWriteBatch()
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("batch-key-%06d", i))
		val := []byte(fmt.Sprintf("batch-val-%06d", i))
		if err := batch.Put(key, val); err != nil {
			t.Fatalf("batch.Put: %v", err)
		}
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit: %v", err)
	}

	// Verify readable
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("batch-key-%06d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("batch-val-%06d", i)
		if string(val) != expected {
			t.Fatalf("Get(%s) = %q, want %q", key, val, expected)
		}
	}

	// Checkpoint + Close and reopen
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := openSyncModeStore(t, dir, kvstoreapi.SyncNone)
	defer s2.Close()

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("batch-key-%06d", i))
		val, err := s2.Get(key)
		if err != nil {
			t.Fatalf("Reopen Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("batch-val-%06d", i)
		if string(val) != expected {
			t.Fatalf("Reopen Get(%s) = %q, want %q", key, val, expected)
		}
	}
}

func TestSyncMode_None_Checkpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "syncmode-none-cp-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := openSyncModeStore(t, dir, kvstoreapi.SyncNone)

	// Put keys, then checkpoint
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("cp-key-%05d", i))
		val := []byte(fmt.Sprintf("cp-val-%05d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	// Checkpoint fsyncs segments regardless of SyncMode
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Write more keys AFTER checkpoint (in WAL only)
	for i := 200; i < 250; i++ {
		key := []byte(fmt.Sprintf("cp-key-%05d", i))
		val := []byte(fmt.Sprintf("cp-val-%05d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — should recover checkpoint (200 keys) + WAL replay (50 keys)
	s2 := openSyncModeStore(t, dir, kvstoreapi.SyncNone)
	defer s2.Close()

	for i := 0; i < 250; i++ {
		key := []byte(fmt.Sprintf("cp-key-%05d", i))
		val, err := s2.Get(key)
		if err != nil {
			t.Fatalf("Reopen Get(%s): %v", key, err)
		}
		expected := fmt.Sprintf("cp-val-%05d", i)
		if string(val) != expected {
			t.Fatalf("Reopen Get(%s) = %q, want %q", key, val, expected)
		}
	}
}
