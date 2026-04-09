package internal

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// openBatchTestStore opens a temporary KVStore for batch testing.
func openBatchTestStore(t *testing.T) kvstoreapi.Store {
	t.Helper()
	dir := t.TempDir()
	cfg := kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 64 * 1024 * 1024,
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestWriteBatch_Basic puts 100 keys in a batch, commits, then verifies all readable.
func TestWriteBatch_Basic(t *testing.T) {
	s := openBatchTestStore(t)

	batch := s.NewWriteBatch()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		if err := batch.Put(key, val); err != nil {
			t.Fatalf("batch.Put(%d): %v", i, err)
		}
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit: %v", err)
	}

	// Verify all keys are readable
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expected := []byte(fmt.Sprintf("val-%05d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if string(val) != string(expected) {
			t.Fatalf("Get(%d): got %q, want %q", i, val, expected)
		}
	}
}

// TestWriteBatch_MixedOps puts keys then deletes some in the same batch.
func TestWriteBatch_MixedOps(t *testing.T) {
	s := openBatchTestStore(t)

	// First, put some keys that we'll delete in the batch
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("existing-%05d", i))
		val := []byte(fmt.Sprintf("old-val-%05d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("pre-Put(%d): %v", i, err)
		}
	}

	// Batch: put new keys + delete some existing keys
	batch := s.NewWriteBatch()
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("new-%05d", i))
		val := []byte(fmt.Sprintf("new-val-%05d", i))
		if err := batch.Put(key, val); err != nil {
			t.Fatalf("batch.Put(%d): %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("existing-%05d", i))
		if err := batch.Delete(key); err != nil {
			t.Fatalf("batch.Delete(%d): %v", i, err)
		}
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit: %v", err)
	}

	// Verify new keys exist
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("new-%05d", i))
		expected := []byte(fmt.Sprintf("new-val-%05d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get new(%d): %v", i, err)
		}
		if string(val) != string(expected) {
			t.Fatalf("Get new(%d): got %q, want %q", i, val, expected)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("existing-%05d", i))
		_, err := s.Get(key)
		if err != kvstoreapi.ErrKeyNotFound {
			t.Fatalf("Get deleted(%d): expected ErrKeyNotFound, got %v", i, err)
		}
	}

	// Verify non-deleted existing keys still exist
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("existing-%05d", i))
		expected := []byte(fmt.Sprintf("old-val-%05d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get existing(%d): %v", i, err)
		}
		if string(val) != string(expected) {
			t.Fatalf("Get existing(%d): got %q, want %q", i, val, expected)
		}
	}
}

// TestWriteBatch_Empty commits an empty batch — should succeed.
func TestWriteBatch_Empty(t *testing.T) {
	s := openBatchTestStore(t)
	batch := s.NewWriteBatch()
	if err := batch.Commit(); err != nil {
		t.Fatalf("empty batch.Commit: %v", err)
	}
}

// TestWriteBatch_DoubleCommit verifies error on second commit.
func TestWriteBatch_DoubleCommit(t *testing.T) {
	s := openBatchTestStore(t)

	batch := s.NewWriteBatch()
	batch.Put([]byte("key"), []byte("val"))
	if err := batch.Commit(); err != nil {
		t.Fatalf("first Commit: %v", err)
	}

	// Second commit should fail
	if err := batch.Commit(); err != kvstoreapi.ErrBatchCommitted {
		t.Fatalf("second Commit: expected ErrBatchCommitted, got %v", err)
	}

	// Put after commit should also fail
	if err := batch.Put([]byte("key2"), []byte("val2")); err != kvstoreapi.ErrBatchCommitted {
		t.Fatalf("Put after Commit: expected ErrBatchCommitted, got %v", err)
	}

	// Delete after commit should also fail
	if err := batch.Delete([]byte("key")); err != kvstoreapi.ErrBatchCommitted {
		t.Fatalf("Delete after Commit: expected ErrBatchCommitted, got %v", err)
	}
}

// TestWriteBatch_Discard verifies discard doesn't write.
func TestWriteBatch_Discard(t *testing.T) {
	s := openBatchTestStore(t)

	batch := s.NewWriteBatch()
	batch.Put([]byte("discard-key"), []byte("discard-val"))
	batch.Discard()

	// Key should not exist
	_, err := s.Get([]byte("discard-key"))
	if err != kvstoreapi.ErrKeyNotFound {
		t.Fatalf("Get after Discard: expected ErrKeyNotFound, got %v", err)
	}

	// Operations after discard should fail
	if err := batch.Put([]byte("key"), []byte("val")); err != kvstoreapi.ErrBatchCommitted {
		t.Fatalf("Put after Discard: expected ErrBatchCommitted, got %v", err)
	}
	if err := batch.Commit(); err != kvstoreapi.ErrBatchCommitted {
		t.Fatalf("Commit after Discard: expected ErrBatchCommitted, got %v", err)
	}
}

// TestWriteBatch_LargeValues tests the blob path (values > 256 bytes).
func TestWriteBatch_LargeValues(t *testing.T) {
	s := openBatchTestStore(t)

	// Generate large values (512 bytes each)
	rng := rand.New(rand.NewSource(42))
	largeVal := make([]byte, 512)
	rng.Read(largeVal)

	batch := s.NewWriteBatch()
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("blob-key-%05d", i))
		if err := batch.Put(key, largeVal); err != nil {
			t.Fatalf("batch.Put large(%d): %v", i, err)
		}
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit: %v", err)
	}

	// Verify all large values are readable
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("blob-key-%05d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get large(%d): %v", i, err)
		}
		if len(val) != 512 {
			t.Fatalf("Get large(%d): got len=%d, want 512", i, len(val))
		}
	}
}

// TestWriteBatch_Concurrent tests multiple batches from different goroutines.
func TestWriteBatch_Concurrent(t *testing.T) {
	s := openBatchTestStore(t)

	const numGoroutines = 10
	const keysPerBatch = 50

	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			batch := s.NewWriteBatch()
			for i := 0; i < keysPerBatch; i++ {
				key := []byte(fmt.Sprintf("g%02d-key-%05d", gid, i))
				val := []byte(fmt.Sprintf("g%02d-val-%05d", gid, i))
				if err := batch.Put(key, val); err != nil {
					errs <- fmt.Errorf("goroutine %d Put(%d): %v", gid, i, err)
					return
				}
			}
			if err := batch.Commit(); err != nil {
				errs <- fmt.Errorf("goroutine %d Commit: %v", gid, err)
				return
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	// Verify all keys
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < keysPerBatch; i++ {
			key := []byte(fmt.Sprintf("g%02d-key-%05d", g, i))
			expected := []byte(fmt.Sprintf("g%02d-val-%05d", g, i))
			val, err := s.Get(key)
			if err != nil {
				t.Fatalf("Get g%d key%d: %v", g, i, err)
			}
			if string(val) != string(expected) {
				t.Fatalf("Get g%d key%d: got %q, want %q", g, i, val, expected)
			}
		}
	}
}

// TestWriteBatch_Persistence tests that batch writes survive close+reopen.
func TestWriteBatch_Persistence(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("batch-persist-%d", rand.Int63()))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	cfg := kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	// Write batch and close
	{
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}

		batch := s.NewWriteBatch()
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("persist-%05d", i))
			val := []byte(fmt.Sprintf("persist-val-%05d", i))
			batch.Put(key, val)
		}
		if err := batch.Commit(); err != nil {
			t.Fatalf("batch.Commit: %v", err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen and verify
	{
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("persist-%05d", i))
			expected := []byte(fmt.Sprintf("persist-val-%05d", i))
			val, err := s.Get(key)
			if err != nil {
				t.Fatalf("Get after reopen(%d): %v", i, err)
			}
			if string(val) != string(expected) {
				t.Fatalf("Get after reopen(%d): got %q, want %q", i, val, expected)
			}
		}
	}
}

// TestWriteBatch_DeleteNonExistent tests that deleting a non-existent key in a batch fails.
func TestWriteBatch_DeleteNonExistent(t *testing.T) {
	s := openBatchTestStore(t)

	batch := s.NewWriteBatch()
	batch.Delete([]byte("nonexistent-key"))
	err := batch.Commit()
	if err != kvstoreapi.ErrKeyNotFound {
		t.Fatalf("Delete nonexistent: expected ErrKeyNotFound, got %v", err)
	}
}