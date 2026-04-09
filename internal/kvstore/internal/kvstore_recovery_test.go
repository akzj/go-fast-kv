package internal

import (
	"bytes"
	"fmt"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── Recovery Tests ─────────────────────────────────────────────────
// These tests verify that data survives Close → Open cycles.
// Each test writes data, closes the store, re-opens at the same directory,
// and verifies all data is intact.

// TestRecovery_BasicPutGet writes N keys, closes, re-opens, verifies all keys.
func TestRecovery_BasicPutGet(t *testing.T) {
	dir := t.TempDir()
	const N = 100

	// Phase 1: write
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			val := []byte(fmt.Sprintf("value-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put(%s): %v", key, err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open and verify
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			expected := []byte(fmt.Sprintf("value-%04d", i))
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get(%s) = %q, want %q", key, got, expected)
			}
		}
	}
}

// TestRecovery_WALReplayNoCheckpoint writes data WITHOUT calling Checkpoint,
// then closes and re-opens. WAL replay must recover the data.
func TestRecovery_WALReplayNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	const N = 50

	// Phase 1: write without checkpoint
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("wal-key-%04d", i))
			val := []byte(fmt.Sprintf("wal-value-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put(%s): %v", key, err)
			}
		}
		// Explicitly NOT calling Checkpoint — data is only in WAL + segments
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open — WAL replay should recover everything
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("wal-key-%04d", i))
			expected := []byte(fmt.Sprintf("wal-value-%04d", i))
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get(%s) after WAL replay: %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get(%s) = %q, want %q", key, got, expected)
			}
		}
	}
}

// TestRecovery_CheckpointThenMore writes batch1, checkpoints, writes batch2,
// closes, re-opens. Both batches must be present.
func TestRecovery_CheckpointThenMore(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write batch1 → checkpoint → write batch2 → close
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		// Batch 1
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("batch1-%04d", i))
			val := []byte(fmt.Sprintf("value1-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put batch1(%s): %v", key, err)
			}
		}

		if err := s.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}

		// Batch 2 (after checkpoint — only in WAL)
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("batch2-%04d", i))
			val := []byte(fmt.Sprintf("value2-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put batch2(%s): %v", key, err)
			}
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open and verify both batches
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		// Verify batch 1
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("batch1-%04d", i))
			expected := []byte(fmt.Sprintf("value1-%04d", i))
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get batch1(%s): %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get batch1(%s) = %q, want %q", key, got, expected)
			}
		}

		// Verify batch 2
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("batch2-%04d", i))
			expected := []byte(fmt.Sprintf("value2-%04d", i))
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get batch2(%s): %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get batch2(%s) = %q, want %q", key, got, expected)
			}
		}
	}
}

// TestRecovery_WriteBatch uses WriteBatch API to write 100 keys,
// closes, re-opens, verifies all 100 keys.
func TestRecovery_WriteBatch(t *testing.T) {
	dir := t.TempDir()
	const N = 100

	// Phase 1: write via WriteBatch
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		batch := s.NewWriteBatch()
		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("batch-key-%04d", i))
			val := []byte(fmt.Sprintf("batch-value-%04d", i))
			if err := batch.Put(key, val); err != nil {
				t.Fatalf("batch.Put(%s): %v", key, err)
			}
		}
		if err := batch.Commit(); err != nil {
			t.Fatalf("batch.Commit: %v", err)
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open and verify
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("batch-key-%04d", i))
			expected := []byte(fmt.Sprintf("batch-value-%04d", i))
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get(%s) after WriteBatch recovery: %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get(%s) = %q, want %q", key, got, expected)
			}
		}
	}
}

// TestRecovery_Delete verifies that deleted keys remain deleted after recovery.
func TestRecovery_Delete(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: put, then delete some keys
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		// Write 20 keys
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("del-key-%04d", i))
			val := []byte(fmt.Sprintf("del-value-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put(%s): %v", key, err)
			}
		}

		// Delete even-numbered keys
		for i := 0; i < 20; i += 2 {
			key := []byte(fmt.Sprintf("del-key-%04d", i))
			if err := s.Delete(key); err != nil {
				t.Fatalf("Delete(%s): %v", key, err)
			}
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open and verify
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("del-key-%04d", i))
			_, err := s.Get(key)
			if i%2 == 0 {
				// Even keys were deleted — should be ErrKeyNotFound
				if err != kvstoreapi.ErrKeyNotFound {
					t.Errorf("Get(%s) = err %v, want ErrKeyNotFound (key was deleted)", key, err)
				}
			} else {
				// Odd keys should still exist
				if err != nil {
					t.Errorf("Get(%s): %v (key should exist)", key, err)
				}
			}
		}
	}
}

// TestRecovery_LargeDataset writes 10000 keys, closes, re-opens,
// verifies via Scan that all 10000 are present.
func TestRecovery_LargeDataset(t *testing.T) {
	dir := t.TempDir()
	const N = 10000

	// Phase 1: write 10000 keys
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("large-%06d", i))
			val := []byte(fmt.Sprintf("val-%06d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put(%s): %v", key, err)
			}
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open and verify via Scan
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		// Verify via Scan
		iter := s.Scan([]byte("large-"), []byte("large-~"))
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if count != N {
			t.Errorf("Scan returned %d keys, want %d", count, N)
		}

		// Also spot-check a few keys via Get
		for _, i := range []int{0, 100, 999, 5000, 9999} {
			key := []byte(fmt.Sprintf("large-%06d", i))
			expected := []byte(fmt.Sprintf("val-%06d", i))
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get(%s) = %q, want %q", key, got, expected)
			}
		}
	}
}

// TestRecovery_UpdateThenRecover verifies that updated values persist.
func TestRecovery_UpdateThenRecover(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write, then update
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		// Initial write
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("upd-key-%04d", i))
			val := []byte(fmt.Sprintf("original-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put(%s): %v", key, err)
			}
		}

		// Update even keys
		for i := 0; i < 20; i += 2 {
			key := []byte(fmt.Sprintf("upd-key-%04d", i))
			val := []byte(fmt.Sprintf("updated-%04d", i))
			if err := s.Put(key, val); err != nil {
				t.Fatalf("Put update(%s): %v", key, err)
			}
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: re-open and verify
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Re-Open: %v", err)
		}
		defer s.Close()

		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("upd-key-%04d", i))
			var expected []byte
			if i%2 == 0 {
				expected = []byte(fmt.Sprintf("updated-%04d", i))
			} else {
				expected = []byte(fmt.Sprintf("original-%04d", i))
			}
			got, err := s.Get(key)
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("Get(%s) = %q, want %q", key, got, expected)
			}
		}
	}
}

// TestRecovery_MultipleReopens opens and closes the store multiple times,
// adding data each time, verifying cumulative state.
func TestRecovery_MultipleReopens(t *testing.T) {
	dir := t.TempDir()

	// Round 1: write keys 0-9
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open round 1: %v", err)
		}
		for i := 0; i < 10; i++ {
			if err := s.Put([]byte(fmt.Sprintf("multi-%04d", i)), []byte(fmt.Sprintf("r1-%04d", i))); err != nil {
				t.Fatalf("Put round 1: %v", err)
			}
		}
		s.Close()
	}

	// Round 2: re-open, verify round 1, write keys 10-19
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open round 2: %v", err)
		}
		// Verify round 1
		for i := 0; i < 10; i++ {
			if _, err := s.Get([]byte(fmt.Sprintf("multi-%04d", i))); err != nil {
				t.Errorf("Round 2 verify round 1 key %d: %v", i, err)
			}
		}
		// Write round 2
		for i := 10; i < 20; i++ {
			if err := s.Put([]byte(fmt.Sprintf("multi-%04d", i)), []byte(fmt.Sprintf("r2-%04d", i))); err != nil {
				t.Fatalf("Put round 2: %v", err)
			}
		}
		s.Close()
	}

	// Round 3: re-open, verify all 20 keys
	{
		s, err := Open(kvstoreapi.Config{Dir: dir})
		if err != nil {
			t.Fatalf("Open round 3: %v", err)
		}
		defer s.Close()

		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("multi-%04d", i))
			_, err := s.Get(key)
			if err != nil {
				t.Errorf("Round 3 verify key %d: %v", i, err)
			}
		}
	}
}
