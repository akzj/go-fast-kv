package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// TestWALSegmentedOperations tests WAL segmentation behavior.
func TestWALSegmentedOperations(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Write some data - should create segments
	for i := 0; i < 100; i++ {
		err := s.Put([]byte("key"+string(rune('0'+i%10))), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get WAL segments
	wal := s.(*store).wal
	segs := wal.(interface{ ListSegments() []string }).ListSegments()

	// Should have segments (may be 1 or more depending on write pattern)
	t.Logf("Segments after writes: %d", len(segs))
	for _, seg := range segs {
		t.Logf("  Segment: %s", seg)
	}
}

// TestCheckpointWALCleanup tests that checkpoint correctly cleans up WAL segments.
func TestCheckpointWALCleanup(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	for i := 0; i < 50; i++ {
		err := s.Put([]byte("key"+string(rune('0'+i%10))), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get segments before checkpoint
	wal := s.(*store).wal
	segsBefore := wal.(interface{ ListSegments() []string }).ListSegments()
	t.Logf("Segments before checkpoint: %d", len(segsBefore))

	// Do checkpoint
	err = s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	// Get segments after checkpoint
	segsAfter := wal.(interface{ ListSegments() []string }).ListSegments()
	t.Logf("Segments after checkpoint: %d", len(segsAfter))

	// After checkpoint, old segments should be cleaned up
	// There should be at most 1 active segment
	if len(segsAfter) > 1 {
		t.Errorf("Old segments should be cleaned up after checkpoint, got %d segments", len(segsAfter))
	}

	s.Close()
}

// TestWALRecoveryFromSegments tests recovery with segmented WAL.
func TestWALRecoveryFromSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	// First instance: write data and checkpoint
	{
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 20; i++ {
			key := []byte("key" + fmt.Sprintf("%d", i))
			val := []byte("value-" + fmt.Sprintf("%d", i))
			err := s.Put(key, val)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Verify data
		val, err := s.Get([]byte("key0"))
		if err != nil {
			t.Fatal(err)
		}
		if string(val) != "value-0" {
			t.Fatalf("expected value-0, got %s", val)
		}

		// Checkpoint
		err = s.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}

		// Get WAL segments
		wal := s.(*store).wal
		segs := wal.(interface{ ListSegments() []string }).ListSegments()
		t.Logf("Segments after checkpoint: %d", len(segs))

		s.Close()
	}

	// Second instance: recover from checkpoint
	{
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		// Verify recovered data
		for i := 0; i < 20; i++ {
			key := []byte("key" + fmt.Sprintf("%d", i))
			expected := "value-" + fmt.Sprintf("%d", i)
			val, err := s.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key %s: %v", key, err)
			}
			if string(val) != expected {
				t.Fatalf("Wrong value for key %s: expected %s, got %s", key, expected, val)
			}
		}

		// Verify no extra segments remain
		wal := s.(*store).wal
		segs := wal.(interface{ ListSegments() []string }).ListSegments()
		t.Logf("Segments after recovery: %d", len(segs))
	}
}

// TestDeleteSegmentsBefore tests the WAL DeleteSegmentsBefore functionality.
func TestDeleteSegmentsBefore(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	wal := s.(*store).wal

	// Write enough data to create multiple segments
	for i := 0; i < 100; i++ {
		err := s.Put([]byte("key"+string(rune('0'+i%10))), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get segments before deletion
	segsBefore := wal.(interface{ ListSegments() []string }).ListSegments()
	t.Logf("Segments before delete: %d", len(segsBefore))

	// Get current LSN
	currentLSN := wal.CurrentLSN()
	t.Logf("Current LSN: %d", currentLSN)

	// Delete segments before current LSN (should delete all but active)
	err = wal.(interface{ DeleteSegmentsBefore(lsn uint64) error }).DeleteSegmentsBefore(currentLSN)
	if err != nil {
		t.Fatal(err)
	}

	// Get segments after deletion
	segsAfter := wal.(interface{ ListSegments() []string }).ListSegments()
	t.Logf("Segments after delete: %d", len(segsAfter))

	// Should have at most 1 segment (the active one)
	if len(segsAfter) > 1 {
		t.Errorf("Expected at most 1 segment after delete, got %d", len(segsAfter))
	}

	s.Close()
}

// TestCheckpointMetadataExists tests that checkpoint creates metadata.
func TestCheckpointMetadataExists(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Write data
	for i := 0; i < 10; i++ {
		err := s.Put([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Do checkpoint
	err = s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	// Check checkpoint file exists
	cpPath := filepath.Join(dir, "checkpoint")
	if _, err := os.Stat(cpPath); err != nil {
		t.Fatalf("Checkpoint file should exist: %v", err)
	}

	s.Close()
}

// TestTwoPhaseCheckpointBehavior tests the non-blocking nature of checkpoint.
func TestTwoPhaseCheckpointBehavior(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough data
	for i := 0; i < 50; i++ {
		err := s.Put([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Do checkpoint - should complete quickly (Phase 1)
	err = s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	// Store should still be responsive after checkpoint
	for i := 0; i < 10; i++ {
		err := s.Put([]byte("newkey"), []byte("newvalue"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify data
	val, err := s.Get([]byte("newkey"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "newvalue" {
		t.Fatalf("expected newvalue, got %s", val)
	}

	s.Close()
}

// TestSegmentNamingFormat tests that segments are named correctly.
func TestSegmentNamingFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := kvstoreapi.Config{Dir: dir}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	wal := s.(*store).wal

	// Write some data
	for i := 0; i < 20; i++ {
		err := s.Put([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Manually rotate to create sealed segments
	wal.Rotate()
	for i := 0; i < 10; i++ {
		err := s.Put([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
	}
	wal.Rotate()

	segs := wal.ListSegments()

	// Check naming format
	for _, seg := range segs {
		t.Logf("Segment: %s", seg)

		if strings.Contains(seg, "active") {
			// Active segment: wal.{begin}.active.log
			if !strings.HasSuffix(seg, ".active.log") {
				t.Errorf("Active segment should end with .active.log: %s", seg)
			}
		} else {
			// Sealed segment: wal.{begin}.{end}.log
			if !strings.HasSuffix(seg, ".log") {
				t.Errorf("Sealed segment should end with .log: %s", seg)
			}
			// Should have two numbers separated by .
			parts := strings.Split(seg, ".")
			if len(parts) != 4 || parts[0] != "wal" || parts[3] != "log" {
				t.Errorf("Segment name should have 4 parts (wal, begin, end, log): %s", seg)
			}
		}
	}

	s.Close()
}
