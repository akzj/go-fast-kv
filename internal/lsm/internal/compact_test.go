package internal

import (
	"fmt"
	"sync"
	"testing"
	"time"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
)

func TestAutoCompaction(t *testing.T) {
	// Create LSM store with small memtable size
	dir := t.TempDir()
	
	cfg := lsmapi.Config{
		Dir:           dir,
		MemtableSize:  1024, // 1KB - very small for testing
	}
	
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer l.Close()
	
	// Add many entries to trigger compaction
	for i := uint64(0); i < 1000; i++ {
		l.SetPageMapping(i, i*100)
	}
	
	// Trigger compaction directly
	l.MaybeCompact()
	
	// Wait for background compaction to complete (non-blocking flush)
	l.WaitForCompaction()
	
	// Check that SSTable was created
	segments := l.manifest.Segments()
	t.Logf("Segments after compaction: %v", segments)
	
	// Should have at least one segment from compaction
	if len(segments) < 1 {
		t.Error("Expected at least one SSTable segment after compaction")
	}
	
	// Verify data is still accessible
	for i := uint64(0); i < 50; i++ {
		vaddr, ok := l.GetPageMapping(i)
		if !ok {
			t.Errorf("Key %d not found after compaction", i)
		}
		if vaddr != i*100 {
			t.Errorf("Key %d has wrong value: got %d, want %d", i, vaddr, i*100)
		}
	}
}

func TestAutoCompactionTrigger(t *testing.T) {
	dir := t.TempDir()
	
	cfg := lsmapi.Config{
		Dir:           dir,
		MemtableSize:  1024, // 1KB
	}
	
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer l.Close()
	
	// Add entries until compaction triggers
	for i := uint64(0); i < 200; i++ {
		l.SetPageMapping(i, i*100)
	}
	
	// Trigger compaction
	l.MaybeCompact()
	
	// Verify data is still accessible after compaction
	for i := uint64(0); i < 50; i++ {
		vaddr, ok := l.GetPageMapping(i)
		if !ok {
			t.Errorf("Key %d not found after compaction", i)
		}
		if vaddr != i*100 {
			t.Errorf("Key %d has wrong value: got %d, want %d", i, vaddr, i*100)
		}
	}
}

func TestCompactLoop(t *testing.T) {
	dir := t.TempDir()
	
	cfg := lsmapi.Config{
		Dir:           dir,
		MemtableSize:  512, // Very small
	}
	
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	
	// Fill memtable
	for i := uint64(0); i < 500; i++ {
		l.SetPageMapping(i, i)
	}
	
	// Close should wait for compaction
	l.Close()
	
	// Verify we can read data
	l2, err := New(cfg)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer l2.Close()
	
	// Check some data is accessible
	v, ok := l2.GetPageMapping(100)
	if !ok {
		t.Error("Data not accessible after reopen")
	}
	if v != 100 {
		t.Errorf("Wrong value: got %d, want %d", v, 100)
	}
}

func TestMaybeCompact(t *testing.T) {
	dir := t.TempDir()
	
	cfg := lsmapi.Config{
		Dir:           dir,
		MemtableSize:  1024,
	}
	
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer l.Close()
	
	// Initially no compaction needed
	err = l.MaybeCompact()
	if err != nil {
		t.Errorf("MaybeCompact failed: %v", err)
	}
	
	// Add enough data
	for i := uint64(0); i < 200; i++ {
		l.SetPageMapping(i, i*10)
	}
	
	// Now compaction should trigger
	err = l.MaybeCompact()
	if err != nil {
		t.Errorf("MaybeCompact failed: %v", err)
	}
	
	// Data should still be accessible
	v, ok := l.GetPageMapping(50)
	if !ok {
		t.Error("Data lost after MaybeCompact")
	}
	if v != 500 {
		t.Errorf("Wrong value: got %d, want %d", v, 500)
	}
}

// TestNonBlockingCompaction verifies that writes are not blocked during compaction.
// Background flush runs in goroutine while writes continue immediately.
func TestNonBlockingCompaction(t *testing.T) {
	dir := t.TempDir()
	
	cfg := lsmapi.Config{
		Dir:           dir,
		MemtableSize:  1024, // Small to trigger frequent compaction
	}
	
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer l.Close()
	
	// Fill and trigger compaction multiple times while writing
	var wg sync.WaitGroup
	errors := make(chan error, 10)
	
	// Writer goroutine - should not be blocked by compaction
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 500; i++ {
			l.SetPageMapping(i, i*100)
			// Trigger compaction periodically
			if i%50 == 0 {
				l.MaybeCompact()
			}
		}
	}()
	
	// Reader goroutine - should always succeed
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, ok := l.GetPageMapping(uint64(i%100) + 500) // May or may not exist
			_ = ok
			time.Sleep(1 * time.Millisecond)
		}
	}()
	
	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Success - no blocking
	case <-time.After(5 * time.Second):
		t.Error("Compaction blocked writes (timeout)")
		errors <- fmt.Errorf("timeout")
	}
	
	// Verify all written data is accessible
	for i := uint64(0); i < 500; i++ {
		v, ok := l.GetPageMapping(i)
		if !ok {
			t.Errorf("Key %d not found after concurrent compaction", i)
		}
		if v != i*100 {
			t.Errorf("Key %d wrong value: got %d, want %d", i, v, i*100)
		}
	}
}
