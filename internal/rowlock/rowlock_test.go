package rowlock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/go-fast-kv/internal/rowlock/api"
)

func TestNew(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("New returned nil")
	}
}

func TestAcquireAndRelease(t *testing.T) {
	m := New()
	defer m.Close()

	ctx := LockContext{TxnID: 1, TimeoutMs: 1000}

	// Acquire exclusive lock
	if !m.Acquire("table1:row1", ctx, LockExclusive) {
		t.Fatal("Acquire failed for unlocked row")
	}

	// Verify it's locked
	if !m.IsLocked("table1:row1") {
		t.Fatal("IsLocked returned false after Acquire")
	}

	// Verify locked by correct txn
	if !m.IsLockedByTxn("table1:row1", 1) {
		t.Fatal("IsLockedByTxn returned false for correct txn")
	}

	// Release
	m.Release("table1:row1", 1)

	// Verify unlocked
	if m.IsLocked("table1:row1") {
		t.Fatal("IsLocked returned true after Release")
	}
}

func TestTryAcquire(t *testing.T) {
	m := New()
	defer m.Close()

	// TryAcquire on unlocked row
	if !m.TryAcquire("table1:row1", 1, LockExclusive) {
		t.Fatal("TryAcquire failed for unlocked row")
	}

	// TryAcquire on locked row by different txn
	if m.TryAcquire("table1:row1", 2, LockExclusive) {
		t.Fatal("TryAcquire succeeded for row locked by different txn")
	}

	// TryAcquire on same txn
	if !m.TryAcquire("table1:row1", 1, LockShared) {
		t.Fatal("TryAcquire failed for same txn")
	}

	m.Release("table1:row1", 1)
	m.Release("table1:row1", 1)

	// Now should succeed
	if !m.TryAcquire("table1:row1", 2, LockExclusive) {
		t.Fatal("TryAcquire failed after Release")
	}
}

func TestAcquireTimeout(t *testing.T) {
	m := New()
	defer m.Close()

	// Txn 1 acquires exclusive lock
	m.TryAcquire("table1:row1", 1, LockExclusive)

	// Txn 2 tries to acquire with short timeout
	ctx := LockContext{TxnID: 2, TimeoutMs: 50}

	start := time.Now()
	if m.Acquire("table1:row1", ctx, LockExclusive) {
		t.Fatal("Acquire succeeded when it should have timed out")
	}
	elapsed := time.Since(start)

	// Should have waited close to the timeout
	if elapsed < 40*time.Millisecond {
		t.Fatalf("Acquire returned too quickly: %v", elapsed)
	}

	m.Release("table1:row1", 1)
}

func TestAcquireNoTimeout(t *testing.T) {
	m := New()
	defer m.Close()

	// Txn 1 acquires immediately
	m.TryAcquire("table1:row1", 1, LockExclusive)

	var acquired bool
	var wg sync.WaitGroup
	wg.Add(1)
	
	// Txn 2 tries to acquire - should block since exclusive is held
	go func() {
		defer wg.Done()
		ctx := LockContext{TxnID: 2, TimeoutMs: 5000}
		acquired = m.Acquire("table1:row1", ctx, LockExclusive)
	}()

	// Let it block
	time.Sleep(50 * time.Millisecond)
	
	// Verify Txn2 is still waiting (not yet acquired)
	if acquired {
		t.Fatal("Txn2 should be blocked, not yet acquired")
	}
	
	// Release exclusive - Txn2 should now acquire
	m.Release("table1:row1", 1)
	
	// Wait for Txn2 to complete acquisition
	wg.Wait()
	
	if !acquired {
		t.Fatal("Txn2 should have acquired after Txn1 released")
	}

	m.Release("table1:row1", 2)
}

func TestSharedLocks(t *testing.T) {
	m := New()
	defer m.Close()

	// Txn 1 acquires shared lock
	if !m.TryAcquire("table1:row1", 1, LockShared) {
		t.Fatal("TryAcquire failed for unlocked row")
	}

	// Txn 2 acquires shared lock
	if !m.TryAcquire("table1:row1", 2, LockShared) {
		t.Fatal("TryAcquire failed for row with shared lock")
	}

	// Txn 3 tries exclusive - should fail
	if m.TryAcquire("table1:row1", 3, LockExclusive) {
		t.Fatal("TryAcquire succeeded for row with shared lock")
	}

	// Release all shared locks
	m.Release("table1:row1", 1)
	m.Release("table1:row1", 2)

	// Now exclusive should work
	if !m.TryAcquire("table1:row1", 3, LockExclusive) {
		t.Fatal("TryAcquire failed after shared locks released")
	}
}

func TestSharedLockUpgrade(t *testing.T) {
	m := New()
	defer m.Close()

	// Txn 1 acquires shared lock
	m.TryAcquire("table1:row1", 1, LockShared)

	// Same txn acquires exclusive - should upgrade
	if !m.TryAcquire("table1:row1", 1, LockExclusive) {
		t.Fatal("TryAcquire failed for same txn upgrading to exclusive")
	}

	// Verify mode is exclusive
	if m.GetLockMode("table1:row1") != LockExclusive {
		t.Fatal("Lock mode should be exclusive after upgrade")
	}

	m.Release("table1:row1", 1)
}

func TestReleaseAll(t *testing.T) {
	m := New()
	defer m.Close()

	// Txn 1 acquires multiple locks
	m.TryAcquire("table1:row1", 1, LockExclusive)
	m.TryAcquire("table1:row2", 1, LockExclusive)
	m.TryAcquire("table2:row1", 1, LockExclusive)

	// Verify locked
	if !m.IsLocked("table1:row1") || !m.IsLocked("table1:row2") || !m.IsLocked("table2:row1") {
		t.Fatal("Rows should be locked")
	}

	// Release all for txn 1
	m.ReleaseAll(1)

	// Verify all unlocked
	if m.IsLocked("table1:row1") || m.IsLocked("table1:row2") || m.IsLocked("table2:row1") {
		t.Fatal("Rows should be unlocked after ReleaseAll")
	}
}

func TestReleaseNotHeld(t *testing.T) {
	m := New()
	defer m.Close()

	// Txn 1 acquires lock
	m.TryAcquire("table1:row1", 1, LockExclusive)

	// Txn 2 tries to release - should be no-op
	m.Release("table1:row1", 2)

	// Lock should still be held
	if !m.IsLocked("table1:row1") {
		t.Fatal("Lock should still be held after release by non-holder")
	}

	m.Release("table1:row1", 1)
}

func TestLockStats(t *testing.T) {
	m := New()
	defer m.Close()

	// Acquire some locks
	m.TryAcquire("table1:row1", 1, LockExclusive)
	m.TryAcquire("table1:row2", 1, LockShared)
	m.TryAcquire("table1:row3", 2, LockShared)

	stats := m.LockStats()

	if stats.TotalLocks != 3 {
		t.Fatalf("Expected 3 locks, got %d", stats.TotalLocks)
	}

	if len(stats.ShardStats) != 16 {
		t.Fatalf("Expected 16 shards, got %d", len(stats.ShardStats))
	}

	// Verify locks are distributed
	totalFromShards := int64(0)
	for _, s := range stats.ShardStats {
		totalFromShards += int64(s.Locks)
	}
	if totalFromShards != 3 {
		t.Fatalf("Total locks from shards (%d) doesn't match (%d)", totalFromShards, 3)
	}
}

func TestConcurrentAcquire(t *testing.T) {
	m := New()
	defer m.Close()

	const goroutines = 10
	var acquired int32
	var wg sync.WaitGroup

	// Each goroutine tries to acquire the same row with shared lock
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			ctx := LockContext{TxnID: txnID, TimeoutMs: 2000}
			if m.Acquire("table1:row1", ctx, LockShared) {
				atomic.AddInt32(&acquired, 1)
				// Hold briefly then release
				time.Sleep(10 * time.Millisecond)
				m.Release("table1:row1", txnID)
			}
		}(uint64(i + 1))
	}

	wg.Wait()

	// All should have acquired shared locks
	if acquired != goroutines {
		t.Fatalf("Expected %d acquisitions, got %d", goroutines, acquired)
	}
}

func TestConcurrentExclusiveBlocks(t *testing.T) {
	m := New()
	defer m.Close()

	var exclusiveAcquired int32
	blocked := make(chan struct{})

	// Txn 1 acquires exclusive
	m.TryAcquire("table1:row1", 1, LockExclusive)

	// Txn 2 tries exclusive - should block
	go func() {
		ctx := LockContext{TxnID: 2, TimeoutMs: 5000}
		if m.Acquire("table1:row1", ctx, LockExclusive) {
			atomic.AddInt32(&exclusiveAcquired, 1)
		}
		close(blocked)
	}()

	// Let it block
	time.Sleep(100 * time.Millisecond)

	// Verify blocked by checking that it's still waiting (exclusiveAcquired == 0)
	if atomic.LoadInt32(&exclusiveAcquired) != 0 {
		t.Fatal("Exclusive should be blocked")
	}

	// Release exclusive
	m.Release("table1:row1", 1)

	// Wait for blocked goroutine to acquire
	<-blocked

	// Exclusive should have acquired
	if atomic.LoadInt32(&exclusiveAcquired) != 1 {
		t.Fatal("Exclusive should have acquired after release")
	}

	m.Release("table1:row1", 2)
}

func TestDifferentRowsNoContention(t *testing.T) {
	m := New()
	defer m.Close()

	// Acquiring different rows should not contend
	for i := 0; i < 100; i++ {
		rowKey := "table1:row" + string(rune('a'+i))
		if !m.TryAcquire(rowKey, 1, LockExclusive) {
			t.Fatalf("Failed to acquire row %s", rowKey)
		}
	}

	stats := m.LockStats()
	if stats.TotalLocks != 100 {
		t.Fatalf("Expected 100 locks, got %d", stats.TotalLocks)
	}

	m.ReleaseAll(1)
}

func TestLockOrdering(t *testing.T) {
	// This test demonstrates that lock ordering is the caller's responsibility
	m := New()
	defer m.Close()

	// Simulate sorted acquisition: row1 < row2 < row3
	keys := []string{"a", "b", "c"} // already sorted
	for _, k := range keys {
		if !m.TryAcquire(k, 1, LockExclusive) {
			t.Fatalf("Failed to acquire %s", k)
		}
	}

	// Txn 2 tries to acquire in reverse order - would deadlock without ordering
	// In real usage, caller must ensure sorted acquisition
	for _, k := range keys {
		m.Release(k, 1)
	}
}

func TestGetLockMode(t *testing.T) {
	m := New()
	defer m.Close()

	// Unlocked row
	mode := m.GetLockMode("nonexistent")
	if mode != api.LockMode(255) {
		t.Fatalf("Expected mode 255 for unlocked row, got %d", mode)
	}

	// Exclusive lock
	m.TryAcquire("table1:row1", 1, LockExclusive)
	mode = m.GetLockMode("table1:row1")
	if mode != LockExclusive {
		t.Fatalf("Expected LockExclusive, got %d", mode)
	}

	// Shared lock
	m.TryAcquire("table1:row2", 2, LockShared)
	mode = m.GetLockMode("table1:row2")
	if mode != LockShared {
		t.Fatalf("Expected LockShared, got %d", mode)
	}

	m.Release("table1:row1", 1)
	m.Release("table1:row2", 2)
}

func TestClose(t *testing.T) {
	m := New()

	// Acquire some locks
	m.TryAcquire("table1:row1", 1, LockExclusive)
	m.TryAcquire("table1:row2", 2, LockExclusive)

	// Close should release all
	m.Close()

	// Verify unlocked
	if m.IsLocked("table1:row1") || m.IsLocked("table1:row2") {
		t.Fatal("Rows should be unlocked after Close")
	}
}

func TestSameTxnMultipleAcquire(t *testing.T) {
	m := New()
	defer m.Close()

	// Same txn acquires same row multiple times with shared lock
	// Our implementation uses a set, so duplicate acquires don't add more holders
	m.TryAcquire("table1:row1", 1, LockShared)
	m.TryAcquire("table1:row1", 1, LockShared)
	m.TryAcquire("table1:row1", 1, LockShared)

	// Only one holder in the set
	if m.GetLockMode("table1:row1") != LockShared {
		t.Fatal("Mode should be shared")
	}

	// One release removes the only holder
	m.Release("table1:row1", 1)
	if m.IsLocked("table1:row1") {
		t.Fatal("Row should be unlocked after release")
	}
}

func TestMixedSharedExclusive(t *testing.T) {
	m := New()
	defer m.Close()

	// Multiple shared locks
	m.TryAcquire("table1:row1", 1, LockShared)
	m.TryAcquire("table1:row1", 2, LockShared)
	m.TryAcquire("table1:row1", 3, LockShared)

	// Verify mode is still shared
	if m.GetLockMode("table1:row1") != LockShared {
		t.Fatal("Mode should be shared")
	}

	// Release one
	m.Release("table1:row1", 2)

	// Mode should still be shared
	if m.GetLockMode("table1:row1") != LockShared {
		t.Fatal("Mode should still be shared")
	}

	// Release all
	m.ReleaseAll(1)
	m.ReleaseAll(3)

	// Now unlocked
	if m.IsLocked("table1:row1") {
		t.Fatal("Row should be unlocked")
	}
}

func BenchmarkAcquire(b *testing.B) {
	m := New()
	defer m.Close()
	for i := 0; i < b.N; i++ {
		rowKey := "table:" + string(rune('0'+i%256))
		m.TryAcquire(rowKey, 1, LockShared)
		m.Release(rowKey, 1)
	}
}

func BenchmarkConcurrentAcquire(b *testing.B) {
	m := New()
	defer m.Close()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rowKey := "table:" + string(rune('0'+i%16))
			m.TryAcquire(rowKey, 1, LockShared)
			m.Release(rowKey, 1)
			i++
		}
	})
}
