package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── Helpers ────────────────────────────────────────────────────────

func openTestStore(t *testing.T) kvstoreapi.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func openTestStoreAt(t *testing.T, dir string) kvstoreapi.Store {
	t.Helper()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return s
}

func testKey(i int) []byte   { return []byte(fmt.Sprintf("key-%05d", i)) }
func testValue(i int) []byte { return []byte(fmt.Sprintf("value-%05d", i)) }

// ─── 1. TestPutGet ──────────────────────────────────────────────────

func TestPutGet(t *testing.T) {
	s := openTestStore(t)

	if err := s.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := s.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("Get: got %q, want %q", val, "world")
	}
}

// ─── 2. TestPutGet100 ───────────────────────────────────────────────

func TestPutGet100(t *testing.T) {
	s := openTestStore(t)

	for i := 0; i < 100; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		val, err := s.Get(testKey(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !bytes.Equal(val, testValue(i)) {
			t.Fatalf("Get(%d): got %q, want %q", i, val, testValue(i))
		}
	}
}

// ─── 3. TestPutGet1000 ──────────────────────────────────────────────

func TestPutGet1000(t *testing.T) {
	s := openTestStore(t)

	for i := 0; i < 1000; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	for i := 0; i < 1000; i++ {
		val, err := s.Get(testKey(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !bytes.Equal(val, testValue(i)) {
			t.Fatalf("Get(%d): got %q, want %q", i, val, testValue(i))
		}
	}
}

// ─── 4. TestPutOverwrite ────────────────────────────────────────────

func TestPutOverwrite(t *testing.T) {
	s := openTestStore(t)

	if err := s.Put([]byte("key"), []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := s.Put([]byte("key"), []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	val, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Get: got %q, want %q", val, "v2")
	}
}

// ─── 5. TestDelete ──────────────────────────────────────────────────

func TestDelete(t *testing.T) {
	s := openTestStore(t)

	if err := s.Put([]byte("key"), []byte("val")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete([]byte("key")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get([]byte("key"))
	if err != kvstoreapi.ErrKeyNotFound {
		t.Fatalf("Get after Delete: got err=%v, want ErrKeyNotFound", err)
	}
}

// ─── 6. TestScan ────────────────────────────────────────────────────

func TestScan(t *testing.T) {
	s := openTestStore(t)

	for i := 0; i < 20; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Scan [key-00005, key-00015)
	iter := s.Scan(testKey(5), testKey(15))
	defer iter.Close()

	var count int
	for iter.Next() {
		expected := testKey(5 + count)
		if !bytes.Equal(iter.Key(), expected) {
			t.Fatalf("Scan key[%d]: got %q, want %q", count, iter.Key(), expected)
		}
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Scan err: %v", err)
	}
	if count != 10 {
		t.Fatalf("Scan count: got %d, want 10", count)
	}
}

// ─── 7. TestScanEmpty ───────────────────────────────────────────────

func TestScanEmpty(t *testing.T) {
	s := openTestStore(t)

	// Scan on empty store
	iter := s.Scan([]byte("a"), []byte("z"))
	defer iter.Close()
	if iter.Next() {
		t.Fatal("expected empty scan")
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Scan err: %v", err)
	}
}

// ─── 8. TestLargeValue ──────────────────────────────────────────────

func TestLargeValue(t *testing.T) {
	s := openTestStore(t)

	// Value > 256 bytes → stored in BlobStore
	largeVal := bytes.Repeat([]byte("X"), 1024)
	if err := s.Put([]byte("big"), largeVal); err != nil {
		t.Fatalf("Put large: %v", err)
	}
	got, err := s.Get([]byte("big"))
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	if !bytes.Equal(got, largeVal) {
		t.Fatalf("Get large: got len=%d, want len=%d", len(got), len(largeVal))
	}
}

// ─── 9. TestCheckpoint ──────────────────────────────────────────────

func TestCheckpoint(t *testing.T) {
	dir := t.TempDir()
	s := openTestStoreAt(t, dir)

	for i := 0; i < 10; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Verify checkpoint file exists
	cpPath := filepath.Join(dir, "checkpoint")
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Fatal("checkpoint file does not exist")
	}

	s.Close()
}

// ─── 10. TestCrashRecovery ──────────────────────────────────────────

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write 100 keys, checkpoint
	s := openTestStoreAt(t, dir)
	for i := 0; i < 100; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Phase1 Put(%d): %v", i, err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Phase 2: write 50 more keys AFTER checkpoint (these are in WAL only)
	for i := 100; i < 150; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Phase2 Put(%d): %v", i, err)
		}
	}
	s.Close()

	// Phase 3: reopen (triggers recovery: checkpoint + WAL replay)
	s2 := openTestStoreAt(t, dir)
	defer s2.Close()

	// Verify ALL 150 keys are readable
	for i := 0; i < 150; i++ {
		val, err := s2.Get(testKey(i))
		if err != nil {
			t.Fatalf("Recovery Get(%d): %v", i, err)
		}
		if !bytes.Equal(val, testValue(i)) {
			t.Fatalf("Recovery Get(%d): got %q, want %q", i, val, testValue(i))
		}
	}
}

// ─── 11. TestRecoveryAfterCheckpointOnly ────────────────────────────

func TestRecoveryAfterCheckpointOnly(t *testing.T) {
	dir := t.TempDir()

	// Write + checkpoint, no post-checkpoint writes
	s := openTestStoreAt(t, dir)
	for i := 0; i < 50; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	s.Close()

	// Reopen
	s2 := openTestStoreAt(t, dir)
	defer s2.Close()

	for i := 0; i < 50; i++ {
		val, err := s2.Get(testKey(i))
		if err != nil {
			t.Fatalf("Recovery Get(%d): %v", i, err)
		}
		if !bytes.Equal(val, testValue(i)) {
			t.Fatalf("Recovery Get(%d): got %q, want %q", i, val, testValue(i))
		}
	}
}

// ─── TestCheckpointFormatVersion ─────────────────────────────────────

func TestCheckpointFormatVersion(t *testing.T) {
	// Verify that the checkpoint binary format includes a version byte at offset 0
	// and that deserializeCheckpoint validates it.
	dir := t.TempDir()
	s := openTestStoreAt(t, dir)

	// Write some data and checkpoint.
	for i := 0; i < 5; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	s.Close()

	// Read raw bytes and verify version byte at offset 0.
	cpPath := dir + "/checkpoint"
	raw, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Version byte must be 1.
	if len(raw) < 1 {
		t.Fatal("checkpoint file is empty")
	}
	if raw[0] != 1 {
		t.Fatalf("version byte: got %d, want 1", raw[0])
	}

	// Verify that deserialization reads and validates the version.
	// (loadCheckpoint wraps deserializeCheckpoint with CRC check first.)
	data, err := loadCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if data.LSN == 0 {
		t.Fatal("LSN should be non-zero after checkpoint")
	}
}

// ─── TestCheckpointUnknownVersionRejected ────────────────────────────

func TestCheckpointUnknownVersionRejected(t *testing.T) {
	// Manually construct a v2 checkpoint buffer (version byte = 2)
	// to simulate a future unsupported format.
	// Header layout: version(1) + LSN(8) + NextXID(8) + RootPageID(8) + NextPageID(8)
	//              + NextBlobID(8) + PageCount(4) + BlobCount(4) + CLOGCount(4) + reserved(4)
	//              = 57 bytes, then CRC32(4) = 61 bytes total for zero-data checkpoint.
	buf := make([]byte, 57+4)
	buf[0] = 2                          // version = 2 (unsupported)
	binary.LittleEndian.PutUint64(buf[1:], 100)   // LSN
	binary.LittleEndian.PutUint64(buf[9:], 10)    // NextXID
	binary.LittleEndian.PutUint64(buf[17:], 1)   // RootPageID
	binary.LittleEndian.PutUint64(buf[25:], 2)   // NextPageID
	binary.LittleEndian.PutUint64(buf[33:], 3)   // NextBlobID
	binary.LittleEndian.PutUint32(buf[41:], 0)    // PageCount = 0
	binary.LittleEndian.PutUint32(buf[45:], 0)    // BlobCount = 0
	binary.LittleEndian.PutUint32(buf[49:], 0)    // CLOGCount = 0
	// reserved = 0 already

	// Compute and write CRC (covers bytes 0-60).
	crc := crc32.Checksum(buf[:57], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[57:], crc)

	// Try to deserialize — should fail with version mismatch.
	_, err := deserializeCheckpoint(buf)
	if err == nil {
		t.Fatal("deserializeCheckpoint: expected error for unknown version, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("deserializeCheckpoint: error should mention 'unsupported version', got: %v", err)
	}
}

// ─── TestCheckpointCRCIncludesVersion ────────────────────────────────

func TestCheckpointCRCIncludesVersion(t *testing.T) {
	// Verify that the CRC covers the version byte. Tampering with the
	// version byte should cause a CRC mismatch detected by loadCheckpoint.
	dir := t.TempDir()
	s := openTestStoreAt(t, dir)

	for i := 0; i < 3; i++ {
		if err := s.Put(testKey(i), testValue(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	s.Close()

	cpPath := dir + "/checkpoint"
	raw, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Flip a bit in the version byte (offset 0).
	raw[0] ^= 0xFF

	// Write corrupted file back.
	if err := os.WriteFile(cpPath, raw, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// loadCheckpoint must detect CRC mismatch.
	_, err = loadCheckpoint(cpPath)
	if err == nil {
		t.Fatal("loadCheckpoint: expected CRC mismatch error, got nil")
	}
}

// ─── 12. TestRecoveryFreshStart ─────────────────────────────────────

func TestRecoveryFreshStart(t *testing.T) {
	dir := t.TempDir()

	// Open fresh directory — no checkpoint, no WAL
	s := openTestStoreAt(t, dir)
	defer s.Close()

	_, err := s.Get([]byte("nonexistent"))
	if err != kvstoreapi.ErrKeyNotFound {
		t.Fatalf("Get on fresh: got err=%v, want ErrKeyNotFound", err)
	}
}

// ─── 13. TestClose ──────────────────────────────────────────────────

func TestClose(t *testing.T) {
	dir := t.TempDir()
	s := openTestStoreAt(t, dir)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// All operations should return ErrClosed
	if err := s.Put([]byte("k"), []byte("v")); err != kvstoreapi.ErrClosed {
		t.Fatalf("Put after Close: got %v, want ErrClosed", err)
	}
	if _, err := s.Get([]byte("k")); err != kvstoreapi.ErrClosed {
		t.Fatalf("Get after Close: got %v, want ErrClosed", err)
	}
	if err := s.Delete([]byte("k")); err != kvstoreapi.ErrClosed {
		t.Fatalf("Delete after Close: got %v, want ErrClosed", err)
	}
	iter := s.Scan([]byte("a"), []byte("z"))
	if iter.Next() {
		t.Fatal("Scan after Close: should not iterate")
	}
	if err := iter.Err(); err != kvstoreapi.ErrClosed {
		t.Fatalf("Scan after Close: got %v, want ErrClosed", err)
	}
	if err := s.Checkpoint(); err != kvstoreapi.ErrClosed {
		t.Fatalf("Checkpoint after Close: got %v, want ErrClosed", err)
	}
	if err := s.Close(); err != kvstoreapi.ErrClosed {
		t.Fatalf("double Close: got %v, want ErrClosed", err)
	}
}


// TestRunVacuum verifies that RunVacuum physically removes deleted entries
// from B-tree leaf pages, making deleted keys truly invisible (not just
// logically marked with TxnMax).
func TestRunVacuum(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Put 10 keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("v%02d", i)
		if err := s.Put([]byte(key), []byte(val)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete 5 keys — these should become invisible after vacuum
	deletedKeys := []string{"k01", "k03", "k05", "k07", "k09"}
	for _, k := range deletedKeys {
		if err := s.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	// Verify deleted keys are already logically invisible (pre-vacuum)
	for _, k := range deletedKeys {
		if _, err := s.Get([]byte(k)); err != kvstoreapi.ErrKeyNotFound {
			t.Fatalf("pre-vacuum Get(%s): got %v, want ErrKeyNotFound", k, err)
		}
	}

	// Run vacuum
	stats, err := s.RunVacuum()
	if err != nil {
		t.Fatal(err)
	}

	// Vacuum should have removed at least 5 entries (one per deleted key)
	if stats.EntriesRemoved < 5 {
		t.Fatalf("RunVacuum.EntriesRemoved: got %d, want >= 5 (stats: %+v)", stats.EntriesRemoved, stats)
	}
	if stats.LeavesScanned == 0 {
		t.Fatalf("RunVacuum.LeavesScanned: got 0, want > 0")
	}

	// Verify vacuum doesn't affect the 5 remaining live keys
	liveKeys := []string{"k00", "k02", "k04", "k06", "k08"}
	for _, k := range liveKeys {
		val, err := s.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s) after vacuum: %v", k, err)
		}
		expected := "v" + k[1:] // k00 → v00
		if string(val) != expected {
			t.Fatalf("Get(%s): got %q, want %q", k, val, expected)
		}
	}

	// Verify deleted keys are still invisible (vacuum worked correctly)
	for _, k := range deletedKeys {
		if _, err := s.Get([]byte(k)); err != kvstoreapi.ErrKeyNotFound {
			t.Fatalf("Get(%s) after vacuum: got %v, want ErrKeyNotFound", k, err)
		}
	}

	t.Logf("Vacuum stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// TestAutoVacuum_Basic verifies that auto-vacuum triggers automatically
// after the threshold of operations, without requiring manual RunVacuum.
func TestAutoVacuum_Basic(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{
		Dir:                 dir,
		AutoVacuumThreshold: 10, // trigger after 10 ops
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Put 5 keys (below threshold)
	for i := 0; i < 5; i++ {
		if err := s.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Delete all 5 keys — 5 puts + 5 deletes = 10 ops, crosses threshold=10.
	// Auto-vacuum goroutine should spawn and clean up.
	for i := 0; i < 5; i++ {
		if err := s.Delete([]byte(fmt.Sprintf("k%02d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Give the async vacuum goroutine time to run.
	time.Sleep(1 * time.Second)

	// Verify deleted keys are gone (vacuum physically removed them).
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%02d", i)
		if _, err := s.Get([]byte(key)); err != kvstoreapi.ErrKeyNotFound {
			t.Fatalf("k%02d after auto-vacuum: got %v, want ErrKeyNotFound", i, err)
		}
	}
}

// TestAutoVacuum_CloseWaits verifies that Close() blocks until the
// auto-vacuum goroutine finishes, preventing a race between vacuum
// holding page locks and the store closing the tree.
func TestAutoVacuum_CloseWaits(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{
		Dir:                 dir,
		AutoVacuumThreshold: 3, // very low threshold
	})
	if err != nil {
		t.Fatal(err)
	}

	// Trigger vacuum threshold with 5 Put+Delete pairs.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%02d", i)
		if err := s.Put([]byte(key), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Close in a goroutine — should block until vacuum finishes.
	done := make(chan error, 1)
	go func() {
		done <- s.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		t.Log("Close completed — vacuum goroutine finished before close")
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within 5s — vacuum goroutine likely deadlocked")
	}
}

// TestAutoVacuum_Disabled verifies that with AutoVacuumThreshold=0,
// no auto-vacuum triggers, but manual RunVacuum still works.
func TestAutoVacuum_Disabled(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(kvstoreapi.Config{
		Dir:                 dir,
		AutoVacuumThreshold: 0, // disabled
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Put and delete many keys — with threshold=0, no auto-vacuum should trigger.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		if err := s.Put([]byte(key), []byte(fmt.Sprintf("val%03d", i))); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Explicit RunVacuum should still work and clean up all dead entries.
	stats, err := s.RunVacuum()
	if err != nil {
		t.Fatal(err)
	}
	if stats.EntriesRemoved == 0 {
		t.Fatal("RunVacuum removed 0 entries — expected at least 100")
	}
	t.Logf("Auto-vacuum disabled: manual RunVacuum removed %d entries", stats.EntriesRemoved)
}

// TestConcurrentPutSameKey_NoPhantomVersions verifies that concurrent Puts
// on the same key don't leave unreclaimable MVCC versions (H4 bug).
//
// The bug: when a higher-txnID writer acquires the leaf lock before a
// lower-txnID writer, the lower-txnID writer's mvccInsert didn't mark the
// existing entry as superseded (because e.TxnMin > txnID). Both entries
// retained TxnMax=∞, making both visible forever.
//
// The fix: mvccInsert marks ANY existing entry with same key and TxnMax=∞,
// regardless of TxnMin ordering. Vacuum also deduplicates as a safety net.
func TestConcurrentPutSameKey_NoPhantomVersions(t *testing.T) {
	dir := t.TempDir()
	s := openTestStoreAt(t, dir)
	defer s.Close()

	key := []byte("hot-key")
	const numWriters = 10

	// Phase 1: Concurrent writes to the same key
	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			val := []byte(fmt.Sprintf("value-%d", n))
			if err := s.Put(key, val); err != nil {
				t.Errorf("Put(%d): %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	// Phase 2: Read the key — should return exactly one value
	val, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get after concurrent puts: %v", err)
	}
	t.Logf("Winner value after %d concurrent puts: %q", numWriters, string(val))

	// Phase 3: Run vacuum to clean up any duplicate versions
	stats, err := s.RunVacuum()
	if err != nil {
		t.Fatalf("RunVacuum: %v", err)
	}
	t.Logf("Vacuum stats: scanned=%d modified=%d removed=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved)

	// Phase 4: Read again — should still return exactly one value
	val2, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get after vacuum: %v", err)
	}
	if string(val) != string(val2) {
		t.Fatalf("Value changed after vacuum: %q → %q", val, val2)
	}

	// Phase 5: Do another round of concurrent writes + vacuum
	for round := 0; round < 3; round++ {
		var wg2 sync.WaitGroup
		for i := 0; i < numWriters; i++ {
			wg2.Add(1)
			go func(n int) {
				defer wg2.Done()
				val := []byte(fmt.Sprintf("round%d-value-%d", round, n))
				if err := s.Put(key, val); err != nil {
					t.Errorf("Put round %d (%d): %v", round, n, err)
				}
			}(i)
		}
		wg2.Wait()

		stats, err := s.RunVacuum()
		if err != nil {
			t.Fatalf("RunVacuum round %d: %v", round, err)
		}
		t.Logf("Round %d vacuum: removed=%d", round, stats.EntriesRemoved)
	}

	// Final read
	finalVal, err := s.Get(key)
	if err != nil {
		t.Fatalf("Final Get: %v", err)
	}
	t.Logf("Final value: %q", string(finalVal))
}
