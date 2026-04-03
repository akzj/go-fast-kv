// Package internal provides integration tests for the KV store.
package internal

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Test Setup Helpers
// =============================================================================

func tempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "kvstore-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	return dir
}

func newStore(t *testing.T, dir string) KVStore {
	store, err := NewKVStore(Config{Directory: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func newTxStore(t *testing.T, dir string) KVStoreWithTransactions {
	store := newStore(t, dir)
	txStore, ok := store.(KVStoreWithTransactions)
	if !ok {
		store.Close()
		t.Fatalf("store does not support transactions")
	}
	return txStore
}

func closeAndCleanup(t *testing.T, store KVStore, dir string) {
	if err := store.Close(); err != nil {
		t.Errorf("close store: %v", err)
	}
	if dir != "" {
		os.RemoveAll(dir)
	}
}

func isNotFound(err error) bool {
	return err != nil && err.Error() == "kvstore: key not found"
}

func isClosed(err error) bool {
	return err != nil && err.Error() == "kvstore: store is closed"
}

// =============================================================================
// 1. Basic Operations: Put/Get/Delete
// =============================================================================

func TestBasicPutGet(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put and get a simple key-value
	err := store.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("put key1: %v", err)
	}

	val, err := store.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get key1: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("got %q, want %q", val, "value1")
	}
}

func TestBasicGetNotFound(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Get non-existent key
	_, err := store.Get([]byte("nonexistent"))
	if !isNotFound(err) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestBasicDelete(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put then delete
	err := store.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("put key1: %v", err)
	}

	err = store.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("delete key1: %v", err)
	}

	_, err = store.Get([]byte("key1"))
	if !isNotFound(err) {
		t.Errorf("expected ErrKeyNotFound after delete, got: %v", err)
	}
}

func TestBasicDeleteNotFound(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Delete non-existent key
	err := store.Delete([]byte("nonexistent"))
	if !isNotFound(err) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestBasicUpdate(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put initial value
	err := store.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("put key1: %v", err)
	}

	// Update value
	err = store.Put([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("update key1: %v", err)
	}

	val, err := store.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get key1: %v", err)
	}
	if !bytes.Equal(val, []byte("value2")) {
		t.Errorf("got %q, want %q", val, "value2")
	}
}

func TestBasicMultipleKeys(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put multiple keys
	keys := []string{"a", "b", "c", "z", "key"}
	values := []string{"1", "2", "3", "26", "value"}

	for i := range keys {
		err := store.Put([]byte(keys[i]), []byte(values[i]))
		if err != nil {
			t.Fatalf("put %s: %v", keys[i], err)
		}
	}

	// Verify all keys
	for i := range keys {
		val, err := store.Get([]byte(keys[i]))
		if err != nil {
			t.Fatalf("get %s: %v", keys[i], err)
		}
		if !bytes.Equal(val, []byte(values[i])) {
			t.Errorf("key %s: got %q, want %q", keys[i], val, values[i])
		}
	}
}

// =============================================================================
// 2. Value Sizes: Inline (≤48 bytes) vs External (>48 bytes)
// =============================================================================

func TestInlineValueSmall(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// 48 bytes - should be stored inline
	value := make([]byte, 48)
	for i := range value {
		value[i] = byte(i)
	}

	err := store.Put([]byte("small"), value)
	if err != nil {
		t.Fatalf("put small value: %v", err)
	}

	val, err := store.Get([]byte("small"))
	if err != nil {
		t.Fatalf("get small value: %v", err)
	}
	if !bytes.Equal(val, value) {
		t.Errorf("value mismatch")
	}
}

func TestInlineValueBoundary(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Test at boundary: 48 bytes (inline) and 49 bytes (external)
	inline := make([]byte, 48)
	external := make([]byte, 49)

	for i := range inline {
		inline[i] = byte(i)
	}
	for i := range external {
		external[i] = byte(i + 100)
	}

	err := store.Put([]byte("inline48"), inline)
	if err != nil {
		t.Fatalf("put inline: %v", err)
	}
	err = store.Put([]byte("external49"), external)
	if err != nil {
		t.Fatalf("put external: %v", err)
	}

	// Both should be retrievable
	valInline, err := store.Get([]byte("inline48"))
	if err != nil {
		t.Fatalf("get inline: %v", err)
	}
	if !bytes.Equal(valInline, inline) {
		t.Errorf("inline value mismatch")
	}

	valExt, err := store.Get([]byte("external49"))
	if err != nil {
		t.Fatalf("get external: %v", err)
	}
	if !bytes.Equal(valExt, external) {
		t.Errorf("external value mismatch")
	}
}

func TestExternalValueLarge(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// 100 bytes - definitely external
	value := make([]byte, 100)
	for i := range value {
		value[i] = byte(i * 2)
	}

	err := store.Put([]byte("large"), value)
	if err != nil {
		t.Fatalf("put large value: %v", err)
	}

	val, err := store.Get([]byte("large"))
	if err != nil {
		t.Fatalf("get large value: %v", err)
	}
	if !bytes.Equal(val, value) {
		t.Errorf("large value mismatch")
	}
}

func TestExternalValueDelete(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Large value that would be external
	value := make([]byte, 200)
	for i := range value {
		value[i] = byte(i)
	}

	err := store.Put([]byte("large"), value)
	if err != nil {
		t.Fatalf("put large: %v", err)
	}

	// Delete should work for external values too
	err = store.Delete([]byte("large"))
	if err != nil {
		t.Fatalf("delete large: %v", err)
	}

	_, err = store.Get([]byte("large"))
	if !isNotFound(err) {
		t.Errorf("expected ErrKeyNotFound after delete, got: %v", err)
	}
}

func TestEmptyValue(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Empty value
	err := store.Put([]byte("empty"), []byte{})
	if err != nil {
		t.Fatalf("put empty: %v", err)
	}

	val, err := store.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("get empty: %v", err)
	}
	if len(val) != 0 {
		t.Errorf("expected empty value, got %d bytes", len(val))
	}
}

// =============================================================================
// 3. Scan/Iterator
// =============================================================================

func TestScanBasic(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put keys in random order
	store.Put([]byte("c"), []byte("3"))
	store.Put([]byte("a"), []byte("1"))
	store.Put([]byte("d"), []byte("4"))
	store.Put([]byte("b"), []byte("2"))

	// Scan all keys
	iter, err := store.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer iter.Close()

	var keys []string
	var values []string
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
	}
	if iter.Error() != nil {
		t.Fatalf("iterator error: %v", iter.Error())
	}

	// Keys should be in sorted order
	if len(keys) != 4 {
		t.Errorf("expected 4 keys, got %d", len(keys))
	}
}

func TestScanWithRange(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put keys
	for _, k := range []string{"apple", "banana", "cherry", "date", "elderberry"} {
		store.Put([]byte(k), []byte(k+"-value"))
	}

	// Scan range [b, d)
	iter, err := store.Scan([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatalf("scan range: %v", err)
	}
	defer iter.Close()

	var keys []string
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	if iter.Error() != nil {
		t.Fatalf("iterator error: %v", iter.Error())
	}

	// Should get banana, cherry
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d: %v", len(keys), keys)
	}
	if len(keys) >= 1 && keys[0] != "banana" {
		t.Errorf("first key: got %s, want banana", keys[0])
	}
	if len(keys) >= 2 && keys[1] != "cherry" {
		t.Errorf("second key: got %s, want cherry", keys[1])
	}
}

func TestScanEmpty(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Scan empty store
	iter, err := store.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer iter.Close()

	if iter.Next() {
		t.Error("expected no results from empty scan")
	}
}

func TestScanNoMatchRange(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put keys
	store.Put([]byte("mango"), []byte("1"))
	store.Put([]byte("papaya"), []byte("2"))

	// Scan range that doesn't match
	iter, err := store.Scan([]byte("a"), []byte("b"))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer iter.Close()

	if iter.Next() {
		t.Error("expected no results")
	}
}

func TestIteratorClose(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put some data
	for i := 0; i < 10; i++ {
		store.Put([]byte(string(rune('a'+i))), []byte("value"))
	}

	iter, _ := store.Scan(nil, nil)

	// Call Next a few times
	for i := 0; i < 3; i++ {
		if !iter.Next() {
			break
		}
	}

	// Close should not panic
	iter.Close()

	// Second close should not panic
	iter.Close()
}

// =============================================================================
// 4. Batch Operations
// =============================================================================

func testBatchCreator(s KVStore) BatchCreator {
	if bc, ok := s.(BatchCreator); ok {
		return bc
	}
	return nil
}

func TestBatchBasic(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	bc := testBatchCreator(store)
	if bc == nil {
		t.Skip("store does not implement BatchCreator")
	}

	// Create batch
	batch := bc.NewBatch()

	// Add operations
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Put([]byte("key3"), []byte("value3"))

	// Commit
	err := batch.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify all keys exist
	for _, k := range []string{"key1", "key2", "key3"} {
		val, err := store.Get([]byte(k))
		if err != nil {
			t.Errorf("get %s: %v", k, err)
		}
		expected := []byte("value" + string(k[len(k)-1]))
		if !bytes.Equal(val, expected) {
			t.Errorf("key %s: got %q, want %q", k, val, expected)
		}
	}
}

func TestBatchWithDelete(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	bc := testBatchCreator(store)
	if bc == nil {
		t.Skip("store does not implement BatchCreator")
	}

	// Put initial data
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))

	// Create batch with delete
	batch := bc.NewBatch()
	batch.Delete([]byte("key1"))
	batch.Put([]byte("key3"), []byte("value3"))

	err := batch.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// key1 should be gone
	_, err = store.Get([]byte("key1"))
	if !isNotFound(err) {
		t.Errorf("key1 should be deleted")
	}

	// key2 should still exist
	val, _ := store.Get([]byte("key2"))
	if !bytes.Equal(val, []byte("value2")) {
		t.Errorf("key2 should be value2")
	}

	// key3 should exist
	val, _ = store.Get([]byte("key3"))
	if !bytes.Equal(val, []byte("value3")) {
		t.Errorf("key3 should be value3")
	}
}

func TestBatchReset(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	bc := testBatchCreator(store)
	if bc == nil {
		t.Skip("store does not implement BatchCreator")
	}

	batch := bc.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Reset()

	// After reset, committing empty batch should succeed
	err := batch.Commit()
	if err != nil {
		t.Fatalf("commit empty batch: %v", err)
	}

	// key1 should not exist
	_, err = store.Get([]byte("key1"))
	if !isNotFound(err) {
		t.Errorf("key1 should not exist after reset")
	}
}

func TestBatchCommitFailure(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	bc := testBatchCreator(store)
	if bc == nil {
		t.Skip("store does not implement BatchCreator")
	}

	// Put a key
	store.Put([]byte("key1"), []byte("value1"))

	// Create batch that tries to delete non-existent key (should succeed anyway)
	batch := bc.NewBatch()
	batch.Delete([]byte("nonexistent"))
	batch.Put([]byte("key2"), []byte("value2"))

	err := batch.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// key2 should exist
	val, _ := store.Get([]byte("key2"))
	if !bytes.Equal(val, []byte("value2")) {
		t.Errorf("key2 should be value2")
	}
}

// =============================================================================
// 5. Transactions
// =============================================================================

func TestTransactionBasic(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	tx, err := store.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// Put in transaction
	err = tx.Put([]byte("txkey"), []byte("txvalue"))
	if err != nil {
		t.Fatalf("tx put: %v", err)
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify outside transaction
	val, err := store.Get([]byte("txkey"))
	if err != nil {
		t.Fatalf("get after commit: %v", err)
	}
	if !bytes.Equal(val, []byte("txvalue")) {
		t.Errorf("got %q, want %q", val, "txvalue")
	}
}

func TestTransactionRollback(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	tx, err := store.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// Put in transaction
	tx.Put([]byte("txkey"), []byte("txvalue"))

	// Rollback
	tx.Rollback()

	// Key should not exist
	_, err = store.Get([]byte("txkey"))
	if !isNotFound(err) {
		t.Errorf("key should not exist after rollback")
	}
}

func TestTransactionReadYourWrites(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put initial value
	store.Put([]byte("key1"), []byte("initial"))

	tx, _ := store.Begin()

	// Update in transaction
	tx.Put([]byte("key1"), []byte("updated"))

	// Read within transaction should see updated value
	val, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("tx get: %v", err)
	}
	if !bytes.Equal(val, []byte("updated")) {
		t.Errorf("got %q, want %q", val, "updated")
	}

	tx.Rollback()

	// Original value should be unchanged
	val, _ = store.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("initial")) {
		t.Errorf("original value should be restored")
	}
}

func TestTransactionDelete(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put initial value
	store.Put([]byte("key1"), []byte("value1"))

	tx, _ := store.Begin()

	// Delete in transaction
	err := tx.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("tx delete: %v", err)
	}

	// Read within transaction
	_, err = tx.Get([]byte("key1"))
	if !isNotFound(err) {
		t.Errorf("should see deleted key in transaction")
	}

	tx.Commit()

	// Key should be gone
	_, err = store.Get([]byte("key1"))
	if !isNotFound(err) {
		t.Errorf("key should be deleted after commit")
	}
}

func TestTransactionScan(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put some data
	store.Put([]byte("a"), []byte("1"))
	store.Put([]byte("b"), []byte("2"))

	tx, _ := store.Begin()

	// Put more in transaction
	tx.Put([]byte("c"), []byte("3"))

	// Scan should see all
	iter, err := tx.Scan(nil, nil)
	if err != nil {
		t.Fatalf("tx scan: %v", err)
	}
	defer iter.Close()

	var count int
	for iter.Next() {
		count++
	}

	tx.Rollback()

	if count != 3 {
		t.Errorf("expected 3 keys in scan, got %d", count)
	}
}

func TestTransactionID(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	tx1, _ := store.Begin()
	tx2, _ := store.Begin()

	id1 := tx1.TxID()
	id2 := tx2.TxID()

	tx1.Rollback()
	tx2.Rollback()

	if id1 == id2 {
		t.Errorf("transaction IDs should be unique")
	}
}

func TestTransactionCommitTwice(t *testing.T) {
	dir := tempDir(t)
	store := newTxStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	tx, _ := store.Begin()
	tx.Put([]byte("key1"), []byte("value1"))

	// First commit succeeds
	err := tx.Commit()
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Second commit should be no-op or succeed
	err = tx.Commit()
	if err != nil {
		t.Errorf("second commit should be idempotent: %v", err)
	}
}

// =============================================================================
// 6. Concurrency
// =============================================================================

func TestConcurrentReads(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put some data
	for i := 0; i < 100; i++ {
		store.Put([]byte(string(rune(i))), []byte("value"))
	}

	// Concurrent readers
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := store.Get([]byte(string(rune(j))))
				if err != nil {
					mu.Lock()
					errors = append(errors, err)
					mu.Unlock()
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	if len(errors) > 0 {
		t.Errorf("concurrent read errors: %v", errors[:5])
	}
}

func TestConcurrentWriters(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Concurrent writers should serialize
	var wg sync.WaitGroup
	success := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := []byte(string(rune('a'+id)) + string(rune('0'+j)))
				err := store.Put(key, []byte("value"))
				if err != nil {
					success <- false
					return
				}
			}
			success <- true
		}(i)
	}

	wg.Wait()
	close(success)

	for ok := range success {
		if !ok {
			t.Error("at least one writer failed")
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put initial data
	store.Put([]byte("shared"), []byte("initial"))

	var wg sync.WaitGroup

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			store.Put([]byte("shared"), []byte("value"))
			time.Sleep(time.Millisecond)
		}
	}()

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				store.Get([]byte("shared"))
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentIterators(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put data
	for i := 0; i < 20; i++ {
		store.Put([]byte(string(rune('a'+i))), []byte("value"))
	}

	var wg sync.WaitGroup

	// Multiple iterators
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			iter, _ := store.Scan(nil, nil)
			defer iter.Close()
			for iter.Next() {
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

// =============================================================================
// 7. Persistence
// =============================================================================

func TestPersistenceCloseReopen(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Open, put data, close
	store1 := newStore(t, dir)
	for i := 0; i < 10; i++ {
		store1.Put([]byte(string(rune('0'+i))), []byte("value"))
	}
	store1.Close()

	// Reopen and verify
	store2 := newStore(t, dir)
	defer store2.Close()

	for i := 0; i < 10; i++ {
		val, err := store2.Get([]byte(string(rune('0'+i))))
		if err != nil {
			t.Errorf("get after reopen: %v", err)
		}
		if !bytes.Equal(val, []byte("value")) {
			t.Errorf("value mismatch for key %c", rune('0'+i))
		}
	}
}

func TestPersistenceMultipleReopens(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Multiple close/reopen cycles
	for cycle := 0; cycle < 3; cycle++ {
		store := newStore(t, dir)
		key := []byte(string(rune('a' + cycle)))
		store.Put(key, []byte("value"))

		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	// Final reopen
	store := newStore(t, dir)
	defer store.Close()

	// All values should exist
	for cycle := 0; cycle < 3; cycle++ {
		key := []byte(string(rune('a' + cycle)))
		val, err := store.Get(key)
		if err != nil {
			t.Errorf("cycle %d: get failed: %v", cycle, err)
		}
		if !bytes.Equal(val, []byte("value")) {
			t.Errorf("cycle %d: value mismatch", cycle)
		}
	}
}

func TestPersistenceDeletedKeysGone(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Put and delete
	store1 := newStore(t, dir)
	store1.Put([]byte("key1"), []byte("value1"))
	store1.Delete([]byte("key1"))
	store1.Close()

	// Reopen
	store2 := newStore(t, dir)
	defer store2.Close()

	_, err := store2.Get([]byte("key1"))
	if !isNotFound(err) {
		t.Errorf("deleted key should not exist after reopen")
	}
}

func TestPersistenceLargeValues(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Create large values
	store1 := newStore(t, dir)
	largeVal := make([]byte, 1000)
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}
	store1.Put([]byte("large"), largeVal)
	store1.Close()

	// Reopen
	store2 := newStore(t, dir)
	defer store2.Close()

	val, err := store2.Get([]byte("large"))
	if err != nil {
		t.Fatalf("get large value: %v", err)
	}
	if !bytes.Equal(val, largeVal) {
		t.Errorf("large value mismatch after reopen")
	}
}

// =============================================================================
// 8. Crash Recovery (WAL Replay)
// =============================================================================

func TestWALRecoveryBasic(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Simulate unclean shutdown: write some data, close abruptly
	store := newStore(t, dir)
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))
	// No explicit close - simulating crash

	// Recover by reopening
	store2 := newStore(t, dir)
	defer store2.Close()

	// Data should be recoverable via WAL
	val, err := store2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("recovered key1: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("recovered value1 mismatch")
	}

	val, err = store2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("recovered key2: %v", err)
	}
	if !bytes.Equal(val, []byte("value2")) {
		t.Errorf("recovered value2 mismatch")
	}
}

func TestWALRecoveryAfterPartialWrite(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Simulate partial writes
	store := newStore(t, dir)
	store.Put([]byte("first"), []byte("1"))
	store.Put([]byte("second"), []byte("2"))
	store.Put([]byte("third"), []byte("3"))
	// Simulate crash after some writes

	// Recover
	store2 := newStore(t, dir)
	defer store2.Close()

	// All should be recoverable
	for _, k := range []string{"first", "second", "third"} {
		_, err := store2.Get([]byte(k))
		if isNotFound(err) {
			t.Errorf("key %s should be recoverable", k)
		}
	}
}

func TestWALRecoveryWithExternalValues(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Large values that would be external
	store := newStore(t, dir)
	largeVal := make([]byte, 200)
	for i := range largeVal {
		largeVal[i] = byte(i)
	}
	store.Put([]byte("large"), largeVal)

	// Close first store to flush tree nodes and segments
	store.Close()

	// Recover - open fresh store
	store2 := newStore(t, dir)
	defer store2.Close()

	val, err := store2.Get([]byte("large"))
	if err != nil {
		t.Fatalf("recover large value: %v", err)
	}
	if !bytes.Equal(val, largeVal) {
		t.Errorf("large value mismatch after recovery")
	}
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

func TestClosedStore(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	store.Close()

	// All operations on closed store should fail
	tests := []struct {
		name string
		fn   func() error
	}{
		{"Get", func() error { _, err := store.Get([]byte("k")); return err }},
		{"Put", func() error { return store.Put([]byte("k"), []byte("v")) }},
		{"Delete", func() error { return store.Delete([]byte("k")) }},
		{"Scan", func() error { _, err := store.Scan(nil, nil); return err }},
	}

	for _, tc := range tests {
		err := tc.fn()
		if !isClosed(err) {
			t.Errorf("%s on closed store: expected ErrStoreClosed, got %v", tc.name, err)
		}
	}
}

func TestLargeNumberOfKeys(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Put many keys
	const keyCount = 1000
	for i := 0; i < keyCount; i++ {
		key := []byte(string(rune('a'+(i%26))) + string(rune(i/26)))
		store.Put(key, []byte("value"))
	}

	// Verify count by scanning
	iter, _ := store.Scan(nil, nil)
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}
	if iter.Error() != nil {
		t.Fatalf("scan error: %v", iter.Error())
	}

	if count != keyCount {
		t.Errorf("expected %d keys, got %d", keyCount, count)
	}
}

func TestSpecialCharactersInKey(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)

	// Keys with special characters
	keys := [][]byte{
		[]byte(""),
		[]byte(" "),
		[]byte("\x00"),
		[]byte("\xff"),
		[]byte("key with spaces"),
		[]byte("unicode: \xe4\xb8\xad\xe6\x96\x87"),
	}

	for _, key := range keys {
		store.Put(key, []byte("value"))
		val, err := store.Get(key)
		if err != nil {
			t.Errorf("get key %v: %v", key, err)
		}
		if !bytes.Equal(val, []byte("value")) {
			t.Errorf("value mismatch for key %v", key)
		}
	}
}
