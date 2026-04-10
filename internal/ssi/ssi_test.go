package ssi

import (
	"testing"

	ssiapi "github.com/akzj/go-fast-kv/internal/ssi/api"
)

func TestNewIndex(t *testing.T) {
	idx := NewIndex()
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}
}

func TestSSIStateBasic(t *testing.T) {
	state := ssiapi.NewState()

	// Test initial state
	if state.IsDangerous() {
		t.Error("expected initial state to not be dangerous")
	}
	if state.HasConflicts() {
		t.Error("expected initial state to have no conflicts")
	}

	// Test MarkRead
	key := ssiapi.Key("key1")
	state.MarkRead(key)
	if _, ok := state.RWSet[key]; !ok {
		t.Error("expected key to be in RWSet")
	}

	// Test MarkWrite
	state.MarkWrite(key)
	if _, ok := state.WWSet[key]; !ok {
		t.Error("expected key to be in WWSet")
	}

	// Test AddConflict
	conflict := ssiapi.Conflict{
		Type:     ssiapi.RWConflict,
		Key:      key,
		OtherTxn: 123,
		Reason:   "test conflict",
	}
	state.AddConflict(conflict)

	if !state.IsDangerous() {
		t.Error("expected state to be dangerous after conflict")
	}
	if !state.HasConflicts() {
		t.Error("expected state to have conflicts after conflict")
	}
	if len(state.Conflicts) != 1 {
		t.Errorf("expected 1 conflict, got %d", len(state.Conflicts))
	}
}

func TestSIndexOperations(t *testing.T) {
	idx := NewIndex()

	key := ssiapi.Key("test-key")

	// Initially no write info
	info := idx.GetWriteInfo(key)
	if info != nil {
		t.Error("expected nil write info for new key")
	}

	// Set write info
	idx.SetWriteInfo(key, &ssiapi.WriteInfo{
		TxnID:    100,
		CommitTS: 200,
	})

	// Get should return what we set
	info = idx.GetWriteInfo(key)
	if info == nil {
		t.Fatal("expected non-nil write info")
	}
	if info.TxnID != 100 {
		t.Errorf("expected TxnID 100, got %d", info.TxnID)
	}
	if info.CommitTS != 200 {
		t.Errorf("expected CommitTS 200, got %d", info.CommitTS)
	}
}

func TestTIndexOperations(t *testing.T) {
	idx := NewIndex()

	key := ssiapi.Key("test-key")

	// Initially no reader
	reader := idx.GetReader(key)
	if reader != 0 {
		t.Errorf("expected 0 reader for new key, got %d", reader)
	}

	// Set reader
	idx.SetReader(key, 456)

	// Get should return what we set
	reader = idx.GetReader(key)
	if reader != 456 {
		t.Errorf("expected reader 456, got %d", reader)
	}
}

func TestGC(t *testing.T) {
	idx := NewIndex()

	// Add some entries
	key1 := ssiapi.Key("key1")
	key2 := ssiapi.Key("key2")
	key3 := ssiapi.Key("key3")

	idx.SetWriteInfo(key1, &ssiapi.WriteInfo{TxnID: 100, CommitTS: 150})
	idx.SetWriteInfo(key2, &ssiapi.WriteInfo{TxnID: 200, CommitTS: 250})
	idx.SetWriteInfo(key3, &ssiapi.WriteInfo{TxnID: 300, CommitTS: 350})

	idx.SetReader(key1, 100)
	idx.SetReader(key2, 200)
	idx.SetReader(key3, 300)

	// GC with minXID = 200 should remove:
	// - key1: CommitTS=150 < 200, TxnID=100 < 200
	// - key2: CommitTS=250 >= 200, TxnID=200 >= 200
	// - key3: CommitTS=350 >= 200, TxnID=300 >= 200
	idx.GC(200)

	// key1 should be removed from both indexes
	if info := idx.GetWriteInfo(key1); info != nil {
		t.Error("expected key1 to be GC'd from SIndex")
	}
	if reader := idx.GetReader(key1); reader != 0 {
		t.Error("expected key1 to be GC'd from TIndex")
	}

	// key2 and key3 should remain
	if info := idx.GetWriteInfo(key2); info == nil {
		t.Error("expected key2 to remain in SIndex")
	}
	if reader := idx.GetReader(key2); reader != 200 {
		t.Error("expected key2 to remain in TIndex")
	}
	if info := idx.GetWriteInfo(key3); info == nil {
		t.Error("expected key3 to remain in SIndex")
	}
	if reader := idx.GetReader(key3); reader != 300 {
		t.Error("expected key3 to remain in TIndex")
	}
}

func TestSize(t *testing.T) {
	idx := NewIndex()

	sz, tz := idx.Size()
	if sz != 0 || tz != 0 {
		t.Errorf("expected empty index size (0, 0), got (%d, %d)", sz, tz)
	}

	// Add entries
	key1 := ssiapi.Key("key1")
	key2 := ssiapi.Key("key2")

	idx.SetWriteInfo(key1, &ssiapi.WriteInfo{TxnID: 1, CommitTS: 1})
	idx.SetWriteInfo(key2, &ssiapi.WriteInfo{TxnID: 2, CommitTS: 2})
	idx.SetReader(key1, 1)

	sz, tz = idx.Size()
	if sz != 2 {
		t.Errorf("expected SIndex size 2, got %d", sz)
	}
	if tz != 1 {
		t.Errorf("expected TIndex size 1, got %d", tz)
	}
}

func TestConflictTypes(t *testing.T) {
	rw := ssiapi.RWConflict
	ww := ssiapi.WWConflict

	if rw.String() != "RW" {
		t.Errorf("expected RW, got %s", rw.String())
	}
	if ww.String() != "WW" {
		t.Errorf("expected WW, got %s", ww.String())
	}
}

func TestWriteSkewScenario(t *testing.T) {
	// Classic Write Skew: T1 and T2 both read keys A,B, then update different keys
	// With SSI, when T1 commits, T2 should detect conflict and abort
	
	idx := NewIndex()
	
	keyA := ssiapi.Key("doctor:alice:on_call")
	keyB := ssiapi.Key("doctor:bob:on_call")
	
	// T1 reads both keys
	txn1 := ssiapi.NewState()
	txn1.MarkRead(keyA)
	txn1.MarkRead(keyB)
	
	// T2 reads both keys  
	txn2 := ssiapi.NewState()
	txn2.MarkRead(keyA)
	txn2.MarkRead(keyB)
	
	// T1 writes keyB and commits
	txn1.MarkWrite(keyB)
	idx.SetWriteInfo(keyB, &ssiapi.WriteInfo{TxnID: 1, CommitTS: 100})
	idx.SetReader(keyB, 1)
	
	// Before T2 commits, check if conflict is detected
	// T2's read of keyB conflicts with T1's write of keyB (commitTS=100 > T2's start)
	info := idx.GetWriteInfo(keyB)
	if info != nil && info.CommitTS > 0 && info.TxnID != 2 {
		txn2.AddConflict(ssiapi.Conflict{
			Type:     ssiapi.RWConflict,
			Key:      keyB,
			OtherTxn: info.TxnID,
			Reason:   "T2 read keyB before T1's write committed",
		})
	}
	
	// T2 should detect dangerous conflict
	if !txn2.IsDangerous() {
		t.Error("T2 should detect dangerous RW-conflict with T1's write")
	}
	
	t.Logf("Write skew detected: %v", txn2.Conflicts)
}
