package internal

import (
	"bytes"
	"fmt"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// ─── Serialization Tests ────────────────────────────────────────────

func TestSerializeDeserializeLeaf(t *testing.T) {
	s := NewNodeSerializer()
	node := &btreeapi.Node{
		IsLeaf:  true,
		Count:   2,
		HighKey: []byte("zzz"),
		Next:    42,
		Entries: []btreeapi.LeafEntry{
			{Key: []byte("aaa"), TxnMin: 1, TxnMax: btreeapi.TxnMaxInfinity, Value: btreeapi.Value{Inline: []byte("val1")}},
			{Key: []byte("bbb"), TxnMin: 2, TxnMax: 5, Value: btreeapi.Value{BlobID: 99}},
		},
	}

	data, err := s.Serialize(node)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(data) != btreeapi.PageSize {
		t.Fatalf("expected %d bytes, got %d", btreeapi.PageSize, len(data))
	}

	got, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if !got.IsLeaf || got.Count != 2 || got.Next != 42 {
		t.Fatalf("header mismatch: isLeaf=%v count=%d next=%d", got.IsLeaf, got.Count, got.Next)
	}
	if !bytes.Equal(got.HighKey, []byte("zzz")) {
		t.Fatalf("HighKey mismatch: %q", got.HighKey)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got.Entries))
	}

	e0 := got.Entries[0]
	if !bytes.Equal(e0.Key, []byte("aaa")) || e0.TxnMin != 1 || e0.TxnMax != btreeapi.TxnMaxInfinity {
		t.Fatalf("entry 0 mismatch: %+v", e0)
	}
	if !bytes.Equal(e0.Value.Inline, []byte("val1")) {
		t.Fatalf("entry 0 value mismatch: %q", e0.Value.Inline)
	}

	e1 := got.Entries[1]
	if !bytes.Equal(e1.Key, []byte("bbb")) || e1.TxnMin != 2 || e1.TxnMax != 5 {
		t.Fatalf("entry 1 mismatch: %+v", e1)
	}
	if e1.Value.BlobID != 99 {
		t.Fatalf("entry 1 blobID mismatch: %d", e1.Value.BlobID)
	}
}

func TestSerializeDeserializeInternal(t *testing.T) {
	s := NewNodeSerializer()
	node := &btreeapi.Node{
		IsLeaf:   false,
		Count:    2,
		HighKey:  []byte("mmm"),
		Next:     7,
		Keys:     [][]byte{[]byte("ddd"), []byte("hhh")},
		Children: []uint64{10, 20, 30},
	}

	data, err := s.Serialize(node)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	got, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if got.IsLeaf || got.Count != 2 || got.Next != 7 {
		t.Fatalf("header mismatch")
	}
	if !bytes.Equal(got.HighKey, []byte("mmm")) {
		t.Fatalf("HighKey mismatch")
	}
	if len(got.Keys) != 2 || len(got.Children) != 3 {
		t.Fatalf("keys/children count mismatch: %d/%d", len(got.Keys), len(got.Children))
	}
	if !bytes.Equal(got.Keys[0], []byte("ddd")) || !bytes.Equal(got.Keys[1], []byte("hhh")) {
		t.Fatalf("keys mismatch")
	}
	if got.Children[0] != 10 || got.Children[1] != 20 || got.Children[2] != 30 {
		t.Fatalf("children mismatch: %v", got.Children)
	}
}

// ─── Basic Put/Get Tests ────────────────────────────────────────────

func newTestTree() btreeapi.BTree {
	pages := NewMemPageProvider()
	return New(btreeapi.Config{}, pages, nil)
}

func TestPutGetSingle(t *testing.T) {
	tree := newTestTree()
	if err := tree.Put([]byte("hello"), []byte("world"), 1); err != nil {
		t.Fatal(err)
	}
	val, err := tree.Get([]byte("hello"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("expected 'world', got %q", val)
	}
}

func TestPutGet100Keys(t *testing.T) {
	tree := newTestTree()
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		if err := tree.Put([]byte(k), []byte(v), 1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		got, err := tree.Get([]byte(k), 1)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("Get %s: expected %q, got %q", k, v, got)
		}
	}
}

func TestPutGet1000Keys(t *testing.T) {
	tree := newTestTree()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("value-%06d", i)
		if err := tree.Put([]byte(k), []byte(v), 1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("value-%06d", i)
		got, err := tree.Get([]byte(k), 1)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("Get %s: expected %q, got %q", k, v, got)
		}
	}
}

// ─── Overwrite Test ─────────────────────────────────────────────────

func TestPutOverwrite(t *testing.T) {
	tree := newTestTree()
	if err := tree.Put([]byte("key"), []byte("v1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("key"), []byte("v2"), 2); err != nil {
		t.Fatal(err)
	}
	// txnID=2 should see v2
	val, err := tree.Get([]byte("key"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected 'v2', got %q", val)
	}
}

// ─── Delete Test ────────────────────────────────────────────────────

func TestDeleteAndGet(t *testing.T) {
	tree := newTestTree()
	if err := tree.Put([]byte("key"), []byte("val"), 1); err != nil {
		t.Fatal(err)
	}
	if err := tree.Delete([]byte("key"), 2); err != nil {
		t.Fatal(err)
	}
	// txnID=2 should NOT see the key
	_, err := tree.Get([]byte("key"), 2)
	if err != btreeapi.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// ─── Scan Tests ─────────────────────────────────────────────────────

func TestScanRange(t *testing.T) {
	tree := newTestTree()
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		if err := tree.Put([]byte(k), []byte(v), 1); err != nil {
			t.Fatal(err)
		}
	}

	// Scan [key-0010, key-0020)
	iter := tree.Scan([]byte("key-0010"), []byte("key-0020"), 1)
	defer iter.Close()

	var keys []string
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 10 {
		t.Fatalf("expected 10 keys, got %d: %v", len(keys), keys)
	}
	if keys[0] != "key-0010" || keys[9] != "key-0019" {
		t.Fatalf("unexpected range: first=%s last=%s", keys[0], keys[9])
	}
}

func TestScanEmpty(t *testing.T) {
	tree := newTestTree()
	if err := tree.Put([]byte("aaa"), []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	iter := tree.Scan([]byte("zzz"), []byte("zzzz"), 1)
	defer iter.Close()
	if iter.Next() {
		t.Fatal("expected empty scan")
	}
}

// ─── Split Test ─────────────────────────────────────────────────────

func TestSplitTriggered(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)

	// Insert enough keys to trigger multiple splits
	n := 200
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("value-%06d-padding-to-make-entries-larger", i)
		if err := tree.Put([]byte(k), []byte(v), 1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Verify all keys still readable
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("value-%06d-padding-to-make-entries-larger", i)
		got, err := tree.Get([]byte(k), 1)
		if err != nil {
			t.Fatalf("Get %s after split: %v", k, err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("Get %s: expected %q, got %q", k, v, got)
		}
	}

	// Should have multiple pages
	if pages.PageCount() < 3 {
		t.Fatalf("expected multiple pages after splits, got %d", pages.PageCount())
	}
}

// ─── MVCC Tests ─────────────────────────────────────────────────────

func TestMVCCDifferentVersions(t *testing.T) {
	tree := newTestTree()
	// txn 1 writes v1
	if err := tree.Put([]byte("key"), []byte("v1"), 1); err != nil {
		t.Fatal(err)
	}
	// txn 2 writes v2
	if err := tree.Put([]byte("key"), []byte("v2"), 2); err != nil {
		t.Fatal(err)
	}
	// txn 1 should still see v1 (txnMin=1, txnMax=2 → visible to txnID=1 since 2 > 1)
	val, err := tree.Get([]byte("key"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("txn 1 expected 'v1', got %q", val)
	}
	// txn 2 should see v2
	val, err = tree.Get([]byte("key"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("txn 2 expected 'v2', got %q", val)
	}
}

func TestMVCCDeleteOldTxnStillSees(t *testing.T) {
	tree := newTestTree()
	// txn 1 writes
	if err := tree.Put([]byte("key"), []byte("val"), 1); err != nil {
		t.Fatal(err)
	}
	// txn 3 deletes
	if err := tree.Delete([]byte("key"), 3); err != nil {
		t.Fatal(err)
	}
	// txn 2 (before delete) should still see it (TxnMax=3, 3 > 2)
	val, err := tree.Get([]byte("key"), 2)
	if err != nil {
		t.Fatalf("txn 2 should see key: %v", err)
	}
	if !bytes.Equal(val, []byte("val")) {
		t.Fatalf("expected 'val', got %q", val)
	}
	// txn 3 should NOT see it
	_, err = tree.Get([]byte("key"), 3)
	if err != btreeapi.ErrKeyNotFound {
		t.Fatalf("txn 3 expected ErrKeyNotFound, got %v", err)
	}
}

// ─── HighKey Test ───────────────────────────────────────────────────

func TestHighKeyAfterSplit(t *testing.T) {
	pages := NewMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil).(*bTree)

	// Insert enough to trigger split
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("value-%04d-padding-to-fill-page-faster!!", i)
		if err := tree.Put([]byte(k), []byte(v), 1); err != nil {
			t.Fatal(err)
		}
	}

	// Read root — should be internal node after splits
	root, err := pages.ReadPage(tree.rootPageID.Load())
	if err != nil {
		t.Fatal(err)
	}
	if root.IsLeaf && len(root.Entries) > 50 {
		t.Fatal("expected splits to have occurred")
	}

	// Walk all leaves via right-links, verify HighKey chain
	// Find leftmost leaf
	pid := tree.rootPageID.Load()
	for {
		node, err := pages.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		if node.IsLeaf {
			// Walk the leaf chain
			prevHighKey := []byte(nil)
			for {
				if prevHighKey != nil && node.HighKey != nil {
					if bytes.Compare(prevHighKey, node.HighKey) >= 0 {
						t.Fatalf("HighKey not increasing: prev=%q cur=%q", prevHighKey, node.HighKey)
					}
				}
				// All entries should be < HighKey (if HighKey is set)
				for _, e := range node.Entries {
					if node.HighKey != nil && bytes.Compare(e.Key, node.HighKey) >= 0 {
						t.Fatalf("entry key %q >= HighKey %q", e.Key, node.HighKey)
					}
				}
				prevHighKey = node.HighKey
				if node.Next == 0 {
					// Rightmost leaf should have nil HighKey
					if node.HighKey != nil {
						t.Fatalf("rightmost leaf should have nil HighKey, got %q", node.HighKey)
					}
					break
				}
				node, err = pages.ReadPage(node.Next)
				if err != nil {
					t.Fatal(err)
				}
			}
			break
		}
		pid = node.Children[0]
	}
}

// ─── Scan with Splits ───────────────────────────────────────────────

func TestScanAcrossSplits(t *testing.T) {
	tree := newTestTree()
	n := 500
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("val-%06d", i)
		if err := tree.Put([]byte(k), []byte(v), 1); err != nil {
			t.Fatal(err)
		}
	}

	// Full scan
	iter := tree.Scan([]byte("key-"), []byte("key-999999"), 1)
	defer iter.Close()
	count := 0
	var lastKey string
	for iter.Next() {
		k := string(iter.Key())
		if lastKey != "" && k <= lastKey {
			t.Fatalf("scan not in order: %q after %q", k, lastKey)
		}
		lastKey = k
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("expected %d keys in scan, got %d", n, count)
	}
}
