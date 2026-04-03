// Package internal provides the B+Tree implementation.
package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/akzj/go-fast-kv/pkg/btree/api"
	objstore "github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// mockObjectStore implements objstore.ObjectStore for testing.
type mockObjectStore struct {
	pages    map[objstore.ObjectID][]byte
	blobs    map[objstore.ObjectID][]byte
	nextPage uint64
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		pages:    make(map[objstore.ObjectID][]byte),
		blobs:    make(map[objstore.ObjectID][]byte),
		nextPage: 1000,
	}
}

var _ objstore.ObjectStore = (*mockObjectStore)(nil)

func (m *mockObjectStore) MakeObjectID(t objstore.ObjectType, seq uint64) objstore.ObjectID {
	return objstore.MakeObjectID(t, seq)
}

func (m *mockObjectStore) GetType() objstore.ObjectType {
	return 0
}

func (m *mockObjectStore) GetSequence() uint64 {
	return 0
}

func (m *mockObjectStore) AllocPage(ctx context.Context) (objstore.ObjectID, error) {
	id := objstore.MakeObjectID(objstore.ObjectTypePage, m.nextPage)
	m.nextPage++
	return id, nil
}

func (m *mockObjectStore) WritePage(ctx context.Context, id objstore.ObjectID, data []byte) (objstore.ObjectID, error) {
	m.pages[id] = make([]byte, len(data))
	copy(m.pages[id], data)
	return id, nil
}

func (m *mockObjectStore) ReadPage(ctx context.Context, id objstore.ObjectID) ([]byte, error) {
	data, ok := m.pages[id]
	if !ok {
		return nil, objstore.ErrObjectNotFound
	}
	return data, nil
}

func (m *mockObjectStore) DeletePage(ctx context.Context, id objstore.ObjectID) error {
	delete(m.pages, id)
	return nil
}

func (m *mockObjectStore) WriteBlob(ctx context.Context, data []byte) (objstore.ObjectID, error) {
	id := objstore.MakeObjectID(objstore.ObjectTypeBlob, m.nextPage)
	m.nextPage++
	m.blobs[id] = make([]byte, len(data))
	copy(m.blobs[id], data)
	return id, nil
}

func (m *mockObjectStore) ReadBlob(ctx context.Context, id objstore.ObjectID) ([]byte, error) {
	data, ok := m.blobs[id]
	if !ok {
		return nil, objstore.ErrObjectNotFound
	}
	return data, nil
}

func (m *mockObjectStore) DeleteBlob(ctx context.Context, id objstore.ObjectID) error {
	delete(m.blobs, id)
	return nil
}

func (m *mockObjectStore) Sync(ctx context.Context) error {
	return nil
}

func (m *mockObjectStore) Close() error {
	return nil
}

func (m *mockObjectStore) Delete(ctx context.Context, id objstore.ObjectID) error {
	delete(m.pages, id)
	delete(m.blobs, id)
	return nil
}

func (m *mockObjectStore) GetLocation(ctx context.Context, id objstore.ObjectID) (objstore.ObjectLocation, error) {
	return objstore.ObjectLocation{}, nil
}

func (m *mockObjectStore) GetSegmentIDs(ctx context.Context) []uint64 {
	return nil
}

func (m *mockObjectStore) GetSegmentType(ctx context.Context, segID uint64) objstore.SegmentType {
	return objstore.SegmentTypePage
}

func (m *mockObjectStore) GetSegmentMeta(ctx context.Context, segID uint64) (*objstore.SegmentMeta, error) {
	return nil, nil
}

func (m *mockObjectStore) CompactSegment(ctx context.Context, segID uint64) error {
	return nil
}

func (m *mockObjectStore) DeleteSegment(ctx context.Context, segID uint64) error {
	return nil
}

func (m *mockObjectStore) MarkObjectDeleted(ctx context.Context, id objstore.ObjectID, size uint32) {
}

func (m *mockObjectStore) GetActiveSegmentID(ctx context.Context, segType objstore.SegmentType) (uint64, error) {
	return 1, nil
}

// Test helpers
func setupBTree(t *testing.T) (*BTreeImpl, *mockObjectStore) {
	store := newMockObjectStore()
	
	// Create BTreeImpl directly since NewBTree has an issue returning it
	bt := &BTreeImpl{
		root:     1,
		store:    store,
		order:    256,
		pageSize: 4096,
		inlineThreshold: 512,
		cache:    make(map[api.PageID]*page),
		config:   api.DefaultBTreeConfig(),
		blobIDs:  make(map[objstore.ObjectID]struct{}),
	}

	// Initialize root page
	ctx := context.Background()
	rootPage := newPage(true)
	rootPage.parentHint = 0
	bt.cache[bt.root] = rootPage

	// Write root page to store
	pageData, _ := rootPage.MarshalBinary()
	objID := objstore.MakeObjectID(objstore.ObjectTypePage, uint64(bt.root))
	store.WritePage(ctx, objID, pageData)

	return bt, store
}

func TestBTreePutGet(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Put some key-value pairs
	tests := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, tt := range tests {
		err := bt.Put(ctx, []byte(tt.key), []byte(tt.value))
		if err != nil {
			t.Fatalf("Put(%s) failed: %v", tt.key, err)
		}
	}

	// Get them back
	for _, tt := range tests {
		val, found, err := bt.Get(ctx, []byte(tt.key))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", tt.key, err)
		}
		if !found {
			t.Fatalf("Get(%s): not found", tt.key)
		}
		if string(val) != tt.value {
			t.Fatalf("Get(%s): got %q, want %q", tt.key, val, tt.value)
		}
	}
}

func TestBTreeDelete(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Put a key-value pair
	err := bt.Put(ctx, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify it exists
	_, found, _ := bt.Get(ctx, []byte("key1"))
	if !found {
		t.Fatal("key1 not found after put")
	}

	// Delete it
	err = bt.Delete(ctx, []byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, found, _ = bt.Get(ctx, []byte("key1"))
	if found {
		t.Fatal("key1 still found after delete")
	}
}

func TestBTreeScan(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Insert multiple key-value pairs
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		bt.Put(ctx, []byte(k), []byte(k+"-value"))
	}

	// Scan range [b, d)
	var scanned []string
	err := bt.Scan(ctx, []byte("b"), []byte("d"), func(key, value []byte) bool {
		scanned = append(scanned, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Should get b and c (not d, since end is exclusive)
	expected := []string{"b", "c"}
	if len(scanned) != len(expected) {
		t.Fatalf("Scan: got %d keys, want %d", len(scanned), len(expected))
	}
	for i := range expected {
		if scanned[i] != expected[i] {
			t.Errorf("Scan[%d]: got %s, want %s", i, scanned[i], expected[i])
		}
	}
}

func TestBTreeUpdate(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Put initial value
	bt.Put(ctx, []byte("key1"), []byte("value1"))

	// Update value
	bt.Put(ctx, []byte("key1"), []byte("value2"))

	// Get updated value
	val, _, _ := bt.Get(ctx, []byte("key1"))
	if string(val) != "value2" {
		t.Fatalf("Update: got %q, want %q", val, "value2")
	}
}

func TestBTreeLargeValue(t *testing.T) {
	bt, store := setupBTree(t)
	ctx := context.Background()

	// Create a large value that exceeds inline threshold
	largeValue := make([]byte, 1000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Put large value
	err := bt.Put(ctx, []byte("large"), largeValue)
	if err != nil {
		t.Fatalf("Put large value failed: %v", err)
	}

	// Get it back
	val, found, err := bt.Get(ctx, []byte("large"))
	if err != nil {
		t.Fatalf("Get large value failed: %v", err)
	}
	if !found {
		t.Fatal("large value not found")
	}
	if len(val) != len(largeValue) {
		t.Fatalf("Get large value: got %d bytes, want %d", len(val), len(largeValue))
	}
	// Verify content
	for i := range val {
		if val[i] != largeValue[i] {
			t.Fatalf("Get large value: mismatch at byte %d", i)
		}
	}

	// Verify blob was created in store
	if len(store.blobs) == 0 {
		t.Fatal("No blobs created in store")
	}
}

func TestBTreeClose(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Put some data
	bt.Put(ctx, []byte("key1"), []byte("value1"))

	// Close
	err := bt.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestBTreeFlush(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Put some data
	bt.Put(ctx, []byte("key1"), []byte("value1"))

	// Flush
	err := bt.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestBTreeIter(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Insert multiple key-value pairs
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		bt.Put(ctx, []byte(k), []byte(k+"-value"))
	}

	// Create iterator
	iter, err := bt.CreateScanIter(nil, nil)
	if err != nil {
		t.Fatalf("CreateScanIter failed: %v", err)
	}
	defer iter.Close()

	// Iterate
	var scanned []string
	for {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iter.Next failed: %v", err)
		}
		if key == nil {
			break
		}
		scanned = append(scanned, string(key))
	}

	if len(scanned) != len(keys) {
		t.Fatalf("Iter: got %d keys, want %d", len(scanned), len(keys))
	}
}

func TestPageMarshal(t *testing.T) {
	p := newPage(true)
	p.keys = [][]byte{[]byte("key1"), []byte("key2")}
	p.values = [][]byte{[]byte("value1"), []byte("value2")}
	p.numKeys = 2

	data, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	p2 := &page{}
	err = p2.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if p2.numKeys != p.numKeys {
		t.Fatalf("numKeys: got %d, want %d", p2.numKeys, p.numKeys)
	}
	for i := 0; i < int(p.numKeys); i++ {
		if string(p2.keys[i]) != string(p.keys[i]) {
			t.Errorf("key[%d]: got %q, want %q", i, p2.keys[i], p.keys[i])
		}
		if string(p2.values[i]) != string(p.values[i]) {
			t.Errorf("value[%d]: got %q, want %q", i, p2.values[i], p.values[i])
		}
	}
}

func TestBTreeMultipleValues(t *testing.T) {
	bt, _ := setupBTree(t)
	ctx := context.Background()

	// Insert many key-value pairs
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := bt.Put(ctx, []byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Put(%s) failed: %v", key, err)
		}
	}

	// Verify all can be retrieved
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		expected := fmt.Sprintf("value%03d", i)
		val, found, err := bt.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", key, err)
		}
		if !found {
			t.Fatalf("Get(%s): not found", key)
		}
		if string(val) != expected {
			t.Fatalf("Get(%s): got %q, want %q", key, val, expected)
		}
	}
}
