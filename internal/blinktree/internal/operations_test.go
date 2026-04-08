package internal

import (
	"bytes"
	"testing"

	blinktree "github.com/akzj/go-fast-kv/internal/blinktree/api"
)

// =============================================================================
// NodeOperations Tests
// =============================================================================

func TestNodeOperations_Search(t *testing.T) {
	nodeOps := NewNodeOperations()

	t.Run("SearchEmptyLeaf", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		idx := nodeOps.Search(node, intKey(100))
		if idx != 0 {
			t.Errorf("expected 0, got %d", idx)
		}
	})

	t.Run("SearchLeaf", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    3,
			Capacity: MaxNodeCapacity,
		}
		entries := []LeafEntry{
			{Key: intKey(10), Value: MakeInlineValue([]byte("val10"))},
			{Key: intKey(20), Value: MakeInlineValue([]byte("val20"))},
			{Key: intKey(30), Value: MakeInlineValue([]byte("val30"))},
		}
		StoreLeafEntries(node, entries)

		// Lower-bound search: returns first index where entry.Key >= key
		tests := []struct {
			key      []byte
			expected int
		}{
			{intKey(5), 0},  // Before first → 0
			{intKey(10), 0}, // At first → 0 (lower bound)
			{intKey(15), 1}, // Between first and second → 1
			{intKey(20), 1}, // At second → 1
			{intKey(25), 2}, // Between second and third → 2
			{intKey(30), 2}, // At third → 2
			{intKey(35), 3}, // After last → 3
		}

		for _, tc := range tests {
			idx := nodeOps.Search(node, tc.key)
			if idx != tc.expected {
				t.Errorf("Search(%x): expected %d, got %d", tc.key, tc.expected, idx)
			}
		}
	})

	t.Run("SearchInternal", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeInternal,
			Level:    1,
			Count:    3,
			Capacity: MaxNodeCapacity,
		}
		entries := []InternalEntry{
			{Key: intKey(10), Child: PageID(100)},
			{Key: intKey(20), Child: PageID(200)},
			{Key: intKey(30), Child: PageID(300)},
		}
		StoreInternalEntries(node, entries)

		tests := []struct {
			key      []byte
			expected int
		}{
			{intKey(5), 0},  // Before first → 0
			{intKey(10), 0}, // At first → 0
			{intKey(15), 1}, // Between first and second → 1
			{intKey(20), 1}, // At second → 1
			{intKey(25), 2}, // Between second and third → 2
			{intKey(30), 2}, // At third → 2
			{intKey(35), 3}, // After last → 3
		}

		for _, tc := range tests {
			idx := nodeOps.Search(node, tc.key)
			if idx != tc.expected {
				t.Errorf("Search(%x): expected %d, got %d", tc.key, tc.expected, idx)
			}
		}
	})
}

func TestNodeOperations_Insert(t *testing.T) {
	nodeOps := NewNodeOperations()

	t.Run("InsertIntoEmptyLeaf", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		value := MakeInlineValue([]byte("test"))
		newNode, splitKey, err := nodeOps.Insert(node, intKey(100), value)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		if newNode != nil {
			t.Error("expected no split for first insert")
		}
		if splitKey != nil {
			t.Errorf("expected nil splitKey, got %x", splitKey)
		}
		entries := ExtractLeafEntries(node)
		if node.Count != 1 {
			t.Errorf("expected count 1, got %d", node.Count)
		}
		if !bytes.Equal(entries[0].Key, intKey(100)) {
			t.Errorf("expected key intKey(100), got %x", entries[0].Key)
		}
	})

	t.Run("InsertInOrder", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		for i := uint64(1); i <= 10; i++ {
			value := MakeInlineValue([]byte("val"))
			_, _, err := nodeOps.Insert(node, intKey(i*10), value)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}
		if node.Count != 10 {
			t.Errorf("expected count 10, got %d", node.Count)
		}
		entries := ExtractLeafEntries(node)
		for i := 0; i < 10; i++ {
			expected := intKey(uint64((i + 1) * 10))
			if !bytes.Equal(entries[i].Key, expected) {
				t.Errorf("entry %d: expected key %x, got %x", i, expected, entries[i].Key)
			}
		}
	})

	t.Run("InsertOutOfOrder", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		keys := []uint64{50, 10, 30, 20, 40}
		for _, k := range keys {
			value := MakeInlineValue([]byte("val"))
			_, _, err := nodeOps.Insert(node, intKey(k), value)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", k, err)
			}
		}
		entries := ExtractLeafEntries(node)
		expectedKeys := []uint64{10, 20, 30, 40, 50}
		for i, expected := range expectedKeys {
			if !bytes.Equal(entries[i].Key, intKey(expected)) {
				t.Errorf("entry %d: expected key %x, got %x", i, intKey(expected), entries[i].Key)
			}
		}
	})

	t.Run("InsertTriggersSplit", func(t *testing.T) {
		// Realistic leaf capacity: (4096 - 98) / 130 = 30
		leafCapacity := uint16((4096 - NodeHeaderSize) / LeafEntrySize)
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: leafCapacity,
		}
		// Fill node to capacity, then one more triggers split
		for i := uint64(1); i <= uint64(leafCapacity)+1; i++ {
			value := MakeInlineValue([]byte("val"))
			newNode, splitKey, err := nodeOps.Insert(node, intKey(i), value)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
			if i == uint64(leafCapacity)+1 {
				if newNode == nil {
					t.Error("expected split at capacity+1")
				}
				if splitKey == nil {
					t.Error("expected non-nil split key")
				}
			}
		}
	})

	t.Run("InsertDuplicateKey", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		value1 := MakeInlineValue([]byte("val1"))
		_, _, err := nodeOps.Insert(node, intKey(100), value1)
		if err != nil {
			t.Fatalf("Insert 1 failed: %v", err)
		}
		value2 := MakeInlineValue([]byte("val2"))
		_, _, err = nodeOps.Insert(node, intKey(100), value2)
		if err != nil {
			t.Fatalf("Insert 2 failed: %v", err)
		}
		// Duplicate should replace
		if node.Count != 1 {
			t.Errorf("expected count 1, got %d", node.Count)
		}
	})

	t.Run("InsertIntoInternalNode", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeInternal,
			Level:    1,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		value := MakeInlineValue([]byte("test"))
		_, _, err := nodeOps.Insert(node, intKey(100), value)
		if err != ErrInvalidNode {
			t.Errorf("expected ErrInvalidNode, got %v", err)
		}
	})
}

func TestNodeOperations_Split(t *testing.T) {
	nodeOps := NewNodeOperations()

	t.Run("SplitLeafNode", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    10,
			Capacity: MaxNodeCapacity,
		}
		entries := make([]LeafEntry, 10)
		for i := 0; i < 10; i++ {
			entries[i] = LeafEntry{Key: intKey(uint64(i + 1)), Value: MakeInlineValue([]byte("val"))}
		}
		StoreLeafEntries(node, entries)

		left, right, splitKey := nodeOps.Split(node)

		// Check split key is median (key 6, since median index = 5)
		if !bytes.Equal(splitKey, intKey(6)) {
			t.Errorf("expected splitKey intKey(6), got %x", splitKey)
		}

		// Check left node has first half
		if left.Count != 5 {
			t.Errorf("left count: expected 5, got %d", left.Count)
		}
		leftEntries := ExtractLeafEntries(left)
		for i := 0; i < int(left.Count); i++ {
			if !bytes.Equal(leftEntries[i].Key, intKey(uint64(i+1))) {
				t.Errorf("left entry %d: expected %x, got %x", i, intKey(uint64(i+1)), leftEntries[i].Key)
			}
		}

		// Check right node has second half
		if right.Count != 5 {
			t.Errorf("right count: expected 5, got %d", right.Count)
		}
		rightEntries := ExtractLeafEntries(right)
		for i := 0; i < int(right.Count); i++ {
			expected := intKey(uint64(i + 6))
			if !bytes.Equal(rightEntries[i].Key, expected) {
				t.Errorf("right entry %d: expected %x, got %x", i, expected, rightEntries[i].Key)
			}
		}

		// Nodes should not share RawData
		if &left.RawData == &right.RawData {
			t.Error("left and right nodes share same RawData")
		}
	})

	t.Run("SplitInternalNode", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeInternal,
			Level:    1,
			Count:    6,
			Capacity: MaxNodeCapacity,
		}
		entries := make([]InternalEntry, 6)
		for i := 0; i < 6; i++ {
			entries[i] = InternalEntry{
				Key:   intKey(uint64((i + 1) * 10)),
				Child: PageID(uint64(i) * 100),
			}
		}
		StoreInternalEntries(node, entries)

		left, right, splitKey := nodeOps.Split(node)

		// Median is at index 3 (0-indexed), which is key 40
		if !bytes.Equal(splitKey, intKey(40)) {
			t.Errorf("expected splitKey intKey(40), got %x", splitKey)
		}

		// Left has 3 entries (indices 0,1,2) - keys 10,20,30
		if left.Count != 3 {
			t.Errorf("left count: expected 3, got %d", left.Count)
		}
		leftEntries := ExtractInternalEntries(left)
		if !bytes.Equal(leftEntries[0].Key, intKey(10)) ||
			!bytes.Equal(leftEntries[1].Key, intKey(20)) ||
			!bytes.Equal(leftEntries[2].Key, intKey(30)) {
			t.Error("left entries incorrect")
		}

		// Right has 3 entries (indices 3,4,5) - keys 40,50,60
		if right.Count != 3 {
			t.Errorf("right count: expected 3, got %d", right.Count)
		}
		rightEntries := ExtractInternalEntries(right)
		if !bytes.Equal(rightEntries[0].Key, intKey(40)) ||
			!bytes.Equal(rightEntries[1].Key, intKey(50)) ||
			!bytes.Equal(rightEntries[2].Key, intKey(60)) {
			t.Error("right entries incorrect")
		}
	})

	t.Run("SplitOddCount", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    5,
			Capacity: MaxNodeCapacity,
		}
		entries := make([]LeafEntry, 5)
		for i := 0; i < 5; i++ {
			entries[i] = LeafEntry{Key: intKey(uint64(i + 1)), Value: MakeInlineValue([]byte("val"))}
		}
		StoreLeafEntries(node, entries)

		_, _, splitKey := nodeOps.Split(node)
		// Median is index 2 (key 3)
		if !bytes.Equal(splitKey, intKey(3)) {
			t.Errorf("expected splitKey intKey(3), got %x", splitKey)
		}
	})
}

func TestNodeOperations_UpdateHighKey(t *testing.T) {
	nodeOps := NewNodeOperations()

	t.Run("UpdateHighKeyEmptyNode", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		highKey := nodeOps.UpdateHighKey(node)
		if highKey != nil {
			t.Errorf("expected nil for empty node, got %x", highKey)
		}
	})

	t.Run("UpdateHighKeyLeaf", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    5,
			Capacity: MaxNodeCapacity,
		}
		entries := []LeafEntry{
			{Key: intKey(10), Value: MakeInlineValue([]byte("a"))},
			{Key: intKey(20), Value: MakeInlineValue([]byte("b"))},
			{Key: intKey(30), Value: MakeInlineValue([]byte("c"))},
			{Key: intKey(40), Value: MakeInlineValue([]byte("d"))},
			{Key: intKey(50), Value: MakeInlineValue([]byte("e"))},
		}
		StoreLeafEntries(node, entries)

		highKey := nodeOps.UpdateHighKey(node)
		if !bytes.Equal(highKey, intKey(50)) {
			t.Errorf("expected intKey(50), got %x", highKey)
		}
	})

	t.Run("UpdateHighKeyInternal", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeInternal,
			Level:    1,
			Count:    3,
			Capacity: MaxNodeCapacity,
		}
		entries := []InternalEntry{
			{Key: intKey(10), Child: PageID(100)},
			{Key: intKey(20), Child: PageID(200)},
			{Key: intKey(30), Child: PageID(300)},
		}
		StoreInternalEntries(node, entries)

		highKey := nodeOps.UpdateHighKey(node)
		if !bytes.Equal(highKey, intKey(30)) {
			t.Errorf("expected intKey(30), got %x", highKey)
		}
	})
}

func TestNodeOperations_SerializeDeserialize(t *testing.T) {
	nodeOps := NewNodeOperations()

	t.Run("SerializeDeserializeLeaf", func(t *testing.T) {
		original := &NodeFormat{
			NodeType:    NodeTypeLeaf,
			Count:       3,
			Capacity:    MaxNodeCapacity,
			HighSibling: PageID(1000),
			LowSibling:  PageID(0),
			HighKey:     intKey(300),
		}
		entries := []LeafEntry{
			{Key: intKey(100), Value: MakeInlineValue([]byte("val100"))},
			{Key: intKey(200), Value: MakeInlineValue([]byte("val200"))},
			{Key: intKey(300), Value: MakeInlineValue([]byte("val300"))},
		}
		StoreLeafEntries(original, entries)

		data := nodeOps.Serialize(original)
		restored, err := nodeOps.Deserialize(data)
		if err != nil {
			t.Fatalf("Deserialize failed: %v", err)
		}

		if restored.NodeType != original.NodeType {
			t.Errorf("NodeType: expected %d, got %d", original.NodeType, restored.NodeType)
		}
		if restored.Count != original.Count {
			t.Errorf("Count: expected %d, got %d", original.Count, restored.Count)
		}
		if !bytes.Equal(restored.HighKey, original.HighKey) {
			t.Errorf("HighKey: expected %x, got %x", original.HighKey, restored.HighKey)
		}

		restoredEntries := ExtractLeafEntries(restored)
		for i := 0; i < int(original.Count); i++ {
			if !bytes.Equal(restoredEntries[i].Key, entries[i].Key) {
				t.Errorf("entry %d key: expected %x, got %x", i, entries[i].Key, restoredEntries[i].Key)
			}
		}
	})

	t.Run("SerializeDeserializeInternal", func(t *testing.T) {
		original := &NodeFormat{
			NodeType:    NodeTypeInternal,
			Level:       2,
			Count:       3,
			Capacity:    MaxNodeCapacity,
			HighSibling: PageID(500),
		}
		entries := []InternalEntry{
			{Key: intKey(100), Child: PageID(100)},
			{Key: intKey(200), Child: PageID(200)},
			{Key: intKey(300), Child: PageID(300)},
		}
		StoreInternalEntries(original, entries)

		data := nodeOps.Serialize(original)
		restored, err := nodeOps.Deserialize(data)
		if err != nil {
			t.Fatalf("Deserialize failed: %v", err)
		}

		if restored.NodeType != NodeTypeInternal {
			t.Error("expected internal node type")
		}
		if restored.Level != 2 {
			t.Errorf("Level: expected 2, got %d", restored.Level)
		}
		if restored.Count != 3 {
			t.Errorf("Count: expected 3, got %d", restored.Count)
		}

		restoredEntries := ExtractInternalEntries(restored)
		for i := 0; i < 3; i++ {
			if !bytes.Equal(restoredEntries[i].Key, entries[i].Key) {
				t.Errorf("entry %d key: expected %x, got %x", i, entries[i].Key, restoredEntries[i].Key)
			}
			if restoredEntries[i].Child != entries[i].Child {
				t.Errorf("entry %d child mismatch", i)
			}
		}
	})

	t.Run("DeserializeInvalidData", func(t *testing.T) {
		_, err := nodeOps.Deserialize([]byte("too short"))
		if err != ErrInvalidNode {
			t.Errorf("expected ErrInvalidNode, got %v", err)
		}
	})
}

// =============================================================================
// InMemoryNodeManager Tests
// =============================================================================

func TestInMemoryNodeManager(t *testing.T) {
	nodeOps := NewNodeOperations()
	mgr := NewInMemoryNodeManager(nodeOps)

	t.Run("CreateLeaf", func(t *testing.T) {
		node, addr := mgr.CreateLeaf()
		if node == nil {
			t.Fatal("expected non-nil node")
		}
		if !addr.IsValid() {
			t.Error("expected valid address")
		}
		if node.NodeType != NodeTypeLeaf {
			t.Errorf("expected leaf node, got %d", node.NodeType)
		}
		if node.Count != 0 {
			t.Errorf("expected empty node, count=%d", node.Count)
		}
	})

	t.Run("CreateInternal", func(t *testing.T) {
		node, addr := mgr.CreateInternal(1)
		if node == nil {
			t.Fatal("expected non-nil node")
		}
		if !addr.IsValid() {
			t.Error("expected valid address")
		}
		if node.NodeType != NodeTypeInternal {
			t.Errorf("expected internal node, got %d", node.NodeType)
		}
		if node.Level != 1 {
			t.Errorf("expected level 1, got %d", node.Level)
		}
	})

	t.Run("PersistAndLoad", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    2,
			Capacity: MaxNodeCapacity,
		}
		entries := []LeafEntry{
			{Key: intKey(100), Value: MakeInlineValue([]byte("a"))},
			{Key: intKey(200), Value: MakeInlineValue([]byte("b"))},
		}
		StoreLeafEntries(node, entries)

		// Create the node first to get a PageID, then persist with it
		leafNode, pageID := mgr.CreateLeaf()
		*leafNode = *node // Copy data into the storage-allocated node

		err := mgr.Persist(leafNode, pageID)
		if err != nil {
			t.Fatalf("Persist failed: %v", err)
		}

		loaded, err := mgr.Load(pageID)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if loaded.Count != 2 {
			t.Errorf("expected count 2, got %d", loaded.Count)
		}

		loadedEntries := ExtractLeafEntries(loaded)
		if !bytes.Equal(loadedEntries[0].Key, intKey(100)) || !bytes.Equal(loadedEntries[1].Key, intKey(200)) {
			t.Error("loaded entries don't match")
		}
	})

	t.Run("LoadNonExistent", func(t *testing.T) {
		_, err := mgr.Load(PageID(999))
		if err != ErrNodeNotFound {
			t.Errorf("expected ErrNodeNotFound, got %v", err)
		}
	})
}

// =============================================================================
// Tree Tests
// =============================================================================

func TestTree_OpenClose(t *testing.T) {
	nodeOps := NewNodeOperations()
	nodeMgr := NewInMemoryNodeManager(nodeOps)
	tree := newTreeImpl(nodeOps, nodeMgr, true)

	if tree.IsClosed() {
		t.Error("new tree should not be closed")
	}

	err := tree.Open("")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = tree.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !tree.IsClosed() {
		t.Error("closed tree should report IsClosed=true")
	}
}

func TestTree_Put(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	t.Run("PutSingleKey", func(t *testing.T) {
		value := MakeInlineValue([]byte("test value"))
		err := tree.Put(intKey(100), value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	t.Run("PutMultipleKeys", func(t *testing.T) {
		for i := 1; i <= 100; i++ {
			value := MakeInlineValue([]byte("value"))
			err := tree.Put(intKey(uint64(i*10)), value)
			if err != nil {
				t.Fatalf("Put %d failed: %v", i*10, err)
			}
		}
	})

	t.Run("PutDuplicateKey", func(t *testing.T) {
		value1 := MakeInlineValue([]byte("first"))
		err := tree.Put(intKey(500), value1)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		value2 := MakeInlineValue([]byte("second"))
		err = tree.Put(intKey(500), value2)
		if err != nil {
			t.Fatalf("Put duplicate failed: %v", err)
		}
	})

	t.Run("PutAfterClose", func(t *testing.T) {
		tree.Close()
		value := MakeInlineValue([]byte("test"))
		err := tree.Put(intKey(100), value)
		if err != ErrStoreClosed {
			t.Errorf("expected ErrStoreClosed, got %v", err)
		}
	})
}

func TestTree_Get(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Insert test data
	type keyData struct {
		key  []byte
		data []byte
	}
	inserted := make([]keyData, 0, 50)
	for i := 1; i <= 50; i++ {
		key := intKey(uint64(i * 10))
		data := []byte("value for key ")
		data = append(data, byte(i))
		value := MakeInlineValue(data)
		_ = tree.Put(key, value)
		inserted = append(inserted, keyData{key: key, data: data})
	}

	t.Run("GetExistingKeys", func(t *testing.T) {
		for _, kd := range inserted {
			val, err := tree.Get(kd.key)
			if err != nil {
				t.Fatalf("Get %x failed: %v", kd.key, err)
			}
			// Verify value matches (at least length byte)
			if val.Length[7] != byte(len(kd.data)) {
				t.Errorf("Get %x: wrong length", kd.key)
			}
		}
	})

	t.Run("GetNonExistingKey", func(t *testing.T) {
		_, err := tree.Get(intKey(9999))
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("GetFromEmptyTree", func(t *testing.T) {
		emptyTree := NewInMemoryTree()
		_, err := emptyTree.Get(intKey(100))
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
		emptyTree.Close()
	})
}

func TestTree_Delete(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Setup: insert keys
	keys := []uint64{10, 20, 30, 40, 50}
	for _, k := range keys {
		value := MakeInlineValue([]byte("val"))
		_ = tree.Put(intKey(k), value)
	}

	t.Run("DeleteMiddleKey", func(t *testing.T) {
		err := tree.Delete(intKey(30))
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify key is gone
		_, err = tree.Get(intKey(30))
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
		}

		// Other keys still exist
		for _, k := range []uint64{10, 20, 40, 50} {
			_, err := tree.Get(intKey(k))
			if err != nil {
				t.Errorf("key %d should exist after deleting 30", k)
			}
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		err := tree.Delete(intKey(9999))
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("DeleteAllKeys", func(t *testing.T) {
		for _, k := range []uint64{10, 20, 40, 50} {
			err := tree.Delete(intKey(k))
			if err != nil {
				t.Fatalf("Delete %d failed: %v", k, err)
			}
		}

		for _, k := range keys {
			_, err := tree.Get(intKey(k))
			if err != ErrKeyNotFound {
				t.Errorf("key %d should be deleted", k)
			}
		}
	})

	t.Run("DeleteAfterClose", func(t *testing.T) {
		tree.Close()
		err := tree.Delete(intKey(10))
		if err != ErrStoreClosed {
			t.Errorf("expected ErrStoreClosed, got %v", err)
		}
	})
}

func TestTree_Scan(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Insert test data
	for i := 1; i <= 100; i++ {
		value := MakeInlineValue([]byte("val"))
		_ = tree.Put(intKey(uint64(i*10)), value)
	}

	t.Run("ScanAll", func(t *testing.T) {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key()
			if len(key) == 0 {
				t.Error("iterator returned empty key")
			}
			count++
		}
		if count != 100 {
			t.Errorf("expected 100 entries, got %d", count)
		}
	})

	t.Run("ScanRange", func(t *testing.T) {
		iter, err := tree.Scan(intKey(200), intKey(500))
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key()
			if bytes.Compare(key, intKey(200)) < 0 || bytes.Compare(key, intKey(500)) >= 0 {
				t.Errorf("key %x out of range [200, 500)", key)
			}
			count++
		}
		if count != 30 { // 200,210,...,490 = 30 keys
			t.Errorf("expected 30 entries in range, got %d", count)
		}
	})

	t.Run("ScanEmptyRange", func(t *testing.T) {
		iter, err := tree.Scan(intKey(5000), intKey(6000))
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		if count != 0 {
			t.Errorf("expected 0 entries in empty range, got %d", count)
		}
	})

	t.Run("ScanAfterClose", func(t *testing.T) {
		tree.Close()
		_, err := tree.Scan(nil, nil)
		if err != ErrStoreClosed {
			t.Errorf("expected ErrStoreClosed, got %v", err)
		}
	})
}

func TestTree_Batch(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	ops := make([]blinktree.TreeOperation, 100)
	for i := 0; i < 100; i++ {
		ops[i] = blinktree.TreeOperation{
			Type:  blinktree.OpPut,
			Key:   intKey(uint64((i + 1) * 10)),
			Value: MakeInlineValue([]byte("batch")),
		}
	}

	err := tree.Batch(ops)
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	// Verify all inserted
	for i := 1; i <= 100; i++ {
		_, err := tree.Get(intKey(uint64(i * 10)))
		if err != nil {
			t.Errorf("key %d not found after batch", i*10)
		}
	}
}

func TestTree_RootPersistence(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Insert some data
	for i := 1; i <= 20; i++ {
		value := MakeInlineValue([]byte("val"))
		_ = tree.Put(intKey(uint64(i*10)), value)
	}

	// Get root PageID
	rootPageID := tree.GetRootPageID()
	if rootPageID == 0 {
		t.Fatal("expected non-zero root pageID")
	}

	// Create new tree with the same storage and restore root PageID.
	// Note: NewInMemoryTree creates a fresh MemoryPageStorage each time,
	// so data won't be accessible — we only verify root PageID persistence.
	// Data persistence is tested by kvstore persistence tests.
	tree2 := NewInMemoryTree()
	defer tree2.Close()
	tree2.RestoreRootPageID(rootPageID)

	// Verify root PageID was restored correctly
	restoredRootPageID := tree2.GetRootPageID()
	if restoredRootPageID != rootPageID {
		t.Errorf("expected root PageID %d, got %d", rootPageID, restoredRootPageID)
	}

	// Verify root is non-zero (tree was properly initialized with the restored root)
	if restoredRootPageID == 0 {
		t.Error("restored root PageID should not be zero")
	}
}

// =============================================================================
// Tree Iterator Tests
// =============================================================================

func TestTreeIterator(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Insert test data
	for i := 1; i <= 50; i++ {
		value := MakeInlineValue([]byte("val"))
		_ = tree.Put(intKey(uint64(i*10)), value)
	}

	t.Run("IteratorKeyValue", func(t *testing.T) {
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()

		// Should find first entry
		if !iter.Next() {
			t.Fatal("expected at least one entry")
		}

		key := iter.Key()
		if len(key) == 0 {
			t.Error("Key() returned empty")
		}

		val := iter.Value()
		if !val.IsValid() {
			t.Error("Value() returned invalid value")
		}
	})

	t.Run("IteratorOrder", func(t *testing.T) {
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()

		var prevKey []byte
		for iter.Next() {
			key := iter.Key()
			if prevKey != nil && bytes.Compare(key, prevKey) <= 0 {
				t.Errorf("keys not in order: %x <= %x", key, prevKey)
			}
			prevKey = make([]byte, len(key))
			copy(prevKey, key)
		}
	})
}

// =============================================================================
// Concurrent Safety Tests
// =============================================================================

func TestConcurrent_NodeManager(t *testing.T) {
	nodeOps := NewNodeOperations()
	mgr := NewInMemoryNodeManager(nodeOps)

	t.Run("ConcurrentCreateLeaf", func(t *testing.T) {
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					node, addr := mgr.CreateLeaf()
					if node == nil {
						t.Errorf("CreateLeaf returned nil")
					}
					if !addr.IsValid() {
						t.Errorf("CreateLeaf returned invalid addr")
					}
				}
				done <- true
			}()
		}
		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("ConcurrentPersistLoad", func(t *testing.T) {
		// Create a leaf node to get a PageID, then use it for concurrent loads
		leafNode, pageID := mgr.CreateLeaf()
		leafNode.Count = 5
		entries := []LeafEntry{}
		for i := 1; i <= 5; i++ {
			entries = append(entries, LeafEntry{
				Key:   intKey(uint64(i * 10)),
				Value: MakeInlineValue([]byte{byte(i)}),
			})
		}
		StoreLeafEntries(leafNode, entries)

		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					loaded, err := mgr.Load(pageID)
					if err != nil {
						t.Errorf("Load failed: %v", err)
					}
					if loaded == nil {
						t.Errorf("Load returned nil")
					}
				}
				done <- true
			}()
		}
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestConcurrent_Tree(t *testing.T) {
	t.Run("ConcurrentPut", func(t *testing.T) {
		tree := NewInMemoryTree()
		defer tree.Close()

		done := make(chan bool, 5)
		for g := 0; g < 5; g++ {
			go func(goroutineID int) {
				for i := 0; i < 50; i++ {
					key := intKey(uint64(goroutineID*1000 + i))
					value := MakeInlineValue([]byte("val"))
					_ = tree.Put(key, value)
				}
				done <- true
			}(g)
		}
		for i := 0; i < 5; i++ {
			<-done
		}

		// Verify all keys were inserted
		for g := 0; g < 5; g++ {
			for i := 0; i < 50; i++ {
				key := intKey(uint64(g*1000 + i))
				_, err := tree.Get(key)
				if err != nil {
					t.Errorf("key %x not found", key)
				}
			}
		}
	})

	t.Run("ConcurrentGetPut", func(t *testing.T) {
		tree := NewInMemoryTree()
		defer tree.Close()

		// Pre-insert some keys
		for i := 0; i < 100; i++ {
			value := MakeInlineValue([]byte("initial"))
			_ = tree.Put(intKey(uint64(i)), value)
		}

		done := make(chan bool, 5)
		for g := 0; g < 5; g++ {
			go func(goroutineID int) {
				for i := 0; i < 20; i++ {
					key := intKey(uint64((goroutineID*20 + i) % 100))
					if goroutineID%2 == 0 {
						// Readers
						_, _ = tree.Get(key)
					} else {
						// Writers
						value := MakeInlineValue([]byte("updated"))
						_ = tree.Put(key, value)
					}
				}
				done <- true
			}(g)
		}
		for i := 0; i < 5; i++ {
			<-done
		}
	})
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("VeryLargeInsertSequence", func(t *testing.T) {
		tree := NewInMemoryTree()
		defer tree.Close()

		// Insert enough to trigger multiple levels
		for i := 1; i <= 1000; i++ {
			value := MakeInlineValue([]byte("val"))
			err := tree.Put(intKey(uint64(i)), value)
			if err != nil {
				t.Fatalf("Put %d failed: %v", i, err)
			}
		}

		// Verify all present
		for i := 1; i <= 1000; i++ {
			_, err := tree.Get(intKey(uint64(i)))
			if err != nil {
				t.Errorf("key %d missing", i)
			}
		}
	})

	t.Run("DeleteAllAndReinsert", func(t *testing.T) {
		tree := NewInMemoryTree()
		defer tree.Close()

		// Insert
		for i := 1; i <= 100; i++ {
			value := MakeInlineValue([]byte("val"))
			_ = tree.Put(intKey(uint64(i)), value)
		}

		// Delete all
		for i := 1; i <= 100; i++ {
			_ = tree.Delete(intKey(uint64(i)))
		}

		// Reinsert
		for i := 1; i <= 100; i++ {
			value := MakeInlineValue([]byte("val2"))
			err := tree.Put(intKey(uint64(i)), value)
			if err != nil {
				t.Fatalf("Reinsert %d failed: %v", i, err)
			}
		}

		// Verify
		for i := 1; i <= 100; i++ {
			_, err := tree.Get(intKey(uint64(i)))
			if err != nil {
				t.Errorf("key %d missing after reinsert", i)
			}
		}
	})

	t.Run("AlternatingPutDelete", func(t *testing.T) {
		tree := NewInMemoryTree()
		defer tree.Close()

		for round := 0; round < 10; round++ {
			for i := 1; i <= 50; i++ {
				key := intKey(uint64(round*100 + i))
				value := MakeInlineValue([]byte("val"))
				_ = tree.Put(key, value)
			}
			for i := 1; i <= 50; i++ {
				key := intKey(uint64(round*100 + i))
				_ = tree.Delete(key)
			}
		}
	})
}
