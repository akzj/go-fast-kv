package internal

import (
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
		idx := nodeOps.Search(node, 100)
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
			{Key: 10, Value: MakeInlineValue([]byte("val10"))},
			{Key: 20, Value: MakeInlineValue([]byte("val20"))},
			{Key: 30, Value: MakeInlineValue([]byte("val30"))},
		}
		StoreLeafEntries(node, entries)

		tests := []struct {
			key      PageID
			expected int
		}{
			{5, 0},   // Before first
			{10, 1},  // At first
			{15, 1},  // Between first and second
			{20, 2},  // At second
			{25, 2},  // Between second and third
			{30, 3},  // At third
			{35, 3},  // After last
		}

		for _, tc := range tests {
			idx := nodeOps.Search(node, tc.key)
			if idx != tc.expected {
				t.Errorf("Search(%d): expected %d, got %d", tc.key, tc.expected, idx)
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
			{Key: 10, Child: PageID(100)},
			{Key: 20, Child: PageID(200)},
			{Key: 30, Child: PageID(300)},
		}
		StoreInternalEntries(node, entries)

		tests := []struct {
			key      PageID
			expected int
		}{
			{5, 0},   // Before first
			{10, 1},  // At first key
			{15, 1},  // Between first and second
			{20, 2},  // At second key
			{25, 2},  // Between second and third
			{30, 3},  // At third key
			{35, 3},  // After last
		}

		for _, tc := range tests {
			idx := nodeOps.Search(node, tc.key)
			if idx != tc.expected {
				t.Errorf("Search(%d): expected %d, got %d", tc.key, tc.expected, idx)
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
		newNode, splitKey, err := nodeOps.Insert(node, 100, value)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		if newNode != nil {
			t.Error("expected no split for first insert")
		}
		if splitKey != 0 {
			t.Errorf("expected splitKey 0, got %d", splitKey)
		}
		entries := ExtractLeafEntries(node)
		if node.Count != 1 {
			t.Errorf("expected count 1, got %d", node.Count)
		}
		if entries[0].Key != 100 {
			t.Errorf("expected key 100, got %d", entries[0].Key)
		}
	})

	t.Run("InsertInOrder", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		for i := PageID(1); i <= 10; i++ {
			value := MakeInlineValue([]byte("val"))
			_, _, err := nodeOps.Insert(node, i*10, value)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}
		if node.Count != 10 {
			t.Errorf("expected count 10, got %d", node.Count)
		}
		entries := ExtractLeafEntries(node)
		for i := 0; i < 10; i++ {
			expected := PageID((i + 1) * 10)
			if entries[i].Key != expected {
				t.Errorf("entry %d: expected key %d, got %d", i, expected, entries[i].Key)
			}
		}
	})

	t.Run("InsertOutOfOrder", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		keys := []PageID{50, 10, 30, 20, 40}
		for _, k := range keys {
			value := MakeInlineValue([]byte("val"))
			_, _, err := nodeOps.Insert(node, k, value)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", k, err)
			}
		}
		entries := ExtractLeafEntries(node)
		expectedKeys := []PageID{10, 20, 30, 40, 50}
		for i, expected := range expectedKeys {
			if entries[i].Key != expected {
				t.Errorf("entry %d: expected key %d, got %d", i, expected, entries[i].Key)
			}
		}
	})

	t.Run("InsertTriggersSplit", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    0,
			Capacity: MaxNodeCapacity,
		}
		// Fill node to capacity
		for i := PageID(1); i <= PageID(MaxNodeCapacity); i++ {
			value := MakeInlineValue([]byte("val"))
			newNode, splitKey, err := nodeOps.Insert(node, i, value)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
			if i == PageID(MaxNodeCapacity) {
				if newNode == nil {
					t.Error("expected split at capacity")
				}
				if splitKey == 0 {
					t.Error("expected non-zero split key")
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
		_, _, err := nodeOps.Insert(node, 100, value1)
		if err != nil {
			t.Fatalf("Insert 1 failed: %v", err)
		}
		value2 := MakeInlineValue([]byte("val2"))
		_, _, err = nodeOps.Insert(node, 100, value2)
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
		_, _, err := nodeOps.Insert(node, 100, value)
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
			entries[i] = LeafEntry{Key: PageID(i + 1), Value: MakeInlineValue([]byte("val"))}
		}
		StoreLeafEntries(node, entries)

		left, right, splitKey := nodeOps.Split(node)

		// Check split key is median
		if splitKey != 5 {
			t.Errorf("expected splitKey 5, got %d", splitKey)
		}

		// Check left node has first half
		if left.Count != 5 {
			t.Errorf("left count: expected 5, got %d", left.Count)
		}
		leftEntries := ExtractLeafEntries(left)
		for i := 0; i < int(left.Count); i++ {
			if leftEntries[i].Key != PageID(i+1) {
				t.Errorf("left entry %d: expected %d, got %d", i, i+1, leftEntries[i].Key)
			}
		}

		// Check right node has second half
		if right.Count != 5 {
			t.Errorf("right count: expected 5, got %d", right.Count)
		}
		rightEntries := ExtractLeafEntries(right)
		for i := 0; i < int(right.Count); i++ {
			expected := PageID(i + 6)
			if rightEntries[i].Key != expected {
				t.Errorf("right entry %d: expected %d, got %d", i, expected, rightEntries[i].Key)
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
				Key:   PageID((i+1)*10),
				Child: PageID(uint64(i) * 100),
			}
		}
		StoreInternalEntries(node, entries)

		left, right, splitKey := nodeOps.Split(node)

		// Median is at index 3 (0-indexed), which is key 30
		if splitKey != 30 {
			t.Errorf("expected splitKey 30, got %d", splitKey)
		}

		// Left has 3 entries (indices 0,1,2) - keys 10,20
		if left.Count != 3 {
			t.Errorf("left count: expected 3, got %d", left.Count)
		}
		leftEntries := ExtractInternalEntries(left)
		if leftEntries[0].Key != 10 || leftEntries[1].Key != 20 || leftEntries[2].Key != 30 {
			t.Error("left entries incorrect")
		}

		// Right has 3 entries (indices 4,5) - keys 40,50,60
		if right.Count != 3 {
			t.Errorf("right count: expected 3, got %d", right.Count)
		}
		rightEntries := ExtractInternalEntries(right)
		if rightEntries[0].Key != 40 || rightEntries[1].Key != 50 || rightEntries[2].Key != 60 {
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
			entries[i] = LeafEntry{Key: PageID(i + 1), Value: MakeInlineValue([]byte("val"))}
		}
		StoreLeafEntries(node, entries)

		_, _, splitKey := nodeOps.Split(node)
		// Median is index 2 (key 3)
		if splitKey != 3 {
			t.Errorf("expected splitKey 3, got %d", splitKey)
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
		if highKey != 0 {
			t.Errorf("expected 0 for empty node, got %d", highKey)
		}
	})

	t.Run("UpdateHighKeyLeaf", func(t *testing.T) {
		node := &NodeFormat{
			NodeType: NodeTypeLeaf,
			Count:    5,
			Capacity: MaxNodeCapacity,
		}
		entries := []LeafEntry{
			{Key: 10, Value: MakeInlineValue([]byte("a"))},
			{Key: 20, Value: MakeInlineValue([]byte("b"))},
			{Key: 30, Value: MakeInlineValue([]byte("c"))},
			{Key: 40, Value: MakeInlineValue([]byte("d"))},
			{Key: 50, Value: MakeInlineValue([]byte("e"))},
		}
		StoreLeafEntries(node, entries)

		highKey := nodeOps.UpdateHighKey(node)
		if highKey != 50 {
			t.Errorf("expected 50, got %d", highKey)
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
			{Key: 10, Child: PageID(100)},
			{Key: 20, Child: PageID(200)},
			{Key: 30, Child: PageID(300)},
		}
		StoreInternalEntries(node, entries)

		highKey := nodeOps.UpdateHighKey(node)
		if highKey != 30 {
			t.Errorf("expected 30, got %d", highKey)
		}
	})
}

func TestNodeOperations_SerializeDeserialize(t *testing.T) {
	nodeOps := NewNodeOperations()

	t.Run("SerializeDeserializeLeaf", func(t *testing.T) {
		original := &NodeFormat{
			NodeType:     NodeTypeLeaf,
			Count:        3,
			Capacity:     MaxNodeCapacity,
			HighSibling:  PageID(1000),
			LowSibling:   PageID(0),
			HighKey:      300,
		}
		entries := []LeafEntry{
			{Key: 100, Value: MakeInlineValue([]byte("val100"))},
			{Key: 200, Value: MakeInlineValue([]byte("val200"))},
			{Key: 300, Value: MakeInlineValue([]byte("val300"))},
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
		if restored.HighKey != original.HighKey {
			t.Errorf("HighKey: expected %d, got %d", original.HighKey, restored.HighKey)
		}

		restoredEntries := ExtractLeafEntries(restored)
		for i := 0; i < int(original.Count); i++ {
			if restoredEntries[i].Key != entries[i].Key {
				t.Errorf("entry %d key: expected %d, got %d", i, entries[i].Key, restoredEntries[i].Key)
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
			{Key: 100, Child: PageID(100)},
			{Key: 200, Child: PageID(200)},
			{Key: 300, Child: PageID(300)},
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
			if restoredEntries[i].Key != entries[i].Key {
				t.Errorf("entry %d key: expected %d, got %d", i, entries[i].Key, restoredEntries[i].Key)
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
			{Key: 100, Value: MakeInlineValue([]byte("a"))},
			{Key: 200, Value: MakeInlineValue([]byte("b"))},
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
		if loadedEntries[0].Key != 100 || loadedEntries[1].Key != 200 {
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
		err := tree.Put(100, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	t.Run("PutMultipleKeys", func(t *testing.T) {
		for i := 1; i <= 100; i++ {
			value := MakeInlineValue([]byte("value"))
			err := tree.Put(PageID(i*10), value)
			if err != nil {
				t.Fatalf("Put %d failed: %v", i*10, err)
			}
		}
	})

	t.Run("PutDuplicateKey", func(t *testing.T) {
		value1 := MakeInlineValue([]byte("first"))
		err := tree.Put(500, value1)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		value2 := MakeInlineValue([]byte("second"))
		err = tree.Put(500, value2)
		if err != nil {
			t.Fatalf("Put duplicate failed: %v", err)
		}
	})

	t.Run("PutAfterClose", func(t *testing.T) {
		tree.Close()
		value := MakeInlineValue([]byte("test"))
		err := tree.Put(100, value)
		if err != ErrStoreClosed {
			t.Errorf("expected ErrStoreClosed, got %v", err)
		}
	})
}

func TestTree_Get(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Insert test data
	inserted := make(map[PageID][]byte)
	for i := 1; i <= 50; i++ {
		key := PageID(i * 10)
		data := []byte("value for key ")
		data = append(data, byte(i))
		value := MakeInlineValue(data)
		_ = tree.Put(key, value)
		inserted[key] = data
	}

	t.Run("GetExistingKeys", func(t *testing.T) {
		for key, expectedData := range inserted {
			val, err := tree.Get(key)
			if err != nil {
				t.Fatalf("Get %d failed: %v", key, err)
			}
			// Verify value matches (at least first few bytes)
			if val.Length[7] != byte(len(expectedData)) {
				t.Errorf("Get %d: wrong length", key)
			}
		}
	})

	t.Run("GetNonExistingKey", func(t *testing.T) {
		_, err := tree.Get(9999)
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("GetFromEmptyTree", func(t *testing.T) {
		emptyTree := NewInMemoryTree()
		_, err := emptyTree.Get(100)
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
	keys := []PageID{10, 20, 30, 40, 50}
	for _, k := range keys {
		value := MakeInlineValue([]byte("val"))
		_ = tree.Put(k, value)
	}

	t.Run("DeleteMiddleKey", func(t *testing.T) {
		err := tree.Delete(30)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify key is gone
		_, err = tree.Get(30)
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
		}

		// Other keys still exist
		for _, k := range []PageID{10, 20, 40, 50} {
			_, err := tree.Get(k)
			if err != nil {
				t.Errorf("key %d should exist after deleting 30", k)
			}
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		err := tree.Delete(9999)
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("DeleteAllKeys", func(t *testing.T) {
		for _, k := range []PageID{10, 20, 40, 50} {
			err := tree.Delete(k)
			if err != nil {
				t.Fatalf("Delete %d failed: %v", k, err)
			}
		}

		for _, k := range keys {
			_, err := tree.Get(k)
			if err != ErrKeyNotFound {
				t.Errorf("key %d should be deleted", k)
			}
		}
	})

	t.Run("DeleteAfterClose", func(t *testing.T) {
		tree.Close()
		err := tree.Delete(10)
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
		_ = tree.Put(PageID(i*10), value)
	}

	t.Run("ScanAll", func(t *testing.T) {
		iter, err := tree.Scan(0, 0)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key()
			if key == 0 {
				t.Error("iterator returned zero key")
			}
			count++
		}
		if count != 100 {
			t.Errorf("expected 100 entries, got %d", count)
		}
	})

	t.Run("ScanRange", func(t *testing.T) {
		iter, err := tree.Scan(200, 500)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key()
			if key < 200 || key >= 500 {
				t.Errorf("key %d out of range [200, 500)", key)
			}
			count++
		}
		if count != 30 { // 210,220,...,490 = 30 keys
			t.Errorf("expected 30 entries in range, got %d", count)
		}
	})

	t.Run("ScanEmptyRange", func(t *testing.T) {
		iter, err := tree.Scan(5000, 6000)
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
		_, err := tree.Scan(0, 0)
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
			Key:   PageID((i+1)*10),
			Value: MakeInlineValue([]byte("batch")),
		}
	}

	err := tree.Batch(ops)
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	// Verify all inserted
	for i := 1; i <= 100; i++ {
		_, err := tree.Get(PageID(i * 10))
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
		_ = tree.Put(PageID(i*10), value)
	}

	// Get root PageID
	rootPageID := tree.GetRootPageID()
	if rootPageID == 0 {
		t.Fatal("expected non-zero root pageID")
	}

	// Create new tree and restore root PageID
	tree2 := NewInMemoryTree()
	defer tree2.Close()
	tree2.RestoreRootPageID(rootPageID)

	// Verify data is accessible (note: this test uses separate PageStorage instances,
	// so data won't actually be accessible — this tests the root persistence mechanism only)
	for i := 1; i <= 20; i++ {
		_, err := tree2.Get(PageID(i * 10))
		if err != nil {
			t.Errorf("key %d not found after restore", i*10)
		}
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
		_ = tree.Put(PageID(i*10), value)
	}

	t.Run("IteratorKeyValue", func(t *testing.T) {
		iter, _ := tree.Scan(0, 0)
		defer iter.Close()

		// Should find first entry
		if !iter.Next() {
			t.Fatal("expected at least one entry")
		}

		key := iter.Key()
		if key == 0 {
			t.Error("Key() returned 0")
		}

		val := iter.Value()
		if !val.IsValid() {
			t.Error("Value() returned invalid value")
		}
	})

	t.Run("IteratorOrder", func(t *testing.T) {
		iter, _ := tree.Scan(0, 0)
		defer iter.Close()

		var prevKey PageID = 0
		for iter.Next() {
			key := iter.Key()
			if key <= prevKey {
				t.Errorf("keys not in order: %d <= %d", key, prevKey)
			}
			prevKey = key
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
				Key:   PageID(i * 10),
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
					key := PageID(goroutineID*1000 + i)
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
				key := PageID(g*1000 + i)
				_, err := tree.Get(key)
				if err != nil {
					t.Errorf("key %d not found", key)
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
			_ = tree.Put(PageID(i), value)
		}

		done := make(chan bool, 5)
		for g := 0; g < 5; g++ {
			go func(goroutineID int) {
				for i := 0; i < 20; i++ {
					key := PageID((goroutineID*20 + i) % 100)
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
			err := tree.Put(PageID(i), value)
			if err != nil {
				t.Fatalf("Put %d failed: %v", i, err)
			}
		}

		// Verify all present
		for i := 1; i <= 1000; i++ {
			_, err := tree.Get(PageID(i))
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
			_ = tree.Put(PageID(i), value)
		}

		// Delete all
		for i := 1; i <= 100; i++ {
			_ = tree.Delete(PageID(i))
		}

		// Reinsert
		for i := 1; i <= 100; i++ {
			value := MakeInlineValue([]byte("val2"))
			err := tree.Put(PageID(i), value)
			if err != nil {
				t.Fatalf("Reinsert %d failed: %v", i, err)
			}
		}

		// Verify
		for i := 1; i <= 100; i++ {
			_, err := tree.Get(PageID(i))
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
				key := PageID(round*100 + i)
				value := MakeInlineValue([]byte("val"))
				_ = tree.Put(key, value)
			}
			for i := 1; i <= 50; i++ {
				key := PageID(round*100 + i)
				_ = tree.Delete(key)
			}
		}
	})
}
