package internal

import (
	"testing"
)

// TestFullRoundtripDebug traces the full Persist→Load→Deserialize roundtrip
func TestFullRoundtripDebug(t *testing.T) {
	ops := NewNodeOperations()

	// Create node with 1 entry
	key := PageID(6348322678519998343)
	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    1,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}

	// Store entry
	entries := []LeafEntry{{
		Key:   key,
		Value: InlineValue{},
	}}
	StoreLeafEntries(node, entries)

	t.Logf("After StoreLeafEntries: Count=%d, RawData len=%d", node.Count, len(node.RawData))

	// Extract to verify
	extracted := ExtractLeafEntries(node)
	t.Logf("Extracted entries: len=%d, key[0]=%d", len(extracted), extracted[0].Key)

	// Serialize
	serialized := ops.Serialize(node)
	t.Logf("Serialized len=%d, data[56:64]=%x", len(serialized), serialized[56:64])

	// Deserialize
	loaded, err := ops.Deserialize(serialized)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	t.Logf("After Deserialize: Count=%d, RawData len=%d", loaded.Count, len(loaded.RawData))

	// Extract from loaded node
	loadedEntries := ExtractLeafEntries(loaded)
	t.Logf("Loaded entries: len=%d", len(loadedEntries))

	if len(loadedEntries) != len(entries) {
		t.Errorf("Entry count mismatch: got %d, want %d", len(loadedEntries), len(entries))
	}

	if len(loadedEntries) > 0 && loadedEntries[0].Key != key {
		t.Errorf("Key mismatch: got %d, want %d", loadedEntries[0].Key, key)
	}

	// Simulate search
	searchIdx := ops.Search(loaded, key)
	t.Logf("Search returned idx=%d", searchIdx)

	// The search logic
	if searchIdx > 0 && len(loadedEntries) >= searchIdx && loadedEntries[searchIdx-1].Key == key {
		t.Log("SEARCH WOULD FIND KEY - PASS")
	} else {
		t.Log("SEARCH WOULD NOT FIND KEY - FAIL")
	}
}
