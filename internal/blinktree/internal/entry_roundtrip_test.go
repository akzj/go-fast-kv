package internal

import (
	"testing"
)

func TestEntryRoundtrip(t *testing.T) {
	// Create node
	node := &NodeFormat{
		NodeType: 0, // Leaf
		Count:    1,
		Capacity: 255,
		RawData:  make([]byte, 0),
	}
	
	// Create entry with known key
	knownKey := PageID(6348322678519998343)
	entry := LeafEntry{
		Key:   knownKey,
		Value: InlineValue{},
	}
	
	t.Logf("Before store: entry.Key=%d", entry.Key)
	
	// Store entries
	StoreLeafEntries(node, []LeafEntry{entry})
	
	t.Logf("After store: RawData len=%d, RawData[0:8]=%x", len(node.RawData), node.RawData[0:8])
	
	// Extract entries
	entries := ExtractLeafEntries(node)
	
	t.Logf("After extract: len(entries)=%d", len(entries))
	if len(entries) > 0 {
		t.Logf("entries[0].Key=%d", entries[0].Key)
	}
	
	if len(entries) == 0 || entries[0].Key != knownKey {
		t.Errorf("FAILURE: Expected key=%d, got key=%d", knownKey, entries[0].Key)
	} else {
		t.Log("SUCCESS: Key matches!")
	}
}
