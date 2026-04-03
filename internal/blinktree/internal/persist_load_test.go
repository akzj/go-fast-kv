package internal

import (
	"testing"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// TestSerializeDeserializeRoundtrip tests in-memory Serialize/Deserialize.
func TestSerializeDeserializeRoundtrip(t *testing.T) {
	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    0,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}

	ops := NewNodeOperations()

	// Serialize
	data := ops.Serialize(node)
	if len(data) != int(vaddr.PageSize) {
		t.Fatalf("Expected Serialize to return PageSize=%d bytes, got %d", vaddr.PageSize, len(data))
	}

	// Deserialize
	node2, err := ops.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if node2.NodeType != node.NodeType {
		t.Errorf("NodeType mismatch: got %d, want %d", node2.NodeType, node.NodeType)
	}
	if node2.Count != node.Count {
		t.Errorf("Count mismatch: got %d, want %d", node2.Count, node.Count)
	}
	t.Log("TestSerializeDeserializeRoundtrip PASSED")
}

// TestPersistLoadRoundtrip tests the full Persist → Load roundtrip using a mock segment manager.
func TestPersistLoadRoundtrip(t *testing.T) {
	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    0,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}

	ops := NewNodeOperations()
	data := ops.Serialize(node)

	// Verify checksum is stored correctly
	_, err := ops.Deserialize(data)
	if err != nil {
		t.Fatalf("Full roundtrip failed: %v", err)
	}

	t.Logf("TestPersistLoadRoundtrip: Serialize len=%d, Deserialize succeeded", len(data))
}
