package internal

import (
    "fmt"
    "testing"
)

func TestSerializeSize(t *testing.T) {
    nodeOps := NewNodeOperations()
    
    // Create empty leaf
    node := &NodeFormat{
        NodeType: NodeTypeLeaf,
        Count:    0,
        Capacity: MaxNodeCapacity,
    }
    
    data := nodeOps.Serialize(node)
    fmt.Printf("Serialize returns: %d bytes\n", len(data))
    fmt.Printf("Expected: %d = %d + %d*%d\n", 
        NodeHeaderSize + int(MaxNodeCapacity)*LeafEntrySize,
        NodeHeaderSize, MaxNodeCapacity, LeafEntrySize)
}
