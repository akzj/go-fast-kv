package internal

import (
	"testing"
)

func TestDebugSegmentReadWrite(t *testing.T) {
	dir, err := mkTempDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	
	cfg := Config{
		Directory:   dir,
		SegmentSize: 1 << 30,
	}
	
	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	
	// Ensure we have an active segment
	seg := mgr.ActiveSegment()
	if seg == nil {
		seg, _ = mgr.CreateSegment()
	}
	
	// Simulate what CreateLeaf does: Persist empty leaf node
	// Create 4096 bytes like Serialize would
	nodeData := make([]byte, vaddr.PageSize)
	nodeData[0] = 0 // NodeType = Leaf
	nodeData[3] = 0 // Count = 0
	
	addr, err := seg.Append(nodeData)
	if err != nil {
		t.Fatal(err)
	}
	
	t.Logf("Created leaf at SegmentID=%d, Offset=%d", addr.SegmentID, addr.Offset)
	
	// Now try to read it back like Load would
	seg2 := mgr.GetSegment(addr.SegmentID)
	if seg2 == nil {
		t.Fatal("GetSegment returned nil")
	}
	
	readData, err := seg2.ReadAt(int64(addr.Offset), vaddr.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	
	t.Logf("Read back len=%d", len(readData))
	t.Logf("First 64 bytes: %x", readData[:64])
	t.Logf("NodeType=%d, Count=%d", readData[0], readData[3])
	
	if len(readData) < 64 {
		t.Fatalf("Read back only %d bytes, expected %d", len(readData), vaddr.PageSize)
	}
	
	if readData[0] != 0 {
		t.Errorf("NodeType mismatch: wrote 0, read %d", readData[0])
	}
	if readData[3] != 0 {
		t.Errorf("Count mismatch: wrote 0, read %d", readData[3])
	}
	
	t.Log("All bytes match!")
}
