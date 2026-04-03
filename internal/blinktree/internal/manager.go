package internal

import (
	"fmt"
	"sync"

	"github.com/akzj/go-fast-kv/internal/storage"
	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// nodeManager implements NodeManager interface using storage.SegmentManager.
type nodeManager struct {
	segmentMgr storage.SegmentManager
	nodeOps    NodeOperations
	mu         sync.Mutex
}

// NewNodeManager creates a new NodeManager.
func NewNodeManager(segmentMgr storage.SegmentManager, nodeOps NodeOperations) NodeManager {
	return &nodeManager{
		segmentMgr: segmentMgr,
		nodeOps:    nodeOps,
	}
}

// CreateLeaf initializes a new empty leaf node.
func (mgr *nodeManager) CreateLeaf() (*NodeFormat, VAddr) {
	fmt.Printf("DEBUG CreateLeaf: segmentMgr=%v\n", mgr.segmentMgr)
	fmt.Printf("DEBUG CreateLeaf: ActiveSegment=%v\n", mgr.segmentMgr.ActiveSegment())
	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    0,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}
	addr, err := mgr.Persist(node)
	fmt.Printf("DEBUG CreateLeaf: Persist returned addr=%v, err=%v\n", addr, err)
	if err != nil {
		return nil, VAddr{}
	}
	return node, addr
}

// CreateInternal initializes a new internal node at given level.
func (mgr *nodeManager) CreateInternal(level uint8) (*NodeFormat, VAddr) {
	node := &NodeFormat{
		NodeType: NodeTypeInternal,
		Level:    level,
		Count:    0,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}
	addr, err := mgr.Persist(node)
	if err != nil {
		return nil, VAddr{}
	}
	return node, addr
}

// Persist appends node to append-only storage.
func (mgr *nodeManager) Persist(node *NodeFormat) (VAddr, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	data := mgr.nodeOps.Serialize(node)
	segment := mgr.segmentMgr.ActiveSegment()
	if segment == nil {
		return VAddr{}, storage.ErrSegmentNotActive
	}
	return segment.Append(data)
}

// Load reads node from storage by VAddr.
func (mgr *nodeManager) Load(addr VAddr) (*NodeFormat, error) {
	segmentID := vaddr.SegmentID(addr.SegmentID)
	offset := int64(addr.Offset)

	segment := mgr.segmentMgr.GetSegment(segmentID)
	if segment == nil {
		return nil, ErrNodeNotFound
	}

	// Read full node (header + max entries)
	size := NodeHeaderSize + int(MaxNodeCapacity)*LeafEntrySize
	data := make([]byte, size)
	_, err := segment.ReadAt(offset, size)
	if err != nil {
		return nil, ErrNodeNotFound
	}

	return mgr.nodeOps.Deserialize(data)
}

// UpdateParent updates parent node's child pointer after a split.
func (mgr *nodeManager) UpdateParent(parentVAddr, oldChild, newChild VAddr, splitKey PageID) error {
	parent, err := mgr.Load(parentVAddr)
	if err != nil {
		return err
	}

	entries := ExtractInternalEntries(parent)
	for i := 0; i < int(parent.Count); i++ {
		if entries[i].Child == oldChild {
			if i+1 < int(parent.Capacity) {
				copy(entries[i+2:], entries[i+1:])
			}
			entries[i+1] = InternalEntry{Key: splitKey, Child: newChild}
			parent.Count++
			StoreInternalEntries(parent, entries)
			break
		}
	}

	_, err = mgr.Persist(parent)
	return err
}
