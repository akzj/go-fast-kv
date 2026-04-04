package internal

import (
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
	// Calculate actual capacity based on page size and entry size
	// (PageSize - header) / LeafEntrySize = (4096 - 56) / 72 = 56
	leafCapacity := uint16((vaddr.PageSize - NodeHeaderSize) / LeafEntrySize)

	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    0,
		Capacity: leafCapacity,
		RawData:  make([]byte, 0),
	}
	addr, err := mgr.Persist(node)
	if err != nil {
		return nil, VAddr{}
	}
	return node, addr
}

// CreateInternal initializes a new internal node at given level.
func (mgr *nodeManager) CreateInternal(level uint8) (*NodeFormat, VAddr) {
	node, addr := mgr.createInternalNode(level)
	if node == nil {
		return nil, VAddr{}
	}
	_, err := mgr.Persist(node)
	if err != nil {
		return nil, VAddr{}
	}
	return node, addr
}

// createInternalNode creates a new internal node struct without persisting.
func (mgr *nodeManager) createInternalNode(level uint8) (*NodeFormat, VAddr) {
	// Calculate actual capacity based on page size and entry size
	// (PageSize - header) / InternalEntrySize = (4096 - 56) / 24 = 168
	internalCapacity := uint16((vaddr.PageSize - NodeHeaderSize) / InternalEntrySize)

	node := &NodeFormat{
		NodeType: NodeTypeInternal,
		Level:    level,
		Count:    0,
		Capacity: internalCapacity,
		RawData:  make([]byte, 0),
	}
	// Allocate address for later persistence
	addr := VAddr{} // Will be assigned on Persist
	return node, addr
}

// Persist appends node to append-only storage.
func (mgr *nodeManager) Persist(node *NodeFormat) (VAddr, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	data := mgr.nodeOps.Serialize(node)
	
	// Pad to page alignment (segment requires page-aligned writes)
	pageSize := int(vaddr.PageSize)
	if len(data)%pageSize != 0 {
		padding := pageSize - (len(data) % pageSize)
		data = append(data, make([]byte, padding)...)
	}
	
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

	// Read PageSize bytes (Serialize outputs PageSize, not variable size)
	data, err := segment.ReadAt(offset, vaddr.PageSize)
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
