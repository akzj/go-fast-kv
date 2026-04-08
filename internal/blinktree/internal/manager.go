package internal

import (
	"sync"

	"github.com/akzj/go-fast-kv/internal/storage"
	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// pageNodeManager implements NodeManager interface using storage.PageStorage.
// B-tree code works with stable PageIDs; PageStorage maps PageID → VAddr internally.
type pageNodeManager struct {
	pageStorage storage.PageStorage
	nodeOps    NodeOperations
	mu         sync.Mutex
}

// CreateLeaf creates a new empty leaf node.
func (mgr *pageNodeManager) CreateLeaf() (*NodeFormat, PageID) {
	leafCapacity := uint16((vaddr.PageSize - NodeHeaderSize) / LeafEntrySize)

	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    0,
		Capacity: leafCapacity,
		RawData:  make([]byte, 0),
	}

	// Create page in storage, get back (node, pageID)
	mgr.persistNode(node)
	pageID, _, err := mgr.pageStorage.CreatePage(mgr.nodeOps.Serialize(node))
	if err != nil {
		return nil, 0
	}
	return node, pageID
}

// CreateInternal creates a new internal node at given level.
func (mgr *pageNodeManager) CreateInternal(level uint8) (*NodeFormat, PageID) {
	internalCapacity := uint16((vaddr.PageSize - NodeHeaderSize) / InternalEntrySize)

	node := &NodeFormat{
		NodeType: NodeTypeInternal,
		Level:    level,
		Count:    0,
		Capacity: internalCapacity,
		RawData:  make([]byte, 0),
	}

	mgr.persistNode(node)
	pageID, _, err := mgr.pageStorage.CreatePage(mgr.nodeOps.Serialize(node))
	if err != nil {
		return nil, 0
	}
	return node, pageID
}

// persistNode ensures node.RawData is populated before serialization.
func (mgr *pageNodeManager) persistNode(node *NodeFormat) {
	if node.NodeType == NodeTypeLeaf && len(node.RawData) == 0 {
		entries := ExtractLeafEntries(node)
		StoreLeafEntries(node, entries)
	} else if node.NodeType == NodeTypeInternal && len(node.RawData) == 0 {
		entries := ExtractInternalEntries(node)
		StoreInternalEntries(node, entries)
	}
}

// Persist writes the node to storage for the given PageID.
// PageID is stable — this updates the content at the same logical address.
func (mgr *pageNodeManager) Persist(node *NodeFormat, pageID PageID) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.persistNode(node)
	_, err := mgr.pageStorage.WritePage(pageID, mgr.nodeOps.Serialize(node))
	return err
}

// Load reads the node for the given PageID.
func (mgr *pageNodeManager) Load(pageID PageID) (*NodeFormat, error) {
	data, err := mgr.pageStorage.ReadPage(pageID)
	if err != nil {
		return nil, ErrNodeNotFound
	}
	return mgr.nodeOps.Deserialize(data)
}
