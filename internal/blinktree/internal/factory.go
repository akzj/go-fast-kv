package internal

import (
	"sync"

	"github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// In-Memory NodeManager for Testing
// =============================================================================

// inMemoryNodeManager stores nodes in a map (for testing).
type inMemoryNodeManager struct {
	nodeOps  NodeOperations
	nodes    map[vaddr.VAddr]*NodeFormat
	nextAddr vaddr.VAddr
	mu       sync.Mutex
}

// NewTree creates a new B-link-tree.
func NewTree(nodeOps NodeOperations, nodeMgr NodeManager, isMutator bool) Tree {
	return newTreeImpl(nodeOps, nodeMgr, isMutator)
}

// NewTreeMutator creates a mutable B-link-tree.
func NewTreeMutator(nodeOps NodeOperations, nodeMgr NodeManager) TreeMutator {
	return newTreeImpl(nodeOps, nodeMgr, true)
}

// NewInMemoryNodeManager creates a node manager that stores nodes in memory.
func NewInMemoryNodeManager(nodeOps NodeOperations) NodeManager {
	return &inMemoryNodeManager{
		nodeOps:  nodeOps,
		nodes:    make(map[vaddr.VAddr]*NodeFormat),
		nextAddr: vaddr.VAddr{SegmentID: 1, Offset: vaddr.PageSize}, // Start at PageSize to avoid addr 0
	}
}

// addrForNode returns the address for a node if it already exists, or allocates a new one.
func (mgr *inMemoryNodeManager) addrForNode(node *NodeFormat) VAddr {
	for addr, n := range mgr.nodes {
		if n == node {
			return addr
		}
	}
	// Node doesn't exist yet, allocate new address
	addr := mgr.nextAddr
	mgr.nextAddr.Offset += vaddr.PageSize
	return addr
}

func (mgr *inMemoryNodeManager) CreateLeaf() (*NodeFormat, VAddr) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	node := &NodeFormat{
		NodeType: NodeTypeLeaf,
		Count:    0,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}
	addr := mgr.nextAddr
	mgr.nextAddr.Offset += vaddr.PageSize
	mgr.nodes[addr] = node
	return node, addr
}

func (mgr *inMemoryNodeManager) CreateInternal(level uint8) (*NodeFormat, VAddr) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	node := &NodeFormat{
		NodeType: NodeTypeInternal,
		Level:    level,
		Count:    0,
		Capacity: MaxNodeCapacity,
		RawData:  make([]byte, 0),
	}
	addr := mgr.nextAddr
	mgr.nextAddr.Offset += vaddr.PageSize
	mgr.nodes[addr] = node
	return node, addr
}

func (mgr *inMemoryNodeManager) Persist(node *NodeFormat) (VAddr, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// Serialize entries to RawData so Load can read them
	if node.NodeType == NodeTypeLeaf && len(node.RawData) == 0 {
		// Extract and re-store to ensure RawData is populated
		entries := ExtractLeafEntries(node)
		StoreLeafEntries(node, entries)
	} else if node.NodeType == NodeTypeInternal && len(node.RawData) == 0 {
		entries := ExtractInternalEntries(node)
		StoreInternalEntries(node, entries)
	}

	addr := mgr.addrForNode(node)
	mgr.nodes[addr] = node
	return addr, nil
}

func (mgr *inMemoryNodeManager) Load(addr VAddr) (*NodeFormat, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	node, ok := mgr.nodes[addr]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

func (mgr *inMemoryNodeManager) UpdateParent(parentVAddr, oldChild, newChild VAddr, splitKey PageID) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	parent, ok := mgr.nodes[parentVAddr]
	if !ok {
		return ErrNodeNotFound
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
	return nil
}

// NewInMemoryTree creates a tree with an in-memory node manager for testing.
func NewInMemoryTree() TreeMutator {
	nodeOps := NewNodeOperations()
	nodeMgr := NewInMemoryNodeManager(nodeOps)
	tree := newTreeImpl(nodeOps, nodeMgr, true)
	_ = tree.Open("")
	return tree
}
