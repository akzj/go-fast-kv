package internal

import (
	"github.com/akzj/go-fast-kv/internal/storage"
)

// =============================================================================
// NodeManager Factory
// =============================================================================

// NewNodeManager creates a NodeManager using the given storage.SegmentManager.
// Internally wraps it in a SegmentManagerPageStorage.
func NewNodeManager(segmentMgr storage.SegmentManager, nodeOps NodeOperations) NodeManager {
	pageStorage := storage.NewSegmentManagerPageStorage(segmentMgr)
	return newPageNodeManager(pageStorage, nodeOps)
}

func newPageNodeManager(pageStorage storage.PageStorage, nodeOps NodeOperations) NodeManager {
	return &pageNodeManager{
		pageStorage: pageStorage,
		nodeOps:     nodeOps,
	}
}

// NewInMemoryNodeManager creates a NodeManager using MemoryPageStorage.
// For testing only.
func NewInMemoryNodeManager(nodeOps NodeOperations) NodeManager {
	return newPageNodeManager(storage.NewMemoryPageStorage(), nodeOps)
}

// =============================================================================
// In-Memory Tree for Testing
// =============================================================================

// NewInMemoryTree creates a tree with an in-memory node manager for testing.
func NewInMemoryTree() TreeMutator {
	nodeOps := NewNodeOperations()
	pageStorage := storage.NewMemoryPageStorage()
	nodeMgr := newPageNodeManager(pageStorage, nodeOps)
	tree := newTreeImpl(nodeOps, nodeMgr, true)
	_ = tree.Open("")
	return tree
}

// Tree factory functions
func NewTree(nodeOps NodeOperations, nodeMgr NodeManager, isMutator bool) Tree {
	return newTreeImpl(nodeOps, nodeMgr, isMutator)
}

func NewTreeMutator(nodeOps NodeOperations, nodeMgr NodeManager) TreeMutator {
	return newTreeImpl(nodeOps, nodeMgr, true)
}
