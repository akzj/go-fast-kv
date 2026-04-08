// Package blinktree provides the B-link-tree index API.
package blinktree

import (
	blinktreeapi "github.com/akzj/go-fast-kv/internal/blinktree/api"
	"github.com/akzj/go-fast-kv/internal/blinktree/internal"
	"github.com/akzj/go-fast-kv/internal/storage"
)

// Re-export all interfaces from api package.
type (
	NodeFormat     = blinktreeapi.NodeFormat
	LeafEntry      = blinktreeapi.LeafEntry
	InternalEntry  = blinktreeapi.InternalEntry
	InlineValue    = blinktreeapi.InlineValue
	TreeOperation  = blinktreeapi.TreeOperation
	TreeIterator   = blinktreeapi.TreeIterator
	NodeOperations = blinktreeapi.NodeOperations
	NodeManager    = blinktreeapi.NodeManager
	Tree           = blinktreeapi.Tree
	TreeMutator    = blinktreeapi.TreeMutator
	PageID         = blinktreeapi.PageID
	OperationType  = blinktreeapi.OperationType
)

// Re-export constants.
const (
	ExternalThreshold = blinktreeapi.ExternalThreshold
	NodeHeaderSize    = blinktreeapi.NodeHeaderSize
	MaxNodeCapacity   = blinktreeapi.MaxNodeCapacity
	LeafEntrySize     = blinktreeapi.LeafEntrySize
	InternalEntrySize = blinktreeapi.InternalEntrySize
	NodeTypeLeaf      = blinktreeapi.NodeTypeLeaf
	NodeTypeInternal  = blinktreeapi.NodeTypeInternal
	OpPut             = blinktreeapi.OpPut
	OpDelete          = blinktreeapi.OpDelete
)

// Re-export errors.
var (
	ErrKeyNotFound   = blinktreeapi.ErrKeyNotFound
	ErrStoreClosed   = blinktreeapi.ErrStoreClosed
	ErrWriteLocked   = blinktreeapi.ErrWriteLocked
	ErrNodeNotFound  = blinktreeapi.ErrNodeNotFound
	ErrInvalidNode   = blinktreeapi.ErrInvalidNode
	ErrNodeFull      = blinktreeapi.ErrNodeFull
	ErrKeyTooLarge   = blinktreeapi.ErrKeyTooLarge
	ErrValueTooLarge = blinktreeapi.ErrValueTooLarge
)

// Factory functions.
func NewNodeOperations() NodeOperations {
	return internal.NewNodeOperations()
}

func NewNodeManager(segmentMgr storage.SegmentManager, nodeOps NodeOperations) NodeManager {
	return internal.NewNodeManager(segmentMgr, nodeOps)
}

func NewTree(nodeOps NodeOperations, nodeMgr NodeManager) Tree {
	return internal.NewTree(nodeOps, nodeMgr, false)
}

func NewTreeMutator(nodeOps NodeOperations, nodeMgr NodeManager) TreeMutator {
	return internal.NewTreeMutator(nodeOps, nodeMgr)
}

func NewInMemoryTree() TreeMutator {
	return internal.NewInMemoryTree()
}
