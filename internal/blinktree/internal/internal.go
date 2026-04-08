// Package internal provides the implementation of the blinktree package.
package internal

import (
	blinktree "github.com/akzj/go-fast-kv/internal/blinktree/api"
)

// Re-export types from api package
type (
	NodeFormat     = blinktree.NodeFormat
	LeafEntry      = blinktree.LeafEntry
	InternalEntry  = blinktree.InternalEntry
	InlineValue    = blinktree.InlineValue
	TreeOperation  = blinktree.TreeOperation
	OperationType  = blinktree.OperationType
	PageID         = blinktree.PageID
	Tree           = blinktree.Tree
	TreeMutator    = blinktree.TreeMutator
	TreeIterator   = blinktree.TreeIterator
	NodeOperations = blinktree.NodeOperations
	NodeManager    = blinktree.NodeManager
)

const (
	NodeTypeLeaf      = blinktree.NodeTypeLeaf
	NodeTypeInternal  = blinktree.NodeTypeInternal
	MaxNodeCapacity   = blinktree.MaxNodeCapacity
	LeafEntrySize     = blinktree.LeafEntrySize
	InternalEntrySize = blinktree.InternalEntrySize
	NodeHeaderSize    = blinktree.NodeHeaderSize
	OpPut             = blinktree.OpPut
	OpDelete          = blinktree.OpDelete
)

// Re-export errors
var (
	ErrKeyNotFound   = blinktree.ErrKeyNotFound
	ErrStoreClosed   = blinktree.ErrStoreClosed
	ErrWriteLocked   = blinktree.ErrWriteLocked
	ErrNodeNotFound  = blinktree.ErrNodeNotFound
	ErrInvalidNode   = blinktree.ErrInvalidNode
	ErrNodeFull      = blinktree.ErrNodeFull
	ErrKeyTooLarge   = blinktree.ErrKeyTooLarge
	ErrValueTooLarge = blinktree.ErrValueTooLarge
)
