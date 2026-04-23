package internal

import (
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
)

// NodePageAdapter wraps a RealPageProvider and implements btreeapi.PageProvider
// for backward compatibility with consumers (vacuum, etc.) that still use
// *btreeapi.Node.
type NodePageAdapter struct {
	provider *RealPageProvider
}

// NewNodePageAdapter creates a NodePageAdapter wrapping the given RealPageProvider.
func NewNodePageAdapter(provider *RealPageProvider) *NodePageAdapter {
	return &NodePageAdapter{provider: provider}
}

// AllocPage delegates to the underlying provider.
func (a *NodePageAdapter) AllocPage() pagestoreapi.PageID {
	return a.provider.AllocPage()
}

// ReadPage reads a page and converts it to *btreeapi.Node.
func (a *NodePageAdapter) ReadPage(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	return a.provider.ReadPageNode(pageID)
}

// ReadPageUncached reads directly and converts to *btreeapi.Node.
func (a *NodePageAdapter) ReadPageUncached(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	return a.provider.ReadPageNodeUncached(pageID)
}

// WritePage serializes a *btreeapi.Node and writes it.
func (a *NodePageAdapter) WritePage(pageID pagestoreapi.PageID, node *btreeapi.Node) error {
	return a.provider.WritePageNode(pageID, node)
}

// MemNodePageAdapter wraps a MemPageProvider and implements btreeapi.PageProvider.
type MemNodePageAdapter struct {
	provider *MemPageProvider
}

// NewMemNodePageAdapter creates a MemNodePageAdapter.
func NewMemNodePageAdapter(provider *MemPageProvider) *MemNodePageAdapter {
	return &MemNodePageAdapter{provider: provider}
}

// AllocPage delegates to the underlying provider.
func (a *MemNodePageAdapter) AllocPage() pagestoreapi.PageID {
	return a.provider.AllocPage()
}

// ReadPage reads a page and converts it to *btreeapi.Node.
func (a *MemNodePageAdapter) ReadPage(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	return a.provider.ReadPageNode(pageID)
}

// ReadPageUncached reads directly and converts to *btreeapi.Node.
func (a *MemNodePageAdapter) ReadPageUncached(pageID pagestoreapi.PageID) (*btreeapi.Node, error) {
	return a.provider.ReadPageNodeUncached(pageID)
}

// WritePage serializes a *btreeapi.Node and writes it.
func (a *MemNodePageAdapter) WritePage(pageID pagestoreapi.PageID, node *btreeapi.Node) error {
	return a.provider.WritePageNode(pageID, node)
}
