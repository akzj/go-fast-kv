package internal

import pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"

// pageStore is the internal interface for B-tree page storage.
// It works directly with *Page (slotted page []byte view), eliminating
// serialize/deserialize overhead.
//
// This replaces btreeapi.PageProvider for internal btree operations.
// External consumers (vacuum, kvstore) still use btreeapi.PageProvider
// through adapter methods on the concrete providers.
type pageStore interface {
	// AllocPage allocates a new page and returns its PageID.
	AllocPage() pagestoreapi.PageID

	// ReadPage reads a page from the given PageID.
	// The returned *Page may be a shared reference — callers must
	// not modify it unless they hold an exclusive write lock.
	ReadPage(pageID pagestoreapi.PageID) (*Page, error)

	// WritePage writes a page to the given PageID.
	WritePage(pageID pagestoreapi.PageID, page *Page) error

	// ReadPageForWrite returns a clone of the page, safe for in-place mutation.
	// Must be used when the caller intends to modify the page data (under WLock).
	ReadPageForWrite(pageID pagestoreapi.PageID) (*Page, error)

	// ReadPageUncached reads directly from the underlying store without
	// going through the LRU cache.
	ReadPageUncached(pageID pagestoreapi.PageID) (*Page, error)
}
