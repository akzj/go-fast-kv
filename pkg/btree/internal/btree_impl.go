// Package internal provides the B+Tree implementation.
package internal

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/akzj/go-fast-kv/pkg/btree/api"
	objstore "github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// BTreeImpl implements the BTree interface using a B-link-tree structure.
// Uses page-level latches instead of a global mutex for better concurrency.
type BTreeImpl struct {
	root     api.PageID
	store    objstore.ObjectStore
	order    uint16
	pageSize uint32
	inlineThreshold uint32

	// Page cache: maps PageID to in-memory page
	cache map[api.PageID]*page

	// Latch table: per-page latches for lock coupling
	latchTable sync.Map // map[PageID]*Latch

	// Configuration
	config api.BTreeConfig

	// Blob ID tracking for deletion (optional - for cleanup)
	blobIDs map[objstore.ObjectID]struct{}

	// Global latch for structural changes (root splits, etc.)
	rootMu sync.Mutex
}

// Ensure BTreeImpl implements api.BTree
var _ api.BTree = (*BTreeImpl)(nil)

// NewBTree creates a new BTree with the given ObjectStore.
func NewBTree(store objstore.ObjectStore, config api.BTreeConfig) (*api.BTreeImpl, error) {
	if config.PageSize == 0 {
		config = api.DefaultBTreeConfig()
	}
	if config.Order == 0 {
		config.Order = 256
	}
	if config.InlineThreshold == 0 {
		config.InlineThreshold = 512
	}

	bt := &BTreeImpl{
		root:     1, // Root page ID starts at 1
		store:    store,
		order:    config.Order,
		pageSize: config.PageSize,
		inlineThreshold: config.InlineThreshold,
		cache:    make(map[api.PageID]*page),
		config:   config,
		blobIDs:  make(map[objstore.ObjectID]struct{}),
	}

	// Allocate and initialize root page (as leaf for empty tree)
	ctx := context.Background()
	rootPage := newPage(true)
	rootPage.parentHint = 0

	// Write root page to store
	pageData, err := rootPage.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal root page: %w", err)
	}

	objID := writePageIDToStore(bt.root)
	_, err = bt.store.WritePage(ctx, objID, pageData)
	if err != nil {
		return nil, fmt.Errorf("write root page: %w", err)
	}

	bt.cache[bt.root] = rootPage

	// Return as api.BTreeImpl type alias
	return (*api.BTreeImpl)(nil), nil
}

// getLatch gets or creates a latch for a page.
func (bt *BTreeImpl) getLatch(pageID api.PageID) *Latch {
	latch, _ := bt.latchTable.LoadOrStore(pageID, newLatch())
	return latch.(*Latch)
}

// Get retrieves a value by key.
func (bt *BTreeImpl) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	// Use lock coupling to traverse down
	latches := bt.findLeafPageLatched(ctx, key, true)
	if len(latches) == 0 {
		return nil, false, nil
	}
	defer bt.releaseLatches(latches)

	leafPage := latches[len(latches)-1].page
	if leafPage == nil {
		return nil, false, nil
	}

	// Get value from leaf page
	value, found := leafPage.get(key)
	if !found {
		return nil, false, nil
	}

	// Check if it's a blob reference
	actualValue, blobID, isBlob, err := api.UnmarshalBTreeValue(value)
	if err != nil {
		return nil, false, err
	}

	if isBlob {
		// Fetch blob from ObjectStore
		blobData, err := bt.store.ReadBlob(ctx, blobID)
		if err != nil {
			return nil, false, err
		}
		return blobData, true, nil
	}

	return actualValue, true, nil
}

// Put inserts or updates a key-value pair.
func (bt *BTreeImpl) Put(ctx context.Context, key []byte, value []byte) error {
	// Handle large values: write to blob storage first
	var storedValue []byte
	var err error

	if len(value) >= int(bt.inlineThreshold) {
		// Write value as blob
		blobID, err := bt.store.WriteBlob(ctx, value)
		if err != nil {
			return fmt.Errorf("write blob: %w", err)
		}

		// Encode as blob reference
		storedValue = make([]byte, 1+8)
		storedValue[0] = byte(api.BTreeValueBlob)
		putUint64BE(storedValue[1:9], uint64(blobID))

		// Track blob ID for potential cleanup
		bt.blobIDs[blobID] = struct{}{}
	} else {
		// Store inline
		storedValue, err = api.MarshalBTreeValue(value, bt.inlineThreshold)
		if err != nil {
			return fmt.Errorf("marshal inline value: %w", err)
		}
	}

	// Insert into tree with write lock on leaf
	return bt.insert(ctx, key, storedValue)
}

// Delete removes a key from the tree.
func (bt *BTreeImpl) Delete(ctx context.Context, key []byte) error {
	// Find leaf page with write lock
	latches := bt.findLeafPageLatched(ctx, key, false)
	if len(latches) == 0 {
		return nil
	}
	defer bt.releaseLatches(latches)

	leafPage := latches[len(latches)-1].page
	if leafPage == nil {
		return nil
	}

	// Check if key exists
	idx := leafPage.search(key)
	if idx >= int(leafPage.numKeys) || !bytes.Equal(key, leafPage.keys[idx]) {
		return nil // Key not found, nothing to delete
	}

	// Check if value is a blob and delete it
	value := leafPage.values[idx]
	if len(value) > 0 && api.BTreeValueFlag(value[0]) == api.BTreeValueBlob {
		blobID := objstore.ObjectID(getUint64BE(value[1:9]))
		bt.store.Delete(ctx, blobID)
		delete(bt.blobIDs, blobID)
	}

	// Delete from leaf page
	leafPage.delete(key)

	// Persist the leaf page
	if err := bt.writePageByID(ctx, leafPage); err != nil {
		return err
	}

	// Check for underflow and handle if needed
	if int(leafPage.numKeys) < 128 {
		bt.handleUnderflow(ctx, leafPage)
	}

	return nil
}

// Scan performs a range scan from start to end.
func (bt *BTreeImpl) Scan(ctx context.Context, start []byte, end []byte, iterator func(key, value []byte) bool) error {
	// Use lock coupling to find starting leaf page
	latches := bt.findLeafPageLatched(ctx, start, true)
	if len(latches) == 0 {
		return nil
	}
	
	// Keep root latch for entire scan, release others
	rootLatch := latches[0]
	
	// Release non-leaf latches
	for i := 1; i < len(latches)-1; i++ {
		latches[i].latch.RUnlock()
	}
	
	leafPage := latches[len(latches)-1].page
	if leafPage == nil {
		rootLatch.latch.RUnlock()
		return nil
	}

	// Scan through leaf pages
	for {
		if !leafPage.scan(start, end, func(key, value []byte) bool {
			// Resolve blob references
			actualValue, _, isBlob, err := api.UnmarshalBTreeValue(value)
			if err != nil {
				return false
			}

			if isBlob {
				// value contains blob ID after flag byte
				blobID := objstore.ObjectID(getUint64BE(value[1:9]))
				blobData, err := bt.store.ReadBlob(ctx, blobID)
				if err != nil {
					return false
				}
				return iterator(key, blobData)
			}

			return iterator(key, actualValue)
		}) {
			rootLatch.latch.RUnlock()
			return nil // Iterator returned false
		}

		// Move to next leaf page
		if leafPage.nextPageID == 0 {
			break
		}

		// Lock coupling: lock next page before releasing current
		nextLatch := bt.getLatch(leafPage.nextPageID)
		nextLatch.RLock()
		latches[len(latches)-1].latch.RUnlock()
		latches[len(latches)-1].page = nil // Mark as released
		
		leafPage, err := bt.loadPageByID(ctx, leafPage.nextPageID)
		if err != nil {
			rootLatch.latch.RUnlock()
			return err
		}
		if leafPage == nil {
			break
		}
		latches[len(latches)-1].page = leafPage
	}

	rootLatch.latch.RUnlock()
	return nil
}

// CreateScanIter creates a new scan iterator.
func (bt *BTreeImpl) CreateScanIter(start []byte, end []byte) (api.BTreeIter, error) {
	ctx := context.Background()

	// Use lock coupling to find starting leaf page
	latches := bt.findLeafPageLatched(ctx, start, true)
	if len(latches) == 0 {
		return &btreeIter{
			bt:           bt,
			start:        start,
			end:          end,
			currentPage:  nil,
			currentIdx:   0,
		}, nil
	}

	return &btreeIter{
		bt:           bt,
		start:        start,
		end:          end,
		currentPage:  latches[len(latches)-1].page,
		currentIdx:   0,
		latches:      latches,
	}, nil
}

// Load loads a page from ObjectStore into the cache.
func (bt *BTreeImpl) Load(ctx context.Context, pageID api.PageID) error {
	latch := bt.getLatch(pageID)
	latch.Lock()
	defer latch.Unlock()

	_, err := bt.loadPageByID(ctx, pageID)
	return err
}

// Flush writes all dirty pages to ObjectStore.
func (bt *BTreeImpl) Flush(ctx context.Context) error {
	bt.rootMu.Lock()
	defer bt.rootMu.Unlock()

	for pageID, p := range bt.cache {
		if p.isDirty() {
			if err := bt.writePageLocked(ctx, pageID, p); err != nil {
				return err
			}
		}
	}

	return bt.store.Sync(ctx)
}

// Close closes the BTree.
func (bt *BTreeImpl) Close() error {
	bt.rootMu.Lock()
	defer bt.rootMu.Unlock()

	ctx := context.Background()

	// Write all dirty pages
	for pageID, p := range bt.cache {
		if p.isDirty() {
			if err := bt.writePageLocked(ctx, pageID, p); err != nil {
				return err
			}
		}
	}

	bt.cache = nil
	return nil
}

// --- Internal helper methods ---

// pageWithLatch holds a page and its latch for lock coupling.
type pageWithLatch struct {
	page      *page
	latch     *Latch
	writeLock bool // true if we hold an exclusive lock
}

// findLeafPageLatched finds the leaf page containing the given key with lock coupling.
// For read=true, uses shared locks; for read=false, uses exclusive locks.
func (bt *BTreeImpl) findLeafPageLatched(ctx context.Context, key []byte, read bool) []pageWithLatch {
	bt.rootMu.Lock()
	rootID := bt.root
	bt.rootMu.Unlock()

	// Start from root with lock
	rootLatch := bt.getLatch(rootID)
	if read {
		rootLatch.RLock()
	} else {
		rootLatch.Lock()
	}

	result := []pageWithLatch{{page: nil, latch: rootLatch, writeLock: !read}}

	current, err := bt.loadPageByID(ctx, rootID)
	if err != nil || current == nil {
		return result
	}
	result[0].page = current

	// Traverse down to leaf
	for !current.isLeaf() {
		idx := current.search(key)
		// Use child at idx (or last child if key > all)
		childIdx := idx
		if childIdx >= int(current.numKeys) {
			childIdx = int(current.numKeys) - 1
		}
		if childIdx < 0 {
			childIdx = 0
		}

		childID := current.childPageIDs[childIdx]
		
		// Lock child before unlocking parent (lock coupling)
		childLatch := bt.getLatch(childID)
		if read {
			childLatch.RLock()
		} else {
			childLatch.Lock()
		}
		
		// Release parent latch
		result[len(result)-1].latch.RUnlock()
		
		// Load child page
		current, err = bt.loadPageByID(ctx, childID)
		if err != nil || current == nil {
			// Return what we have so far
			return result
		}
		
		result = append(result, pageWithLatch{page: current, latch: childLatch, writeLock: !read})
	}

	return result
}

// releaseLatches releases all latches in the slice.
func (bt *BTreeImpl) releaseLatches(latches []pageWithLatch) {
	for _, pl := range latches {
		if pl.writeLock {
			pl.latch.Unlock()
		} else {
			pl.latch.RUnlock()
		}
	}
}

// loadPageByID loads a page from cache or ObjectStore (latch must be held).
func (bt *BTreeImpl) loadPageByID(ctx context.Context, pageID api.PageID) (*page, error) {
	// Check cache first
	if p, ok := bt.cache[pageID]; ok {
		return p, nil
	}

	// Load from ObjectStore
	objID := writePageIDToStore(pageID)
	data, err := bt.store.ReadPage(ctx, objID)
	if err != nil {
		if err == objstore.ErrObjectNotFound {
			return nil, nil
		}
		return nil, err
	}

	p := &page{}
	if err := p.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("unmarshal page %d: %w", pageID, err)
	}

	p.clearDirty()
	bt.cache[pageID] = p
	return p, nil
}

// writePageByID writes a page to ObjectStore (must be called with latch held).
func (bt *BTreeImpl) writePageByID(ctx context.Context, p *page) error {
	// Find page ID from cache
	var pageID api.PageID
	for id, cached := range bt.cache {
		if cached == p {
			pageID = id
			break
		}
	}
	if pageID == 0 {
		return fmt.Errorf("page not found in cache")
	}

	return bt.writePageLocked(ctx, pageID, p)
}

// writePageLocked writes a page to ObjectStore.
func (bt *BTreeImpl) writePageLocked(ctx context.Context, pageID api.PageID, p *page) error {
	data, err := p.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal page: %w", err)
	}

	objID := writePageIDToStore(pageID)
	_, err = bt.store.WritePage(ctx, objID, data)
	if err != nil {
		return fmt.Errorf("write page: %w", err)
	}

	p.clearDirty()
	return nil
}

// insert inserts a key-value pair into the tree.
func (bt *BTreeImpl) insert(ctx context.Context, key []byte, value []byte) error {
	// Find leaf page with write latch
	latches := bt.findLeafPageLatched(ctx, key, false)
	if len(latches) == 0 {
		return fmt.Errorf("root page not found")
	}
	defer bt.releaseLatches(latches)

	leafPage := latches[len(latches)-1].page
	if leafPage == nil {
		return fmt.Errorf("root page not found")
	}

	// Insert into leaf page
	splitNeeded := leafPage.insert(key, value)

	// Write leaf page
	if err := bt.writePageByID(ctx, leafPage); err != nil {
		return err
	}

	if splitNeeded {
		// Split leaf page
		bt.rootMu.Lock()
		midKey, newRightPage, _ := leafPage.split()
		newPageID := bt.allocatePageID()

		// Set parent for new page
		newRightPage.parentHint = leafPage.parentHint

		// Update sibling pointers
		newRightPage.nextPageID = leafPage.nextPageID
		leafPage.nextPageID = newPageID

		// Write new right page
		if err := bt.writePageLocked(ctx, newPageID, newRightPage); err != nil {
			bt.rootMu.Unlock()
			return err
		}

		// Insert separator key into parent
		err := bt.insertIntoParentLocked(ctx, leafPage, midKey, newRightPage, newPageID)
		bt.rootMu.Unlock()
		return err
	}

	return nil
}

// insertIntoParentLocked inserts a separator key and new child into the parent.
// Called with rootMu held.
func (bt *BTreeImpl) insertIntoParentLocked(ctx context.Context, leftPage *page, midKey []byte, rightPage *page, rightPageID api.PageID) error {
	// Get or create parent
	var parentPage *page
	var parentPageID api.PageID

	if leftPage.parentHint == 0 {
		// Need to create new root
		return bt.createNewRootLocked(ctx, leftPage, midKey, rightPage, rightPageID)
	}

	parentPageID = leftPage.parentHint
	
	// Lock parent for modification
	parentLatch := bt.getLatch(parentPageID)
	parentLatch.Lock()
	defer parentLatch.Unlock()

	parentPage, err := bt.loadPageByID(ctx, parentPageID)
	if err != nil {
		return err
	}
	if parentPage == nil {
		return fmt.Errorf("parent page not found: %d", parentPageID)
	}

	// Insert separator key and child pointer
	splitNeeded := parentPage.insertChild(midKey, rightPageID)

	// Update child pointers' parent hints
	rightPage.parentHint = parentPageID

	// Write parent page
	if err := bt.writePageLocked(ctx, parentPageID, parentPage); err != nil {
		return err
	}

	// Write updated right page
	if err := bt.writePageLocked(ctx, rightPageID, rightPage); err != nil {
		return err
	}

	if splitNeeded {
		// Split parent page
		sepKey, newParentPage, _ := parentPage.split()
		newParentPageID := bt.allocatePageID()

		// Update parent hints for children in new page
		for i := range newParentPage.childPageIDs {
			childPage, _ := bt.loadPageByID(ctx, newParentPage.childPageIDs[i])
			if childPage != nil {
				childPage.parentHint = newParentPageID
				bt.writePageLocked(ctx, newParentPage.childPageIDs[i], childPage)
			}
		}

		newParentPage.parentHint = parentPage.parentHint

		// Write new parent page
		if err := bt.writePageLocked(ctx, newParentPageID, newParentPage); err != nil {
			return err
		}

		// Recursively insert into grandparent
		if parentPage.parentHint == 0 {
			// Create new root
			return bt.createNewRootLocked(ctx, parentPage, sepKey, newParentPage, newParentPageID)
		}

		return bt.insertIntoParentLocked(ctx, parentPage, sepKey, newParentPage, newParentPageID)
	}

	return nil
}

// createNewRootLocked creates a new root page with two children.
// Called with rootMu held.
func (bt *BTreeImpl) createNewRootLocked(ctx context.Context, leftPage *page, midKey []byte, rightPage *page, rightPageID api.PageID) error {
	newRoot := newPage(false) // Internal page
	newRootPageID := bt.allocatePageID()

	// Update child parent hints
	leftPage.parentHint = newRootPageID
	rightPage.parentHint = newRootPageID

	// Set up new root with two children
	newRoot.keys = append(newRoot.keys, midKey)
	newRoot.childPageIDs = append(newRoot.childPageIDs, bt.findPageID(leftPage), rightPageID)
	newRoot.numKeys = 1

	// Write all pages
	leftPageID := bt.findPageID(leftPage)
	if err := bt.writePageLocked(ctx, leftPageID, leftPage); err != nil {
		return err
	}
	if err := bt.writePageLocked(ctx, rightPageID, rightPage); err != nil {
		return err
	}
	if err := bt.writePageLocked(ctx, newRootPageID, newRoot); err != nil {
		return err
	}

	// Update root
	bt.root = newRootPageID
	bt.cache[newRootPageID] = newRoot

	// Add latch for new root
	bt.getLatch(newRootPageID)

	return nil
}

// allocatePageID allocates a new page ID from ObjectStore.
func (bt *BTreeImpl) allocatePageID() api.PageID {
	ctx := context.Background()
	objID, err := bt.store.AllocPage(ctx)
	if err != nil {
		panic(fmt.Sprintf("allocate page: %v", err))
	}
	return readPageIDFromStore(objID)
}

// findPageID finds the page ID for a given page in cache.
func (bt *BTreeImpl) findPageID(p *page) api.PageID {
	for id, cached := range bt.cache {
		if cached == p {
			return id
		}
	}
	return 0
}

// handleUnderflow handles page underflow after deletion.
func (bt *BTreeImpl) handleUnderflow(ctx context.Context, page *page) {
	// For MVP, we don't implement complex redistribution/merging
	// This is a placeholder for future enhancement
}

// putUint64BE encodes uint64 in big-endian format.
func putUint64BE(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

// getUint64BE decodes uint64 from big-endian format.
func getUint64BE(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// btreeIter implements BTreeIter for scanning.
type btreeIter struct {
	bt           *BTreeImpl
	start        []byte
	end          []byte
	currentPage  *page
	currentIdx   int
	startPassed  bool
	latches      []pageWithLatch // latches to release on Close
}

// Next returns the next key-value pair.
func (iter *btreeIter) Next() ([]byte, []byte, error) {
	ctx := context.Background()

	// If we haven't found the start position yet, advance to it
	for iter.currentPage != nil {
		for iter.currentIdx < int(iter.currentPage.numKeys) {
			key := iter.currentPage.keys[iter.currentIdx]
			value := iter.currentPage.values[iter.currentIdx]

			// Check start bound
			if iter.start != nil && bytes.Compare(key, iter.start) < 0 {
				iter.currentIdx++
				continue
			}

			// Check end bound
			if iter.end != nil && bytes.Compare(key, iter.end) >= 0 {
				return nil, nil, nil
			}

			// Skip if already past this key
			if iter.startPassed {
				// Process this key
				iter.currentIdx++
				return iter.processValue(ctx, key, value)
			}

			// First key that meets criteria
			iter.startPassed = true
			iter.currentIdx++
			return iter.processValue(ctx, key, value)
		}

		// Move to next page
		if iter.currentPage.nextPageID == 0 {
			return nil, nil, nil
		}

		// Lock coupling: lock next page before releasing current
		nextLatch := iter.bt.getLatch(iter.currentPage.nextPageID)
		nextLatch.RLock()
		
		// Release current page latch
		if len(iter.latches) > 0 {
			iter.latches[len(iter.latches)-1].latch.RUnlock()
			iter.latches[len(iter.latches)-1].page = nil
		}
		
		var err error
		iter.currentPage, err = iter.bt.loadPageByID(ctx, iter.currentPage.nextPageID)
		if err != nil {
			return nil, nil, err
		}
		if iter.currentPage == nil {
			return nil, nil, nil
		}
		
		// Update latches
		if len(iter.latches) > 0 {
			iter.latches[len(iter.latches)-1].page = iter.currentPage
		}
		iter.currentIdx = 0
	}

	return nil, nil, nil
}

// processValue handles blob resolution for a value.
func (iter *btreeIter) processValue(ctx context.Context, key, value []byte) ([]byte, []byte, error) {
	actualValue, _, isBlob, err := api.UnmarshalBTreeValue(value)
	if err != nil {
		return nil, nil, err
	}

	if isBlob {
		blobID := objstore.ObjectID(getUint64BE(value[1:9]))
		actualValue, err = iter.bt.store.ReadBlob(ctx, blobID)
		if err != nil {
			return nil, nil, err
		}
	}

	return key, actualValue, nil
}

// Close closes the iterator.
func (iter *btreeIter) Close() error {
	// Release all latches
	for _, pl := range iter.latches {
		if pl.page != nil {
			pl.latch.RUnlock()
		}
	}
	iter.currentPage = nil
	iter.latches = nil
	return nil
}

// Helper functions for ObjectStore integration

// writePageIDToStore encodes PageID to ObjectID for ObjectStore
func writePageIDToStore(pageID api.PageID) objstore.ObjectID {
	return objstore.MakeObjectID(objstore.ObjectTypePage, uint64(pageID))
}

// readPageIDFromStore decodes PageID from ObjectID
func readPageIDFromStore(objID objstore.ObjectID) api.PageID {
	return api.PageID(objID.GetSequence())
}
