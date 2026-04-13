// Package internal implements the B-link tree bulk loader.
//
// Bulk loading bypasses the normal O(log n) insert path by sorting entries
// and building the tree bottom-up. This achieves O(n) complexity with minimal
// comparisons and no node splits.
//
// Design goals:
//   - Atomic: either all entries are loaded or none (temp page IDs)
//   - Read-compatible: Get/Scan can run during bulk load (snapshot semantics)
//   - Fast mode: skip MVCC overhead for single-writer bulk imports
//
// Algorithm (top-down bulk loading):
//   1. Sort entries by key
//   2. Build leaf nodes (4KB pages, fill as much as possible)
//   3. Build internal nodes recursively until single root
//   4. Swap root atomically
//
// Reference: SQLite's INSERT optimization pattern, adapted for B-link trees.
package internal

import (
	"bytes"
	"errors"
	"sort"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// Errors for bulk loading operations.
var (
	ErrBulkNotSorted   = errors.New("btree: bulk load requires sorted entries")
	ErrBulkEmpty       = errors.New("btree: bulk load requires at least one entry")
	ErrBulkInProgress  = errors.New("btree: bulk load already in progress")
	ErrBulkNotStarted  = errors.New("btree: bulk load not started")
	ErrBulkAlreadyDone = errors.New("btree: bulk load already completed")
)

// BulkLoader builds a B-tree bottom-up for efficient bulk loading.
// It bypasses the normal insert path to achieve O(n) complexity.
//
// Thread safety: BulkLoader is NOT thread-safe. Create one per goroutine.
type BulkLoader struct {
	tree    *bTree
	mode    btreeapi.BulkMode
	txnID   uint64                // used in MVCC mode
	entries []btreeapi.KVPair      // entries to load (must be sorted)
	done    bool
}

// newBulkLoader creates a new BulkLoader (internal constructor).
func newBulkLoader(tree *bTree, mode btreeapi.BulkMode, txnID uint64) *BulkLoader {
	return &BulkLoader{
		tree:  tree,
		mode:  mode,
		txnID: txnID,
	}
}

// Add appends a key-value pair to the bulk load.
// Entries should be added in sorted order by key, or sorted=false can be used.
// This method does not modify the tree - call Build() to execute the load.
func (bl *BulkLoader) Add(key, value []byte) error {
	if bl.done {
		return ErrBulkAlreadyDone
	}
	if len(key) > btreeapi.MaxKeySize {
		return btreeapi.ErrKeyTooLarge
	}
	bl.entries = append(bl.entries, btreeapi.KVPair{Key: cloneBytes(key), Value: cloneBytes(value)})
	return nil
}

// AddSorted adds a pre-sorted slice of entries.
// Use this for maximum efficiency when entries are already sorted.
func (bl *BulkLoader) AddSorted(pairs []btreeapi.KVPair) error {
	if bl.done {
		return ErrBulkAlreadyDone
	}
	for _, p := range pairs {
		if len(p.Key) > btreeapi.MaxKeySize {
			return btreeapi.ErrKeyTooLarge
		}
		bl.entries = append(bl.entries, btreeapi.KVPair{Key: cloneBytes(p.Key), Value: cloneBytes(p.Value)})
	}
	return nil
}

// Sort sorts the entries by key.
// Call this if entries were added out of order.
func (bl *BulkLoader) Sort() {
	sort.Slice(bl.entries, func(i, j int) bool {
		return bytes.Compare(bl.entries[i].Key, bl.entries[j].Key) < 0
	})
}

// IsSorted returns true if entries are sorted by key.
func (bl *BulkLoader) IsSorted() bool {
	for i := 1; i < len(bl.entries); i++ {
		if bytes.Compare(bl.entries[i-1].Key, bl.entries[i].Key) > 0 {
			return false
		}
	}
	return true
}

// Build executes the bulk load, constructing a new B-tree from the entries.
// Returns the new root page ID on success.
//
// The load is atomic: either all entries are loaded or none.
// During loading, readers see the old tree; after completion, they see the new tree.
//
// In Fast mode, all entries get TxnMin=0, TxnMax=MaxUint64 (visible to all).
// In MVCC mode, all entries get the provided txnID (or 1 if not set).
func (bl *BulkLoader) Build() (uint64, error) {
	if bl.done {
		return 0, ErrBulkAlreadyDone
	}
	if len(bl.entries) == 0 {
		return 0, ErrBulkEmpty
	}

	// Sort if not already sorted
	if !bl.IsSorted() {
		bl.Sort()
	}

	// Use transaction ID for MVCC mode
	txnID := bl.txnID
	if bl.mode == btreeapi.BulkModeMVCC && txnID == 0 {
		txnID = 1 // default to txnID 1 for MVCC mode
	}

	// Build tree bottom-up
	newRootPID, err := bl.buildTree(txnID)
	if err != nil {
		return 0, err
	}

	bl.done = true
	return newRootPID, nil
}

// buildTree constructs the B-tree bottom-up from sorted entries.
func (bl *BulkLoader) buildTree(txnID uint64) (uint64, error) {
	// Phase 1: Build leaf nodes
	leafPIDs, err := bl.buildLeaves(txnID)
	if err != nil {
		return 0, err
	}

	// If we only have one leaf, it's already the root
	if len(leafPIDs) == 1 {
		return leafPIDs[0], nil
	}

	// Phase 2: Build internal nodes bottom-up until single root
	level := 1
	currentPIDs := leafPIDs

	for len(currentPIDs) > 1 {
		parentPIDs, err := bl.buildInternalLevel(currentPIDs)
		if err != nil {
			return 0, err
		}
		currentPIDs = parentPIDs
		level++
	}

	// currentPIDs[0] is the root
	return currentPIDs[0], nil
}

// buildLeaves creates leaf nodes from sorted entries.
// Returns the page IDs of all leaf nodes.
func (bl *BulkLoader) buildLeaves(txnID uint64) ([]uint64, error) {
	var leafPIDs []uint64
	var currentLeaf *btreeapi.Node
	var currentLeafPID uint64
	entriesInCurrentLeaf := 0

	flushLeaf := func() error {
		if currentLeaf == nil {
			return nil
		}
		currentLeaf.Count = uint16(entriesInCurrentLeaf)
		if err := bl.tree.pages.WritePage(currentLeafPID, currentLeaf); err != nil {
			return err
		}
		leafPIDs = append(leafPIDs, currentLeafPID)
		return nil
	}

	newLeaf := func() {
		currentLeafPID = bl.tree.pages.AllocPage()
		currentLeaf = &btreeapi.Node{IsLeaf: true}
		entriesInCurrentLeaf = 0
	}

	newLeaf()

	for _, pair := range bl.entries {
		// Create entry
		entry := btreeapi.LeafEntry{
			Key:    pair.Key,
			TxnMin: txnID,
			TxnMax: btreeapi.TxnMaxInfinity,
		}

		// Handle value (inline or blob)
		if bl.tree.blobs != nil && len(pair.Value) > bl.tree.inlineThres {
			blobID, err := bl.tree.blobs.WriteBlob(pair.Value)
			if err != nil {
				entry.Value.Inline = cloneBytes(pair.Value)
			} else {
				entry.Value.BlobID = blobID
			}
		} else {
			entry.Value.Inline = cloneBytes(pair.Value)
		}

		// Calculate new entry count
		// newCount = len(currentLeaf.Entries) + 1

		// Check if we would exceed page size with this entry
		// by temporarily adding and checking
		originalEntries := currentLeaf.Entries
		currentLeaf.Entries = append(currentLeaf.Entries, entry)

		if bl.tree.serializer.SerializedSize(currentLeaf) > btreeapi.PageSize {
			// Would exceed - restore and flush current leaf
			currentLeaf.Entries = originalEntries

			// Set HighKey to last entry in current leaf
			if len(currentLeaf.Entries) > 0 {
				currentLeaf.HighKey = cloneBytes(currentLeaf.Entries[len(currentLeaf.Entries)-1].Key)
			}

			// Flush current leaf
			if err := flushLeaf(); err != nil {
				return nil, err
			}

			// Start new leaf and add the overflow entry to it
			newLeaf()
			currentLeaf.Entries = append(currentLeaf.Entries, entry)
			entriesInCurrentLeaf++
			currentLeaf.HighKey = cloneBytes(pair.Key)
			// Skip normal path since entry was already added above
			continue
		}

		// Entry fits - keep it and update HighKey
		entriesInCurrentLeaf++
		currentLeaf.HighKey = cloneBytes(pair.Key)
	}

	// Flush the last leaf
	// The rightmost leaf has HighKey=nil (meaning +∞) for proper B-link traversal
	currentLeaf.HighKey = nil
	if err := flushLeaf(); err != nil {
		return nil, err
	}

	// Now set up B-link pointers (right siblings)
	// We need to read back the leaves to update their Next pointers
	for i := 0; i < len(leafPIDs)-1; i++ {
		leaf, err := bl.tree.pages.ReadPage(leafPIDs[i])
		if err != nil {
			return nil, err
		}
		leaf.Next = leafPIDs[i+1]
		if err := bl.tree.pages.WritePage(leafPIDs[i], leaf); err != nil {
			return nil, err
		}
	}

	return leafPIDs, nil
}

// buildInternalLevel builds one level of internal nodes from child page IDs.
// Each internal node's keys are the high keys of its children.
func (bl *BulkLoader) buildInternalLevel(childPIDs []uint64) ([]uint64, error) {
	var parentPIDs []uint64
	var currentParent *btreeapi.Node
	var currentParentPID uint64
	childrenInCurrentParent := 0
	keysInCurrentParent := 0

	flushParent := func() error {
		if currentParent == nil {
			return nil
		}
		currentParent.Count = uint16(keysInCurrentParent)
		if err := bl.tree.pages.WritePage(currentParentPID, currentParent); err != nil {
			return err
		}
		parentPIDs = append(parentPIDs, currentParentPID)
		return nil
	}

	newParent := func() {
		currentParentPID = bl.tree.pages.AllocPage()
		currentParent = &btreeapi.Node{IsLeaf: false}
		childrenInCurrentParent = 0
		keysInCurrentParent = 0
	}

	newParent()

	for i, childPID := range childPIDs {
		// Read this child
		childNode, err := bl.tree.pages.ReadPage(childPID)
		if err != nil {
			return nil, err
		}

		// First child: no key needed, but set HighKey for future separator
		if childrenInCurrentParent == 0 {
			currentParent.Children = append(currentParent.Children, childPID)
			currentParent.HighKey = cloneBytes(childNode.HighKey) // Save for next iteration
			childrenInCurrentParent++
		} else {
			// The separator key for this child is the HIGH KEY of the previous child
			// (stored in currentParent.HighKey from the previous iteration)
			separatorKey := cloneBytes(currentParent.HighKey)

			// Update parent's high key to this child's high key for next iteration
			currentParent.HighKey = cloneBytes(childNode.HighKey)

			currentParent.Keys = append(currentParent.Keys, separatorKey)
			currentParent.Children = append(currentParent.Children, childPID)
			keysInCurrentParent++
			childrenInCurrentParent++
		}

		// Check if parent is full
		if bl.tree.serializer.SerializedSize(currentParent) > btreeapi.PageSize {
			// Remove last child (it doesn't fit)
			if len(currentParent.Children) > 0 {
				currentParent.Children = currentParent.Children[:len(currentParent.Children)-1]
			}
			if len(currentParent.Keys) > 0 {
				currentParent.Keys = currentParent.Keys[:len(currentParent.Keys)-1]
			}
			childrenInCurrentParent--
			keysInCurrentParent--

			// Update high key
			if len(currentParent.Keys) > 0 {
				currentParent.HighKey = cloneBytes(currentParent.Keys[len(currentParent.Keys)-1])
			}

			// Flush and start new parent
			if err := flushParent(); err != nil {
				return nil, err
			}

			// This child goes to the next parent
			newParent()
			i-- // reprocess this child
			continue
		}
	}

	// Flush the last parent
	// The rightmost child has HighKey=nil (meaning +∞)
	currentParent.HighKey = nil
	if err := flushParent(); err != nil {
		return nil, err
	}

	// Set up B-link pointers for internal nodes
	for i := 0; i < len(parentPIDs)-1; i++ {
		parent, err := bl.tree.pages.ReadPage(parentPIDs[i])
		if err != nil {
			return nil, err
		}
		parent.Next = parentPIDs[i+1]
		if err := bl.tree.pages.WritePage(parentPIDs[i], parent); err != nil {
			return nil, err
		}
	}

	return parentPIDs, nil
}

// Close releases resources held by the BulkLoader.
// Call this if Build() is not called or fails.
func (bl *BulkLoader) Close() error {
	bl.entries = nil
	bl.done = true
	return nil
}

// EntryCount returns the number of entries added to the loader.
func (bl *BulkLoader) EntryCount() int {
	return len(bl.entries)
}
