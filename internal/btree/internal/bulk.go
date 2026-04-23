// Package internal implements the B-link tree bulk loader.
//
// Bulk loading bypasses the normal O(log n) insert path by sorting entries
// and building the tree bottom-up. This achieves O(n) complexity with minimal
// comparisons and no node splits.
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
type BulkLoader struct {
	tree    *bTree
	mode    btreeapi.BulkMode
	txnID   uint64
	entries []btreeapi.KVPair
	done    bool
}

func newBulkLoader(tree *bTree, mode btreeapi.BulkMode, txnID uint64) *BulkLoader {
	return &BulkLoader{
		tree:  tree,
		mode:  mode,
		txnID: txnID,
	}
}

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

func (bl *BulkLoader) Sort() {
	sort.Slice(bl.entries, func(i, j int) bool {
		return bytes.Compare(bl.entries[i].Key, bl.entries[j].Key) < 0
	})
}

func (bl *BulkLoader) IsSorted() bool {
	for i := 1; i < len(bl.entries); i++ {
		if bytes.Compare(bl.entries[i-1].Key, bl.entries[i].Key) > 0 {
			return false
		}
	}
	return true
}

func (bl *BulkLoader) Build() (uint64, error) {
	if bl.done {
		return 0, ErrBulkAlreadyDone
	}
	if len(bl.entries) == 0 {
		return 0, ErrBulkEmpty
	}
	if !bl.IsSorted() {
		bl.Sort()
	}

	txnID := bl.txnID
	if bl.mode == btreeapi.BulkModeMVCC && txnID == 0 {
		txnID = 1
	}

	newRootPID, err := bl.buildTree(txnID)
	if err != nil {
		return 0, err
	}

	bl.done = true
	return newRootPID, nil
}

func (bl *BulkLoader) buildTree(txnID uint64) (uint64, error) {
	leafPIDs, err := bl.buildLeaves(txnID)
	if err != nil {
		return 0, err
	}

	if len(leafPIDs) == 1 {
		return leafPIDs[0], nil
	}

	currentPIDs := leafPIDs
	for len(currentPIDs) > 1 {
		parentPIDs, err := bl.buildInternalLevel(currentPIDs)
		if err != nil {
			return 0, err
		}
		currentPIDs = parentPIDs
	}

	return currentPIDs[0], nil
}

func (bl *BulkLoader) buildLeaves(txnID uint64) ([]uint64, error) {
	var leafPIDs []uint64
	var currentLeaf *Page
	var currentLeafPID uint64

	flushLeaf := func() error {
		if currentLeaf == nil {
			return nil
		}
		if err := bl.tree.pages.WritePage(currentLeafPID, currentLeaf); err != nil {
			return err
		}
		leafPIDs = append(leafPIDs, currentLeafPID)
		return nil
	}

	newLeaf := func() {
		currentLeafPID = bl.tree.pages.AllocPage()
		currentLeaf = NewLeafPage()
	}

	newLeaf()

	for _, pair := range bl.entries {
		// Handle value (inline or blob)
		var blobID uint64
		var inlineVal []byte
		if bl.tree.blobs != nil && len(pair.Value) > bl.tree.inlineThres {
			bid, err := bl.tree.blobs.WriteBlob(pair.Value)
			if err == nil {
				blobID = bid
			} else {
				inlineVal = cloneBytes(pair.Value)
			}
		} else {
			inlineVal = cloneBytes(pair.Value)
		}

		isBlobRef := blobID > 0
		cellSize := LeafCellSize(len(pair.Key), len(inlineVal), isBlobRef)

		// Check if entry fits in current leaf
		if currentLeaf.FreeSpace() < cellSize+2 { // +2 for slot
			// Set HighKey to the first key of the NEXT leaf (this entry's key).
			// HighKey is the exclusive upper bound: keys < HighKey are in this leaf,
			// keys >= HighKey go to the right sibling.
			currentLeaf.SetHighKey(pair.Key)

			// Flush current leaf
			if err := flushLeaf(); err != nil {
				return nil, err
			}

			// Start new leaf
			newLeaf()
		}

		// Insert entry
		pos := currentLeaf.Count()
		err := currentLeaf.InsertLeafEntry(pos, pair.Key, txnID, btreeapi.TxnMaxInfinity, inlineVal, blobID)
		if err != nil {
			return nil, err
		}
	}

	// Flush the last leaf — rightmost has HighKey=nil (+∞)
	if err := flushLeaf(); err != nil {
		return nil, err
	}

	// Set up B-link pointers (right siblings)
	for i := 0; i < len(leafPIDs)-1; i++ {
		leaf, err := bl.tree.pages.ReadPage(leafPIDs[i])
		if err != nil {
			return nil, err
		}
		leaf.SetNext(leafPIDs[i+1])
		if err := bl.tree.pages.WritePage(leafPIDs[i], leaf); err != nil {
			return nil, err
		}
	}

	return leafPIDs, nil
}

func (bl *BulkLoader) buildInternalLevel(childPIDs []uint64) ([]uint64, error) {
	var parentPIDs []uint64
	var currentParent *Page
	var currentParentPID uint64
	var lastChildHighKey []byte // tracks the high key of the previous child

	flushParent := func() error {
		if currentParent == nil {
			return nil
		}
		if err := bl.tree.pages.WritePage(currentParentPID, currentParent); err != nil {
			return err
		}
		parentPIDs = append(parentPIDs, currentParentPID)
		return nil
	}

	newParent := func() {
		currentParentPID = bl.tree.pages.AllocPage()
		currentParent = NewInternalPage()
		lastChildHighKey = nil
	}

	newParent()

	for _, childPID := range childPIDs {
		// Read child to get its high key for separator
		childPage, err := bl.tree.pages.ReadPage(childPID)
		if err != nil {
			return nil, err
		}

		if currentParent.Count() == 0 && currentParent.Child0() == 0 {
			// First child: set as child0
			currentParent.SetChild0(childPID)
			lastChildHighKey = cloneBytes(childPage.HighKey())
			continue
		}

		// The separator key is the high key of the PREVIOUS child
		separatorKey := lastChildHighKey
		lastChildHighKey = cloneBytes(childPage.HighKey())

		// Check if entry fits
		cellSize := InternalCellSize(len(separatorKey))
		if currentParent.FreeSpace() < cellSize+2 {
			// Flush current parent and start new one
			if err := flushParent(); err != nil {
				return nil, err
			}
			newParent()
			// This child becomes child0 of the new parent
			currentParent.SetChild0(childPID)
			lastChildHighKey = cloneBytes(childPage.HighKey())
			continue
		}

		pos := currentParent.Count()
		if err := currentParent.InsertInternalEntry(pos, separatorKey, childPID); err != nil {
			return nil, err
		}
	}

	// Flush the last parent — rightmost has HighKey=nil (+∞)
	if err := flushParent(); err != nil {
		return nil, err
	}

	// Set up B-link pointers for internal nodes
	for i := 0; i < len(parentPIDs)-1; i++ {
		parent, err := bl.tree.pages.ReadPage(parentPIDs[i])
		if err != nil {
			return nil, err
		}
		parent.SetNext(parentPIDs[i+1])
		if err := bl.tree.pages.WritePage(parentPIDs[i], parent); err != nil {
			return nil, err
		}
	}

	return parentPIDs, nil
}

func (bl *BulkLoader) Close() error {
	bl.entries = nil
	bl.done = true
	return nil
}

func (bl *BulkLoader) EntryCount() int {
	return len(bl.entries)
}
