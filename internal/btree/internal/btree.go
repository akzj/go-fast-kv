package internal

import (
	"bytes"
	"sync"
	"sync/atomic"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/lock"
)

type bTree struct {
	pages       btreeapi.PageProvider
	blobs       btreeapi.BlobWriter
	serializer  btreeapi.NodeSerializer
	rootPageID  atomic.Uint64        // atomic for concurrent reads (§3.8.8)
	inlineThres int
	closed      atomic.Bool          // atomic for concurrent close check
	pageLocks   *lock.PageRWLocks    // per-page RwLock manager (§3.8.1)
	bootstrapMu sync.Mutex           // protects root creation when rootPageID == 0
	visCheck    func(txnMin, txnMax, readTxnID uint64) bool // MVCC visibility via CLOG (nil = default range check)
}

// New creates a new BTree instance.
func New(cfg btreeapi.Config, pages btreeapi.PageProvider, blobs btreeapi.BlobWriter) btreeapi.BTree {
	thresh := cfg.InlineThreshold
	if thresh <= 0 {
		thresh = btreeapi.InlineThreshold
	}
	return &bTree{
		pages:          pages,
		blobs:          blobs,
		serializer:     NewNodeSerializer(),
		inlineThres:    thresh,
		pageLocks:      lock.New(),
		visCheck:       cfg.VisibilityChecker,
	}
}

// RootPageID returns the current root node's PageID.
func (t *bTree) RootPageID() uint64 { return t.rootPageID.Load() }

// SetRootPageID sets the root node's PageID.
func (t *bTree) SetRootPageID(pid uint64) { t.rootPageID.Store(pid) }

// Close releases resources.
func (t *bTree) Close() error {
	t.closed.Store(true)
	return nil
}

// PageLocks returns the per-page RwLock manager used by this B-tree.
// This is needed by the Vacuum process to acquire the same page locks
// as Put/Delete/Get/Scan, preventing concurrent corruption during
// leaf page rewrites.
func (t *bTree) PageLocks() *lock.PageRWLocks {
	return t.pageLocks
}

// NewBulkLoader creates a new BulkLoader for efficient bulk loading.
// Entries should be sorted by key before calling Build(), or the loader
// will sort them automatically.
func (t *bTree) NewBulkLoader(mode btreeapi.BulkMode) btreeapi.BulkLoader {
	return newBulkLoader(t, mode, 0)
}

// NewBulkLoaderWithTxn creates a BulkLoader with an explicit transaction ID
// for MVCC mode. All loaded entries will have the given TxnMin.
func (t *bTree) NewBulkLoaderWithTxn(mode btreeapi.BulkMode, txnID uint64) btreeapi.BulkLoader {
	return newBulkLoader(t, mode, txnID)
}

// isVisible checks if a version (txnMin, txnMax) is visible.
// If a VisibilityChecker is configured (CLOG-based), it delegates to that.
// Otherwise, falls back to the default range check for backward compatibility.
func (t *bTree) isVisible(txnMin, txnMax, txnID uint64) bool {
	if t.visCheck != nil {
		return t.visCheck(txnMin, txnMax, txnID)
	}
	return txnMin <= txnID && txnMax > txnID
}

// ─── Put (§3.8.3) ──────────────────────────────────────────────────

func (t *bTree) Put(key, value []byte, txnID uint64) error {
	if t.closed.Load() {
		return btreeapi.ErrClosed
	}
	if len(key) > btreeapi.MaxKeySize {
		return btreeapi.ErrKeyTooLarge
	}

	// Bootstrap: create root leaf if empty (synchronized)
	if t.rootPageID.Load() == 0 {
		t.bootstrapMu.Lock()
		if t.rootPageID.Load() == 0 { // double-check under lock
			pid := t.pages.AllocPage()
			root := &btreeapi.Node{IsLeaf: true}
			if err := t.pages.WritePage(pid, root); err != nil {
				t.bootstrapMu.Unlock()
				return err
			}
			t.rootPageID.Store(pid)
		}
		t.bootstrapMu.Unlock()
	}

	// Phase 1: Search down to leaf (read locks only), record path
	path, err := t.searchPath(key)
	if err != nil {
		return err
	}

	// Phase 2: Write-lock the leaf
	leafPID := path[len(path)-1]
	t.pageLocks.WLock(leafPID)
	leaf, err := t.pages.ReadPage(leafPID)
	if err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}

	// B-link correction under write lock: the leaf may have split since we searched
	for leaf.HighKey != nil && bytes.Compare(key, leaf.HighKey) >= 0 && leaf.Next != 0 {
		nextPID := leaf.Next
		t.pageLocks.WUnlock(leafPID)
		leafPID = nextPID
		t.pageLocks.WLock(leafPID)
		leaf, err = t.pages.ReadPage(leafPID)
		if err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
	}

	// Phase 3: MVCC insert
	t.mvccInsert(leaf, key, value, txnID)
	leaf.Count = uint16(len(leaf.Entries))

	// Check if split needed
	if t.serializer.SerializedSize(leaf) <= btreeapi.PageSize {
		if err := t.pages.WritePage(leafPID, leaf); err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
		t.pageLocks.WUnlock(leafPID)
		return nil
	}

	// Phase 4: Split the leaf (still under write lock)
	splitKey, right := t.splitNode(leaf)
	rightPID := t.pages.AllocPage()

	// Set up B-link pointers
	right.Next = leaf.Next
	leaf.Next = rightPID
	right.HighKey = cloneBytes(leaf.HighKey) // right inherits original HighKey
	leaf.HighKey = cloneBytes(splitKey)       // left gets splitKey as HighKey

	if err := t.pages.WritePage(rightPID, right); err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}
	if err := t.pages.WritePage(leafPID, leaf); err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}
	t.pageLocks.WUnlock(leafPID)

	// Phase 5: Propagate split upward (each level locks independently)
	return t.propagateSplit(path[:len(path)-1], splitKey, rightPID)
}

func (t *bTree) mvccInsert(leaf *btreeapi.Node, key, value []byte, txnID uint64) {
	// Mark the current visible version as superseded by this transaction.
	// The page write lock guarantees only one writer modifies the leaf at a time,
	// so there is at most one entry with TxnMax == TxnMaxInfinity for a given key.
	//
	// Note: we do NOT filter by e.TxnMin <= txnID. When concurrent writers race
	// for the same leaf, a higher-txnID writer may acquire the lock first. If a
	// lower-txnID writer then acquires the lock, it must still mark the existing
	// entry as superseded — otherwise both entries retain TxnMax=∞ and vacuum
	// can never reclaim the "losing" version. The MVCC visibility rules and
	// abort-restoration in vacuum handle all commit/abort orderings correctly.
	for i := range leaf.Entries {
		e := &leaf.Entries[i]
		if bytes.Equal(e.Key, key) && e.TxnMax == btreeapi.TxnMaxInfinity {
			e.TxnMax = txnID
			break
		}
	}

	// Build new entry
	entry := btreeapi.LeafEntry{
		Key:    cloneBytes(key),
		TxnMin: txnID,
		TxnMax: btreeapi.TxnMaxInfinity,
	}
	if t.blobs != nil && len(value) > t.inlineThres {
		blobID, err := t.blobs.WriteBlob(value)
		if err == nil {
			entry.Value.BlobID = blobID
		} else {
			entry.Value.Inline = cloneBytes(value)
		}
	} else {
		entry.Value.Inline = cloneBytes(value)
	}

	// Insert maintaining (Key ASC, TxnMin DESC) order
	pos := t.findInsertPos(leaf, key, txnID)
	leaf.Entries = append(leaf.Entries, btreeapi.LeafEntry{})
	copy(leaf.Entries[pos+1:], leaf.Entries[pos:])
	leaf.Entries[pos] = entry
}

func (t *bTree) findInsertPos(leaf *btreeapi.Node, key []byte, txnMin uint64) int {
	for i, e := range leaf.Entries {
		cmp := bytes.Compare(key, e.Key)
		if cmp < 0 {
			return i
		}
		if cmp == 0 && txnMin > e.TxnMin {
			return i
		}
	}
	return len(leaf.Entries)
}

// ─── Search (§3.8.2 search phase) ──────────────────────────────────

// searchPath traverses from root to the target leaf using read locks,
// returning the path of all node PageIDs visited (internal + leaf).
// Each node is read-locked, then unlocked before moving to the next.
func (t *bTree) searchPath(key []byte) (path []uint64, err error) {
	currentPID := t.rootPageID.Load()
	for {
		t.pageLocks.RLock(currentPID)
		node, err := t.pages.ReadPage(currentPID)
		if err != nil {
			t.pageLocks.RUnlock(currentPID)
			return nil, err
		}

		// B-link right-link correction
		if node.HighKey != nil && bytes.Compare(key, node.HighKey) >= 0 && node.Next != 0 {
			nextPID := node.Next
			t.pageLocks.RUnlock(currentPID)
			currentPID = nextPID
			continue
		}

		path = append(path, currentPID)

		if node.IsLeaf {
			t.pageLocks.RUnlock(currentPID)
			return path, nil
		}

		childPID := findChild(node, key)
		t.pageLocks.RUnlock(currentPID)
		currentPID = childPID
	}
}

func findChild(node *btreeapi.Node, key []byte) uint64 {
	for i, k := range node.Keys {
		if bytes.Compare(key, k) < 0 {
			return node.Children[i]
		}
	}
	return node.Children[len(node.Children)-1]
}

// ─── Split propagation (§3.8.4) ────────────────────────────────────

func (t *bTree) propagateSplit(path []uint64, splitKey []byte, newChildPID uint64) error {
	for i := len(path) - 1; i >= 0; i-- {
		parentPID := path[i]
		t.pageLocks.WLock(parentPID)
		parent, err := t.pages.ReadPage(parentPID)
		if err != nil {
			t.pageLocks.WUnlock(parentPID)
			return err
		}

		// B-link correction: parent may have been split concurrently
		for parent.HighKey != nil && bytes.Compare(splitKey, parent.HighKey) >= 0 && parent.Next != 0 {
			nextPID := parent.Next
			t.pageLocks.WUnlock(parentPID)
			parentPID = nextPID
			t.pageLocks.WLock(parentPID)
			parent, err = t.pages.ReadPage(parentPID)
			if err != nil {
				t.pageLocks.WUnlock(parentPID)
				return err
			}
		}

		insertInternalEntry(parent, splitKey, newChildPID)
		parent.Count = uint16(len(parent.Keys))

		if t.serializer.SerializedSize(parent) <= btreeapi.PageSize {
			if err := t.pages.WritePage(parentPID, parent); err != nil {
				t.pageLocks.WUnlock(parentPID)
				return err
			}
			t.pageLocks.WUnlock(parentPID)
			return nil // Done — no further propagation needed
		}

		// Parent also needs to split
		newSplitKey, right := t.splitInternalNode(parent)
		newParentPID := t.pages.AllocPage()
		right.Next = parent.Next
		parent.Next = newParentPID
		right.HighKey = cloneBytes(parent.HighKey)
		parent.HighKey = cloneBytes(newSplitKey)

		if err := t.pages.WritePage(newParentPID, right); err != nil {
			t.pageLocks.WUnlock(parentPID)
			return err
		}
		if err := t.pages.WritePage(parentPID, parent); err != nil {
			t.pageLocks.WUnlock(parentPID)
			return err
		}
		t.pageLocks.WUnlock(parentPID)

		splitKey = newSplitKey
		newChildPID = newParentPID
	}

	// Reached root and still need to split → create new root
	// Serialize root creation to prevent two concurrent propagateSplits
	// from both creating a new root (one would be lost).
	t.bootstrapMu.Lock()
	newRoot := &btreeapi.Node{
		IsLeaf:   false,
		Count:    1,
		Keys:     [][]byte{cloneBytes(splitKey)},
		Children: []uint64{t.rootPageID.Load(), newChildPID},
	}
	newRootPID := t.pages.AllocPage()
	if err := t.pages.WritePage(newRootPID, newRoot); err != nil {
		t.bootstrapMu.Unlock()
		return err
	}
	t.rootPageID.Store(newRootPID)
	t.bootstrapMu.Unlock()
	return nil
}

func (t *bTree) splitNode(node *btreeapi.Node) (splitKey []byte, right *btreeapi.Node) {
	if node.IsLeaf {
		return t.splitLeafNode(node)
	}
	return t.splitInternalNode(node)
}

func (t *bTree) splitLeafNode(node *btreeapi.Node) (splitKey []byte, right *btreeapi.Node) {
	entries := node.Entries
	mid := len(entries) / 2

	// Don't split in the middle of a version chain — find a key boundary
	origMid := mid
	for mid < len(entries)-1 && bytes.Equal(entries[mid].Key, entries[mid-1].Key) {
		mid++
	}
	// If we went all the way to the end, try going left
	if mid >= len(entries)-1 {
		mid = origMid
		for mid > 1 && bytes.Equal(entries[mid].Key, entries[mid-1].Key) {
			mid--
		}
	}

	// All entries share the same key (e.g., many MVCC versions of one key).
	// Using the actual key as splitKey would violate the exclusive HighKey
	// invariant: LEFT would contain key "K" but have HighKey = "K", making
	// the latest version unreachable. Use a synthetic splitKey = key + 0x00
	// so that HighKey > actual key, preserving correct routing.
	allSameKey := mid <= 1 && len(entries) > 1 &&
		bytes.Equal(entries[0].Key, entries[len(entries)-1].Key)

	if allSameKey {
		mid = len(entries) / 2
		splitKey = append(cloneBytes(entries[0].Key), 0x00)
	} else {
		splitKey = cloneBytes(entries[mid].Key)
	}

	right = &btreeapi.Node{
		IsLeaf:  true,
		Entries: cloneLeafEntries(entries[mid:]),
		Count:   uint16(len(entries) - mid),
	}
	node.Entries = entries[:mid]
	node.Count = uint16(mid)
	return splitKey, right
}

func (t *bTree) splitInternalNode(node *btreeapi.Node) (splitKey []byte, right *btreeapi.Node) {
	mid := len(node.Keys) / 2
	splitKey = cloneBytes(node.Keys[mid])

	right = &btreeapi.Node{
		IsLeaf:   false,
		Keys:     cloneBytesSlice(node.Keys[mid+1:]),
		Children: cloneUint64Slice(node.Children[mid+1:]),
		Count:    uint16(len(node.Keys) - mid - 1),
	}
	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]
	node.Count = uint16(mid)
	return splitKey, right
}

func insertInternalEntry(node *btreeapi.Node, key []byte, childPID uint64) {
	pos := 0
	for pos < len(node.Keys) && bytes.Compare(key, node.Keys[pos]) > 0 {
		pos++
	}
	node.Keys = append(node.Keys, nil)
	copy(node.Keys[pos+1:], node.Keys[pos:])
	node.Keys[pos] = cloneBytes(key)

	node.Children = append(node.Children, 0)
	copy(node.Children[pos+2:], node.Children[pos+1:])
	node.Children[pos+1] = childPID
}

// ─── Get (§3.8.2) ──────────────────────────────────────────────────

func (t *bTree) Get(key []byte, txnID uint64) ([]byte, error) {
	if t.closed.Load() {
		return nil, btreeapi.ErrClosed
	}

	currentPID := t.rootPageID.Load()
	if currentPID == 0 {
		return nil, btreeapi.ErrKeyNotFound
	}

	for {
		t.pageLocks.RLock(currentPID)
		node, err := t.pages.ReadPage(currentPID)
		if err != nil {
			t.pageLocks.RUnlock(currentPID)
			return nil, err
		}

		// B-link correction: if key >= HighKey, follow right-link
		if node.HighKey != nil && bytes.Compare(key, node.HighKey) >= 0 && node.Next != 0 {
			nextPID := node.Next
			t.pageLocks.RUnlock(currentPID)
			currentPID = nextPID
			continue
		}

		if node.IsLeaf {
			// Find visible entry
			for i := range node.Entries {
				e := &node.Entries[i]
				cmp := bytes.Compare(e.Key, key)
				if cmp > 0 {
					break
				}
				if cmp == 0 && t.isVisible(e.TxnMin, e.TxnMax, txnID) {
					t.pageLocks.RUnlock(currentPID)
					return t.resolveValue(&e.Value)
				}
			}
			t.pageLocks.RUnlock(currentPID)
			return nil, btreeapi.ErrKeyNotFound
		}

		// Internal node: descend to child
		childPID := findChild(node, key)
		t.pageLocks.RUnlock(currentPID)
		currentPID = childPID
	}
}

func (t *bTree) resolveValue(v *btreeapi.Value) ([]byte, error) {
	if v.BlobID > 0 && t.blobs != nil {
		return t.blobs.ReadBlob(v.BlobID)
	}
	return cloneBytes(v.Inline), nil
}

// ─── Delete (§3.8.5) ───────────────────────────────────────────────

func (t *bTree) Delete(key []byte, txnID uint64) error {
	if t.closed.Load() {
		return btreeapi.ErrClosed
	}
	if t.rootPageID.Load() == 0 {
		return btreeapi.ErrKeyNotFound
	}

	// Phase 1: Search down to leaf (read locks only)
	path, err := t.searchPath(key)
	if err != nil {
		return err
	}

	// Phase 2: Write-lock the leaf
	leafPID := path[len(path)-1]
	t.pageLocks.WLock(leafPID)
	leaf, err := t.pages.ReadPage(leafPID)
	if err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}

	// B-link correction under write lock
	for leaf.HighKey != nil && bytes.Compare(key, leaf.HighKey) >= 0 && leaf.Next != 0 {
		nextPID := leaf.Next
		t.pageLocks.WUnlock(leafPID)
		leafPID = nextPID
		t.pageLocks.WLock(leafPID)
		leaf, err = t.pages.ReadPage(leafPID)
		if err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
	}

	// Phase 3: MVCC delete (mark TxnMax)
	// Two cases:
	// 1. Our own entry (txnMin == txnID): always find it, even if self-deleted.
	//    This enables SQL rollback via self-delete (txnMax=txnXID → invisible).
	// 2. Other entries: must be visible via normal MVCC rules.
	for i := range leaf.Entries {
		e := &leaf.Entries[i]
		isOwn := e.TxnMin == txnID
		if bytes.Equal(e.Key, key) && (isOwn || t.isVisible(e.TxnMin, e.TxnMax, txnID)) {
			e.TxnMax = txnID
			if err := t.pages.WritePage(leafPID, leaf); err != nil {
				t.pageLocks.WUnlock(leafPID)
				return err
			}
			t.pageLocks.WUnlock(leafPID)
			return nil
		}
	}
	t.pageLocks.WUnlock(leafPID)
	return btreeapi.ErrKeyNotFound
}

// ─── Scan (§3.8.6) ─────────────────────────────────────────────────

func (t *bTree) Scan(start, end []byte, txnID uint64) btreeapi.Iterator {
	it := &iterator{
		tree:     t,
		endKey:   end,
		txnID:    txnID,
		visCheck: t.visCheck,
	}
	if t.closed.Load() || t.rootPageID.Load() == 0 {
		it.done = true
		return it
	}

	// Find the starting leaf using read locks
	currentPID := t.rootPageID.Load()
	for {
		t.pageLocks.RLock(currentPID)
		node, err := t.pages.ReadPage(currentPID)
		if err != nil {
			t.pageLocks.RUnlock(currentPID)
			it.err = err
			it.done = true
			return it
		}

		// B-link correction
		if node.HighKey != nil && bytes.Compare(start, node.HighKey) >= 0 && node.Next != 0 {
			nextPID := node.Next
			t.pageLocks.RUnlock(currentPID)
			currentPID = nextPID
			continue
		}

		if node.IsLeaf {
			// Copy entries while holding the read lock, then release
			it.curNode = cloneNode(node)
			t.pageLocks.RUnlock(currentPID)
			it.curIdx = 0

			// Advance curIdx to the first entry >= start
			for it.curIdx < len(it.curNode.Entries) {
				if bytes.Compare(it.curNode.Entries[it.curIdx].Key, start) >= 0 {
					break
				}
				it.curIdx++
			}
			return it
		}

		childPID := findChild(node, start)
		t.pageLocks.RUnlock(currentPID)
		currentPID = childPID
	}
}

// cloneNode creates a deep copy of a node so the caller doesn't hold
// a reference to page data that might change. Handles both leaf and
// internal nodes.
func cloneNode(node *btreeapi.Node) *btreeapi.Node {
	clone := &btreeapi.Node{
		IsLeaf:  node.IsLeaf,
		Count:   node.Count,
		HighKey: cloneBytes(node.HighKey),
		Next:    node.Next,
	}
	if node.IsLeaf {
		clone.Entries = cloneLeafEntries(node.Entries)
	} else {
		if len(node.Keys) > 0 {
			clone.Keys = cloneBytesSlice(node.Keys)
		}
		if len(node.Children) > 0 {
			clone.Children = cloneUint64Slice(node.Children)
		}
	}
	return clone
}

type iterator struct {
	tree     *bTree
	endKey   []byte
	txnID    uint64
	visCheck func(txnMin, txnMax, readTxnID uint64) bool // snapshot-based visibility (nil = use tree default)
	curNode  *btreeapi.Node
	curIdx   int
	curKey   []byte
	curValue []byte
	lastKey  []byte
	err      error
	done     bool
}

func (it *iterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	for {
		// Move to next leaf if needed
		for it.curIdx >= len(it.curNode.Entries) {
			if it.curNode.Next == 0 {
				it.done = true
				return false
			}
			// Read next leaf with read lock, clone, release
			nextPID := it.curNode.Next
			it.tree.pageLocks.RLock(nextPID)
			node, err := it.tree.pages.ReadPage(nextPID)
			if err != nil {
				it.tree.pageLocks.RUnlock(nextPID)
				it.err = err
				return false
			}
			it.curNode = cloneNode(node)
			it.tree.pageLocks.RUnlock(nextPID)
			it.curIdx = 0
		}

		e := &it.curNode.Entries[it.curIdx]
		it.curIdx++

		// Check end boundary
		if it.endKey != nil && bytes.Compare(e.Key, it.endKey) >= 0 {
			it.done = true
			return false
		}

		// Skip duplicate keys (dedup: only first visible version per key)
		if it.lastKey != nil && bytes.Equal(e.Key, it.lastKey) {
			continue
		}

		// Visibility check (uses per-scan snapshot if available)
		visible := false
		if it.visCheck != nil {
			visible = it.visCheck(e.TxnMin, e.TxnMax, it.txnID)
		} else {
			visible = it.tree.isVisible(e.TxnMin, e.TxnMax, it.txnID)
		}
		if visible {
			val, err := it.tree.resolveValue(&e.Value)
			if err != nil {
				it.err = err
				return false
			}
			it.curKey = e.Key
			it.curValue = val
			it.lastKey = e.Key
			return true
		}
	}
}

func (it *iterator) Key() []byte   { return it.curKey }
func (it *iterator) Value() []byte { return it.curValue }
func (it *iterator) Err() error    { return it.err }
func (it *iterator) Close()        {}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func cloneLeafEntries(entries []btreeapi.LeafEntry) []btreeapi.LeafEntry {
	out := make([]btreeapi.LeafEntry, len(entries))
	for i, e := range entries {
		out[i] = btreeapi.LeafEntry{
			Key:    cloneBytes(e.Key),
			TxnMin: e.TxnMin,
			TxnMax: e.TxnMax,
			Value: btreeapi.Value{
				Inline: cloneBytes(e.Value.Inline),
				BlobID: e.Value.BlobID,
			},
		}
	}
	return out
}

func cloneBytesSlice(s [][]byte) [][]byte {
	out := make([][]byte, len(s))
	for i, b := range s {
		out[i] = cloneBytes(b)
	}
	return out
}

func cloneUint64Slice(s []uint64) []uint64 {
	out := make([]uint64, len(s))
	copy(out, s)
	return out
}
