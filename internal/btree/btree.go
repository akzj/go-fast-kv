package btree

import (
	"bytes"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

type bTree struct {
	pages       btreeapi.PageProvider
	blobs       btreeapi.BlobWriter
	serializer  btreeapi.NodeSerializer
	rootPageID  uint64
	inlineThres int
	closed      bool
}

// New creates a new BTree instance.
func New(cfg btreeapi.Config, pages btreeapi.PageProvider, blobs btreeapi.BlobWriter) btreeapi.BTree {
	thresh := cfg.InlineThreshold
	if thresh <= 0 {
		thresh = btreeapi.InlineThreshold
	}
	return &bTree{
		pages:       pages,
		blobs:       blobs,
		serializer:  NewNodeSerializer(),
		inlineThres: thresh,
	}
}

// RootPageID returns the current root node's PageID.
func (t *bTree) RootPageID() uint64 { return t.rootPageID }

// SetRootPageID sets the root node's PageID.
func (t *bTree) SetRootPageID(pid uint64) { t.rootPageID = pid }

// Close releases resources.
func (t *bTree) Close() error {
	t.closed = true
	return nil
}

// ─── Put ────────────────────────────────────────────────────────────

func (t *bTree) Put(key, value []byte, txnID uint64) error {
	if t.closed {
		return btreeapi.ErrClosed
	}
	if len(key) > btreeapi.MaxKeySize {
		return btreeapi.ErrKeyTooLarge
	}

	// Bootstrap: create root leaf if empty
	if t.rootPageID == 0 {
		pid := t.pages.AllocPage()
		root := &btreeapi.Node{IsLeaf: true}
		if err := t.pages.WritePage(pid, root); err != nil {
			return err
		}
		t.rootPageID = pid
	}

	// Search down to leaf, recording path
	path, leaf, leafPID, err := t.searchLeaf(key)
	if err != nil {
		return err
	}

	// MVCC insert: mark old visible version, insert new entry
	t.mvccInsert(leaf, key, value, txnID)
	leaf.Count = uint16(len(leaf.Entries))

	// Check if split needed
	if t.serializer.SerializedSize(leaf) <= btreeapi.PageSize {
		return t.pages.WritePage(leafPID, leaf)
	}

	// Split and propagate
	return t.splitAndPropagate(leaf, leafPID, path)
}

func (t *bTree) mvccInsert(leaf *btreeapi.Node, key, value []byte, txnID uint64) {
	// Mark old visible version
	for i := range leaf.Entries {
		e := &leaf.Entries[i]
		if bytes.Equal(e.Key, key) && e.TxnMax == btreeapi.TxnMaxInfinity && e.TxnMin <= txnID {
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

// ─── Search ─────────────────────────────────────────────────────────

// searchLeaf traverses from root to the target leaf, returning the path
// of internal node PageIDs, the leaf node, and the leaf's PageID.
func (t *bTree) searchLeaf(key []byte) (path []uint64, leaf *btreeapi.Node, leafPID uint64, err error) {
	pid := t.rootPageID
	for {
		node, err := t.pages.ReadPage(pid)
		if err != nil {
			return nil, nil, 0, err
		}
		// B-link right-link correction
		if node.HighKey != nil && bytes.Compare(key, node.HighKey) >= 0 && node.Next != 0 {
			pid = node.Next
			continue
		}
		if node.IsLeaf {
			return path, node, pid, nil
		}
		path = append(path, pid)
		pid = findChild(node, key)
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

// ─── Split ──────────────────────────────────────────────────────────

func (t *bTree) splitAndPropagate(node *btreeapi.Node, nodePID uint64, path []uint64) error {
	splitKey, right := t.splitNode(node)
	rightPID := t.pages.AllocPage()

	// Set up B-link pointers
	right.Next = node.Next
	right.HighKey = cloneBytes(node.HighKey) // right inherits original HighKey
	node.Next = rightPID
	node.HighKey = cloneBytes(splitKey) // left gets splitKey as HighKey

	if err := t.pages.WritePage(nodePID, node); err != nil {
		return err
	}
	if err := t.pages.WritePage(rightPID, right); err != nil {
		return err
	}

	// Propagate split upward
	for i := len(path) - 1; i >= 0; i-- {
		parentPID := path[i]
		parent, err := t.pages.ReadPage(parentPID)
		if err != nil {
			return err
		}
		// B-link correction on parent
		for parent.HighKey != nil && bytes.Compare(splitKey, parent.HighKey) >= 0 && parent.Next != 0 {
			parentPID = parent.Next
			parent, err = t.pages.ReadPage(parentPID)
			if err != nil {
				return err
			}
		}

		insertInternalEntry(parent, splitKey, rightPID)
		parent.Count = uint16(len(parent.Keys))

		if t.serializer.SerializedSize(parent) <= btreeapi.PageSize {
			return t.pages.WritePage(parentPID, parent)
		}

		// Parent also needs split
		splitKey, right = t.splitInternalNode(parent)
		rightPID = t.pages.AllocPage()
		right.Next = parent.Next
		right.HighKey = cloneBytes(parent.HighKey)
		parent.Next = rightPID
		parent.HighKey = cloneBytes(splitKey)

		if err := t.pages.WritePage(parentPID, parent); err != nil {
			return err
		}
		if err := t.pages.WritePage(rightPID, right); err != nil {
			return err
		}
	}

	// Need new root
	newRoot := &btreeapi.Node{
		IsLeaf:   false,
		Count:    1,
		Keys:     [][]byte{cloneBytes(splitKey)},
		Children: []uint64{t.rootPageID, rightPID},
	}
	newRootPID := t.pages.AllocPage()
	if err := t.pages.WritePage(newRootPID, newRoot); err != nil {
		return err
	}
	t.rootPageID = newRootPID
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

	// Don't split in the middle of a version chain
	for mid < len(entries)-1 && bytes.Equal(entries[mid].Key, entries[mid-1].Key) {
		mid++
	}
	// If we went all the way to the end, try going left
	if mid >= len(entries)-1 {
		mid = len(entries) / 2
		for mid > 1 && bytes.Equal(entries[mid].Key, entries[mid-1].Key) {
			mid--
		}
	}

	splitKey = cloneBytes(entries[mid].Key)
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

// ─── Get ────────────────────────────────────────────────────────────

func (t *bTree) Get(key []byte, txnID uint64) ([]byte, error) {
	if t.closed {
		return nil, btreeapi.ErrClosed
	}
	if t.rootPageID == 0 {
		return nil, btreeapi.ErrKeyNotFound
	}

	_, leaf, _, err := t.searchLeaf(key)
	if err != nil {
		return nil, err
	}

	for i := range leaf.Entries {
		e := &leaf.Entries[i]
		cmp := bytes.Compare(e.Key, key)
		if cmp > 0 {
			break
		}
		if cmp == 0 && e.TxnMin <= txnID && e.TxnMax > txnID {
			return t.resolveValue(&e.Value)
		}
	}
	return nil, btreeapi.ErrKeyNotFound
}

func (t *bTree) resolveValue(v *btreeapi.Value) ([]byte, error) {
	if v.BlobID > 0 && t.blobs != nil {
		return t.blobs.ReadBlob(v.BlobID)
	}
	return cloneBytes(v.Inline), nil
}

// ─── Delete ─────────────────────────────────────────────────────────

func (t *bTree) Delete(key []byte, txnID uint64) error {
	if t.closed {
		return btreeapi.ErrClosed
	}
	if t.rootPageID == 0 {
		return btreeapi.ErrKeyNotFound
	}

	_, leaf, leafPID, err := t.searchLeaf(key)
	if err != nil {
		return err
	}

	for i := range leaf.Entries {
		e := &leaf.Entries[i]
		if bytes.Equal(e.Key, key) && e.TxnMin <= txnID && e.TxnMax > txnID {
			e.TxnMax = txnID
			return t.pages.WritePage(leafPID, leaf)
		}
	}
	return btreeapi.ErrKeyNotFound
}

// ─── Scan ───────────────────────────────────────────────────────────

func (t *bTree) Scan(start, end []byte, txnID uint64) btreeapi.Iterator {
	it := &iterator{
		tree:   t,
		endKey: end,
		txnID:  txnID,
	}
	if t.closed || t.rootPageID == 0 {
		it.done = true
		return it
	}

	// Find the starting leaf
	_, leaf, _, err := t.searchLeaf(start)
	if err != nil {
		it.err = err
		it.done = true
		return it
	}
	it.curNode = leaf
	it.curIdx = 0

	// Advance curIdx to the first entry >= start
	for it.curIdx < len(leaf.Entries) {
		if bytes.Compare(leaf.Entries[it.curIdx].Key, start) >= 0 {
			break
		}
		it.curIdx++
	}

	return it
}

type iterator struct {
	tree     *bTree
	endKey   []byte
	txnID    uint64
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
			node, err := it.tree.pages.ReadPage(it.curNode.Next)
			if err != nil {
				it.err = err
				return false
			}
			it.curNode = node
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

		// Visibility check
		if e.TxnMin <= it.txnID && e.TxnMax > it.txnID {
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
