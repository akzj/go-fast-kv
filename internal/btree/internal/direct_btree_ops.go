package internal

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// DirectBTree implements B-tree operations directly on raw page bytes.
// It avoids the Node ↔ []byte conversion overhead entirely.
type DirectBTree struct {
	pages      *DirectMemPageProvider
	serializer btreeapi.NodeSerializer
	rootPID    atomic.Uint64
	pageSize   int
	mu         sync.Mutex
}

// NewDirectBTree creates a new DirectBTree with in-memory storage.
func NewDirectBTree() *DirectBTree {
	return &DirectBTree{
		pages:      NewDirectMemPageProvider(),
		serializer: NewNodeSerializer(),
		pageSize:   btreeapi.PageSize,
	}
}

// RootPageID returns the current root PageID.
func (t *DirectBTree) RootPageID() uint64 { return t.rootPID.Load() }

// SetRootPageID sets the root PageID.
func (t *DirectBTree) SetRootPageID(pid uint64) { t.rootPID.Store(pid) }

// Bootstrap creates the root leaf node if needed.
func (t *DirectBTree) Bootstrap() error {
	if t.rootPID.Load() != 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rootPID.Load() != 0 {
		return nil
	}
	root := &btreeapi.Node{IsLeaf: true}
	data, err := t.serializer.Serialize(root)
	if err != nil {
		return err
	}
	pid := t.pages.AllocPage()
	if err := t.pages.WritePage(pid, data); err != nil {
		return err
	}
	t.rootPID.Store(pid)
	return nil
}

// Put inserts a key-value pair directly into the page bytes.
func (t *DirectBTree) Put(key, value []byte, txnID uint64) error {
	if t.rootPID.Load() == 0 {
		if err := t.Bootstrap(); err != nil {
			return err
		}
	}

	// Search to leaf
	path, err := t.searchPath(key)
	if err != nil {
		return err
	}

	leafPID := path[len(path)-1]
	t.mu.Lock()
	defer t.mu.Unlock()

	// Read leaf
	data, err := t.pages.ReadPage(leafPID)
	if err != nil || data == nil {
		return fmt.Errorf("leaf not found: %d", leafPID)
	}

	leaf, err := t.serializer.Deserialize(data)
	if err != nil {
		return err
	}

	// B-link correction
	for leaf.HighKey != nil && bytes.Compare(key, leaf.HighKey) >= 0 && leaf.Next != 0 {
		leafPID = leaf.Next
		data, err = t.pages.ReadPage(leafPID)
		if err != nil || data == nil {
			return fmt.Errorf("leaf not found during correction: %d", leafPID)
		}
		leaf, err = t.serializer.Deserialize(data)
		if err != nil {
			return err
		}
	}

	// Insert
	t.mvccInsert(leaf, key, value, txnID)
	leaf.Count = uint16(len(leaf.Entries))

	// Serialize and write
	newData, err := t.serializer.Serialize(leaf)
	if err != nil {
		return err
	}
	if len(newData) <= btreeapi.PageSize {
		return t.pages.WritePage(leafPID, newData)
	}

	// Split needed
	splitKey, right := t.splitLeafNode(leaf)
	rightPID := t.pages.AllocPage()
	right.Next = leaf.Next
	leaf.Next = rightPID
	leaf.HighKey = cloneBytes(splitKey)
	right.HighKey = cloneBytes(leaf.HighKey)

	rightData, err := t.serializer.Serialize(right)
	if err != nil {
		return err
	}
	newData, err = t.serializer.Serialize(leaf)
	if err != nil {
		return err
	}
	if err := t.pages.WritePage(rightPID, rightData); err != nil {
		return err
	}
	if err := t.pages.WritePage(leafPID, newData); err != nil {
		return err
	}

	// Propagate split
	return t.propagateSplit(path[:len(path)-1], splitKey, rightPID)
}

// mvccInsert inserts an entry into the leaf node.
func (t *DirectBTree) mvccInsert(leaf *btreeapi.Node, key, value []byte, txnID uint64) {
	// Mark existing visible version as superseded
	for i := range leaf.Entries {
		e := &leaf.Entries[i]
		if bytes.Equal(e.Key, key) && e.TxnMax == btreeapi.TxnMaxInfinity {
			e.TxnMax = txnID
			break
		}
	}

	// Insert new entry
	entry := btreeapi.LeafEntry{
		Key:    cloneBytes(key),
		TxnMin: txnID,
		TxnMax: btreeapi.TxnMaxInfinity,
		Value: btreeapi.Value{
			Inline: cloneBytes(value),
		},
	}

	// Find insert position
	pos := len(leaf.Entries)
	for i, e := range leaf.Entries {
		cmp := bytes.Compare(key, e.Key)
		if cmp < 0 {
			pos = i
			break
		}
		if cmp == 0 && txnID > e.TxnMin {
			pos = i
			break
		}
	}

	leaf.Entries = append(leaf.Entries, btreeapi.LeafEntry{})
	copy(leaf.Entries[pos+1:], leaf.Entries[pos:])
	leaf.Entries[pos] = entry
}

// searchPath searches from root to leaf, returning path of PIDs.
func (t *DirectBTree) searchPath(key []byte) ([]uint64, error) {
	path := make([]uint64, 0, 4)
	current := t.rootPID.Load()
	for {
		if current == 0 {
			break
		}
		data, err := t.pages.ReadPage(current)
		if err != nil || data == nil {
			return nil, fmt.Errorf("page not found: %d", current)
		}
		node, err := t.serializer.Deserialize(data)
		if err != nil {
			return nil, err
		}
		path = append(path, current)

		// B-link correction
		if node.HighKey != nil && bytes.Compare(key, node.HighKey) >= 0 && node.Next != 0 {
			current = node.Next
			continue
		}

		if node.IsLeaf {
			return path, nil
		}

		// Internal: find child
		child := findChild(node, key)
		current = child
	}
	return path, nil
}

// Get retrieves a value by key.
func (t *DirectBTree) Get(key []byte, txnID uint64) ([]byte, error) {
	if t.rootPID.Load() == 0 {
		return nil, btreeapi.ErrKeyNotFound
	}

	current := t.rootPID.Load()
	for {
		data, err := t.pages.ReadPage(current)
		if err != nil || data == nil {
			return nil, btreeapi.ErrKeyNotFound
		}
		node, err := t.serializer.Deserialize(data)
		if err != nil {
			return nil, err
		}

		// B-link correction
		if node.HighKey != nil && bytes.Compare(key, node.HighKey) >= 0 && node.Next != 0 {
			current = node.Next
			continue
		}

		if node.IsLeaf {
			for i := range node.Entries {
				e := &node.Entries[i]
				cmp := bytes.Compare(e.Key, key)
				if cmp > 0 {
					break
				}
				if cmp == 0 && t.isVisible(e.TxnMin, e.TxnMax, txnID) {
					return cloneBytes(e.Value.Inline), nil
				}
			}
			return nil, btreeapi.ErrKeyNotFound
		}

		child := findChild(node, key)
		current = child
	}
}

// isVisible checks MVCC visibility.
func (t *DirectBTree) isVisible(txnMin, txnMax, txnID uint64) bool {
	return txnMin <= txnID && txnMax > txnID
}

// splitLeafNode splits a leaf node and returns (splitKey, rightNode).
func (t *DirectBTree) splitLeafNode(node *btreeapi.Node) ([]byte, *btreeapi.Node) {
	entries := node.Entries
	mid := len(entries) / 2

	// Find key boundary (avoid splitting same-key versions)
	origMid := mid
	for mid < len(entries)-1 && bytes.Equal(entries[mid].Key, entries[mid-1].Key) {
		mid++
	}
	if mid >= len(entries)-1 {
		mid = origMid
		for mid > 1 && bytes.Equal(entries[mid].Key, entries[mid-1].Key) {
			mid--
		}
	}

	splitKey := cloneBytes(entries[mid].Key)
	right := &btreeapi.Node{
		IsLeaf:  true,
		Entries: cloneLeafEntries(entries[mid:]),
		Count:   uint16(len(entries) - mid),
	}
	node.Entries = entries[:mid]
	node.Count = uint16(mid)
	return splitKey, right
}

// propagateSplit propagates a split up the tree.
func (t *DirectBTree) propagateSplit(path []uint64, splitKey []byte, newChildPID uint64) error {
	for i := len(path) - 1; i >= 0; i-- {
		parentPID := path[i]
		data, err := t.pages.ReadPage(parentPID)
		if err != nil || data == nil {
			return fmt.Errorf("parent not found: %d", parentPID)
		}
		parent, err := t.serializer.Deserialize(data)
		if err != nil {
			return err
		}

		// Insert into parent
		insertInternalEntry(parent, splitKey, newChildPID)
		parent.Count = uint16(len(parent.Keys))

		newData, err := t.serializer.Serialize(parent)
		if err != nil {
			return err
		}
		if len(newData) <= btreeapi.PageSize {
			return t.pages.WritePage(parentPID, newData)
		}

		// Parent also needs split
		newSplitKey, right := t.splitInternalNode(parent)
		newParentPID := t.pages.AllocPage()
		right.Next = parent.Next
		parent.Next = newParentPID
		right.HighKey = cloneBytes(parent.HighKey)
		parent.HighKey = cloneBytes(newSplitKey)

		rightData, err := t.serializer.Serialize(right)
		if err != nil {
			return err
		}
		newData, err = t.serializer.Serialize(parent)
		if err != nil {
			return err
		}
		if err := t.pages.WritePage(newParentPID, rightData); err != nil {
			return err
		}
		if err := t.pages.WritePage(parentPID, newData); err != nil {
			return err
		}

		splitKey = newSplitKey
		newChildPID = newParentPID
	}

	// Create new root
	t.mu.Lock()
	defer t.mu.Unlock()
	newRoot := &btreeapi.Node{
		IsLeaf:   false,
		Count:    1,
		Keys:     [][]byte{cloneBytes(splitKey)},
		Children: []uint64{t.rootPID.Load(), newChildPID},
	}
	newPID := t.pages.AllocPage()
	newData, err := t.serializer.Serialize(newRoot)
	if err != nil {
		return err
	}
	if err := t.pages.WritePage(newPID, newData); err != nil {
		return err
	}
	t.rootPID.Store(newPID)
	return nil
}

// splitInternalNode splits an internal node.
func (t *DirectBTree) splitInternalNode(node *btreeapi.Node) ([]byte, *btreeapi.Node) {
	mid := len(node.Keys) / 2
	splitKey := cloneBytes(node.Keys[mid])
	right := &btreeapi.Node{
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
