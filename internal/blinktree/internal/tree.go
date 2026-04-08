package internal

import (
	"bytes"
	"errors"
	"sync"
)

// tree implements the Tree interface using B-link tree algorithm.
// Key design: B-tree uses stable PageIDs for all child/sibling pointers.
// The underlying storage layer (PageStorage) maps PageID → VAddr internally.
// No CoW re-persist needed — PageID never changes.
// Keys are raw []byte, compared lexicographically with bytes.Compare.
type tree struct {
	nodeOps    NodeOperations
	nodeMgr    NodeManager
	latchMgr   LatchManager
	mu         sync.Mutex
	rootPageID PageID
	rootNode   *NodeFormat
	isClosed   bool
	isMutator  bool
}

func newTreeImpl(nodeOps NodeOperations, nodeMgr NodeManager, isMutator bool) *tree {
	return &tree{
		nodeOps:   nodeOps,
		nodeMgr:   nodeMgr,
		latchMgr:  NewLatchManager(),
		isMutator: isMutator,
	}
}

// Open initializes or opens an existing tree.
func (t *tree) Open(_ string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.rootPageID.IsValid() {
		rootNode, rootPageID := t.nodeMgr.CreateLeaf()
		if rootNode == nil {
			return ErrStoreClosed
		}
		t.rootPageID = rootPageID
		t.rootNode = rootNode
	}
	return nil
}

// GetRootPageID returns the tree root PageID for persistence.
func (t *tree) GetRootPageID() PageID {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rootPageID
}

// RestoreRootPageID restores tree root PageID from a stored value.
func (t *tree) RestoreRootPageID(pageID PageID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rootPageID = pageID
}

// Close releases all resources.
func (t *tree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isClosed = true
	t.rootNode = nil
	return nil
}

// IsClosed returns true if tree is closed.
func (t *tree) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.isClosed
}

// Get retrieves the value for key.
func (t *tree) Get(key []byte) (InlineValue, error) {
	if t.isClosed {
		return InlineValue{}, ErrStoreClosed
	}

	t.mu.Lock()
	rootPageID := t.rootPageID
	t.mu.Unlock()

	if !rootPageID.IsValid() {
		return InlineValue{}, ErrKeyNotFound
	}

	return t.search(rootPageID, key)
}

// search traverses the tree to find key.
func (t *tree) search(pageID PageID, key []byte) (InlineValue, error) {
	node, err := t.nodeMgr.Load(pageID)
	if err != nil {
		return InlineValue{}, ErrNodeNotFound
	}

	if node.NodeType == NodeTypeLeaf {
		idx := t.nodeOps.Search(node, key)
		entries := ExtractLeafEntries(node)
		if node.Count == 0 {
			return InlineValue{}, ErrKeyNotFound
		}
		if idx < len(entries) && bytes.Equal(entries[idx].Key, key) {
			return entries[idx].Value, nil
		}
		return InlineValue{}, ErrKeyNotFound
	}

	entries := ExtractInternalEntries(node)
	if node.Count == 0 {
		return InlineValue{}, ErrKeyNotFound
	}
	idx := t.nodeOps.Search(node, key)
	if idx >= int(node.Count) {
		idx = int(node.Count) - 1
	}
	if idx == 0 {
		return t.search(entries[0].Child, key)
	}
	if bytes.Compare(key, entries[idx].Key) < 0 {
		return t.search(entries[idx-1].Child, key)
	}
	return t.search(entries[idx].Child, key)
}

// Write performs a mutation on the tree.
func (t *tree) Write(op TreeOperation) error {
	if t.isClosed {
		return ErrStoreClosed
	}
	if !t.isMutator {
		return errors.New("blinktree: write not supported on read-only tree")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	switch op.Type {
	case OpPut:
		return t.put(op.Key, op.Value)
	case OpDelete:
		return t.deleteImpl(op.Key)
	default:
		return errors.New("blinktree: unknown operation type")
	}
}

// put inserts or updates a key-value pair.
func (t *tree) put(key []byte, value InlineValue) error {
	rootPageID := t.rootPageID
	if !rootPageID.IsValid() {
		return errors.New("blinktree: tree not initialized")
	}

	// Build stack of PageIDs from root to leaf
	stack := make([]PageID, 0, 16)
	stack = append(stack, rootPageID)

	currentPageID := rootPageID
	for {
		node, err := t.nodeMgr.Load(currentPageID)
		if err != nil {
			return err
		}

		if node.NodeType == NodeTypeLeaf {
			break
		}

		entries := ExtractInternalEntries(node)
		idx := t.nodeOps.Search(node, key)
		if idx >= int(node.Count) {
			if node.Count == 0 {
				return errors.New("blinktree: tree not initialized")
			}
			idx = int(node.Count) - 1
		}
		if idx == 0 {
			childPageID := entries[0].Child
			stack = append(stack, childPageID)
			currentPageID = childPageID
			continue
		}
		if bytes.Compare(key, entries[idx].Key) < 0 {
			childPageID := entries[idx-1].Child
			stack = append(stack, childPageID)
			currentPageID = childPageID
			continue
		}
		childPageID := entries[idx].Child
		stack = append(stack, childPageID)
		currentPageID = childPageID
	}

	leafPageID := stack[len(stack)-1]
	leaf, err := t.nodeMgr.Load(leafPageID)
	if err != nil {
		return err
	}

	newRight, splitKey, err := t.nodeOps.Insert(leaf, key, value)
	if err != nil {
		return err
	}

	if newRight != nil {
		// Split needed: create new right leaf
		newRightNode, newRightPageID := t.nodeMgr.CreateLeaf()
		if newRightNode == nil {
			return ErrStoreClosed
		}

		// Copy split data to new right node
		*newRightNode = *newRight

		// Set sibling links: left → right, right → old HighSibling
		leaf.HighSibling = newRightPageID
		newRightNode.LowSibling = leafPageID

		// Persist both leaves (PageID is stable, no parent pointer updates needed)
		if err := t.nodeMgr.Persist(leaf, leafPageID); err != nil {
			return err
		}
		if err := t.nodeMgr.Persist(newRightNode, newRightPageID); err != nil {
			return err
		}

		// Propagate split up the tree
		return t.propagateSplit(stack, leafPageID, newRightPageID, splitKey)
	}

	// No split: just persist the modified leaf
	if err := t.nodeMgr.Persist(leaf, leafPageID); err != nil {
		return err
	}
	return nil
}

// propagateSplit handles split propagation up the tree.
// leftPageID is the original leaf's PageID.
// rightPageID is the new right node's PageID.
func (t *tree) propagateSplit(stack []PageID, leftPageID, rightPageID PageID, splitKey []byte) error {
	if len(stack) == 1 {
		// Leaf was root — create new root
		return t.splitRoot(leftPageID, rightPageID, splitKey)
	}

	parentIdx := len(stack) - 2
	parentPageID := stack[parentIdx]
	parent, err := t.nodeMgr.Load(parentPageID)
	if err != nil {
		return err
	}

	entries := ExtractInternalEntries(parent)

	// Now insert the new entry for right child after the left child
	if int(parent.Count)+1 > int(parent.Capacity) {
		// Parent is full — split parent
		// First insert the new entry, then split
		newEntries := make([]InternalEntry, parent.Count+1)
		idx := t.nodeOps.Search(parent, splitKey)
		if idx > int(parent.Count) {
			idx = int(parent.Count)
		}
		copy(newEntries, entries[:idx])
		newEntries[idx] = InternalEntry{Key: copyKey(splitKey), Child: rightPageID}
		copy(newEntries[idx+1:], entries[idx:])
		parent.Count++
		StoreInternalEntries(parent, newEntries)
		entries = newEntries

		// Split the parent
		_, newParentRight, newSplitKey := t.nodeOps.Split(parent)

		// Set sibling links
		newParentRight.LowSibling = parentPageID // Will be updated below

		// Create new right parent node
		newParentNode, newParentPageID := t.nodeMgr.CreateInternal(parent.Level)
		if newParentNode == nil {
			return ErrStoreClosed
		}
		*newParentNode = *newParentRight

		// Update left parent's sibling
		parent.HighSibling = newParentPageID

		// Persist both halves
		if err := t.nodeMgr.Persist(parent, parentPageID); err != nil {
			return err
		}
		if err := t.nodeMgr.Persist(newParentNode, newParentPageID); err != nil {
			return err
		}

		// Recurse up
		newStack := make([]PageID, parentIdx+1)
		copy(newStack, stack[:parentIdx+1])
		return t.propagateSplit(newStack, parentPageID, newParentPageID, newSplitKey)
	}

	// Parent has room — insert new entry after left child
	newEntries := make([]InternalEntry, parent.Count+1)
	idx := t.nodeOps.Search(parent, splitKey)
	if idx > int(parent.Count) {
		idx = int(parent.Count)
	}
	// Standard sorted insert: copy before idx, insert at idx, copy after idx
	copy(newEntries, entries[:idx])
	newEntries[idx] = InternalEntry{Key: copyKey(splitKey), Child: rightPageID}
	copy(newEntries[idx+1:], entries[idx:])
	parent.Count++
	StoreInternalEntries(parent, newEntries)

	if err := t.nodeMgr.Persist(parent, parentPageID); err != nil {
		return err
	}
	return nil
}

// splitRoot creates a new root after old root split.
func (t *tree) splitRoot(leftPageID, rightPageID PageID, splitKey []byte) error {
	newRootNode, newRootPageID := t.nodeMgr.CreateInternal(1)
	if newRootNode == nil {
		return ErrStoreClosed
	}

	entries := make([]InternalEntry, 2)
	// First child gets empty key (sentinel for leftmost child)
	// Must be []byte{} not nil — nil key writes keyLen=0, readKeySlot returns nil,
	// which breaks search comparisons (nil != any key)
	entries[0] = InternalEntry{Key: []byte{}, Child: leftPageID}
	entries[1] = InternalEntry{Key: copyKey(splitKey), Child: rightPageID}
	newRootNode.Count = 2
	newRootNode.HighKey = copyKey(splitKey)
	StoreInternalEntries(newRootNode, entries)

	if err := t.nodeMgr.Persist(newRootNode, newRootPageID); err != nil {
		return err
	}

	t.rootPageID = newRootPageID
	t.rootNode = newRootNode
	return nil
}

// handleUnderflow checks and fixes underfull nodes after delete.
func (t *tree) handleUnderflow(stack []PageID) error {
	if len(stack) == 0 {
		return nil
	}

	for i := len(stack) - 1; i >= 1; i-- {
		nodePageID := stack[i]
		node, err := t.nodeMgr.Load(nodePageID)
		if err != nil {
			return err
		}

		threshold := int(node.Capacity) / 2
		if int(node.Count) >= threshold {
			continue
		}

		if i == 1 && node.Count >= 1 {
			continue
		}

		parentPageID := stack[i-1]
		parent, err := t.nodeMgr.Load(parentPageID)
		if err != nil {
			return err
		}

		entries := ExtractInternalEntries(parent)

		// Find our node in parent's children
		myIdx := -1
		for j := 0; j < int(parent.Count); j++ {
			if entries[j].Child == nodePageID {
				myIdx = j
				break
			}
		}
		if myIdx < 0 {
			continue
		}

		// Try left sibling redistribution
		if myIdx > 0 {
			leftPageID := entries[myIdx-1].Child
			left, err := t.nodeMgr.Load(leftPageID)
			if err == nil && int(left.Count) > threshold {
				if node.NodeType == NodeTypeLeaf {
					t.redistributeLeaf(left, node)
					leafEntries := ExtractLeafEntries(node)
					entries[myIdx].Key = copyKey(leafEntries[0].Key)
				} else {
					t.redistributeInternal(left, node)
					intEntries := ExtractInternalEntries(node)
					entries[myIdx].Key = copyKey(intEntries[0].Key)
				}
				StoreInternalEntries(parent, entries)
				if err := t.nodeMgr.Persist(left, leftPageID); err != nil {
					return err
				}
				if err := t.nodeMgr.Persist(node, nodePageID); err != nil {
					return err
				}
				if err := t.nodeMgr.Persist(parent, parentPageID); err != nil {
					return err
				}
				continue
			}
		}

		// Try right sibling redistribution
		if myIdx < int(parent.Count)-1 {
			rightPageID := entries[myIdx+1].Child
			right, err := t.nodeMgr.Load(rightPageID)
			if err == nil && int(right.Count) > threshold {
				if node.NodeType == NodeTypeLeaf {
					t.redistributeLeafRight(node, right)
					leafEntries := ExtractLeafEntries(right)
					entries[myIdx+1].Key = copyKey(leafEntries[0].Key)
				} else {
					t.redistributeInternalRight(node, right)
					intEntries := ExtractInternalEntries(right)
					entries[myIdx+1].Key = copyKey(intEntries[0].Key)
				}
				StoreInternalEntries(parent, entries)
				if err := t.nodeMgr.Persist(right, rightPageID); err != nil {
					return err
				}
				if err := t.nodeMgr.Persist(node, nodePageID); err != nil {
					return err
				}
				if err := t.nodeMgr.Persist(parent, parentPageID); err != nil {
					return err
				}
				continue
			}
		}

		// Merge with a sibling (prefer left)
		var mergeLeft bool
		var siblingPageID PageID
		if myIdx > 0 {
			siblingPageID = entries[myIdx-1].Child
			mergeLeft = true
		} else if myIdx < int(parent.Count)-1 {
			siblingPageID = entries[myIdx+1].Child
			mergeLeft = false
		} else {
			continue
		}

		sibling, err := t.nodeMgr.Load(siblingPageID)
		if err != nil {
			continue
		}

		if node.NodeType == NodeTypeLeaf {
			t.mergeLeafNodes(sibling, node)
		} else {
			t.mergeInternalNodes(sibling, node)
		}

		if err := t.nodeMgr.Persist(sibling, siblingPageID); err != nil {
			return err
		}

		// Remove entry from parent
		if mergeLeft {
			copy(entries[myIdx:], entries[myIdx+1:])
		} else {
			copy(entries[myIdx+1:], entries[myIdx+2:])
		}
		parent.Count--
		StoreInternalEntries(parent, entries[:parent.Count])

		if err := t.nodeMgr.Persist(parent, parentPageID); err != nil {
			return err
		}

		// Check if root needs to shrink
		if i-1 == 0 && parent.Count == 1 {
			remainingEntries := ExtractInternalEntries(parent)
			childPageID := remainingEntries[0].Child
			child, err := t.nodeMgr.Load(childPageID)
			if err == nil {
				t.rootPageID = childPageID
				t.rootNode = child
			}
		}
	}

	return nil
}

// redistributeLeaf moves last entry from left to beginning of right.
func (t *tree) redistributeLeaf(left, right *NodeFormat) {
	leftEntries := ExtractLeafEntries(left)
	rightEntries := ExtractLeafEntries(right)

	newRightEntries := make([]LeafEntry, right.Count+1)
	newRightEntries[0] = leftEntries[left.Count-1]
	copy(newRightEntries[1:], rightEntries[:right.Count])

	left.Count--
	right.Count++

	StoreLeafEntries(left, leftEntries[:left.Count])
	StoreLeafEntries(right, newRightEntries)
}

// redistributeLeafRight moves first entry from right to end of node.
func (t *tree) redistributeLeafRight(node, right *NodeFormat) {
	nodeEntries := ExtractLeafEntries(node)
	rightEntries := ExtractLeafEntries(right)

	newNodeEntries := make([]LeafEntry, node.Count+1)
	copy(newNodeEntries, nodeEntries[:node.Count])
	newNodeEntries[node.Count] = rightEntries[0]

	node.Count++
	right.Count--

	StoreLeafEntries(node, newNodeEntries)
	StoreLeafEntries(right, rightEntries[1:int(right.Count)+1])
}

// redistributeInternal moves last entry from left to beginning of right.
func (t *tree) redistributeInternal(left, right *NodeFormat) {
	leftEntries := ExtractInternalEntries(left)
	rightEntries := ExtractInternalEntries(right)

	newRightEntries := make([]InternalEntry, right.Count+1)
	newRightEntries[0] = leftEntries[left.Count-1]
	copy(newRightEntries[1:], rightEntries[:right.Count])

	left.Count--
	right.Count++

	StoreInternalEntries(left, leftEntries[:left.Count])
	StoreInternalEntries(right, newRightEntries)
}

// redistributeInternalRight moves first entry from right to end of node.
func (t *tree) redistributeInternalRight(node, right *NodeFormat) {
	nodeEntries := ExtractInternalEntries(node)
	rightEntries := ExtractInternalEntries(right)

	newNodeEntries := make([]InternalEntry, node.Count+1)
	copy(newNodeEntries, nodeEntries[:node.Count])
	newNodeEntries[node.Count] = rightEntries[0]

	node.Count++
	right.Count--

	StoreInternalEntries(node, newNodeEntries)
	StoreInternalEntries(right, rightEntries[1:int(right.Count)+1])
}

// mergeLeafNodes merges right into left.
func (t *tree) mergeLeafNodes(left, right *NodeFormat) {
	leftEntries := ExtractLeafEntries(left)
	rightEntries := ExtractLeafEntries(right)

	newEntries := make([]LeafEntry, left.Count+right.Count)
	copy(newEntries, leftEntries[:left.Count])
	copy(newEntries[left.Count:], rightEntries[:right.Count])
	left.Count += right.Count

	left.HighSibling = right.HighSibling

	StoreLeafEntries(left, newEntries)
}

// mergeInternalNodes merges right into left.
func (t *tree) mergeInternalNodes(left, right *NodeFormat) {
	leftEntries := ExtractInternalEntries(left)
	rightEntries := ExtractInternalEntries(right)

	newEntries := make([]InternalEntry, left.Count+right.Count)
	copy(newEntries, leftEntries[:left.Count])
	copy(newEntries[left.Count:], rightEntries[:right.Count])
	left.Count += right.Count

	left.HighSibling = right.HighSibling

	StoreInternalEntries(left, newEntries)
}

// Put implements TreeMutator.
func (t *tree) Put(key []byte, value InlineValue) error {
	return t.Write(TreeOperation{Type: OpPut, Key: key, Value: value})
}

// Delete implements TreeMutator.
func (t *tree) Delete(key []byte) error {
	return t.Write(TreeOperation{Type: OpDelete, Key: key})
}

// deleteImpl performs the actual delete under lock.
func (t *tree) deleteImpl(key []byte) error {
	rootPageID := t.rootPageID
	if !rootPageID.IsValid() {
		return ErrKeyNotFound
	}

	// Build stack to leaf
	stack := make([]PageID, 0, 16)
	stack = append(stack, rootPageID)

	currentPageID := rootPageID
	for {
		node, err := t.nodeMgr.Load(currentPageID)
		if err != nil {
			return err
		}

		if node.NodeType == NodeTypeLeaf {
			break
		}

		entries := ExtractInternalEntries(node)
		idx := t.nodeOps.Search(node, key)
		if idx >= int(node.Count) {
			if node.Count == 0 {
				return errors.New("blinktree: internal node has no entries")
			}
			idx = int(node.Count) - 1
		}
		childPageID := entries[idx].Child
		stack = append(stack, childPageID)
		currentPageID = childPageID
	}

	// Find and delete key in leaf
	leafPageID := stack[len(stack)-1]
	leaf, err := t.nodeMgr.Load(leafPageID)
	if err != nil {
		return err
	}

	entries := ExtractLeafEntries(leaf)
	idx := t.nodeOps.Search(leaf, key)

	if idx >= int(leaf.Count) || !bytes.Equal(entries[idx].Key, key) {
		return ErrKeyNotFound
	}

	copy(entries[idx:], entries[idx+1:])
	leaf.Count--
	StoreLeafEntries(leaf, entries[:leaf.Count])

	if err := t.nodeMgr.Persist(leaf, leafPageID); err != nil {
		return err
	}

	return t.handleUnderflow(stack)
}

// Scan returns an iterator over key range.
func (t *tree) Scan(start, end []byte) (TreeIterator, error) {
	if t.isClosed {
		return nil, ErrStoreClosed
	}

	t.mu.Lock()
	rootPageID := t.rootPageID
	t.mu.Unlock()

	if !rootPageID.IsValid() {
		return nil, ErrKeyNotFound
	}

	return &treeIterator{
		tree:       t,
		rootPageID: rootPageID,
		start:      start,
		end:        end,
	}, nil
}

// Batch performs multiple mutations atomically.
func (t *tree) Batch(ops []TreeOperation) error {
	if t.isClosed {
		return ErrStoreClosed
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, op := range ops {
		var err error
		switch op.Type {
		case OpPut:
			err = t.put(op.Key, op.Value)
		case OpDelete:
			err = t.Delete(op.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// =============================================================================
// TreeMutator Implementation
// =============================================================================

type treeMutator = tree

// =============================================================================
// TreeIterator Implementation (DFS-based, no sibling chain dependency)
// =============================================================================

type treeIterator struct {
	tree       *tree
	rootPageID PageID
	start      []byte
	end        []byte
	finished   bool
	err        error
	currentKey []byte
	currentVal InlineValue

	// DFS-collected leaves
	leaves   []PageID
	leafIdx  int
	node     *NodeFormat
	entryIdx int
	inited   bool
}

// collectLeaves DFS from pageID, appending all leaf PageIDs in order.
func collectLeaves(nodeMgr NodeManager, pageID PageID, out *[]PageID) error {
	node, err := nodeMgr.Load(pageID)
	if err != nil {
		return err
	}
	if node.NodeType == NodeTypeLeaf {
		*out = append(*out, pageID)
		return nil
	}
	entries := ExtractInternalEntries(node)
	for i := 0; i < int(node.Count); i++ {
		if err := collectLeaves(nodeMgr, entries[i].Child, out); err != nil {
			return err
		}
	}
	return nil
}

func (it *treeIterator) Next() bool {
	if it.finished || it.err != nil {
		return false
	}

	if !it.inited {
		it.inited = true
		it.leaves = make([]PageID, 0, 64)
		if err := collectLeaves(it.tree.nodeMgr, it.rootPageID, &it.leaves); err != nil {
			it.err = err
			return false
		}
		if len(it.leaves) == 0 {
			it.finished = true
			return false
		}
		node, err := it.tree.nodeMgr.Load(it.leaves[0])
		if err != nil {
			it.err = err
			return false
		}
		it.node = node
		it.leafIdx = 0
		it.entryIdx = 0
	}

	for {
		if it.node == nil {
			it.finished = true
			return false
		}

		entries := ExtractLeafEntries(it.node)

		for it.entryIdx < int(it.node.Count) && len(it.start) > 0 && bytes.Compare(entries[it.entryIdx].Key, it.start) < 0 {
			it.entryIdx++
		}

		if it.entryIdx >= int(it.node.Count) {
			it.leafIdx++
			if it.leafIdx >= len(it.leaves) {
				it.finished = true
				return false
			}
			node, err := it.tree.nodeMgr.Load(it.leaves[it.leafIdx])
			if err != nil {
				it.err = err
				return false
			}
			it.node = node
			it.entryIdx = 0
			continue
		}

		if len(it.end) > 0 && bytes.Compare(entries[it.entryIdx].Key, it.end) >= 0 {
			it.finished = true
			return false
		}

		it.currentKey = entries[it.entryIdx].Key
		it.currentVal = entries[it.entryIdx].Value
		it.entryIdx++
		return true
	}
}

func (it *treeIterator) Key() []byte {
	return it.currentKey
}

func (it *treeIterator) Value() InlineValue {
	return it.currentVal
}

func (it *treeIterator) Error() error {
	return it.err
}

func (it *treeIterator) Close() {
	it.node = nil
	it.leaves = nil
}
