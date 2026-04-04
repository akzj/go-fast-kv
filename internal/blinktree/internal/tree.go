package internal

import (
	"encoding/binary"
	"errors"
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// tree implements the Tree interface using B-link tree algorithm.
type tree struct {
	nodeOps   NodeOperations
	nodeMgr   NodeManager
	latchMgr  LatchManager
	mu        sync.Mutex
	rootAddr  VAddr
	rootNode  *NodeFormat
	isClosed  bool
	isMutator bool
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

	if !t.rootAddr.IsValid() {
		rootNode, rootAddr := t.nodeMgr.CreateLeaf()
		if rootNode == nil {
			return ErrStoreClosed
		}
		t.rootAddr = rootAddr
		t.rootNode = rootNode
	}
	return nil
}

// GetRootAddress returns the tree root address as bytes for persistence.
func (t *tree) GetRootAddress() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()

	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], t.rootAddr.SegmentID)
	binary.LittleEndian.PutUint64(data[8:16], t.rootAddr.Offset)
	return data
}

// RestoreRoot restores tree root address from persisted bytes.
func (t *tree) RestoreRoot(data []byte) {
	if len(data) < 16 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rootAddr = VAddr{
		SegmentID: binary.LittleEndian.Uint64(data[0:8]),
		Offset:    binary.LittleEndian.Uint64(data[8:16]),
	}
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
func (t *tree) Get(key PageID) (InlineValue, error) {
	if t.isClosed {
		return InlineValue{}, ErrStoreClosed
	}

	t.mu.Lock()
	rootAddr := t.rootAddr
	t.mu.Unlock()


	if !rootAddr.IsValid() {
		return InlineValue{}, ErrKeyNotFound
	}

	return t.search(rootAddr, key)
}

// search traverses the tree to find key.
func (t *tree) search(addr VAddr, key PageID) (InlineValue, error) {
	node, err := t.nodeMgr.Load(addr)
	if err != nil {
		return InlineValue{}, ErrNodeNotFound
	}

	if node.NodeType == NodeTypeLeaf {
		idx := t.nodeOps.Search(node, key)
		entries := ExtractLeafEntries(node)
		// search returns first position where Key >= key
		// If key exists, it's at idx (or earlier if duplicates, but we use first match)
		if node.Count == 0 {
			return InlineValue{}, ErrKeyNotFound
		}
		if idx < len(entries) && entries[idx].Key == key {
			return entries[idx].Value, nil
		}
		return InlineValue{}, ErrKeyNotFound
	}

	entries := ExtractInternalEntries(node)
	if node.Count == 0 {
		return InlineValue{}, ErrKeyNotFound
	}
	idx := t.nodeOps.Search(node, key)
	// idx is the first entry where Key >= key
	// Key belongs to entries[idx-1]'s child (the range just before idx)
	// If idx == 0, key is less than first entry's key, use entries[0].Child
	// If idx >= Count, key is >= last entry's key, use entries[Count-1].Child
	if idx == 0 {
		return t.search(entries[0].Child, key)
	}
	if idx >= int(node.Count) {
		return t.search(entries[node.Count-1].Child, key)
	}
	return t.search(entries[idx-1].Child, key)
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
func (t *tree) put(key PageID, value InlineValue) error {
	rootAddr := t.rootAddr
	if !rootAddr.IsValid() {
		return errors.New("blinktree: tree not initialized")
	}

	stack := make([]VAddr, 0, 16)
	stack = append(stack, rootAddr)

	currentAddr := rootAddr
	for {
		node, err := t.nodeMgr.Load(currentAddr)
		if err != nil {
			return err
		}

		if node.NodeType == NodeTypeLeaf {
			break
		}

		entries := ExtractInternalEntries(node)
		idx := t.nodeOps.Search(node, key)
		// searchInternal with '<' returns first entry where Key >= key; use idx directly
		if idx >= int(node.Count) {
			if node.Count == 0 {
				return errors.New("blinktree: internal node has no entries")
			}
			idx = int(node.Count) - 1
		}
		childAddr := entries[idx].Child
		stack = append(stack, childAddr)
		currentAddr = childAddr
	}

	leafAddr := stack[len(stack)-1]
	leaf, err := t.nodeMgr.Load(leafAddr)
	if err != nil {
		return err
	}

	newRight, splitKey, err := t.nodeOps.Insert(leaf, key, value)
	if err != nil {
		return err
	}

	if newRight != nil {
		rightAddr, err := t.nodeMgr.Persist(newRight)
		if err != nil {
			return err
		}

		newRight.HighSibling = leaf.HighSibling
		leaf.HighSibling = rightAddr

		// Persist the modified left leaf after split
		_, err = t.nodeMgr.Persist(leaf)
		if err != nil {
			return err
		}

		return t.propagateSplit(stack, leafAddr, rightAddr, splitKey)
	}

	// Persist the modified leaf and update root address if needed
	newAddr, err := t.nodeMgr.Persist(leaf)
	if err != nil {
		return err
	}
	// If leaf was the root, update root address to new persisted location
	if leafAddr == t.rootAddr {
		t.rootAddr = newAddr
	}
	return nil
}

// propagateSplit handles split propagation up the tree.
func (t *tree) propagateSplit(stack []VAddr, leftAddr, rightAddr VAddr, splitKey PageID) error {
	if len(stack) == 1 {
		return t.splitRoot(leftAddr, rightAddr, splitKey)
	}

	parentAddr := stack[len(stack)-2]
	parent, err := t.nodeMgr.Load(parentAddr)
	if err != nil {
		return err
	}

	entries := ExtractInternalEntries(parent)
	idx := t.nodeOps.Search(parent, splitKey)

	if int(parent.Count)+1 > int(parent.Capacity) {
		_, newParent, newSplitKey := t.nodeOps.Split(parent)
		newParentAddr, err := t.nodeMgr.Persist(newParent)
		if err != nil {
			return err
		}

		if splitKey >= newSplitKey {
			newParentEntries := ExtractInternalEntries(newParent)
			insertIdx := t.nodeOps.Search(newParent, splitKey)
			copy(newParentEntries[insertIdx+1:], newParentEntries[insertIdx:])
			newParentEntries[insertIdx] = InternalEntry{Key: splitKey, Child: rightAddr}
			newParent.Count++
			StoreInternalEntries(newParent, newParentEntries)
		} else {
			copy(entries[idx+1:], entries[idx:])
			entries[idx] = InternalEntry{Key: splitKey, Child: rightAddr}
			parent.Count++
			StoreInternalEntries(parent, entries)
		}

		t.nodeMgr.Persist(parent)
		t.nodeMgr.Persist(newParent)

		newStack := stack[:len(stack)-1]
		newStack[len(newStack)-1] = newParentAddr
		return t.propagateSplit(newStack, parentAddr, newParentAddr, newSplitKey)
	}

	// Guard against idx being at or beyond entry bounds
	// This can happen if splitKey is greater than all existing keys
	if idx >= len(entries) {
		idx = len(entries) - 1
	}
	
	// Create new slice with room for one more entry
	newEntries := make([]InternalEntry, parent.Count+1)
	// Copy entries before insertion point
	copy(newEntries, entries[:idx])
	// Copy entries from insertion point onward (shifted by 1)
	if idx < len(entries) {
		copy(newEntries[idx+1:], entries[idx:])
	}
	// Insert the new entry
	newEntries[idx] = InternalEntry{Key: splitKey, Child: rightAddr}
	parent.Count++
	StoreInternalEntries(parent, newEntries)

	_, err = t.nodeMgr.Persist(parent)
	return err
}

// splitRoot creates a new root after old root split.
func (t *tree) splitRoot(leftAddr, rightAddr VAddr, splitKey PageID) error {
	// Create new root node directly - don't use CreateInternal which persists immediately
	// We need to set fields first, then persist once
	internalCapacity := uint16((vaddr.PageSize - NodeHeaderSize) / InternalEntrySize)
	newRoot := &NodeFormat{
		NodeType: NodeTypeInternal,
		Level:    1,
		Count:    0,
		Capacity: internalCapacity,
		RawData:  make([]byte, 0),
	}

	// Create entries slice directly (ExtractInternalEntries returns empty when Count=0)
	entries := make([]InternalEntry, 2)
	// After split: left leaf has keys <= splitKey, right leaf has keys > splitKey.
	// splitKey is the last key of left leaf.
	// With lower-bound search + idx-1 navigation:
	//   - key<=splitKey: idx=1, idx-1=0 → left ✓
	//   - key>splitKey: idx=1, idx-1=0 → left ✓
	entries[0] = InternalEntry{Key: 0, Child: leftAddr}
	entries[1] = InternalEntry{Key: splitKey, Child: rightAddr}
	// IMPORTANT: Set Count BEFORE StoreInternalEntries so RawData is sized correctly
	newRoot.Count = 2
	newRoot.HighKey = splitKey
	StoreInternalEntries(newRoot, entries)

	newRootAddr, err := t.nodeMgr.Persist(newRoot)
	if err != nil {
		return err
	}

	t.rootAddr = newRootAddr
	t.rootNode = newRoot
	return nil
}

// handleUnderflow checks and fixes underfull nodes after delete.
func (t *tree) handleUnderflow(stack []VAddr) error {
	if len(stack) == 0 {
		return nil
	}

	// Start from leaf and propagate up
	for i := len(stack) - 1; i >= 1; i-- {
		nodeAddr := stack[i]
		node, err := t.nodeMgr.Load(nodeAddr)
		if err != nil {
			return err
		}

		// Underflow threshold: less than half capacity (minimum for B-link tree)
		threshold := int(node.Capacity) / 2
		if int(node.Count) >= threshold {
			continue
		}

		// Root node can have just 1 entry
		if i == 1 && node.Count >= 1 {
			continue
		}

		parentAddr := stack[i-1]
		parent, err := t.nodeMgr.Load(parentAddr)
		if err != nil {
			return err
		}

		entries := ExtractInternalEntries(parent)
		idx := t.nodeOps.Search(parent, 0)
		if idx >= int(parent.Count) {
			idx = int(parent.Count) - 1
		}

		// Find our node in parent's children to get siblings
		var myIdx int
		for myIdx = 0; myIdx < int(parent.Count); myIdx++ {
			if entries[myIdx].Child == nodeAddr {
				break
			}
		}

		// Try left sibling
		if myIdx > 0 {
			leftAddr := entries[myIdx-1].Child
			if leftAddr.IsValid() {
				left, err := t.nodeMgr.Load(leftAddr)
				if err == nil {
					if int(left.Count) > threshold {
						// Redistribute: move last entry from left to node
						if node.NodeType == NodeTypeLeaf {
							t.redistributeLeaf(left, node)
							// Update separator key in parent
							leafEntries := ExtractLeafEntries(node)
							entries[myIdx].Key = leafEntries[0].Key
							StoreInternalEntries(parent, entries)
						} else {
							t.redistributeInternal(left, node)
							intEntries := ExtractInternalEntries(node)
							entries[myIdx].Key = intEntries[0].Key
							StoreInternalEntries(parent, entries)
						}
						t.nodeMgr.Persist(left)
						t.nodeMgr.Persist(node)
						t.nodeMgr.Persist(parent)
						continue
					}
				}
			}
		}

		// Try right sibling
		if myIdx < int(parent.Count)-1 {
			rightAddr := entries[myIdx+1].Child
			if rightAddr.IsValid() {
				right, err := t.nodeMgr.Load(rightAddr)
				if err == nil {
					if int(right.Count) > threshold {
						// Redistribute: move first entry from right to node
						if node.NodeType == NodeTypeLeaf {
							t.redistributeLeafRight(node, right)
							leafEntries := ExtractLeafEntries(right)
							entries[myIdx+1].Key = leafEntries[0].Key
							StoreInternalEntries(parent, entries)
						} else {
							t.redistributeInternalRight(node, right)
							intEntries := ExtractInternalEntries(right)
							entries[myIdx+1].Key = intEntries[0].Key
							StoreInternalEntries(parent, entries)
						}
						t.nodeMgr.Persist(right)
						t.nodeMgr.Persist(node)
						t.nodeMgr.Persist(parent)
						continue
					}
				}
			}
		}

		// Merge with a sibling (prefer left)
		var mergeLeft bool
		var siblingAddr VAddr
		if myIdx > 0 {
			siblingAddr = entries[myIdx-1].Child
			mergeLeft = true
		} else if myIdx < int(parent.Count)-1 {
			siblingAddr = entries[myIdx+1].Child
			mergeLeft = false
		} else {
			// No siblings, can't merge
			continue
		}

		sibling, err := t.nodeMgr.Load(siblingAddr)
		if err != nil {
			continue
		}

		// Merge node into sibling
		if node.NodeType == NodeTypeLeaf {
			t.mergeLeafNodes(sibling, node)
		} else {
			t.mergeInternalNodes(sibling, node)
		}

		t.nodeMgr.Persist(sibling)

		// Remove entry from parent
		if mergeLeft {
			// Remove entry at myIdx (which points to the deleted node after sibling)
			copy(entries[myIdx:], entries[myIdx+1:])
		} else {
			// Remove entry at myIdx+1 (right sibling)
			copy(entries[myIdx+1:], entries[myIdx+2:])
		}
		parent.Count--
		StoreInternalEntries(parent, entries)
		t.nodeMgr.Persist(parent)

		// Check if root needs to shrink
		if len(stack) == 2 && parent.Count == 1 && parentAddr == t.rootAddr {
			// Shrink root
			childAddr := entries[0].Child
			child, err := t.nodeMgr.Load(childAddr)
			if err == nil {
				t.rootAddr = childAddr
				t.rootNode = child
			}
		}
	}

	return nil
}

// redistributeLeaf moves entries from left to right (left loses one).
func (t *tree) redistributeLeaf(left, right *NodeFormat) {
	leftEntries := ExtractLeafEntries(left)
	rightEntries := ExtractLeafEntries(right)

	// Move first entry of right to end of left
	lastIdx := int(left.Count) - 1
	leftEntries[lastIdx+1] = rightEntries[0]

	// Shift right entries left
	copy(rightEntries, rightEntries[1:])

	left.Count++
	right.Count--

	StoreLeafEntries(left, leftEntries)
	StoreLeafEntries(right, rightEntries)
}

// redistributeLeafRight moves entry from right to node (right loses one).
func (t *tree) redistributeLeafRight(node, right *NodeFormat) {
	nodeEntries := ExtractLeafEntries(node)
	rightEntries := ExtractLeafEntries(right)

	// Move first entry of right to end of node
	nodeEntries[int(node.Count)] = rightEntries[0]

	// Shift right entries left
	copy(rightEntries, rightEntries[1:])

	node.Count++
	right.Count--

	StoreLeafEntries(node, nodeEntries)
	StoreLeafEntries(right, rightEntries)
}

// redistributeInternal moves entries from left to right.
func (t *tree) redistributeInternal(left, right *NodeFormat) {
	leftEntries := ExtractInternalEntries(left)
	rightEntries := ExtractInternalEntries(right)

	// Move first entry of right to end of left
	lastIdx := int(left.Count) - 1
	leftEntries[lastIdx+1] = rightEntries[0]

	// Shift right entries left
	copy(rightEntries, rightEntries[1:])

	left.Count++
	right.Count--

	StoreInternalEntries(left, leftEntries)
	StoreInternalEntries(right, rightEntries)
}

// redistributeInternalRight moves entry from right to node.
func (t *tree) redistributeInternalRight(node, right *NodeFormat) {
	nodeEntries := ExtractInternalEntries(node)
	rightEntries := ExtractInternalEntries(right)

	// Move first entry of right to end of node
	nodeEntries[int(node.Count)] = rightEntries[0]

	// Shift right entries left
	copy(rightEntries, rightEntries[1:])

	node.Count++
	right.Count--

	StoreInternalEntries(node, nodeEntries)
	StoreInternalEntries(right, rightEntries)
}

// mergeLeafNodes merges right into left.
func (t *tree) mergeLeafNodes(left, right *NodeFormat) {
	leftEntries := ExtractLeafEntries(left)
	rightEntries := ExtractLeafEntries(right)

	copy(leftEntries[int(left.Count):], rightEntries[:int(right.Count)])
	left.Count += right.Count

	// Update sibling link
	left.HighSibling = right.HighSibling

	StoreLeafEntries(left, leftEntries)
}

// mergeInternalNodes merges right into left.
func (t *tree) mergeInternalNodes(left, right *NodeFormat) {
	leftEntries := ExtractInternalEntries(left)
	rightEntries := ExtractInternalEntries(right)

	// The separator key between the two nodes should be the first key of right
	// But we need it in the parent, so we just copy the entries
	copy(leftEntries[int(left.Count):], rightEntries[:int(right.Count)])
	left.Count += right.Count

	// Update sibling link
	left.HighSibling = right.HighSibling

	StoreInternalEntries(left, leftEntries)
}

// Put implements TreeMutator.
func (t *tree) Put(key PageID, value InlineValue) error {
	return t.Write(TreeOperation{Type: OpPut, Key: key, Value: value})
}

// Delete implements TreeMutator.
func (t *tree) Delete(key PageID) error {
	return t.Write(TreeOperation{Type: OpDelete, Key: key})
}

// deleteImpl performs the actual delete under lock.
func (t *tree) deleteImpl(key PageID) error {
	rootAddr := t.rootAddr
	if !rootAddr.IsValid() {
		return ErrKeyNotFound
	}

	// Build stack to leaf
	stack := make([]VAddr, 0, 16)
	stack = append(stack, rootAddr)

	currentAddr := rootAddr
	for {
		node, err := t.nodeMgr.Load(currentAddr)
		if err != nil {
			return err
		}

		if node.NodeType == NodeTypeLeaf {
			break
		}

		entries := ExtractInternalEntries(node)
		idx := t.nodeOps.Search(node, key)
		// searchInternal with '<' returns first entry where Key >= key; use idx directly
		if idx >= int(node.Count) {
			if node.Count == 0 {
				return errors.New("blinktree: internal node has no entries")
			}
			idx = int(node.Count) - 1
		}
		childAddr := entries[idx].Child
		stack = append(stack, childAddr)
		currentAddr = childAddr
	}

	// Find and delete key in leaf
	leafAddr := stack[len(stack)-1]
	leaf, err := t.nodeMgr.Load(leafAddr)
	if err != nil {
		return err
	}

	entries := ExtractLeafEntries(leaf)
	idx := t.nodeOps.Search(leaf, key)

	if idx >= int(leaf.Count) || entries[idx].Key != key {
		return ErrKeyNotFound
	}

	// Remove entry by shifting
	copy(entries[idx:], entries[idx+1:])
	leaf.Count--
	StoreLeafEntries(leaf, entries)

	newAddr, err := t.nodeMgr.Persist(leaf)
	if err != nil {
		return err
	}
	// If leaf was the root, update root address to new persisted location
	if leafAddr == t.rootAddr {
		t.rootAddr = newAddr
	}

	// Check for underflow (merge/redistribute if needed)
	return t.handleUnderflow(stack)
}

// Scan returns an iterator over key range.
func (t *tree) Scan(start, end PageID) (TreeIterator, error) {
	if t.isClosed {
		return nil, ErrStoreClosed
	}

	t.mu.Lock()
	rootAddr := t.rootAddr
	t.mu.Unlock()


	if !rootAddr.IsValid() {
		return nil, ErrKeyNotFound
	}

	return &treeIterator{
		tree:     t,
		current:  VAddr{},
		rootAddr: rootAddr,
		start:    start,
		end:      end,
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

// treeMutator is an alias for tree (implements both Tree and TreeMutator).
type treeMutator = tree

// =============================================================================
// TreeIterator Implementation
// =============================================================================

type treeIterator struct {
	tree        *tree
	current     VAddr
	rootAddr    VAddr
	node        *NodeFormat
	idx         int
	start       PageID
	end         PageID
	finished    bool
	err         error
	currentKey  PageID
	currentVal  InlineValue
}

func (it *treeIterator) Next() bool {
	if it.finished || it.err != nil {
		return false
	}

	// Initialize: find leftmost leaf
	if !it.current.IsValid() {
		addr := it.rootAddr
		for {
			node, err := it.tree.nodeMgr.Load(addr)
			if err != nil {
				it.err = err
				return false
			}
			if node.NodeType == NodeTypeLeaf {
				it.current = addr
				it.node = node
				it.idx = 0
				break
			}
			entries := ExtractInternalEntries(node)
			if node.Count == 0 || len(entries) == 0 {
				it.err = errors.New("blinktree: internal node has no entries")
				return false
			}
			addr = entries[0].Child
		}
	}

	entries := ExtractLeafEntries(it.node)
	
	// Skip entries before start
	for it.idx < int(it.node.Count) && entries[it.idx].Key < it.start {
		it.idx++
	}

	// Check if we're done with current node
	if it.idx >= int(it.node.Count) {
		// Try to advance to next node via sibling
		nextAddr := it.node.HighSibling
		if !nextAddr.IsValid() {
			it.finished = true
			return false
		}
		node, err := it.tree.nodeMgr.Load(nextAddr)
		if err != nil {
			it.err = err
			return false
		}
		it.current = nextAddr
		it.node = node
		it.idx = 0
		entries = ExtractLeafEntries(it.node)
	}

	// Check end boundary
	if it.end > 0 && entries[it.idx].Key >= it.end {
		it.finished = true
		return false
	}

	// Store current entry before advancing
	it.currentKey = entries[it.idx].Key
	it.currentVal = entries[it.idx].Value
	it.idx++
	return true
}

func (it *treeIterator) Key() PageID {
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
}
