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
	if idx >= int(node.Count) {
		idx = int(node.Count) - 1
	}
	if idx == 0 {
		return t.search(entries[0].Child, key)
	}
	if key < entries[idx].Key {
		return t.search(entries[idx-1].Child, key)
	}
	return t.search(entries[idx].Child, key)
}

// addrRemap is a mapping from old VAddr to new VAddr, used during CoW re-persist.
type addrRemap struct {
	oldAddr VAddr
	newAddr VAddr
}

// repersistPath walks the stack from mutatedIdx upward to root,
// applying address remappings to each parent's child pointers, then
// re-persisting the parent. Finally updates t.rootAddr.
//
// remaps contains all (oldAddr → newAddr) pairs that need to be applied
// to child pointers at the level just above mutatedIdx.
// As we move up, only the parent's own remap propagates further.
func (t *tree) repersistPath(stack []VAddr, mutatedIdx int, remaps []addrRemap) error {
	currentRemaps := remaps

	for i := mutatedIdx - 1; i >= 0; i-- {
		parent, err := t.nodeMgr.Load(stack[i])
		if err != nil {
			return err
		}
		entries := ExtractInternalEntries(parent)

		// Apply all remaps to child pointers
		for _, r := range currentRemaps {
			for j := 0; j < int(parent.Count); j++ {
				if entries[j].Child == r.oldAddr {
					entries[j].Child = r.newAddr
					break
				}
			}
		}
		StoreInternalEntries(parent, entries)

		newParentAddr, err := t.nodeMgr.Persist(parent)
		if err != nil {
			return err
		}

		// For the next level up, only the parent's own address changed
		currentRemaps = []addrRemap{{oldAddr: stack[i], newAddr: newParentAddr}}
	}

	// The topmost remap gives us the new root address
	if len(currentRemaps) > 0 {
		t.rootAddr = currentRemaps[0].newAddr
	}
	return nil
}

// updateLeftSibling finds the left sibling of the node at stack[nodeIdx],
// updates its HighSibling to newNodeAddr, persists it, and returns the remap.
// Returns zero-value remap if there is no left sibling.
func (t *tree) updateLeftSibling(stack []VAddr, nodeIdx int, newNodeAddr VAddr) (addrRemap, error) {
	if nodeIdx == 0 || len(stack) < 2 {
		// Node is root, no parent to find siblings in
		return addrRemap{}, nil
	}

	parentAddr := stack[nodeIdx-1]
	parent, err := t.nodeMgr.Load(parentAddr)
	if err != nil {
		return addrRemap{}, err
	}

	entries := ExtractInternalEntries(parent)
	oldNodeAddr := stack[nodeIdx]

	// Find our node in parent's children
	myIdx := -1
	for j := 0; j < int(parent.Count); j++ {
		if entries[j].Child == oldNodeAddr {
			myIdx = j
			break
		}
	}

	if myIdx <= 0 {
		// No left sibling in this parent
		return addrRemap{}, nil
	}

	// Load left sibling and update its HighSibling
	leftSibAddr := entries[myIdx-1].Child
	leftSib, err := t.nodeMgr.Load(leftSibAddr)
	if err != nil {
		return addrRemap{}, err
	}

	leftSib.HighSibling = newNodeAddr
	newLeftSibAddr, err := t.nodeMgr.Persist(leftSib)
	if err != nil {
		return addrRemap{}, err
	}

	return addrRemap{oldAddr: leftSibAddr, newAddr: newLeftSibAddr}, nil
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
		if idx >= int(node.Count) {
			if node.Count == 0 {
				return errors.New("blinktree: tree not initialized")
			}
			idx = int(node.Count) - 1
		}
		if idx == 0 {
			childAddr := entries[0].Child
			stack = append(stack, childAddr)
			currentAddr = childAddr
			continue
		}
		if key < entries[idx].Key {
			childAddr := entries[idx-1].Child
			stack = append(stack, childAddr)
			currentAddr = childAddr
			continue
		}
		childAddr := entries[idx].Child
		stack = append(stack, childAddr)
		currentAddr = childAddr
	}

	leafIdx := len(stack) - 1
	leafAddr := stack[leafIdx]
	leaf, err := t.nodeMgr.Load(leafAddr)
	if err != nil {
		return err
	}

	newRight, splitKey, err := t.nodeOps.Insert(leaf, key, value)
	if err != nil {
		return err
	}

	if newRight != nil {
		// Set sibling link BEFORE persisting
		newRight.HighSibling = leaf.HighSibling

		// Persist right node FIRST
		rightAddr, err := t.nodeMgr.Persist(newRight)
		if err != nil {
			return err
		}

		// Update left leaf's sibling to point to right
		leaf.HighSibling = rightAddr

		// Persist the modified left leaf
		newLeftAddr, err := t.nodeMgr.Persist(leaf)
		if err != nil {
			return err
		}

		// Update left sibling's HighSibling to point to new left addr
		leftSibRemap, err := t.updateLeftSibling(stack, leafIdx, newLeftAddr)
		if err != nil {
			return err
		}

		// Propagate split upward, including sibling remap
		return t.propagateSplit(stack, newLeftAddr, rightAddr, splitKey, leftSibRemap)
	}

	// No split: persist the modified leaf
	newLeafAddr, err := t.nodeMgr.Persist(leaf)
	if err != nil {
		return err
	}

	// If leaf is the root (stack has only 1 entry), just update rootAddr
	if len(stack) == 1 {
		t.rootAddr = newLeafAddr
		return nil
	}

	// Update left sibling's HighSibling to point to new leaf addr
	leftSibRemap, err := t.updateLeftSibling(stack, leafIdx, newLeafAddr)
	if err != nil {
		return err
	}

	// Build remaps: the leaf itself + optionally the left sibling
	remaps := []addrRemap{{oldAddr: leafAddr, newAddr: newLeafAddr}}
	if leftSibRemap.oldAddr.IsValid() {
		remaps = append(remaps, leftSibRemap)
	}

	return t.repersistPath(stack, leafIdx, remaps)
}

// propagateSplit handles split propagation up the tree.
// leftAddr is the NEW address of the left (original) node after persist.
// rightAddr is the NEW address of the right (split) node after persist.
// extraRemap is an additional address remap (e.g., left sibling update) to apply at this level.
func (t *tree) propagateSplit(stack []VAddr, leftAddr, rightAddr VAddr, splitKey PageID, extraRemap addrRemap) error {
	if len(stack) == 1 {
		// Leaf was root — create new root
		return t.splitRoot(leftAddr, rightAddr, splitKey)
	}

	parentIdx := len(stack) - 2
	parentAddr := stack[parentIdx]
	parent, err := t.nodeMgr.Load(parentAddr)
	if err != nil {
		return err
	}

	entries := ExtractInternalEntries(parent)

	// The old child addr is stack[len(stack)-1] (the address we traversed through)
	oldChildAddr := stack[len(stack)-1]

	// Update the existing entry to point to the new left addr
	for j := 0; j < int(parent.Count); j++ {
		if entries[j].Child == oldChildAddr {
			entries[j].Child = leftAddr
			break
		}
	}

	// Apply extra remap (e.g., left sibling update)
	if extraRemap.oldAddr.IsValid() {
		for j := 0; j < int(parent.Count); j++ {
			if entries[j].Child == extraRemap.oldAddr {
				entries[j].Child = extraRemap.newAddr
				break
			}
		}
	}

	StoreInternalEntries(parent, entries)

	// Now insert the new entry for the right child
	idx := t.nodeOps.Search(parent, splitKey)

	if int(parent.Count)+1 > int(parent.Capacity) {
		// Parent is full — need to split parent too

		// Insert the new entry into parent before splitting
		newEntries := make([]InternalEntry, parent.Count+1)
		if idx > int(parent.Count) {
			idx = int(parent.Count)
		}
		copy(newEntries, entries[:idx])
		newEntries[idx] = InternalEntry{Key: splitKey, Child: rightAddr}
		copy(newEntries[idx+1:], entries[idx:])
		parent.Count++
		StoreInternalEntries(parent, newEntries)

		// Split the overfull parent
		_, newParentRight, newSplitKey := t.nodeOps.Split(parent)

		// Set sibling links
		newParentRight.HighSibling = parent.HighSibling

		// Persist right parent first
		newParentRightAddr, err := t.nodeMgr.Persist(newParentRight)
		if err != nil {
			return err
		}

		// Update left parent's sibling
		parent.HighSibling = newParentRightAddr

		// Persist left parent
		newParentLeftAddr, err := t.nodeMgr.Persist(parent)
		if err != nil {
			return err
		}

		// Update left sibling of parent (if any) for sibling chain
		parentSibRemap, err := t.updateLeftSibling(stack[:parentIdx+1], parentIdx, newParentLeftAddr)
		if err != nil {
			return err
		}

		// Recurse: propagate the parent split upward
		newStack := make([]VAddr, parentIdx+1)
		copy(newStack, stack[:parentIdx+1])
		return t.propagateSplit(newStack, newParentLeftAddr, newParentRightAddr, newSplitKey, parentSibRemap)
	}

	// Parent has room — insert the new entry
	newEntries := make([]InternalEntry, parent.Count+1)
	if idx > int(parent.Count) {
		idx = int(parent.Count)
	}
	copy(newEntries, entries[:idx])
	newEntries[idx] = InternalEntry{Key: splitKey, Child: rightAddr}
	copy(newEntries[idx+1:], entries[idx:])
	parent.Count++
	StoreInternalEntries(parent, newEntries)

	// Persist parent
	newParentAddr, err := t.nodeMgr.Persist(parent)
	if err != nil {
		return err
	}

	// If parent is root, just update rootAddr
	if parentIdx == 0 {
		t.rootAddr = newParentAddr
		return nil
	}

	// Otherwise, re-persist path from parent to root
	remaps := []addrRemap{{oldAddr: parentAddr, newAddr: newParentAddr}}
	return t.repersistPath(stack[:parentIdx+1], parentIdx, remaps)
}

// splitRoot creates a new root after old root split.
func (t *tree) splitRoot(leftAddr, rightAddr VAddr, splitKey PageID) error {
	internalCapacity := uint16((vaddr.PageSize - NodeHeaderSize) / InternalEntrySize)
	newRoot := &NodeFormat{
		NodeType: NodeTypeInternal,
		Level:    1,
		Count:    0,
		Capacity: internalCapacity,
		RawData:  make([]byte, 0),
	}

	entries := make([]InternalEntry, 2)
	entries[0] = InternalEntry{Key: 0, Child: leftAddr}
	entries[1] = InternalEntry{Key: splitKey, Child: rightAddr}
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

	for i := len(stack) - 1; i >= 1; i-- {
		nodeAddr := stack[i]
		node, err := t.nodeMgr.Load(nodeAddr)
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

		parentAddr := stack[i-1]
		parent, err := t.nodeMgr.Load(parentAddr)
		if err != nil {
			return err
		}

		entries := ExtractInternalEntries(parent)

		var myIdx int
		for myIdx = 0; myIdx < int(parent.Count); myIdx++ {
			if entries[myIdx].Child == nodeAddr {
				break
			}
		}

		// Try left sibling redistribution
		if myIdx > 0 {
			leftAddr := entries[myIdx-1].Child
			if leftAddr.IsValid() {
				left, err := t.nodeMgr.Load(leftAddr)
				if err == nil && int(left.Count) > threshold {
					if node.NodeType == NodeTypeLeaf {
						t.redistributeLeaf(left, node)
						leafEntries := ExtractLeafEntries(node)
						entries[myIdx].Key = leafEntries[0].Key
					} else {
						t.redistributeInternal(left, node)
						intEntries := ExtractInternalEntries(node)
						entries[myIdx].Key = intEntries[0].Key
					}
					StoreInternalEntries(parent, entries)

					// Re-persist modified nodes
					newLeftAddr, err := t.nodeMgr.Persist(left)
					if err != nil {
						return err
					}
					newNodeAddr, err := t.nodeMgr.Persist(node)
					if err != nil {
						return err
					}

					// Update sibling chain if leaf
					var sibRemaps []addrRemap
					if node.NodeType == NodeTypeLeaf {
						// Left sibling of 'left' needs HighSibling update
						if myIdx-1 > 0 {
							prevAddr := entries[myIdx-2].Child
							prev, perr := t.nodeMgr.Load(prevAddr)
							if perr == nil {
								prev.HighSibling = newLeftAddr
								newPrevAddr, perr := t.nodeMgr.Persist(prev)
								if perr == nil {
									sibRemaps = append(sibRemaps, addrRemap{oldAddr: prevAddr, newAddr: newPrevAddr})
								}
							}
						}
					}

					remaps := []addrRemap{
						{oldAddr: leftAddr, newAddr: newLeftAddr},
						{oldAddr: nodeAddr, newAddr: newNodeAddr},
					}
					remaps = append(remaps, sibRemaps...)

					// Re-persist parent with updated child pointers
					entries = ExtractInternalEntries(parent)
					for _, r := range remaps {
						for j := 0; j < int(parent.Count); j++ {
							if entries[j].Child == r.oldAddr {
								entries[j].Child = r.newAddr
								break
							}
						}
					}
					StoreInternalEntries(parent, entries)
					newParentAddr, err := t.nodeMgr.Persist(parent)
					if err != nil {
						return err
					}

					stack[i] = newNodeAddr
					stack[i-1] = newParentAddr
					if i-1 == 0 {
						t.rootAddr = newParentAddr
					} else {
						err = t.repersistPath(stack, i-1, []addrRemap{{oldAddr: parentAddr, newAddr: newParentAddr}})
						if err != nil {
							return err
						}
					}
					continue
				}
			}
		}

		// Try right sibling redistribution
		if myIdx < int(parent.Count)-1 {
			rightAddr := entries[myIdx+1].Child
			if rightAddr.IsValid() {
				right, err := t.nodeMgr.Load(rightAddr)
				if err == nil && int(right.Count) > threshold {
					if node.NodeType == NodeTypeLeaf {
						t.redistributeLeafRight(node, right)
						leafEntries := ExtractLeafEntries(right)
						entries[myIdx+1].Key = leafEntries[0].Key
					} else {
						t.redistributeInternalRight(node, right)
						intEntries := ExtractInternalEntries(right)
						entries[myIdx+1].Key = intEntries[0].Key
					}
					StoreInternalEntries(parent, entries)

					newRightAddr, err := t.nodeMgr.Persist(right)
					if err != nil {
						return err
					}
					newNodeAddr, err := t.nodeMgr.Persist(node)
					if err != nil {
						return err
					}

					// Update sibling chain
					var sibRemaps []addrRemap
					if node.NodeType == NodeTypeLeaf && myIdx > 0 {
						prevAddr := entries[myIdx-1].Child
						prev, perr := t.nodeMgr.Load(prevAddr)
						if perr == nil {
							prev.HighSibling = newNodeAddr
							newPrevAddr, perr := t.nodeMgr.Persist(prev)
							if perr == nil {
								sibRemaps = append(sibRemaps, addrRemap{oldAddr: prevAddr, newAddr: newPrevAddr})
							}
						}
					}

					remaps := []addrRemap{
						{oldAddr: rightAddr, newAddr: newRightAddr},
						{oldAddr: nodeAddr, newAddr: newNodeAddr},
					}
					remaps = append(remaps, sibRemaps...)

					entries = ExtractInternalEntries(parent)
					for _, r := range remaps {
						for j := 0; j < int(parent.Count); j++ {
							if entries[j].Child == r.oldAddr {
								entries[j].Child = r.newAddr
								break
							}
						}
					}
					StoreInternalEntries(parent, entries)
					newParentAddr, err := t.nodeMgr.Persist(parent)
					if err != nil {
						return err
					}

					stack[i] = newNodeAddr
					stack[i-1] = newParentAddr
					if i-1 == 0 {
						t.rootAddr = newParentAddr
					} else {
						err = t.repersistPath(stack, i-1, []addrRemap{{oldAddr: parentAddr, newAddr: newParentAddr}})
						if err != nil {
							return err
						}
					}
					continue
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
			continue
		}

		sibling, err := t.nodeMgr.Load(siblingAddr)
		if err != nil {
			continue
		}

		if node.NodeType == NodeTypeLeaf {
			t.mergeLeafNodes(sibling, node)
		} else {
			t.mergeInternalNodes(sibling, node)
		}

		newSiblingAddr, err := t.nodeMgr.Persist(sibling)
		if err != nil {
			return err
		}

		// Update sibling chain for merge
		var sibRemaps []addrRemap
		if node.NodeType == NodeTypeLeaf {
			if mergeLeft && myIdx-1 > 0 {
				// Update left-left sibling's HighSibling
				prevAddr := entries[myIdx-2].Child
				prev, perr := t.nodeMgr.Load(prevAddr)
				if perr == nil {
					prev.HighSibling = newSiblingAddr
					newPrevAddr, perr := t.nodeMgr.Persist(prev)
					if perr == nil {
						sibRemaps = append(sibRemaps, addrRemap{oldAddr: prevAddr, newAddr: newPrevAddr})
					}
				}
			} else if !mergeLeft && myIdx > 0 {
				// Update left sibling's HighSibling to point to merged sibling
				prevAddr := entries[myIdx-1].Child
				prev, perr := t.nodeMgr.Load(prevAddr)
				if perr == nil {
					prev.HighSibling = newSiblingAddr
					newPrevAddr, perr := t.nodeMgr.Persist(prev)
					if perr == nil {
						sibRemaps = append(sibRemaps, addrRemap{oldAddr: prevAddr, newAddr: newPrevAddr})
					}
				}
			}
		}

		// Remove entry from parent and update sibling addr
		remaps := []addrRemap{{oldAddr: siblingAddr, newAddr: newSiblingAddr}}
		remaps = append(remaps, sibRemaps...)

		for _, r := range remaps {
			for j := 0; j < int(parent.Count); j++ {
				if entries[j].Child == r.oldAddr {
					entries[j].Child = r.newAddr
					break
				}
			}
		}

		if mergeLeft {
			copy(entries[myIdx:], entries[myIdx+1:])
		} else {
			copy(entries[myIdx+1:], entries[myIdx+2:])
		}
		parent.Count--
		StoreInternalEntries(parent, entries[:parent.Count])

		newParentAddr, err := t.nodeMgr.Persist(parent)
		if err != nil {
			return err
		}
		stack[i-1] = newParentAddr

		if i-1 == 0 && parent.Count == 1 {
			remainingEntries := ExtractInternalEntries(parent)
			childAddr := remainingEntries[0].Child
			child, err := t.nodeMgr.Load(childAddr)
			if err == nil {
				t.rootAddr = childAddr
				t.rootNode = child
			}
		} else if i-1 == 0 {
			t.rootAddr = newParentAddr
		} else {
			err = t.repersistPath(stack, i-1, []addrRemap{{oldAddr: parentAddr, newAddr: newParentAddr}})
			if err != nil {
				return err
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

	leafIdx := len(stack) - 1
	leafAddr := stack[leafIdx]
	leaf, err := t.nodeMgr.Load(leafAddr)
	if err != nil {
		return err
	}

	entries := ExtractLeafEntries(leaf)
	idx := t.nodeOps.Search(leaf, key)

	if idx >= int(leaf.Count) || entries[idx].Key != key {
		return ErrKeyNotFound
	}

	copy(entries[idx:], entries[idx+1:])
	leaf.Count--
	StoreLeafEntries(leaf, entries[:leaf.Count])

	newLeafAddr, err := t.nodeMgr.Persist(leaf)
	if err != nil {
		return err
	}

	if len(stack) == 1 {
		t.rootAddr = newLeafAddr
		return nil
	}

	// Update left sibling
	leftSibRemap, err := t.updateLeftSibling(stack, leafIdx, newLeafAddr)
	if err != nil {
		return err
	}

	remaps := []addrRemap{{oldAddr: leafAddr, newAddr: newLeafAddr}}
	if leftSibRemap.oldAddr.IsValid() {
		remaps = append(remaps, leftSibRemap)
	}

	err = t.repersistPath(stack, leafIdx, remaps)
	if err != nil {
		return err
	}

	// Rebuild stack with fresh addresses for underflow handling
	newStack, err := t.buildStack(key)
	if err != nil {
		// Key was deleted, leaf might be empty — that's OK
		return nil
	}

	return t.handleUnderflow(newStack)
}

// buildStack rebuilds the traversal stack from root to the leaf that would contain key.
func (t *tree) buildStack(key PageID) ([]VAddr, error) {
	rootAddr := t.rootAddr
	if !rootAddr.IsValid() {
		return nil, errors.New("blinktree: tree not initialized")
	}

	stack := make([]VAddr, 0, 16)
	stack = append(stack, rootAddr)

	currentAddr := rootAddr
	for {
		node, err := t.nodeMgr.Load(currentAddr)
		if err != nil {
			return nil, err
		}

		if node.NodeType == NodeTypeLeaf {
			break
		}

		entries := ExtractInternalEntries(node)
		idx := t.nodeOps.Search(node, key)
		if idx >= int(node.Count) {
			if node.Count == 0 {
				return nil, errors.New("blinktree: internal node has no entries")
			}
			idx = int(node.Count) - 1
		}
		childAddr := entries[idx].Child
		stack = append(stack, childAddr)
		currentAddr = childAddr
	}

	return stack, nil
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

type treeMutator = tree

// =============================================================================
// TreeIterator Implementation (DFS-based, no sibling chain dependency)
// =============================================================================

type treeIterator struct {
	tree       *tree
	rootAddr   VAddr
	start      PageID
	end        PageID
	finished   bool
	err        error
	currentKey PageID
	currentVal InlineValue

	// DFS-collected leaves
	leaves    []VAddr // all leaf addrs in order
	leafIdx   int     // current leaf index
	node      *NodeFormat
	entryIdx  int // current entry index within leaf
	inited    bool
}

// collectLeaves performs a DFS from addr, appending all leaf VAddrs in left-to-right order.
func collectLeaves(nodeMgr NodeManager, addr VAddr, out *[]VAddr) error {
	node, err := nodeMgr.Load(addr)
	if err != nil {
		return err
	}
	if node.NodeType == NodeTypeLeaf {
		*out = append(*out, addr)
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

	// First call: collect all leaves via DFS
	if !it.inited {
		it.inited = true
		it.leaves = make([]VAddr, 0, 64)
		if err := collectLeaves(it.tree.nodeMgr, it.rootAddr, &it.leaves); err != nil {
			it.err = err
			return false
		}
		if len(it.leaves) == 0 {
			it.finished = true
			return false
		}
		// Load first leaf
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

		// Skip entries before start
		for it.entryIdx < int(it.node.Count) && entries[it.entryIdx].Key < it.start {
			it.entryIdx++
		}

		// If done with current leaf, advance to next
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

		// Check end boundary
		if it.end > 0 && entries[it.entryIdx].Key >= it.end {
			it.finished = true
			return false
		}

		it.currentKey = entries[it.entryIdx].Key
		it.currentVal = entries[it.entryIdx].Value
		it.entryIdx++
		return true
	}
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
	it.leaves = nil
}
