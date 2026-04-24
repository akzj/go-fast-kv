package internal

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/lock"
)

type bTree struct {
	pages       pageStore
	blobs       btreeapi.BlobWriter
	rootPageID  atomic.Uint64        // atomic for concurrent reads (§3.8.8)
	inlineThres int
	closed      atomic.Bool          // atomic for concurrent close check
	pageLocks   *lock.PageRWLocks    // per-page RwLock manager (§3.8.1)
	bootstrapMu sync.Mutex           // protects root creation when rootPageID == 0
	visCheck    func(txnMin, txnMax, readTxnID uint64) bool // MVCC visibility via CLOG (nil = default range check)

	// Search path statistics for bottleneck analysis.
	searchDepthSum   atomic.Uint64
	searchCount      atomic.Uint64
	rightSiblingNavs atomic.Uint64

	// Split statistics.
	splitCount atomic.Uint64
}

// New creates a new BTree instance.
func New(cfg btreeapi.Config, pages pageStore, blobs btreeapi.BlobWriter) btreeapi.BTree {
	thresh := cfg.InlineThreshold
	if thresh <= 0 {
		thresh = btreeapi.InlineThreshold
	}
	return &bTree{
		pages:       pages,
		blobs:       blobs,
		inlineThres: thresh,
		pageLocks:   lock.New(),
		visCheck:    cfg.VisibilityChecker,
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
func (t *bTree) PageLocks() *lock.PageRWLocks {
	return t.pageLocks
}

// NewBulkLoader creates a new BulkLoader for efficient bulk loading.
func (t *bTree) NewBulkLoader(mode btreeapi.BulkMode) btreeapi.BulkLoader {
	return newBulkLoader(t, mode, 0)
}

// NewBulkLoaderWithTxn creates a BulkLoader with an explicit transaction ID.
func (t *bTree) NewBulkLoaderWithTxn(mode btreeapi.BulkMode, txnID uint64) btreeapi.BulkLoader {
	return newBulkLoader(t, mode, txnID)
}

// isVisible checks if a version (txnMin, txnMax) is visible.
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
			root := NewLeafPage()
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
	leaf, err := t.pages.ReadPageForWrite(leafPID)
	if err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}

	// B-link correction under write lock
	for leaf.HighKey() != nil && bytes.Compare(key, leaf.HighKey()) >= 0 && leaf.Next() != 0 {
		nextPID := leaf.Next()
		t.pageLocks.WUnlock(leafPID)
		leafPID = nextPID
		t.pageLocks.WLock(leafPID)
		leaf, err = t.pages.ReadPageForWrite(leafPID)
		if err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
	}

	// Phase 3: MVCC — mark old version as superseded
	t.mvccMarkOld(leaf, key, txnID)

	// Prepare value
	var blobID uint64
	var inlineVal []byte
	if t.blobs != nil && len(value) > t.inlineThres {
		bid, err := t.blobs.WriteBlob(value)
		if err == nil {
			blobID = bid
		} else {
			inlineVal = value
		}
	} else {
		inlineVal = value
	}

	// Calculate entry size needed
	isBlobRef := blobID > 0
	cellSize := LeafCellSize(len(key), len(inlineVal), isBlobRef)

	// "先判断后分裂" — check if split needed BEFORE insert
	if leaf.FreeSpace() < cellSize+2 { // +2 for new slot
		// Split first, then insert into correct half
		splitKey, right := t.splitLeafPage(leaf)
		rightPID := t.pages.AllocPage()
		t.splitCount.Add(1)

		// Set up B-link pointers
		right.SetNext(leaf.Next())
		leaf.SetNext(rightPID)

		// right inherits original HighKey, left gets splitKey
		rightHK := leaf.HighKey()
		if rightHK != nil {
			right.SetHighKey(rightHK)
		}
		leaf.SetHighKey(splitKey)

		// Determine which page gets the new entry
		pos := leaf.FindInsertPos(key, txnID)
		if bytes.Compare(key, splitKey) >= 0 {
			rPos := right.FindInsertPos(key, txnID)
			right.InsertLeafEntry(rPos, key, txnID, btreeapi.TxnMaxInfinity, inlineVal, blobID)
		} else {
			leaf.InsertLeafEntry(pos, key, txnID, btreeapi.TxnMaxInfinity, inlineVal, blobID)
		}

		if err := t.pages.WritePage(rightPID, right); err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
		if err := t.pages.WritePage(leafPID, leaf); err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
		t.pageLocks.WUnlock(leafPID)

		return t.propagateSplit(path[:len(path)-1], splitKey, rightPID)
	}

	// No split needed — direct insert
	pos := leaf.FindInsertPos(key, txnID)
	if err := leaf.InsertLeafEntry(pos, key, txnID, btreeapi.TxnMaxInfinity, inlineVal, blobID); err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}
	if err := t.pages.WritePage(leafPID, leaf); err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}
	t.pageLocks.WUnlock(leafPID)
	return nil
}

// mvccMarkOld marks the current visible version as superseded.
func (t *bTree) mvccMarkOld(leaf *Page, key []byte, txnID uint64) {
	lo := leaf.SearchLeaf(key)
	count := leaf.Count()
	for i := lo; i < count; i++ {
		if !bytes.Equal(leaf.EntryKey(i), key) {
			break
		}
		if leaf.EntryTxnMax(i) == btreeapi.TxnMaxInfinity {
			leaf.SetEntryTxnMax(i, txnID)
			break
		}
	}
}

// splitLeafPage splits a leaf page, handling MVCC version chain boundaries.
func (t *bTree) splitLeafPage(page *Page) (splitKey []byte, right *Page) {
	count := page.Count()
	mid := count / 2

	// Don't split in the middle of a version chain — find a key boundary
	origMid := mid
	for mid < count-1 && bytes.Equal(page.EntryKey(mid), page.EntryKey(mid-1)) {
		mid++
	}
	// If we went all the way to the end, try going left
	if mid >= count-1 {
		mid = origMid
		for mid > 1 && bytes.Equal(page.EntryKey(mid), page.EntryKey(mid-1)) {
			mid--
		}
	}

	// All entries share the same key — use synthetic splitKey
	allSameKey := mid <= 1 && count > 1 &&
		bytes.Equal(page.EntryKey(0), page.EntryKey(count-1))

	if allSameKey {
		mid = count / 2
		sk := page.EntryKey(0)
		splitKey = make([]byte, len(sk)+1)
		copy(splitKey, sk)
		splitKey[len(sk)] = 0x00
	}

	sk, right := page.SplitLeaf(mid)
	if splitKey == nil {
		splitKey = sk
	}
	return splitKey, right
}

// ─── Search (§3.8.2 search phase) ──────────────────────────────────

func (t *bTree) searchPath(key []byte) (path []uint64, err error) {
	path = make([]uint64, 0, 4) // pre-alloc for typical tree depth
	currentPID := t.rootPageID.Load()
	depth := 0

	for {
		t.pageLocks.RLock(currentPID)
		page, err := t.pages.ReadPage(currentPID)
		if err != nil {
			t.pageLocks.RUnlock(currentPID)
			return nil, err
		}

		depth++

		// B-link right-link correction — BEFORE appending to path.
		// If the key is beyond this page's HighKey, the page was split
		// concurrently and we must follow the right link. Do NOT append
		// the stale page to the path, otherwise propagateSplit would
		// treat a leaf sibling as an internal parent.
		if page.HighKey() != nil && bytes.Compare(key, page.HighKey()) >= 0 && page.Next() != 0 {
			nextPID := page.Next()
			t.pageLocks.RUnlock(currentPID)
			t.rightSiblingNavs.Add(1)
			currentPID = nextPID
			continue
		}

		// This is the correct page for this key — append to path
		path = append(path, currentPID)

		if page.IsLeaf() {
			t.pageLocks.RUnlock(currentPID)
			t.searchDepthSum.Add(uint64(depth))
			t.searchCount.Add(1)
			return path, nil
		}

		childPID := page.FindChild(key)
		t.pageLocks.RUnlock(currentPID)
		currentPID = childPID
	}
}

// ─── Split propagation (§3.8.4) ────────────────────────────────────

func (t *bTree) propagateSplit(path []uint64, splitKey []byte, newChildPID uint64) error {
	for i := len(path) - 1; i >= 0; i-- {
		parentPID := path[i]
		t.pageLocks.WLock(parentPID)
		parent, err := t.pages.ReadPageForWrite(parentPID)
		if err != nil {
			t.pageLocks.WUnlock(parentPID)
			return err
		}

		// B-link correction: parent may have been split concurrently
		for parent.HighKey() != nil && bytes.Compare(splitKey, parent.HighKey()) >= 0 && parent.Next() != 0 {
			nextPID := parent.Next()
			t.pageLocks.WUnlock(parentPID)
			parentPID = nextPID
			t.pageLocks.WLock(parentPID)
			parent, err = t.pages.ReadPageForWrite(parentPID)
			if err != nil {
				t.pageLocks.WUnlock(parentPID)
				return err
			}
		}

		// Find insert position for the split key
		pos := sort.Search(parent.Count(), func(i int) bool {
			return bytes.Compare(splitKey, parent.InternalKey(i)) <= 0
		})

		// Check if internal page needs to split before inserting
		cellSize := InternalCellSize(len(splitKey))
		if parent.FreeSpace() < cellSize+2 {
			// Split internal page first
			newSplitKey, rightPage := parent.SplitInternal(parent.Count() / 2)
			newParentPID := t.pages.AllocPage()

			rightPage.SetNext(parent.Next())
			parent.SetNext(newParentPID)
			rightHK := parent.HighKey()
			if rightHK != nil {
				rightPage.SetHighKey(rightHK)
			}
			parent.SetHighKey(newSplitKey)

			// Insert the split key into the correct half
			if bytes.Compare(splitKey, newSplitKey) >= 0 {
				rPos := sort.Search(rightPage.Count(), func(i int) bool {
					return bytes.Compare(splitKey, rightPage.InternalKey(i)) <= 0
				})
				rightPage.InsertInternalEntry(rPos, splitKey, newChildPID)
			} else {
				pos = sort.Search(parent.Count(), func(i int) bool {
					return bytes.Compare(splitKey, parent.InternalKey(i)) <= 0
				})
				parent.InsertInternalEntry(pos, splitKey, newChildPID)
			}

			if err := t.pages.WritePage(newParentPID, rightPage); err != nil {
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
			continue
		}

		// Insert fits — no split needed
		if err := parent.InsertInternalEntry(pos, splitKey, newChildPID); err != nil {
			t.pageLocks.WUnlock(parentPID)
			return err
		}
		if err := t.pages.WritePage(parentPID, parent); err != nil {
			t.pageLocks.WUnlock(parentPID)
			return err
		}
		t.pageLocks.WUnlock(parentPID)
		return nil // Done — no further propagation needed
	}

	// Reached root and still need to split → create new root
	t.bootstrapMu.Lock()
	newRoot := NewInternalPage()
	newRoot.SetChild0(t.rootPageID.Load())
	newRoot.InsertInternalEntry(0, splitKey, newChildPID)
	newRootPID := t.pages.AllocPage()
	if err := t.pages.WritePage(newRootPID, newRoot); err != nil {
		t.bootstrapMu.Unlock()
		return err
	}
	t.rootPageID.Store(newRootPID)
	t.bootstrapMu.Unlock()
	return nil
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
		page, err := t.pages.ReadPage(currentPID)
		if err != nil {
			t.pageLocks.RUnlock(currentPID)
			return nil, err
		}

		// B-link correction
		if page.HighKey() != nil && bytes.Compare(key, page.HighKey()) >= 0 && page.Next() != 0 {
			nextPID := page.Next()
			t.pageLocks.RUnlock(currentPID)
			currentPID = nextPID
			continue
		}

		if page.IsLeaf() {
			// Find visible entry using binary search
			lo := page.SearchLeaf(key)
			count := page.Count()
			for i := lo; i < count; i++ {
				eKey := page.EntryKey(i)
				cmp := bytes.Compare(eKey, key)
				if cmp > 0 {
					break
				}
				if cmp == 0 && t.isVisible(page.EntryTxnMin(i), page.EntryTxnMax(i), txnID) {
					val := page.EntryValue(i)
					t.pageLocks.RUnlock(currentPID)
					return t.resolveValue(&val)
				}
			}
			t.pageLocks.RUnlock(currentPID)
			return nil, btreeapi.ErrKeyNotFound
		}

		// Internal node: descend to child
		childPID := page.FindChild(key)
		t.pageLocks.RUnlock(currentPID)
		currentPID = childPID
	}
}

func (t *bTree) resolveValue(v *btreeapi.Value) ([]byte, error) {
	if v.BlobID > 0 && t.blobs != nil {
		data, err := t.blobs.ReadBlob(v.BlobID)
		if err != nil {
			return nil, btreeapi.ErrKeyNotFound
		}
		return data, nil
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

	// Phase 1: Search down to leaf
	path, err := t.searchPath(key)
	if err != nil {
		return err
	}

	// Phase 2: Write-lock the leaf
	leafPID := path[len(path)-1]
	t.pageLocks.WLock(leafPID)
	leaf, err := t.pages.ReadPageForWrite(leafPID)
	if err != nil {
		t.pageLocks.WUnlock(leafPID)
		return err
	}

	// B-link correction under write lock
	for leaf.HighKey() != nil && bytes.Compare(key, leaf.HighKey()) >= 0 && leaf.Next() != 0 {
		nextPID := leaf.Next()
		t.pageLocks.WUnlock(leafPID)
		leafPID = nextPID
		t.pageLocks.WLock(leafPID)
		leaf, err = t.pages.ReadPageForWrite(leafPID)
		if err != nil {
			t.pageLocks.WUnlock(leafPID)
			return err
		}
	}

	// Phase 3: MVCC delete (mark TxnMax)
	count := leaf.Count()
	for i := 0; i < count; i++ {
		eKey := leaf.EntryKey(i)
		if !bytes.Equal(eKey, key) {
			continue
		}
		isOwn := leaf.EntryTxnMin(i) == txnID
		if isOwn || t.isVisible(leaf.EntryTxnMin(i), leaf.EntryTxnMax(i), txnID) {
			leaf.SetEntryTxnMax(i, txnID)
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
		page, err := t.pages.ReadPage(currentPID)
		if err != nil {
			t.pageLocks.RUnlock(currentPID)
			it.err = err
			it.done = true
			return it
		}

		// B-link correction
		if page.HighKey() != nil && bytes.Compare(start, page.HighKey()) >= 0 && page.Next() != 0 {
			nextPID := page.Next()
			t.pageLocks.RUnlock(currentPID)
			currentPID = nextPID
			continue
		}

		if page.IsLeaf() {
			// Clone the page so we can release the lock
			it.curPage = page.Clone()
			t.pageLocks.RUnlock(currentPID)
			it.curIdx = 0

			// Advance curIdx to the first entry >= start
			count := it.curPage.Count()
			for it.curIdx < count {
				if bytes.Compare(it.curPage.EntryKey(it.curIdx), start) >= 0 {
					break
				}
				it.curIdx++
			}
			return it
		}

		childPID := page.FindChild(start)
		t.pageLocks.RUnlock(currentPID)
		currentPID = childPID
	}
}

type iterator struct {
	tree     *bTree
	endKey   []byte
	txnID    uint64
	visCheck func(txnMin, txnMax, readTxnID uint64) bool
	curPage  *Page
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

	// Guard against use-after-store-close (KI-2)
	if it.tree.closed.Load() {
		it.done = true
		return false
	}

	for {
		// Move to next leaf if needed
		for it.curIdx >= it.curPage.Count() {
			if it.curPage.Next() == 0 {
				it.done = true
				return false
			}
			// Read next leaf with read lock, clone, release
			nextPID := it.curPage.Next()
			it.tree.pageLocks.RLock(nextPID)
			page, err := it.tree.pages.ReadPage(nextPID)
			if err != nil {
				it.tree.pageLocks.RUnlock(nextPID)
				it.err = err
				return false
			}
			it.curPage = page.Clone()
			it.tree.pageLocks.RUnlock(nextPID)
			it.curIdx = 0
		}

		eKey := it.curPage.EntryKey(it.curIdx)
		eTxnMin := it.curPage.EntryTxnMin(it.curIdx)
		eTxnMax := it.curPage.EntryTxnMax(it.curIdx)
		eVal := it.curPage.EntryValue(it.curIdx)
		it.curIdx++

		// Check end boundary
		if it.endKey != nil && bytes.Compare(eKey, it.endKey) >= 0 {
			it.done = true
			return false
		}

		// Skip duplicate keys (dedup: only first visible version per key)
		if it.lastKey != nil && bytes.Equal(eKey, it.lastKey) {
			continue
		}

		// Visibility check
		visible := false
		if it.visCheck != nil {
			visible = it.visCheck(eTxnMin, eTxnMax, it.txnID)
		} else {
			visible = it.tree.isVisible(eTxnMin, eTxnMax, it.txnID)
		}
		if visible {
			val, err := it.tree.resolveValue(&eVal)
			if err != nil {
				it.err = err
				return false
			}
			it.curKey = eKey
			it.curValue = val
			it.lastKey = eKey
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

// ─── BTree Metrics Accessors ─────────────────────────────────────────

func (t *bTree) GetStats() btreeapi.BTreeStats {
	return btreeapi.BTreeStats{
		SearchDepthSum:   t.searchDepthSum.Load(),
		SearchCount:      t.searchCount.Load(),
		RightSiblingNavs: t.rightSiblingNavs.Load(),
		SplitCount:       t.splitCount.Load(),
	}
}

func (t *bTree) GetAvgSearchDepth() float64 {
	count := t.searchCount.Load()
	if count == 0 {
		return 0
	}
	return float64(t.searchDepthSum.Load()) / float64(count)
}

func (t *bTree) ResetStats() {
	t.searchDepthSum.Store(0)
	t.searchCount.Store(0)
	t.rightSiblingNavs.Store(0)
	t.splitCount.Store(0)
}
