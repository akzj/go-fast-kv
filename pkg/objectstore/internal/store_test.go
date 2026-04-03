package internal

import (
	"context"
	"crypto/rand"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// --- MappingIndex Tests ---

func TestMappingIndex_GetPutDelete(t *testing.T) {
	idx := NewMappingIndex()
	ctx := context.Background()
	_ = ctx

	// Initially empty
	_, ok := idx.Get(api.MakeObjectID(api.ObjectTypePage, 1))
	if ok {
		t.Fatal("expected not found for new index")
	}

	// Put
	loc := api.ObjectLocation{SegmentID: 1, Offset: 100, Size: 4096}
	idx.Put(api.MakeObjectID(api.ObjectTypePage, 1), loc)

	// Get
	got, ok := idx.Get(api.MakeObjectID(api.ObjectTypePage, 1))
	if !ok {
		t.Fatal("expected to find after put")
	}
	if got != loc {
		t.Fatalf("expected %+v, got %+v", loc, got)
	}

	// Delete
	idx.Delete(api.MakeObjectID(api.ObjectTypePage, 1))
	_, ok = idx.Get(api.MakeObjectID(api.ObjectTypePage, 1))
	if ok {
		t.Fatal("expected not found after delete")
	}
}

func TestMappingIndex_Iterate(t *testing.T) {
	idx := NewMappingIndex()

	// Put several entries
	for i := uint64(1); i <= 5; i++ {
		idx.Put(api.MakeObjectID(api.ObjectTypePage, uint64(i)), api.ObjectLocation{SegmentID: api.SegmentID(i), Offset: uint32(i * 100), Size: 4096})
	}

	count := 0
	idx.Iterate(func(objID api.ObjectID, loc api.ObjectLocation) {
		count++
		if objID.GetSequence() < 1 || objID.GetSequence() > 5 {
			t.Errorf("unexpected objID: %d", objID)
		}
	})

	if count != 5 {
		t.Fatalf("expected 5 entries, got %d", count)
	}
}

func TestMappingIndex_Concurrent(t *testing.T) {
	idx := NewMappingIndex()
	ctx := context.Background()
	_ = ctx

	var wg sync.WaitGroup
	N := 100

	// Concurrent writes
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			idx.Put(api.MakeObjectID(api.ObjectTypePage, uint64(i)), api.ObjectLocation{SegmentID: 1, Offset: uint32(i), Size: 4096})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			idx.Get(api.MakeObjectID(api.ObjectTypePage, uint64(i)))
		}(i)
	}

	wg.Wait()

	// Verify count
	count := 0
	idx.Iterate(func(objID api.ObjectID, loc api.ObjectLocation) {
		count++
	})

	if count != N {
		t.Errorf("expected %d entries, got %d", N, count)
	}
}

// --- SegmentManager Tests ---

func TestSegmentManager_CreateAndAppend(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("create segment manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()

	// Get active page segment
	seg, err := sm.getOrCreateActivePage(ctx)
	if err != nil {
		t.Fatalf("get active page segment: %v", err)
	}

	// Append data
	data := make([]byte, api.PageSize)
	rand.Read(data)
	checksum := crc32Update(0, data)

	header := &api.ObjectHeader{
		Magic:    [2]byte{api.MagicByte1, api.MagicByte2},
		Version:  api.HeaderVersion,
		Type:     api.ObjectTypePage,
		Checksum: checksum,
		Size:     uint32(len(data)),
	}

	offset, err := seg.Append(ctx, header, data)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	// Verify file exists
	path := filepath.Join(dir, pageSegmentFileName(seg.ID))
	if _, err := os.Stat(path); err != nil {
		t.Errorf("segment file not found: %v", err)
	}
}

func TestSegmentManager_SealAndRotate(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("create segment manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()

	// Get first segment
	seg1, err := sm.getOrCreateActivePage(ctx)
	if err != nil {
		t.Fatalf("get active page segment: %v", err)
	}

	// Seal it
	if err := sm.SealAndRotate(ctx, api.SegmentTypePage); err != nil {
		t.Fatalf("seal and rotate: %v", err)
	}

	// Get new active segment
	seg2, err := sm.getOrCreateActivePage(ctx)
	if err != nil {
		t.Fatalf("get new active page segment: %v", err)
	}

	if seg1.ID == seg2.ID {
		t.Error("expected different segment IDs after rotation")
	}

	if !seg1.sealed.Load() {
		t.Error("expected first segment to be sealed")
	}
}

func TestSegmentManager_ReadBack(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("create segment manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()

	// Create segment and write
	seg, err := sm.getOrCreateActivePage(ctx)
	if err != nil {
		t.Fatalf("get active page segment: %v", err)
	}

	data := make([]byte, api.PageSize)
	rand.Read(data)
	checksum := crc32Update(0, data)

	header := &api.ObjectHeader{
		Magic:    [2]byte{api.MagicByte1, api.MagicByte2},
		Version:  api.HeaderVersion,
		Type:     api.ObjectTypePage,
		Checksum: checksum,
		Size:     uint32(len(data)),
	}

	offset, err := seg.Append(ctx, header, data)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Seal
	if err := sm.SealAndRotate(ctx, api.SegmentTypePage); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Read back
	seg, err = sm.GetSegment(ctx, seg.ID, api.SegmentTypePage)
	if err != nil {
		t.Fatalf("get sealed segment: %v", err)
	}

	_, readData, err := seg.Read(ctx, offset, uint32(len(data)))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(readData) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(readData))
	}
}

func TestSegmentManager_Sync(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("create segment manager: %v", err)
	}
	defer sm.Close()

	if err := sm.SyncAll(); err != nil {
		t.Fatalf("sync all: %v", err)
	}
}

// --- ObjectStore Tests ---

func TestObjectStore_PageWriteRead(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Allocate page
	pageID, err := store.AllocPage(ctx)
	if err != nil {
		t.Fatalf("alloc page: %v", err)
	}

	if pageID.GetObjectIDType() != api.ObjectTypePage {
		t.Errorf("expected ObjectTypePage, got %v", pageID.GetObjectIDType())
	}

	// Write page
	data := make([]byte, api.PageSize)
	rand.Read(data)

	writtenID, err := store.WritePage(ctx, pageID, data)
	if err != nil {
		t.Fatalf("write page: %v", err)
	}

	// Read back
	readData, err := store.ReadPage(ctx, writtenID)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}

	if len(readData) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(readData))
	}

	if string(readData) != string(data) {
		t.Error("data mismatch")
	}
}

func TestObjectStore_BlobWriteRead(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Write small blob
	blobData := make([]byte, 1024)
	rand.Read(blobData)

	blobID, err := store.WriteBlob(ctx, blobData)
	if err != nil {
		t.Fatalf("write blob: %v", err)
	}

	if blobID.GetObjectIDType() != api.ObjectTypeBlob {
		t.Errorf("expected ObjectTypeBlob, got %v", blobID.GetObjectIDType())
	}

	// Read back
	readData, err := store.ReadBlob(ctx, blobID)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}

	if len(readData) != len(blobData) {
		t.Errorf("expected %d bytes, got %d", len(blobData), len(readData))
	}

	if string(readData) != string(blobData) {
		t.Error("data mismatch")
	}
}

func TestObjectStore_LargeBlobWriteRead(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Write large blob (>= 256MB threshold)
	largeData := make([]byte, api.LargeBlobThreshold+1024)
	rand.Read(largeData)

	blobID, err := store.WriteBlob(ctx, largeData)
	if err != nil {
		t.Fatalf("write large blob: %v", err)
	}

	if blobID.GetObjectIDType() != api.ObjectTypeLarge {
		t.Errorf("expected ObjectTypeLarge, got %v", blobID.GetObjectIDType())
	}

	// Read back
	readData, err := store.ReadBlob(ctx, blobID)
	if err != nil {
		t.Fatalf("read large blob: %v", err)
	}

	if len(readData) != len(largeData) {
		t.Errorf("expected %d bytes, got %d", len(largeData), len(readData))
	}

	if string(readData) != string(largeData) {
		t.Error("data mismatch")
	}
}

func TestObjectStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Write page
	pageID, err := store.AllocPage(ctx)
	if err != nil {
		t.Fatalf("alloc page: %v", err)
	}

	data := make([]byte, api.PageSize)
	rand.Read(data)

	writtenID, err := store.WritePage(ctx, pageID, data)
	if err != nil {
		t.Fatalf("write page: %v", err)
	}

	// Delete
	if err := store.Delete(ctx, writtenID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify not found
	_, err = store.ReadPage(ctx, writtenID)
	if err != api.ErrObjectNotFound {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestObjectStore_GetLocation(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Write blob
	blobData := make([]byte, 1024)
	rand.Read(blobData)

	blobID, err := store.WriteBlob(ctx, blobData)
	if err != nil {
		t.Fatalf("write blob: %v", err)
	}

	// Get location
	loc, err := store.GetLocation(ctx, blobID)
	if err != nil {
		t.Fatalf("get location: %v", err)
	}

	if loc.SegmentID == 0 {
		t.Error("expected non-zero segment ID")
	}

	if loc.Size != uint32(len(blobData)) {
		t.Errorf("expected size %d, got %d", len(blobData), loc.Size)
	}
}

func TestObjectStore_Sync(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	if err := store.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestObjectStore_NotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Read non-existent page
	_, err = store.ReadPage(ctx, api.MakeObjectID(api.ObjectTypePage, 999))
	if err != api.ErrObjectNotFound {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}

	// Read non-existent blob
	_, err = store.ReadBlob(ctx, api.MakeObjectID(api.ObjectTypeBlob, 999))
	if err != api.ErrObjectNotFound {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}

	// GetLocation for non-existent
	_, err = store.GetLocation(ctx, api.MakeObjectID(api.ObjectTypePage, 999))
	if err != api.ErrObjectNotFound {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}
}

// --- Checksum Verification ---

func TestObjectStore_ChecksumVerification(t *testing.T) {
	dir := t.TempDir()
	store, err := NewObjectStore(dir)
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Write page
	pageID, err := store.AllocPage(ctx)
	if err != nil {
		t.Fatalf("alloc page: %v", err)
	}

	data := make([]byte, api.PageSize)
	rand.Read(data)

	writtenID, err := store.WritePage(ctx, pageID, data)
	if err != nil {
		t.Fatalf("write page: %v", err)
	}

	// Corrupt the segment file
	loc, _ := store.GetLocation(ctx, writtenID)
	segPath := filepath.Join(dir, pageSegmentFileName(loc.SegmentID))
	f, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	// Flip a byte in the data section
	f.WriteAt([]byte{0xFF}, int64(loc.Offset)+api.ObjectHeaderSize)
	f.Close()

	// Read should fail checksum
	_, err = store.ReadPage(ctx, writtenID)
	if err == nil {
		t.Error("expected checksum error after corruption")
	}
}

// --- Helper ---

func crc32Update(crc uint32, data []byte) uint32 {
	tab := crc32.MakeTable(crc32.IEEE)
	return crc32.Update(crc, tab, data)
}
