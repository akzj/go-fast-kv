package internal

import (
	"bytes"
	"testing"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// newTestSegMgr creates a SegmentManager in a temporary directory.
func newTestSegMgr(t *testing.T) segmentapi.SegmentManager {
	t.Helper()
	dir := t.TempDir()
	mgr, err := segment.New(segmentapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to create SegmentManager: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

// newTestBlobStore creates a BlobStore with a real SegmentManager.
func newTestBlobStore(t *testing.T) *blobStore {
	t.Helper()
	segMgr := newTestSegMgr(t)
	bs := New(blobstoreapi.Config{}, segMgr)
	return bs.(*blobStore)
}

func TestWriteAndRead(t *testing.T) {
	bs := newTestBlobStore(t)

	data := []byte("hello, blobstore!")
	blobID, entry, err := bs.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if blobID != 1 {
		t.Errorf("expected blobID=1, got %d", blobID)
	}
	if entry.Type != 2 {
		t.Errorf("expected WALEntry.Type=2 (BlobMap), got %d", entry.Type)
	}

	got, err := bs.Read(blobID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Read data mismatch: got %q, want %q", got, data)
	}
}

func TestMultipleBlobs(t *testing.T) {
	bs := newTestBlobStore(t)

	blobs := [][]byte{
		[]byte("first blob"),
		[]byte("second blob with more data"),
		[]byte("third"),
	}

	ids := make([]uint64, len(blobs))
	for i, data := range blobs {
		id, _, err := bs.Write(data)
		if err != nil {
			t.Fatalf("Write[%d] failed: %v", i, err)
		}
		ids[i] = id
	}

	// Verify each blob can be read back
	for i, data := range blobs {
		got, err := bs.Read(ids[i])
		if err != nil {
			t.Fatalf("Read[%d] failed: %v", i, err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("Read[%d] mismatch: got %q, want %q", i, got, data)
		}
	}
}

func TestVariousSizes(t *testing.T) {
	bs := newTestBlobStore(t)

	sizes := []int{1, 100, 10 * 1024, 100 * 1024}
	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 251) // deterministic pattern
		}

		blobID, _, err := bs.Write(data)
		if err != nil {
			t.Fatalf("Write size=%d failed: %v", size, err)
		}

		got, err := bs.Read(blobID)
		if err != nil {
			t.Fatalf("Read size=%d failed: %v", size, err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("Read size=%d: data mismatch (len got=%d, want=%d)", size, len(got), len(data))
		}
	}
}

func TestReadUnallocated(t *testing.T) {
	bs := newTestBlobStore(t)

	_, err := bs.Read(999)
	if err != blobstoreapi.ErrBlobNotFound {
		t.Errorf("expected ErrBlobNotFound, got %v", err)
	}
}

func TestDeleteAndRead(t *testing.T) {
	bs := newTestBlobStore(t)

	blobID, _, err := bs.Write([]byte("to be deleted"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify readable before delete
	_, err = bs.Read(blobID)
	if err != nil {
		t.Fatalf("Read before delete failed: %v", err)
	}

	entry := bs.Delete(blobID)
	if entry.Type != 3 {
		t.Errorf("expected WALEntry.Type=3 (BlobFree), got %d", entry.Type)
	}
	if entry.ID != blobID {
		t.Errorf("expected WALEntry.ID=%d, got %d", blobID, entry.ID)
	}

	_, err = bs.Read(blobID)
	if err != blobstoreapi.ErrBlobNotFound {
		t.Errorf("expected ErrBlobNotFound after delete, got %v", err)
	}
}

func TestWALEntryValues(t *testing.T) {
	bs := newTestBlobStore(t)

	data := []byte("test data for WAL entry")
	blobID, entry, err := bs.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if entry.Type != 2 {
		t.Errorf("WALEntry.Type: got %d, want 2", entry.Type)
	}
	if entry.ID != blobID {
		t.Errorf("WALEntry.ID: got %d, want %d", entry.ID, blobID)
	}
	if entry.VAddr == 0 {
		t.Error("WALEntry.VAddr should not be 0")
	}
	if entry.Size != uint32(len(data)) {
		t.Errorf("WALEntry.Size: got %d, want %d", entry.Size, len(data))
	}
}

func TestRecoveryLoadMapping(t *testing.T) {
	// Create a BlobStore with real segment data
	segMgr := newTestSegMgr(t)
	bs1 := New(blobstoreapi.Config{}, segMgr).(*blobStore)

	data := []byte("recovery test data")
	blobID, _, err := bs1.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Get the mapping entry
	meta := bs1.getMapping(blobID)

	// Create a new BlobStore and load mapping
	bs2 := New(blobstoreapi.Config{}, segMgr).(*blobStore)
	bs2.LoadMapping([]blobstoreapi.MappingEntry{
		{BlobID: blobID, VAddr: meta.VAddr, Size: meta.Size},
	})
	bs2.SetNextBlobID(blobID + 1)

	// Should be able to read the blob
	got, err := bs2.Read(blobID)
	if err != nil {
		t.Fatalf("Read after LoadMapping failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %q, want %q", got, data)
	}
}

func TestRecoveryApplyBlobMap(t *testing.T) {
	segMgr := newTestSegMgr(t)
	bs1 := New(blobstoreapi.Config{}, segMgr).(*blobStore)

	data := []byte("apply blob map test")
	blobID, _, err := bs1.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	meta := bs1.getMapping(blobID)

	// Create a new BlobStore and apply WAL record
	bs2 := New(blobstoreapi.Config{}, segMgr).(*blobStore)
	bs2.ApplyBlobMap(blobID, meta.VAddr, meta.Size)
	bs2.SetNextBlobID(blobID + 1)

	got, err := bs2.Read(blobID)
	if err != nil {
		t.Fatalf("Read after ApplyBlobMap failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %q, want %q", got, data)
	}
}

func TestRecoveryApplyBlobFree(t *testing.T) {
	segMgr := newTestSegMgr(t)
	bs := New(blobstoreapi.Config{}, segMgr).(*blobStore)

	data := []byte("to be freed via recovery")
	blobID, _, err := bs.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify readable
	_, err = bs.Read(blobID)
	if err != nil {
		t.Fatalf("Read before ApplyBlobFree failed: %v", err)
	}

	// Apply free via recovery
	bs.ApplyBlobFree(blobID)

	_, err = bs.Read(blobID)
	if err != blobstoreapi.ErrBlobNotFound {
		t.Errorf("expected ErrBlobNotFound after ApplyBlobFree, got %v", err)
	}
}

func TestCloseReturnsErrClosed(t *testing.T) {
	bs := newTestBlobStore(t)

	if err := bs.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, _, err := bs.Write([]byte("after close"))
	if err != blobstoreapi.ErrClosed {
		t.Errorf("Write after Close: expected ErrClosed, got %v", err)
	}

	_, err = bs.Read(1)
	if err != blobstoreapi.ErrClosed {
		t.Errorf("Read after Close: expected ErrClosed, got %v", err)
	}
}

func TestNextBlobID(t *testing.T) {
	bs := newTestBlobStore(t)

	if got := bs.NextBlobID(); got != 1 {
		t.Errorf("initial NextBlobID: got %d, want 1", got)
	}

	bs.Write([]byte("one"))
	if got := bs.NextBlobID(); got != 2 {
		t.Errorf("after 1 write NextBlobID: got %d, want 2", got)
	}

	bs.Write([]byte("two"))
	bs.Write([]byte("three"))
	if got := bs.NextBlobID(); got != 4 {
		t.Errorf("after 3 writes NextBlobID: got %d, want 4", got)
	}
}

func TestSetNextBlobID(t *testing.T) {
	bs := newTestBlobStore(t)

	bs.SetNextBlobID(100)
	if got := bs.NextBlobID(); got != 100 {
		t.Errorf("after SetNextBlobID(100): got %d, want 100", got)
	}

	// Next allocation should use 100
	blobID, _, err := bs.Write([]byte("after set"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if blobID != 100 {
		t.Errorf("expected blobID=100, got %d", blobID)
	}
}

func TestEmptyBlobWrite(t *testing.T) {
	bs := newTestBlobStore(t)

	// Writing an empty blob should work
	blobID, entry, err := bs.Write([]byte{})
	if err != nil {
		t.Fatalf("Write empty blob failed: %v", err)
	}
	if blobID != 1 {
		t.Errorf("expected blobID=1, got %d", blobID)
	}
	if entry.Size != 0 {
		t.Errorf("expected WALEntry.Size=0, got %d", entry.Size)
	}

	got, err := bs.Read(blobID)
	if err != nil {
		t.Fatalf("Read empty blob failed: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty blob, got %d bytes", len(got))
	}
}
