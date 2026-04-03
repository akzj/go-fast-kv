package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/go-fast-kv/pkg/wal/api"
)

func TestMarshalUnmarshalEntry(t *testing.T) {
	// Test basic entry marshaling and unmarshaling
	entry := &api.WALEntry{
		Type:    api.WALEntryTypeObjectStore,
		Payload: []byte("test payload data"),
	}

	w := &WALImpl{}

	// Marshal
	data, err := w.marshalEntry(entry)
	if err != nil {
		t.Fatalf("marshalEntry failed: %v", err)
	}

	// Verify binary format: [Type:1][Length:4][Payload:n][Checksum:4]
	if len(data) != 9+len(entry.Payload) {
		t.Fatalf("expected length %d, got %d", 9+len(entry.Payload), len(data))
	}

	// Check type byte
	if data[0] != byte(api.WALEntryTypeObjectStore) {
		t.Fatalf("expected type %d, got %d", api.WALEntryTypeObjectStore, data[0])
	}

	// Check length
	length := binary.BigEndian.Uint32(data[1:5])
	if length != uint32(len(entry.Payload)) {
		t.Fatalf("expected length %d, got %d", len(entry.Payload), length)
	}

	// Check payload
	if !bytes.Equal(data[5:5+len(entry.Payload)], entry.Payload) {
		t.Fatalf("payload mismatch")
	}

	// Unmarshal
	unmarshaled, err := w.unmarshalEntry(data)
	if err != nil {
		t.Fatalf("unmarshalEntry failed: %v", err)
	}

	// Verify
	if unmarshaled.Type != entry.Type {
		t.Fatalf("type mismatch: expected %d, got %d", entry.Type, unmarshaled.Type)
	}
	if !bytes.Equal(unmarshaled.Payload, entry.Payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestChecksumMismatch(t *testing.T) {
	w := &WALImpl{}

	// Create valid entry
	entry := &api.WALEntry{
		Type:    api.WALEntryTypeBTree,
		Payload: []byte("test data"),
	}

	data, _ := w.marshalEntry(entry)

	// Corrupt the payload
	data[5] ^= 0xFF

	// Try to unmarshal - should fail
	_, err := w.unmarshalEntry(data)
	if err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestWriteBuffers(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	// Write should queue entry for async processing (no disk I/O yet)
	entry := &api.WALEntry{
		Type:    api.WALEntryTypeObjectStore,
		Payload: []byte("test data"),
	}

	if err := w.Write(entry); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Entry is queued to channel, not yet in buffer (consumer processes asynchronously)
	// Buffer may be empty at this point since consumer hasn't drained the channel yet
	// File size should be 0 (no disk I/O yet)
	info, _ := os.Stat(filepath.Join(dir, "wal_000001.wal"))
	if info.Size() != 0 {
		t.Fatalf("file should be empty, got size %d", info.Size())
	}

	// After Sync, buffer should be empty and data should be on disk
	ctx := context.Background()
	w.Sync(ctx)

	info, _ = os.Stat(filepath.Join(dir, "wal_000001.wal"))
	if info.Size() == 0 {
		t.Fatal("file should have data after Sync")
	}
}

func TestSyncFlushes(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	// Write some entries
	for i := 0; i < 5; i++ {
		entry := &api.WALEntry{
			Type:    api.WALEntryTypeObjectStore,
			Payload: []byte("test data"),
		}
		if err := w.Write(entry); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	lsn := w.GetLastLSN()
	if lsn != 5 {
		t.Fatalf("expected LSN 5, got %d", lsn)
	}

	// Sync
	ctx := context.Background()
	returnedLSN, err := w.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if returnedLSN != 5 {
		t.Fatalf("Sync returned wrong LSN: expected 5, got %d", returnedLSN)
	}

	// File should have data
	info, _ := os.Stat(filepath.Join(dir, "wal_000001.wal"))
	if info.Size() == 0 {
		t.Fatal("file should have data after Sync")
	}
}

func TestReplayCallsHandler(t *testing.T) {
	dir := t.TempDir()

	// Create and write entries
	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	ctx := context.Background()

	// Write entries
	entries := []*api.WALEntry{
		{Type: api.WALEntryTypeObjectStore, Payload: []byte("entry1")},
		{Type: api.WALEntryTypeBTree, Payload: []byte("entry2")},
		{Type: api.WALEntryTypeObjectStore, Payload: []byte("entry3")},
	}

	for _, e := range entries {
		if err := w.Write(e); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Sync to disk
	if _, err := w.Sync(ctx); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Close and reopen
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	w2, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w2.Close()

	// Replay
	var replayed []*api.WALEntry
	handler := func(entry *api.WALEntry) error {
		replayed = append(replayed, entry)
		return nil
	}

	if err := w2.Replay(ctx, 0, handler); err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Verify entries were replayed
	if len(replayed) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(replayed))
	}

	// Verify entry contents
	for i, e := range entries {
		if replayed[i].Type != e.Type {
			t.Fatalf("entry %d type mismatch", i)
		}
		if !bytes.Equal(replayed[i].Payload, e.Payload) {
			t.Fatalf("entry %d payload mismatch", i)
		}
	}
}

func TestCheckpointMarker(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	ctx := context.Background()

	// Write some entries first
	for i := 0; i < 3; i++ {
		entry := &api.WALEntry{
			Type:    api.WALEntryTypeObjectStore,
			Payload: []byte("test"),
		}
		if err := w.Write(entry); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	w.Sync(ctx)

	// Write checkpoint
	cpLSN, err := w.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if cpLSN == 0 {
		t.Fatal("checkpoint LSN should not be 0")
	}

	// Verify checkpoint entry exists in file
	var hasCheckpoint bool
	handler := func(entry *api.WALEntry) error {
		if entry.Type == api.WALEntryTypeCheckpoint {
			hasCheckpoint = true
		}
		return nil
	}

	// Replay to find checkpoint
	w2, _ := NewWAL(dir, 1024*1024)
	defer w2.Close()
	if err := w2.Replay(ctx, 0, handler); err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if !hasCheckpoint {
		t.Fatal("checkpoint entry not found in replay")
	}
}

func TestGetLastLSN(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	// Initial LSN should be 0
	if w.GetLastLSN() != 0 {
		t.Fatalf("initial LSN should be 0, got %d", w.GetLastLSN())
	}

	// Write 3 entries
	for i := 0; i < 3; i++ {
		entry := &api.WALEntry{
			Type:    api.WALEntryTypeObjectStore,
			Payload: []byte("test"),
		}
		w.Write(entry)
	}

	// LSN should be 3
	if w.GetLastLSN() != 3 {
		t.Fatalf("expected LSN 3, got %d", w.GetLastLSN())
	}

	// Sync should not change LSN
	ctx := context.Background()
	w.Sync(ctx)
	if w.GetLastLSN() != 3 {
		t.Fatalf("LSN should still be 3 after sync, got %d", w.GetLastLSN())
	}
}

func TestCloseReopen(t *testing.T) {
	dir := t.TempDir()

	// Create and write
	w1, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		entry := &api.WALEntry{
			Type:    api.WALEntryTypeObjectStore,
			Payload: []byte("test data"),
		}
		if err := w1.Write(entry); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	w1.Sync(ctx)

	if err := w1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	w2, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed on reopen: %v", err)
	}
	defer w2.Close()

	// LSN should be recovered
	if w2.GetLastLSN() != 5 {
		t.Fatalf("expected recovered LSN 5, got %d", w2.GetLastLSN())
	}

	// Replay should return all 5 entries
	var count int
	handler := func(entry *api.WALEntry) error {
		count++
		return nil
	}

	if err := w2.Replay(ctx, 0, handler); err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if count != 5 {
		t.Fatalf("expected 5 entries replayed, got %d", count)
	}
}

func TestReplaySinceLSN(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	ctx := context.Background()

	// Write 5 entries
	for i := 0; i < 5; i++ {
		entry := &api.WALEntry{
			Type:    api.WALEntryTypeObjectStore,
			Payload: []byte("test"),
		}
		if err := w.Write(entry); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	w.Sync(ctx)
	w.Close()

	// Reopen
	w2, _ := NewWAL(dir, 1024*1024)
	defer w2.Close()

	// Replay from LSN 3 (should get entries 4 and 5)
	var count int
	handler := func(entry *api.WALEntry) error {
		count++
		return nil
	}

	if err := w2.Replay(ctx, 3, handler); err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 entries replayed from LSN 3, got %d", count)
	}
}

func TestReplayStopsOnError(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	ctx := context.Background()

	// Write 5 entries
	for i := 0; i < 5; i++ {
		entry := &api.WALEntry{
			Type:    api.WALEntryTypeObjectStore,
			Payload: []byte("test"),
		}
		if err := w.Write(entry); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	w.Sync(ctx)
	w.Close()

	// Reopen
	w2, _ := NewWAL(dir, 1024*1024)
	defer w2.Close()

	// Handler returns error after 2 entries
	var count int
	handler := func(entry *api.WALEntry) error {
		count++
		if count >= 2 {
			return errReplay
		}
		return nil
	}

	err = w2.Replay(ctx, 0, handler)
	if err == nil {
		t.Fatal("expected error from handler")
	}

	if count != 2 {
		t.Fatalf("expected 2 entries before error, got %d", count)
	}
}

var errReplay = os.ErrInvalid

func TestSyncEmptyBuffer(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	ctx := context.Background()

	// Sync with empty buffer
	lsn, err := w.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Should return current LSN (0)
	if lsn != 0 {
		t.Fatalf("expected LSN 0, got %d", lsn)
	}
}

func TestBinaryFormat(t *testing.T) {
	// Verify the binary format is exactly [Type:1][Length:4][Payload:n][Checksum:4]
	entry := &api.WALEntry{
		Type:    api.WALEntryTypeBTree,
		Payload: []byte{0x01, 0x02, 0x03},
	}

	w := &WALImpl{}
	data, _ := w.marshalEntry(entry)

	// Expected: 1 + 4 + 3 + 4 = 12 bytes
	if len(data) != 12 {
		t.Fatalf("expected 12 bytes, got %d", len(data))
	}

	// Manually verify structure
	if data[0] != 0x02 { // WALEntryTypeBTree = 0x02
		t.Fatalf("type byte wrong: got %x", data[0])
	}

	length := binary.BigEndian.Uint32(data[1:5])
	if length != 3 {
		t.Fatalf("length wrong: got %d", length)
	}

	if !bytes.Equal(data[5:8], []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("payload wrong: got %s", hex.EncodeToString(data[5:8]))
	}

	// Checksum is last 4 bytes
	_ = binary.BigEndian.Uint32(data[8:])
}
