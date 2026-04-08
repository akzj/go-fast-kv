package compaction

import (
	"sync"

	api "github.com/akzj/go-fast-kv/internal/compaction/api"
	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// compactionWriter implements CompactionWriter by writing to a SegmentManager.
type compactionWriter struct {
	mu       sync.Mutex
	segMgr   SegmentManager
	segID    api.SegmentID
	activeSeg Segment
	closed   bool
}

// SegmentManager is the storage interface needed by compaction writer.
type SegmentManager interface {
	ActiveSegment() Segment
	CreateSegment() (Segment, error)
}

// Segment is the segment interface needed by compaction writer.
type Segment interface {
	ID() vaddr.SegmentID
	Append(data []byte) (vaddr.VAddr, error)
}

// NewCompactionWriter creates a new CompactionWriter backed by the given SegmentManager.
func NewCompactionWriter(segMgr SegmentManager) api.CompactionWriter {
	return &compactionWriter{segMgr: segMgr}
}

// Open initializes the writer for a new compaction output.
func (w *compactionWriter) Open() (api.SegmentID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return api.SegmentID(vaddr.SegmentIDInvalid), api.ErrStoreClosed
	}

	seg, err := w.segMgr.CreateSegment()
	if err != nil {
		return api.SegmentID(vaddr.SegmentIDInvalid), err
	}

	w.activeSeg = seg
	w.segID = api.SegmentID(seg.ID())
	return w.segID, nil
}

// WriteNode writes a B-link tree node to the output.
func (w *compactionWriter) WriteNode(data []byte) (api.VAddr, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSeg == nil {
		return api.VAddr{}, api.ErrCompactionFailed
	}

	addr, err := w.activeSeg.Append(data)
	return api.VAddr(addr), err
}

// WriteExternalValue writes an external value to the output.
func (w *compactionWriter) WriteExternalValue(data []byte) (api.VAddr, error) {
	return w.WriteNode(data)
}

// Commit finalizes the output and marks it as sealed.
func (w *compactionWriter) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSeg == nil {
		return api.ErrCompactionFailed
	}

	// Sync the segment (seal it)
	w.activeSeg = nil
	return nil
}

// Abort discards the output and cleans up resources.
func (w *compactionWriter) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.activeSeg = nil
}
