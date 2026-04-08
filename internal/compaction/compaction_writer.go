package compaction

import (
	"sync"

	 api "github.com/akzj/go-fast-kv/internal/compaction/api"
	 vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// compactionWriter implements CompactionWriter by writing to a segment provided
// by StorageAccessor. This ensures only one segment is created per compaction.
type compactionWriter struct {
	mu        sync.Mutex
	accessor  StorageAccessor
	outputID  api.SegmentID
	closed    bool
}

// NewCompactionWriter creates a new CompactionWriter backed by a StorageAccessor.
func NewCompactionWriter(accessor StorageAccessor) api.CompactionWriter {
	return &compactionWriter{accessor: accessor}
}

// Open initializes the writer. Delegates to accessor.OpenOutput().
func (w *compactionWriter) Open() (api.SegmentID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return api.SegmentID(vaddr.SegmentIDInvalid), api.ErrStoreClosed
	}

	segID, err := w.accessor.OpenOutput()
	if err != nil {
		return api.SegmentID(vaddr.SegmentIDInvalid), err
	}
	w.outputID = segID
	return segID, nil
}

// WriteNode writes a node to the output via accessor.
func (w *compactionWriter) WriteNode(data []byte) (api.VAddr, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.accessor == nil {
		return api.VAddr{}, api.ErrCompactionFailed
	}

	return w.accessor.WriteNode(data)
}

// WriteExternalValue writes an external value to the output.
func (w *compactionWriter) WriteExternalValue(data []byte) (api.VAddr, error) {
	return w.WriteNode(data)
}

// Commit finalizes the output.
func (w *compactionWriter) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.outputID == 0 {
		return api.ErrCompactionFailed
	}
	w.closed = true
	return nil
}

// Abort discards the output.
func (w *compactionWriter) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accessor.AbortOutput()
	w.closed = true
	w.outputID = 0
}
