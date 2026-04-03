package internal

import (
	"sync"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
	api "github.com/akzj/go-fast-kv/internal/storage/api"
	batchwriter "github.com/akzj/go-fast-kv/internal/batchwriter"
)

// BatchSegment wraps a segment with buffered batch writing.
// This provides event-driven batching for segment writes.
type BatchSegment struct {
	segment *segment
	bw      *batchwriter.BatchWriter
}

// NewBatchSegment creates a new batched segment wrapper.
func NewBatchSegment(id vaddr.SegmentID, file api.FileOperations) (*BatchSegment, error) {
	seg, err := OpenSegment(id, file)
	if err != nil {
		return nil, err
	}

	return &BatchSegment{
		segment: seg,
		bw:      batchwriter.New(1024), // Buffer up to 1024 writes
	}, nil
}

// NewBatchSegmentFromExisting wraps an existing segment with batch writing.
func NewBatchSegmentFromExisting(seg *segment) *BatchSegment {
	return &BatchSegment{
		segment: seg,
		bw:      batchwriter.New(1024),
	}
}

// WriteAsync submits a write to be batched and processed asynchronously.
// Returns immediately. The write is guaranteed to complete before Close().
func (bs *BatchSegment) WriteAsync(data []byte) bool {
	return bs.bw.Write(batchwriter.WriteRequest{
		Data:   data,
		Offset: bs.nextOffset(),
		WriteAt: func(d []byte, offset int64) (int, error) {
			return bs.segment.writeAt(d, offset)
		},
	})
}

// WriteSync submits a write and waits for completion.
func (bs *BatchSegment) WriteSync(data []byte) (vaddr.VAddr, error) {
	offset := bs.nextOffset()
	var result struct {
		n   int
		err error
	}
	done := make(chan struct{})

	bs.bw.Write(batchwriter.WriteRequest{
		Data:   data,
		Offset: offset,
		WriteAt: func(d []byte, o int64) (int, error) {
			n, err := bs.segment.writeAt(d, o)
			result.n = n
			result.err = err
			close(done)
			return n, err
		},
	})

	<-done
	if result.err != nil {
		return vaddr.VAddr{}, result.err
	}

	return vaddr.VAddr{
		SegmentID: uint64(bs.segment.id),
		Offset:    uint64(offset),
	}, nil
}

// nextOffset returns the next write offset within the segment.
// Not thread-safe - caller must ensure single-threaded access or use mutex.
func (bs *BatchSegment) nextOffset() int64 {
	bs.segment.mu.Lock()
	defer bs.segment.mu.Unlock()
	return int64(headerSize + bs.segment.dataSize)
}

// writeAt performs the actual write at the given offset.
// Caller must hold segment lock.
func (s *segment) writeAt(data []byte, offset int64) (int, error) {
	n, err := s.file.WriteAt(data, offset)
	if err != nil {
		return n, err
	}
	s.dataSize += int64(n)
	s.pageCount += uint64(len(data) / vaddr.PageSize)
	return n, nil
}

// Close waits for pending writes and closes the batch writer.
func (bs *BatchSegment) Close() error {
	// Flush pending writes
	if err := bs.bw.Close(); err != nil {
		return err
	}
	// Sync the underlying segment
	return bs.segment.Sync()
}

// Segment returns the underlying segment for direct access.
func (bs *BatchSegment) Segment() *segment {
	return bs.segment
}

// Flush waits for all pending writes to complete.
func (bs *BatchSegment) Flush() error {
	return bs.bw.Close()
}

// SegmentManagerWithBatching wraps segment manager operations with batch writing.
type SegmentManagerWithBatching struct {
	*segmentManager
	batchSegments map[vaddr.SegmentID]*BatchSegment
	batchMu       sync.Mutex
}

// NewSegmentManagerWithBatching creates a segment manager with batch write support.
func NewSegmentManagerWithBatching(config Config) (*SegmentManagerWithBatching, error) {
	sm, err := NewSegmentManager(config)
	if err != nil {
		return nil, err
	}

	return &SegmentManagerWithBatching{
		segmentManager: sm,
		batchSegments:  make(map[vaddr.SegmentID]*BatchSegment),
	}, nil
}

// GetBatchedSegment returns a batched wrapper for the specified segment.
func (sm *SegmentManagerWithBatching) GetBatchedSegment(id vaddr.SegmentID) (*BatchSegment, error) {
	sm.batchMu.Lock()
	defer sm.batchMu.Unlock()

	// Check cache
	if bs, ok := sm.batchSegments[id]; ok {
		return bs, nil
	}

	// Get underlying segment
	seg := sm.GetSegment(id)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}

	// Wrap with batch writer
	bs := NewBatchSegmentFromExisting(seg.(*segment))
	sm.batchSegments[id] = bs

	return bs, nil
}

// Close closes all batched segments and the underlying manager.
func (sm *SegmentManagerWithBatching) Close() error {
	sm.batchMu.Lock()
	for _, bs := range sm.batchSegments {
		bs.Close()
	}
	sm.batchMu.Unlock()

	return sm.segmentManager.Close()
}
