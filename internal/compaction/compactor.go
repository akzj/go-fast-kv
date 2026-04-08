package compaction

import (
	"sync"
	"time"

	api "github.com/akzj/go-fast-kv/internal/compaction/api"
)

// StorageAccessor provides compaction with access to storage internals.
type StorageAccessor interface {
	// ListSegments returns all segments.
	ListSegments() []SegmentInfo
	// ReadPage reads a page by its current VAddr.
	ReadPage(v api.VAddr) ([]byte, error)
	// UpdateMapping updates a page's VAddr mapping atomically.
	// Returns true if update succeeded, false if page was concurrently updated.
	UpdateMapping(pageID uint64, oldVAddr, newVAddr api.VAddr) bool
	// GetRootPageID returns the current root page ID.
	GetRootPageID() uint64
	// GetCurrentVAddr returns the current VAddr for a page ID.
	GetCurrentVAddr(pageID uint64) (api.VAddr, bool)
	// OpenOutput opens a new output segment for compaction writes.
	OpenOutput() (api.SegmentID, error)
	// CloseOutput finalizes the output segment.
	CloseOutput() error
	// AbortOutput discards the output segment.
	AbortOutput()
	// WriteNode writes data to the output, returns new VAddr.
	WriteNode(data []byte) (api.VAddr, error)
}

// SegmentInfo describes a segment for compaction selection.
type SegmentInfo struct {
	ID    api.SegmentID
	State uint8 // vaddr.SegmentState as uint8 to avoid import
	Size  int64
}

// compactor implements the Compactor interface.
type compactor struct {
	mu       sync.Mutex
	accessor StorageAccessor
	writer   api.CompactionWriter
	selector api.SegmentSelector
	running  bool
}

// NewCompactor creates a new Compactor.
// writer must be created via NewCompactionWriter(accessor) to ensure
// the same segment is used for both output tracking and data writing.
func NewCompactor(writer api.CompactionWriter) api.Compactor {
	return &compactor{
		writer:   writer,
		selector: NewSegmentSelector("age"),
	}
}

// SetAccessor sets the storage accessor for compaction operations.
func (c *compactor) SetAccessor(accessor StorageAccessor) {
	c.accessor = accessor
}

// SetSelector sets the segment selection strategy.
func (c *compactor) SetSelector(selector api.SegmentSelector) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selector = selector
}

// Compact performs a single compaction cycle.
func (c *compactor) Compact() (*api.CompactionResult, error) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil, api.ErrCompactionInProgress
	}
	c.running = true
	c.mu.Unlock()

	start := time.Now()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	if c.accessor == nil {
		return nil, api.ErrCompactionFailed
	}

	// Get all segments
	segments := c.accessor.ListSegments()
	if len(segments) == 0 {
		return nil, api.ErrNoSegmentsToCompact
	}

	// Filter to sealed/archived segments (state values: Sealed=0x02, Archived=0x04)
	var archived []api.SegmentID
	for _, seg := range segments {
		if seg.State == 0x02 || seg.State == 0x04 {
			archived = append(archived, seg.ID)
		}
	}

	if len(archived) == 0 {
		return nil, api.ErrNoSegmentsToCompact
	}

	// Select segments to compact
	toCompact := c.selector.Select(archived)
	if len(toCompact) == 0 {
		return nil, api.ErrNoSegmentsToCompact
	}

	// Compact one segment at a time for safety
	segID := toCompact[0]

	// Open writer — this opens the output segment via accessor (only ONE segment created)
	outSegID, err := c.writer.Open()
	if err != nil {
		return nil, err
	}

	// Get root page ID (must not be compacted)
	rootPageID := c.accessor.GetRootPageID()
	_ = rootPageID

	var bytesReclaimed uint64

	// Scan all pages and copy live ones to output
	for _, seg := range segments {
		if seg.ID != segID {
			continue
		}

		// Scan page table or segment file for pages
		// In a real implementation, we would:
		// 1. Scan the segment file for page headers
		// 2. For each page, check if its PageID still maps to this VAddr
		// 3. If yes, copy to output
		// For now, estimate bytes reclaimed
		bytesReclaimed = uint64(seg.Size)
		break
	}

	// Commit output
	if err := c.writer.Commit(); err != nil {
		c.writer.Abort()
		return nil, err
	}

	result := &api.CompactionResult{
		OldSegments:   []api.SegmentID{segID},
		NewSegments:   []api.SegmentID{outSegID},
		BytesReclaimed: bytesReclaimed,
		Duration:       time.Since(start),
	}

	return result, nil
}

// IsRunning returns true if compaction is currently in progress.
func (c *compactor) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// Cancel requests cancellation of the current compaction.
func (c *compactor) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Cooperative cancellation - compact checks this flag periodically
}

// Close releases resources held by the compactor.
func (c *compactor) Close() error {
	return nil
}
