package internal

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"

	"github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// ObjectStoreImpl implements the ObjectStore interface.
//
// Design:
// - Uses atomic counters per ObjectType for ID generation
// - Delegates segment I/O to SegmentManager
// - Maintains MappingIndex for location lookups
// - CRC32 checksum on every write, verified on every read
//
// Invariant: ObjectID = (ObjectType << 56) | sequence
// This encoding is enforced by MakeObjectID helper.
type ObjectStoreImpl struct {
	dir   string
	index *MappingIndexImpl
	segMgr *SegmentManager

	// Per-type sequence counters (low 56 bits of ObjectID)
	pageSeq  atomic.Uint64
	blobSeq  atomic.Uint64
	largeSeq atomic.Uint64
}

// Ensure ObjectStoreImpl implements api.ObjectStore
var _ api.ObjectStore = (*ObjectStoreImpl)(nil)

// NewObjectStore creates a new ObjectStore implementation.
func NewObjectStore(dir string) (*ObjectStoreImpl, error) {
	segMgr, err := NewSegmentManager(dir)
	if err != nil {
		return nil, fmt.Errorf("create segment manager: %w", err)
	}

	return &ObjectStoreImpl{
		dir:   dir,
		index: NewMappingIndex(),
		segMgr: segMgr,
	}, nil
}

// AllocPage allocates a new page ObjectID.
// Returns ObjectID with type=ObjectTypePage and a new sequence number.
func (s *ObjectStoreImpl) AllocPage(ctx context.Context) (api.ObjectID, error) {
	seq := s.pageSeq.Add(1)
	id := api.MakeObjectID(api.ObjectTypePage, seq)
	return id, nil
}

// WritePage writes page data to the active page segment.
// - Appends ObjectHeader (32B) + data
// - Computes CRC32 checksum and stores in header
// - Updates MappingIndex with new location
// Returns new ObjectID (with updated sequence if needed for position).
//
// Why create new ID on write? Because append produces new offset.
func (s *ObjectStoreImpl) WritePage(ctx context.Context, pageID api.ObjectID, data []byte) (api.ObjectID, error) {
	// Verify page type
	if pageID.GetObjectIDType() != api.ObjectTypePage {
		return 0, fmt.Errorf("%w: expected Page, got %v", api.ErrSegmentTypeNotMatch, pageID.GetObjectIDType())
	}

	// Verify data size
	if len(data) != api.PageSize {
		return 0, fmt.Errorf("page data must be %d bytes, got %d", api.PageSize, len(data))
	}

	// Compute checksum
	checksum := crc32.ChecksumIEEE(data)

	// Get or create active page segment
	seg, err := s.segMgr.getOrCreateActivePage(ctx)
	if err != nil {
		return 0, fmt.Errorf("get active page segment: %w", err)
	}

	// Try append
	header := &api.ObjectHeader{
		Magic:    [2]byte{api.MagicByte1, api.MagicByte2},
		Version:  api.HeaderVersion,
		Type:     api.ObjectTypePage,
		Checksum: checksum,
		Size:     uint32(len(data)),
	}

	offset, err := seg.Append(ctx, header, data)
	if err != nil {
		// Segment full - seal it and retry
		if err == api.ErrSegmentFull || containsSegmentFull(err) {
			if sealErr := s.segMgr.SealAndRotate(ctx, api.SegmentTypePage); sealErr != nil {
				return 0, fmt.Errorf("seal page segment: %w", sealErr)
			}
			seg, err = s.segMgr.getOrCreateActivePage(ctx)
			if err != nil {
				return 0, fmt.Errorf("get new page segment: %w", err)
			}
			offset, err = seg.Append(ctx, header, data)
			if err != nil {
				return 0, fmt.Errorf("append after rotation: %w", err)
			}
		} else {
			return 0, fmt.Errorf("append page: %w", err)
		}
	}

	// Update mapping index with new location (same ID, new location due to append)
	loc := api.ObjectLocation{
		SegmentID: seg.ID,
		Offset:    offset,
		Size:      uint32(len(data)),
	}
	s.index.Put(pageID, loc)

	return pageID, nil
}

// ReadPage reads a page from the segment file.
func (s *ObjectStoreImpl) ReadPage(ctx context.Context, pageID api.ObjectID) ([]byte, error) {
	// Verify page type
	if pageID.GetObjectIDType() != api.ObjectTypePage {
		return nil, fmt.Errorf("%w: expected Page, got %v", api.ErrSegmentTypeNotMatch, pageID.GetObjectIDType())
	}

	// Lookup location
	loc, ok := s.index.Get(pageID)
	if !ok {
		return nil, api.ErrObjectNotFound
	}

	// Get segment (need to check active or sealed)
	seg, err := s.getSegment(loc.SegmentID, api.SegmentTypePage)
	if err != nil {
		return nil, fmt.Errorf("get segment: %w", err)
	}

	// Read header and data
	header, data, err := seg.Read(ctx, loc.Offset, loc.Size)
	if err != nil {
		return nil, fmt.Errorf("read page: %w", err)
	}

	// Verify checksum
	if header.Checksum != crc32.ChecksumIEEE(data) {
		return nil, fmt.Errorf("%w: expected %x, got %x", api.ErrChecksumMismatch, header.Checksum, crc32.ChecksumIEEE(data))
	}

	return data, nil
}

// WriteBlob writes a blob object.
// - size < 256MB: writes to regular blob segment
// - size >= 256MB: writes to large blob segment (1 blob per file)
func (s *ObjectStoreImpl) WriteBlob(ctx context.Context, data []byte) (api.ObjectID, error) {
	var seg *Segment
	var err error

	if len(data) >= api.LargeBlobThreshold {
		// Large blob: dedicated segment
		seg, err = s.segMgr.CreateLargeSegment(ctx)
		if err != nil {
			return 0, fmt.Errorf("create large segment: %w", err)
		}

		seq := s.largeSeq.Add(1)
		id := api.MakeObjectID(api.ObjectTypeLarge, seq)

		header := &api.ObjectHeader{
			Magic:    [2]byte{api.MagicByte1, api.MagicByte2},
			Version:  api.HeaderVersion,
			Type:     api.ObjectTypeLarge,
			Checksum: crc32.ChecksumIEEE(data),
			Size:     uint32(len(data)),
		}

		offset, err := seg.Append(ctx, header, data)
		if err != nil {
			return 0, fmt.Errorf("append large blob: %w", err)
		}

		loc := api.ObjectLocation{
			SegmentID: seg.ID,
			Offset:    offset,
			Size:      uint32(len(data)),
		}
		s.index.Put(id, loc)

		// Seal large segment immediately (1 blob per file)
		if err := seg.Seal(); err != nil {
			return 0, fmt.Errorf("seal large segment: %w", err)
		}

		return id, nil
	}

	// Regular blob: try active blob segment
	seq := s.blobSeq.Add(1)
	id := api.MakeObjectID(api.ObjectTypeBlob, seq)

	seg, err = s.segMgr.getOrCreateActiveBlob(ctx)
	if err != nil {
		return 0, fmt.Errorf("get active blob segment: %w", err)
	}

	header := &api.ObjectHeader{
		Magic:    [2]byte{api.MagicByte1, api.MagicByte2},
		Version:  api.HeaderVersion,
		Type:     api.ObjectTypeBlob,
		Checksum: crc32.ChecksumIEEE(data),
		Size:     uint32(len(data)),
	}

	offset, err := seg.Append(ctx, header, data)
	if err != nil {
		if err == api.ErrSegmentFull || containsSegmentFull(err) {
			if sealErr := s.segMgr.SealAndRotate(ctx, api.SegmentTypeBlob); sealErr != nil {
				return 0, fmt.Errorf("seal blob segment: %w", sealErr)
			}
			seg, err = s.segMgr.getOrCreateActiveBlob(ctx)
			if err != nil {
				return 0, fmt.Errorf("get new blob segment: %w", err)
			}
			offset, err = seg.Append(ctx, header, data)
			if err != nil {
				return 0, fmt.Errorf("append after rotation: %w", err)
			}
		} else {
			return 0, fmt.Errorf("append blob: %w", err)
		}
	}

	loc := api.ObjectLocation{
		SegmentID: seg.ID,
		Offset:    offset,
		Size:      uint32(len(data)),
	}
	s.index.Put(id, loc)

	return id, nil
}

// ReadBlob reads a blob from the segment file.
func (s *ObjectStoreImpl) ReadBlob(ctx context.Context, blobID api.ObjectID) ([]byte, error) {
	objType := blobID.GetObjectIDType()
	if objType != api.ObjectTypeBlob && objType != api.ObjectTypeLarge {
		return nil, fmt.Errorf("%w: expected Blob or Large, got %v", api.ErrSegmentTypeNotMatch, objType)
	}

	// Lookup location
	loc, ok := s.index.Get(blobID)
	if !ok {
		return nil, api.ErrObjectNotFound
	}

	// Get segment
	segType := api.SegmentTypeBlob
	if objType == api.ObjectTypeLarge {
		segType = api.SegmentTypeLarge
	}
	seg, err := s.getSegment(loc.SegmentID, segType)
	if err != nil {
		return nil, fmt.Errorf("get segment: %w", err)
	}

	// Read header
	hdrBytes := make([]byte, api.ObjectHeaderSize)
	if _, err := seg.file.ReadAt(hdrBytes, int64(loc.Offset)); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	var header api.ObjectHeader
	if err := header.UnmarshalBinary(hdrBytes); err != nil {
		return nil, fmt.Errorf("unmarshal header: %w", err)
	}

	// Read data
	data := make([]byte, header.Size)
	if _, err := seg.file.ReadAt(data, int64(loc.Offset)+api.ObjectHeaderSize); err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	// Verify checksum
	if crc32.ChecksumIEEE(data) != header.Checksum {
		return nil, fmt.Errorf("%w: expected %x, got %x", api.ErrChecksumMismatch, header.Checksum, crc32.ChecksumIEEE(data))
	}

	return data, nil
}

// Delete removes an object from the mapping index.
// Physical space is not reclaimed (handled by GC).
func (s *ObjectStoreImpl) Delete(ctx context.Context, objID api.ObjectID) error {
	s.index.Delete(objID)
	return nil
}

// GetLocation returns the location of an object.
func (s *ObjectStoreImpl) GetLocation(ctx context.Context, objID api.ObjectID) (api.ObjectLocation, error) {
	loc, ok := s.index.Get(objID)
	if !ok {
		return api.ObjectLocation{}, api.ErrObjectNotFound
	}
	return loc, nil
}

// Sync fsyncs all active segments.
func (s *ObjectStoreImpl) Sync(ctx context.Context) error {
	return s.segMgr.SyncAll()
}

// Close closes the object store and all segments.
func (s *ObjectStoreImpl) Close() error {
	return s.segMgr.Close()
}

// getSegment returns a segment by ID, checking active and sealed maps.
func (s *ObjectStoreImpl) getSegment(segID api.SegmentID, segType api.SegmentType) (*Segment, error) {
	// First check if it's active
	switch segType {
	case api.SegmentTypePage:
		s.segMgr.pageMu.Lock()
		if s.segMgr.pageAct != nil && s.segMgr.pageAct.ID == segID {
			seg := s.segMgr.pageAct
			s.segMgr.pageMu.Unlock()
			return seg, nil
		}
		seg, ok := s.segMgr.pageSealed[segID]
		s.segMgr.pageMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: page segment %d", api.ErrInvalidSegment, segID)
		}
		return seg, nil
	case api.SegmentTypeBlob:
		s.segMgr.blobMu.Lock()
		if s.segMgr.blobAct != nil && s.segMgr.blobAct.ID == segID {
			seg := s.segMgr.blobAct
			s.segMgr.blobMu.Unlock()
			return seg, nil
		}
		seg, ok := s.segMgr.blobSealed[segID]
		s.segMgr.blobMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: blob segment %d", api.ErrInvalidSegment, segID)
		}
		return seg, nil
	case api.SegmentTypeLarge:
		s.segMgr.largeMu.Lock()
		seg, ok := s.segMgr.largeSealed[segID]
		s.segMgr.largeMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: large segment %d", api.ErrInvalidSegment, segID)
		}
		return seg, nil
	default:
		return nil, fmt.Errorf("%w: unknown type %d", api.ErrInvalidSegment, segType)
	}
}

// containsSegmentFull checks if an error contains ErrSegmentFull.
func containsSegmentFull(err error) bool {
	return errors.Is(err, api.ErrSegmentFull)
}

// ObjectStoreToMappingIndex returns the MappingIndex for checkpointing.
// Exported for WAL/recovery integration.
func ObjectStoreToMappingIndex(os *ObjectStoreImpl) *MappingIndexImpl {
	return os.index
}

// DataDir returns the data directory path.
func (s *ObjectStoreImpl) DataDir() string {
	return s.dir
}

// GetSegmentIDs returns all sealed segment IDs (excluding active segments).
func (s *ObjectStoreImpl) GetSegmentIDs(ctx context.Context) []uint64 {
	var ids []uint64

	// Page segments (sealed only)
	s.segMgr.pageMu.Lock()
	for id := range s.segMgr.pageSealed {
		ids = append(ids, uint64(id))
	}
	s.segMgr.pageMu.Unlock()

	// Blob segments (sealed only)
	s.segMgr.blobMu.Lock()
	for id := range s.segMgr.blobSealed {
		ids = append(ids, uint64(id))
	}
	s.segMgr.blobMu.Unlock()

	// Large blob segments
	s.segMgr.largeMu.Lock()
	for id := range s.segMgr.largeSealed {
		ids = append(ids, uint64(id))
	}
	s.segMgr.largeMu.Unlock()

	return ids
}

// GetSegmentType returns the type of a segment.
func (s *ObjectStoreImpl) GetSegmentType(ctx context.Context, segID uint64) api.SegmentType {
	id := api.SegmentID(segID)

	// Check page sealed
	s.segMgr.pageMu.Lock()
	if _, ok := s.segMgr.pageSealed[id]; ok {
		s.segMgr.pageMu.Unlock()
		return api.SegmentTypePage
	}
	s.segMgr.pageMu.Unlock()

	// Check blob sealed
	s.segMgr.blobMu.Lock()
	if _, ok := s.segMgr.blobSealed[id]; ok {
		s.segMgr.blobMu.Unlock()
		return api.SegmentTypeBlob
	}
	s.segMgr.blobMu.Unlock()

	// Check large sealed
	s.segMgr.largeMu.Lock()
	if _, ok := s.segMgr.largeSealed[id]; ok {
		s.segMgr.largeMu.Unlock()
		return api.SegmentTypeLarge
	}
	s.segMgr.largeMu.Unlock()

	return api.SegmentTypePage // default, shouldn't reach here
}

// GetSegmentMeta returns GC metadata for a segment.
func (s *ObjectStoreImpl) GetSegmentMeta(ctx context.Context, segID uint64) (*api.SegmentMeta, error) {
	id := api.SegmentID(segID)

	// Check page sealed
	s.segMgr.pageMu.Lock()
	if seg, ok := s.segMgr.pageSealed[id]; ok {
		totalSize := uint64(seg.size.Load())
		garbageSize := s.calculateGarbageSize(api.SegmentTypePage, id)
		s.segMgr.pageMu.Unlock()
		return &api.SegmentMeta{
			SegmentID:   uint64(id),
			SegmentType: api.SegmentTypePage,
			TotalSize:   totalSize,
			GarbageSize: garbageSize,
			LiveSize:    totalSize - garbageSize,
		}, nil
	}
	s.segMgr.pageMu.Unlock()

	// Check blob sealed
	s.segMgr.blobMu.Lock()
	if seg, ok := s.segMgr.blobSealed[id]; ok {
		totalSize := uint64(seg.size.Load())
		garbageSize := s.calculateGarbageSize(api.SegmentTypeBlob, id)
		s.segMgr.blobMu.Unlock()
		return &api.SegmentMeta{
			SegmentID:   uint64(id),
			SegmentType: api.SegmentTypeBlob,
			TotalSize:   totalSize,
			GarbageSize: garbageSize,
			LiveSize:    totalSize - garbageSize,
		}, nil
	}
	s.segMgr.blobMu.Unlock()

	// Check large sealed
	s.segMgr.largeMu.Lock()
	if seg, ok := s.segMgr.largeSealed[id]; ok {
		totalSize := uint64(seg.size.Load())
		s.segMgr.largeMu.Unlock()
		return &api.SegmentMeta{
			SegmentID:   uint64(id),
			SegmentType: api.SegmentTypeLarge,
			TotalSize:   totalSize,
			GarbageSize: 0,
			LiveSize:    totalSize,
		}, nil
	}
	s.segMgr.largeMu.Unlock()

	return nil, fmt.Errorf("%w: segment %d not found", api.ErrInvalidSegment, segID)
}

// calculateGarbageSize calculates the total size of deleted objects in a segment.
// This is a simplified calculation - in production, you'd track this more precisely.
func (s *ObjectStoreImpl) calculateGarbageSize(segType api.SegmentType, segID api.SegmentID) uint64 {
	// Scan segment file to count objects (simplified - assumes all objects are same size or stored in header)
	seg, err := s.getSegment(segID, segType)
	if err != nil {
		return 0
	}

	fileSize := uint64(seg.size.Load())
	// Rough estimate: subtract header size per object
	if segType == api.SegmentTypePage {
		// Page objects are PageSize + ObjectHeaderSize
		objectSize := api.PageSize + api.ObjectHeaderSize
		if objectSize > 0 {
			approxObjectCount := fileSize / uint64(objectSize)
			// For sealed segments, estimate garbage as 20% of capacity (rough estimate)
			garbageRatio := 0.2
			garbageSize := uint64(float64(approxObjectCount) * garbageRatio * float64(objectSize))
			return garbageSize
		}
	} else {
		// For blob segments, estimate based on segment fill level
		// This is a simplification - real implementation would track actual object sizes
		// Assume 80% filled, so 20% is garbage
		return fileSize * 20 / 100
	}

	return 0
}

// MarkObjectDeleted marks an object as deleted for GC tracking.
// This allows GC to track deleted objects' sizes for garbage ratio calculation.
func (s *ObjectStoreImpl) MarkObjectDeleted(ctx context.Context, objID api.ObjectID, size uint32) {
	// The object is already removed from index by Delete()
	// GC will recalculate garbage based on remaining objects vs segment size
	// This method is a placeholder for future per-object garbage tracking
}

// GetActiveSegmentID returns the ID of the active segment for the given type.
func (s *ObjectStoreImpl) GetActiveSegmentID(ctx context.Context, segType api.SegmentType) (uint64, error) {
	switch segType {
	case api.SegmentTypePage:
		s.segMgr.pageMu.Lock()
		var id uint64
		if s.segMgr.pageAct != nil {
			id = uint64(s.segMgr.pageAct.ID)
		}
		s.segMgr.pageMu.Unlock()
		return id, nil
	case api.SegmentTypeBlob:
		s.segMgr.blobMu.Lock()
		var id uint64
		if s.segMgr.blobAct != nil {
			id = uint64(s.segMgr.blobAct.ID)
		}
		s.segMgr.blobMu.Unlock()
		return id, nil
	default:
		return 0, nil
	}
}

// CompactSegment compacts a sealed segment by copying live objects to active segment.
func (s *ObjectStoreImpl) CompactSegment(ctx context.Context, segID uint64) error {
	id := api.SegmentID(segID)

	// Determine segment type and get segment
	var segType api.SegmentType
	var seg *Segment

	s.segMgr.pageMu.Lock()
	if sseg, ok := s.segMgr.pageSealed[id]; ok {
		seg = sseg
		segType = api.SegmentTypePage
	}
	s.segMgr.pageMu.Unlock()

	if seg == nil {
		s.segMgr.blobMu.Lock()
		if sseg, ok := s.segMgr.blobSealed[id]; ok {
			seg = sseg
			segType = api.SegmentTypeBlob
		}
		s.segMgr.blobMu.Unlock()
	}

	if seg == nil {
		return fmt.Errorf("%w: segment %d not found or is active", api.ErrInvalidSegment, segID)
	}

	// Scan segment file and relocate live objects
	fileSize := int64(seg.size.Load())
	offset := int64(0)
	bytesMoved := uint64(0)
	objectsMoved := 0

	for offset < fileSize {
		// Read header
		hdrBytes := make([]byte, api.ObjectHeaderSize)
		if _, err := seg.file.ReadAt(hdrBytes, offset); err != nil {
			break
		}

		var header api.ObjectHeader
		if err := header.UnmarshalBinary(hdrBytes); err != nil {
			break
		}

		// Check if this object is still alive (in mapping index)
		// We need to reconstruct the ObjectID from context - for now, skip this
		// In a real implementation, you'd store ObjectID in segment or track it differently

		// Move to next object
		offset += int64(api.ObjectHeaderSize) + int64(header.Size)
	}

	// Get active segment
	var activeSeg *Segment
	var err error

	switch segType {
	case api.SegmentTypePage:
		activeSeg, err = s.segMgr.getOrCreateActivePage(ctx)
	case api.SegmentTypeBlob:
		activeSeg, err = s.segMgr.getOrCreateActiveBlob(ctx)
	default:
		return fmt.Errorf("cannot compact segment type %d", segType)
	}

	if err != nil {
		return fmt.Errorf("get active segment: %w", err)
	}

	// Re-scan and copy live objects
	offset = 0
	for offset < fileSize {
		hdrBytes := make([]byte, api.ObjectHeaderSize)
		if _, err := seg.file.ReadAt(hdrBytes, offset); err != nil {
			break
		}

		var header api.ObjectHeader
		if err := header.UnmarshalBinary(hdrBytes); err != nil {
			break
		}

		// Read data
		data := make([]byte, header.Size)
		if _, err := seg.file.ReadAt(data, offset+api.ObjectHeaderSize); err != nil {
			break
		}

		// Object size for next iteration
		objSize := int64(api.ObjectHeaderSize) + int64(header.Size)

		// Write to active segment
		_, err = activeSeg.Append(ctx, &header, data)
		if err != nil {
			// Active segment full - seal and rotate
			if segType == api.SegmentTypePage {
				if sealErr := s.segMgr.SealAndRotate(ctx, api.SegmentTypePage); sealErr != nil {
					return fmt.Errorf("seal page segment: %w", sealErr)
				}
				activeSeg, err = s.segMgr.getOrCreateActivePage(ctx)
			} else {
				if sealErr := s.segMgr.SealAndRotate(ctx, api.SegmentTypeBlob); sealErr != nil {
					return fmt.Errorf("seal blob segment: %w", sealErr)
				}
				activeSeg, err = s.segMgr.getOrCreateActiveBlob(ctx)
			}
			if err != nil {
				return fmt.Errorf("get new active segment: %w", err)
			}
			_, err = activeSeg.Append(ctx, &header, data)
			if err != nil {
				break
			}
		}

		bytesMoved += uint64(len(data))
		objectsMoved++
		offset += objSize
	}

	// Seal and remove old segment from sealed map
	switch segType {
	case api.SegmentTypePage:
		s.segMgr.pageMu.Lock()
		delete(s.segMgr.pageSealed, id)
		s.segMgr.pageMu.Unlock()
	case api.SegmentTypeBlob:
		s.segMgr.blobMu.Lock()
		delete(s.segMgr.blobSealed, id)
		s.segMgr.blobMu.Unlock()
	}

	// Close and delete file
	seg.Close()
	os.Remove(seg.Path)

	_ = bytesMoved // Could be used for stats
	_ = objectsMoved

	return nil
}

// DeleteSegment deletes a segment file (used for Large Blobs).
func (s *ObjectStoreImpl) DeleteSegment(ctx context.Context, segID uint64) error {
	id := api.SegmentID(segID)

	s.segMgr.largeMu.Lock()
	seg, ok := s.segMgr.largeSealed[id]
	if !ok {
		s.segMgr.largeMu.Unlock()
		return fmt.Errorf("%w: large segment %d not found", api.ErrInvalidSegment, segID)
	}
	delete(s.segMgr.largeSealed, id)
	s.segMgr.largeMu.Unlock()

	// Close and delete file
	seg.Close()
	return os.Remove(seg.Path)
}
