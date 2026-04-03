// Package api provides the public interfaces for objectstore module.
// This package contains ONLY interfaces, types, and constants - NO implementation.
// Invariant: Any concrete implementation MUST live in the internal/ package.
package api

import (
	"context"
	"fmt"
)

// =============================================================================
// Types
// =============================================================================

// ObjectID is uint64, high 8 bits store type, low 56 bits store sequence.
// Invariant: High 8 bits determine ObjectType, decoding must verify type consistency.
type ObjectID uint64

// ObjectType defines the storage type of an object.
// Why not just Page/Blob? LargeBlob needs special handling (delete = reclaim file).
type ObjectType uint8

const (
	ObjectTypePage  ObjectType = 0x00 // B+Tree pages, stored in Page Segment
	ObjectTypeBlob  ObjectType = 0x01 // Normal Blobs, stored in Blob Segment
	ObjectTypeLarge ObjectType = 0x02 // Large Blobs (>=256MB), one per Segment
	ObjectTypeMax   ObjectType = 0xFF // Type upper bound for validation
)

// SegmentType defines the type of a segment.
type SegmentType uint8

const (
	SegmentTypePage  SegmentType = 0x00 // Page Segment: 64MB fixed size
	SegmentTypeBlob  SegmentType = 0x01 // Blob Segment: 256MB fixed size
	SegmentTypeLarge SegmentType = 0x02 // Large Blob Segment: elastic size, 1:1 mapping
)

// SegmentID is the unique identifier for a segment.
type SegmentID uint64

// ObjectLocation describes an object's physical storage location.
// Used by Mapping Index for memory and WAL persistence.
// 16-byte compact layout: SegmentID(8) + Offset(4) + Size(4).
type ObjectLocation struct {
	SegmentID SegmentID
	Offset    uint32 // Byte offset within segment
	Size      uint32 // Object data size (excluding Header)
}

// ObjectHeader is the fixed-size header for objects on disk, exactly 32 bytes.
// Layout:
//   - Magic[2]: 0xF0 0xKB (KB = KV database identifier)
//   - Version[1]: Header version number
//   - Type[1]: ObjectType
//   - Checksum[4]: CRC32 checksum
//   - Size[4]: Data size (excluding Header)
//   - Reserved[20]: Reserved for future extension
//
// Invariant: Fixed 32 bytes, must use Marshal/Unmarshal helpers.
type ObjectHeader struct {
	Magic    [2]byte   // 0xF0 0xKB
	Version  uint8     // Header version
	Type     ObjectType
	Checksum uint32    // CRC32 checksum
	Size     uint32    // Data size
	_        [20]byte  // Reserved, fill with zeros
}

// SegmentMeta contains GC metadata for a segment.
type SegmentMeta struct {
	SegmentID   uint64
	SegmentType SegmentType
	TotalSize   uint64 // Total segment file size
	GarbageSize uint64 // Sum of deleted object sizes
	LiveSize    uint64 // TotalSize - GarbageSize
	LiveCount   uint64 // Number of live objects
	GarbageCount uint64 // Number of deleted objects
	DeletedSize uint64 // Recently deleted object sizes (for mod rate calculation)
	LastModificationTime int64 // Last modification timestamp
}

// =============================================================================
// Constants
// =============================================================================

const (
	// PageSize is the standard B+Tree page size in bytes.
	PageSize = 4096

	// ObjectHeaderSize is the fixed header size in bytes.
	// Invariant: ObjectHeader must serialize to exactly 32 bytes.
	// Why not rely on unsafe.Sizeof? Struct padding may vary by compiler/architecture.
	ObjectHeaderSize = 32

	// PageSegmentMaxSize is the maximum size for a page segment (64MB).
	PageSegmentMaxSize = 64 * 1024 * 1024 // 67108864

	// BlobSegmentMaxSize is the maximum size for a blob segment (256MB).
	BlobSegmentMaxSize = 256 * 1024 * 1024 // 268435456

	// LargeBlobThreshold is the minimum size to trigger large blob storage.
	// Blobs >= 256MB get their own segment file (1 blob per file).
	// Why 256MB? Matches BlobSegmentMaxSize for clean boundary behavior.
	LargeBlobThreshold = BlobSegmentMaxSize

	// Magic bytes for ObjectHeader validation.
	// MagicByte2 = 0xKB where KB = KV database identifier
	MagicByte1    = 0xF0
	MagicByte2    = 0x0B // KB = KV database identifier
	HeaderVersion = 1

	// WALEntryTypeObjectStore is the WAL entry type for ObjectStore module.
	WALEntryTypeObjectStore uint8 = 0x01
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrObjectNotFound      = &ObjectStoreError{"object not found"}
	ErrInvalidObjectID     = &ObjectStoreError{"invalid object id"}
	ErrSegmentFull        = &ObjectStoreError{"segment full"}
	ErrInvalidSegment      = &ObjectStoreError{"invalid segment"}
	ErrChecksumMismatch    = &ObjectStoreError{"checksum mismatch"}
	ErrSegmentTypeNotMatch = &ObjectStoreError{"segment type mismatch"}
	ErrInvalidHeader       = &ObjectStoreError{"invalid object header"}
)

// ObjectStoreError represents an objectstore error.
type ObjectStoreError struct {
	msg string
}

func (e *ObjectStoreError) Error() string {
	return e.msg
}

// =============================================================================
// Interfaces
// =============================================================================

// ObjectStore provides a unified interface for page and blob storage.
// Design principle: All writes are append-only, no random writes; GC compacts.
//
// Why not sync delete? Avoids read/write conflicts; GC handles reclamation.
type ObjectStore interface {
	// AllocPage allocates a new Page object.
	// Returns ObjectID, data should be written via WritePage.
	// Invariant: Allocated Page must belong to Page Segment.
	AllocPage(ctx context.Context) (ObjectID, error)

	// ReadPage reads a Page object.
	// pageID is ObjectID, must be ObjectTypePage.
	// Invariant: Checksum is verified on read; returns ErrChecksumMismatch if invalid.
	ReadPage(ctx context.Context, pageID ObjectID) ([]byte, error)

	// WritePage writes Page object data.
	// Appends to current active Page Segment, updates Mapping Index.
	// Returns new ObjectID (new position due to append).
	WritePage(ctx context.Context, pageID ObjectID, data []byte) (ObjectID, error)

	// WriteBlob writes a Blob object.
	// size >= 256MB: writes to Large Blob Segment (elastic size).
	// size < 256MB: writes to normal Blob Segment.
	// Returns ObjectID.
	WriteBlob(ctx context.Context, data []byte) (ObjectID, error)

	// ReadBlob reads a Blob object.
	// blobID is ObjectID, must be ObjectTypeBlob or ObjectTypeLarge.
	ReadBlob(ctx context.Context, blobID ObjectID) ([]byte, error)

	// Delete deletes an object (mark as deleted, actual reclamation by GC).
	// Why not sync delete? Avoids read/write conflicts; GC handles reclamation.
	Delete(ctx context.Context, objID ObjectID) error

	// GetLocation returns the physical location of an object.
	// Used by upper modules like B+Tree to query object locations.
	// Returns ErrObjectNotFound if object doesn't exist.
	GetLocation(ctx context.Context, objID ObjectID) (ObjectLocation, error)

	// Sync forces all pending writes to disk.
	// Typically called before Checkpoint.
	Sync(ctx context.Context) error

	// Close closes the ObjectStore and releases resources.
	Close() error

	// GetSegmentIDs returns IDs of all sealed segments (excluding active).
	// Used by GC to scan segments requiring reclamation.
	GetSegmentIDs(ctx context.Context) []uint64

	// GetSegmentType returns the type of a segment.
	// Used by GC to execute different strategies per type.
	GetSegmentType(ctx context.Context, segID uint64) SegmentType

	// GetSegmentMeta returns GC metadata for a segment.
	// Includes: GarbageSize, TotalSize, LiveCount, GarbageCount, etc.
	GetSegmentMeta(ctx context.Context, segID uint64) (*SegmentMeta, error)

	// CompactSegment compacts a segment.
	// Copies live objects to active segment, updates MappingIndex, writes WAL entry.
	// Deletes old segment file after compaction.
	CompactSegment(ctx context.Context, segID uint64) error

	// DeleteSegment deletes a segment file (used for Large Blobs).
	DeleteSegment(ctx context.Context, segID uint64) error

	// MarkObjectDeleted marks an object as deleted (for GC statistics).
	MarkObjectDeleted(ctx context.Context, objID ObjectID, size uint32)

	// GetActiveSegmentID returns the ID of the active segment (cannot be compacted).
	GetActiveSegmentID(ctx context.Context, segType SegmentType) (uint64, error)
}

// MappingIndex maps ObjectID to physical location.
// Maintained in memory, persisted via WAL.
// 16 bytes per entry: segment_id(8) + offset(4) + size(4).
type MappingIndex interface {
	// Get queries an object's location.
	// Returns (ObjectLocation, exists).
	Get(objID ObjectID) (ObjectLocation, bool)

	// Put updates an object's location (insert or overwrite).
	Put(objID ObjectID, loc ObjectLocation)

	// Delete removes an object's location.
	Delete(objID ObjectID)

	// Iterate traverses all mapping entries.
	// Used for Checkpoint persistence and GC scanning.
	Iterate(func(objID ObjectID, loc ObjectLocation))
}

// =============================================================================
// Helper Functions
// =============================================================================

// GetObjectIDType extracts ObjectType from ObjectID.
func (id ObjectID) GetObjectIDType() ObjectType {
	return ObjectType(id >> 56)
}

// GetSequence extracts the sequence number from ObjectID.
func (id ObjectID) GetSequence() uint64 {
	return uint64(id) & 0x00FFFFFFFFFFFFFF
}

// MakeObjectID constructs an ObjectID.
// Panics if objType > ObjectTypeMax or sequence > 0x00FFFFFFFFFFFFFF.
func MakeObjectID(objType ObjectType, sequence uint64) ObjectID {
	if objType > ObjectTypeMax {
		panic("object type out of range")
	}
	if sequence > 0x00FFFFFFFFFFFFFF {
		panic("sequence out of range")
	}
	return ObjectID(uint64(objType)<<56 | sequence)
}

// MarshalBinary serializes ObjectHeader to 32 bytes.
func (h *ObjectHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, 32)
	b[0] = h.Magic[0]
	b[1] = h.Magic[1]
	b[2] = h.Version
	b[3] = byte(h.Type)
	putUint32LE(b[4:8], h.Checksum)
	putUint32LE(b[8:12], h.Size)
	return b, nil
}

// UnmarshalBinary deserializes ObjectHeader from 32 bytes.
func (h *ObjectHeader) UnmarshalBinary(b []byte) error {
	if len(b) < 32 {
		return &ObjectStoreError{fmt.Sprintf("invalid object header: need 32 bytes, got %d", len(b))}
	}
	h.Magic[0] = b[0]
	h.Magic[1] = b[1]
	h.Version = b[2]
	h.Type = ObjectType(b[3])
	h.Checksum = getUint32LE(b[4:8])
	h.Size = getUint32LE(b[8:12])
	return nil
}

// putUint32LE writes uint32 in little-endian format.
func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// getUint32LE reads uint32 in little-endian format.
func getUint32LE(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
