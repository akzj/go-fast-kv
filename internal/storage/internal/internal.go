// Package internal contains private implementation details for storage.
// This package is not importable by other modules.
//
// Implementation responsibilities:
//   - Segment file format (header, data pages, trailer)
//   - File I/O (buffered writes, alignment)
//   - Segment rotation (Active → Sealed → Archived)
//   - CRC/checksum validation
package internal
