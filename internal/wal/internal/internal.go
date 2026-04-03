// Package internal contains private implementation details for wal.
// This package is not importable by other modules.
//
// Implementation responsibilities:
//   - WAL segment file format
//   - Record serialization/deserialization
//   - CRC32c checksum calculation
//   - Buffer management
//   - Checkpoint creation and persistence
package internal
