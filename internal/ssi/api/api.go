// Package ssiapi defines the Serializable Snapshot Isolation (SSI) interfaces.
package ssiapi

import "errors"

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrSerializationFailure is returned when a transaction detects
	// a write skew conflict during commit validation.
	ErrSerializationFailure = errors.New("ssi: serialization failure (write skew detected)")

	// ErrSSIConflict is a general SSI conflict error.
	ErrSSIConflict = errors.New("ssi: SSI conflict detected")
)

// ─── Conflict Types ─────────────────────────────────────────────────

type ConflictType int

const (
	// RWConflict: read-write conflict (T1 reads X, T2 writes X before T1 commits)
	RWConflict ConflictType = iota

	// WWConflict: write-write conflict (T1 writes X, T2 writes X before T1 commits)
	WWConflict
)

// String returns a human-readable name for the conflict type.
func (c ConflictType) String() string {
	switch c {
	case RWConflict:
		return "RW"
	case WWConflict:
		return "WW"
	default:
		return "Unknown"
	}
}

// ─── Key ──────────────────────────────────────────────────────────

// Key is a string used as a key in SSI tracking.
// We use string instead of []byte because Go doesn't allow []byte as map key.
type Key = string

// ─── Write Info ────────────────────────────────────────────────────

// WriteInfo records the transaction that last wrote a key and its commit timestamp.
type WriteInfo struct {
	TxnID    uint64 // Transaction ID that wrote this key
	CommitTS uint64 // Commit timestamp when the write was committed
}

// ─── Conflict ─────────────────────────────────────────────────────

// Conflict represents a detected SSI conflict.
type Conflict struct {
	Type     ConflictType // RW or WW
	Key      Key         // The key that caused the conflict
	OtherTxn uint64       // The conflicting transaction ID
	Reason   string       // Human-readable description
}

// String returns a human-readable description of the conflict.
func (c *Conflict) String() string {
	return c.Type.String() + " conflict on " + c.Key + " with txn " + string(rune(c.OtherTxn))
}

// ─── SSI State ─────────────────────────────────────────────────────

// State tracks a transaction's read/write sets for SSI validation.
type State struct {
	RWSet     map[Key]struct{} // Keys read by this transaction
	WWSet     map[Key]struct{} // Keys written by this transaction
	Dangerous bool             // Whether dangerous structure detected
	Conflicts []Conflict       // List of detected conflicts
}

// NewState creates a new SSI state for a transaction.
func NewState() *State {
	return &State{
		RWSet:     make(map[Key]struct{}),
		WWSet:     make(map[Key]struct{}),
		Dangerous: false,
		Conflicts: nil,
	}
}

// MarkRead records a key as read by this transaction.
func (s *State) MarkRead(key Key) {
	s.RWSet[key] = struct{}{}
}

// MarkWrite records a key as written by this transaction.
func (s *State) MarkWrite(key Key) {
	s.WWSet[key] = struct{}{}
}

// AddConflict adds a conflict to the state.
func (s *State) AddConflict(c Conflict) {
	s.Dangerous = true
	s.Conflicts = append(s.Conflicts, c)
}

// IsDangerous returns true if the transaction has detected dangerous conflicts.
func (s *State) IsDangerous() bool {
	return s.Dangerous
}

// HasConflicts returns true if the transaction has any conflicts.
func (s *State) HasConflicts() bool {
	return len(s.Conflicts) > 0
}

// ─── SSI Index Interface ───────────────────────────────────────────

// Index is the global SSI index that tracks last writers and readers.
type Index interface {
	// GetWriteInfo returns the last committed write info for a key.
	// Returns nil if no committed write exists.
	GetWriteInfo(key Key) *WriteInfo

	// SetWriteInfo sets the write info for a key after transaction commits.
	SetWriteInfo(key Key, info *WriteInfo)

	// GetReader returns the transaction ID that last read the key.
	// Returns 0 if no reader exists.
	GetReader(key Key) uint64

	// SetReader sets the reader transaction ID for a key.
	SetReader(key Key, txnID uint64)

	// GC removes entries older than minXID from the indexes.
	// This should be called periodically to prevent unbounded growth.
	GC(minXID uint64)

	// Size returns the current size of both indexes.
	Size() (sindexSize, tindexSize int)
}
