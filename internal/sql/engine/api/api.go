// Package api defines the public interfaces for the engine module.
//
// To understand the engine module, read only this file.
//
// The engine module maps SQL row/index CRUD operations to KV operations.
// It uses the encoding module for key encoding and row serialization,
// and the kvstore for actual data storage.
package api

import (
	"errors"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrRowNotFound is returned when a row does not exist.
	ErrRowNotFound = errors.New("engine: row not found")

	// ErrDuplicateKey is returned when inserting a row with a duplicate primary key.
	ErrDuplicateKey = errors.New("engine: duplicate primary key")

	// ErrTableIDNotSet is returned when a table schema has no assigned TableID.
	ErrTableIDNotSet = errors.New("engine: table has no assigned ID")
)

// ─── Row ────────────────────────────────────────────────────────────

// Row represents a single table row.
type Row struct {
	RowID  uint64
	Values []catalogapi.Value // aligned with table columns
}

// ─── RowIterator ────────────────────────────────────────────────────

// RowIterator iterates over table rows.
//
// Usage:
//
//	iter, _ := engine.Scan(table)
//	defer iter.Close()
//	for iter.Next() {
//	    row := iter.Row()
//	}
//	if err := iter.Err(); err != nil { ... }
type RowIterator interface {
	Next() bool
	Row() *Row
	Err() error
	Close()
}

// ─── RowIDIterator ──────────────────────────────────────────────────

// RowIDIterator iterates over rowIDs from an index scan.
//
// Usage:
//
//	iter, _ := index.Scan(tableID, indexID, OpEQ, value)
//	defer iter.Close()
//	for iter.Next() {
//	    rowID := iter.RowID()
//	}
//	if err := iter.Err(); err != nil { ... }
type RowIDIterator interface {
	Next() bool
	RowID() uint64
	Err() error
	Close()
}

// ─── TableEngine ────────────────────────────────────────────────────

// TableEngine provides row-level CRUD on a table.
//
// TableEngine manages auto-increment rowIDs. Each table has an in-memory
// counter initialized from KV on first use. The counter is persisted
// atomically alongside row data using WriteBatch.
//
// If a table has an integer primary key column and the user provides a
// value, that value is used as the rowID. Duplicate PK values are rejected
// with ErrDuplicateKey.
//
// Caller responsibility: the caller (executor/sql.go) must hold a
// DB-level mutex to serialize all DML. The engine itself is NOT
// thread-safe.
type TableEngine interface {
	// Insert inserts a row, assigning a new auto-increment rowID.
	// Returns the assigned rowID.
	//
	// If the table has a PrimaryKey column of TypeInt, the corresponding
	// value from `values` is used as the rowID. In this case, the engine
	// checks for duplicates and returns ErrDuplicateKey if the rowID exists.
	//
	// Otherwise, an auto-increment rowID is assigned.
	//
	// The row data and counter update are written atomically via WriteBatch.
	Insert(table *catalogapi.TableSchema, values []catalogapi.Value) (uint64, error)

	// InsertInto inserts a row into a provided WriteBatch.
	// Does NOT create its own batch. Caller manages batch lifecycle.
	// The batch is used only for the row data; counter is updated in-memory.
	// Caller is responsible for adding counter persistence to the batch.
	//
	// Returns the assigned rowID.
	InsertInto(table *catalogapi.TableSchema, batch kvstoreapi.WriteBatch, values []catalogapi.Value) (uint64, error)

	// Get retrieves a single row by rowID.
	// Returns ErrRowNotFound if the row does not exist.
	Get(table *catalogapi.TableSchema, rowID uint64) (*Row, error)

	// Scan returns an iterator over all rows in the table, ordered by rowID.
	// Caller must call Close() on the returned iterator.
	Scan(table *catalogapi.TableSchema) (RowIterator, error)

	// Delete deletes a row by rowID.
	// Returns ErrRowNotFound if the row does not exist.
	Delete(table *catalogapi.TableSchema, rowID uint64) error

	// DeleteFrom deletes a row via a provided WriteBatch.
	// Does NOT check existence. Caller manages batch lifecycle.
	DeleteFrom(table *catalogapi.TableSchema, batch kvstoreapi.WriteBatch, rowID uint64) error

	// Update replaces a row's values (same rowID).
	// Returns ErrRowNotFound if the row does not exist.
	Update(table *catalogapi.TableSchema, rowID uint64, values []catalogapi.Value) error

	// UpdateIn replaces a row's values via a provided WriteBatch.
	// Does NOT check existence. Caller manages batch lifecycle.
	UpdateIn(table *catalogapi.TableSchema, batch kvstoreapi.WriteBatch, rowID uint64, values []catalogapi.Value) error

	// DropTableData deletes all row data and the metadata key for a table.
	// Uses kvstore.DeleteRange for efficiency.
	DropTableData(tableID uint32) error

	// NextRowID returns the current counter value without advancing it.
	NextRowID(tableID uint32) uint64

	// PersistCounter writes the current row counter value into a WriteBatch.
	PersistCounter(batch kvstoreapi.WriteBatch, tableID uint32) error
}

// ─── IndexEngine ────────────────────────────────────────────────────

// IndexEngine provides secondary index CRUD.
//
// Index entries are stored as KV pairs where the key encodes
// (tableID, indexID, columnValue, rowID) and the value is empty.
type IndexEngine interface {
	// Insert adds an index entry for a row.
	Insert(index *catalogapi.IndexSchema, tableID uint32, indexID uint32,
		value catalogapi.Value, rowID uint64) error

	// Delete removes an index entry for a row.
	Delete(index *catalogapi.IndexSchema, tableID uint32, indexID uint32,
		value catalogapi.Value, rowID uint64) error

	// InsertBatch adds an index entry via a provided WriteBatch.
	// Does NOT encode the key — caller provides the pre-encoded key and value.
	InsertBatch(key []byte, batch kvstoreapi.WriteBatch) error

	// EncodeIndexKey encodes an index key. Exposed so callers can pre-encode
	// keys for batch operations.
	EncodeIndexKey(tableID uint32, indexID uint32, value catalogapi.Value, rowID uint64) []byte

	// Scan returns rowIDs matching a comparison condition on the indexed column.
	//
	// Supported ops: OpEQ, OpLT, OpLE, OpGT, OpGE.
	// OpNE is not supported (falls back to table scan at planner level).
	Scan(tableID uint32, indexID uint32, op encodingapi.CompareOp,
		value catalogapi.Value) (RowIDIterator, error)

	// ScanRange returns rowIDs where the indexed column is in [start, end).
	// If start is nil, scan from beginning of index.
	// If end is nil, scan to end of index.
	ScanRange(tableID uint32, indexID uint32,
		start *catalogapi.Value, end *catalogapi.Value) (RowIDIterator, error)

	// DropIndexData deletes all entries for an index.
	// Uses kvstore.DeleteRange for efficiency.
	DropIndexData(tableID uint32, indexID uint32) error
}
