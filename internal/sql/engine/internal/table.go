// Package internal implements the TableEngine and IndexEngine interfaces.
package internal

import (
	"encoding/binary"
	"fmt"
	"sync"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
)

// Compile-time interface check.
var _ engineapi.TableEngine = (*tableEngine)(nil)

// tableEngine implements engineapi.TableEngine.
type tableEngine struct {
	store   kvstoreapi.Store
	encoder api.KeyEncoder
	codec   api.RowCodec

	// mu protects rowCounters.
	mu          sync.Mutex
	rowCounters map[uint32]uint64 // tableID → next rowID (in-memory cache)
}

// NewTableEngine creates a new TableEngine.
func NewTableEngine(store kvstoreapi.Store, encoder api.KeyEncoder, codec api.RowCodec) engineapi.TableEngine {
	return &tableEngine{
		store:       store,
		encoder:     encoder,
		codec:       codec,
		rowCounters: make(map[uint32]uint64),
	}
}

// ─── Metadata key helpers ───────────────────────────────────────────

// encodeMetaKey returns the metadata key: t{tableID}m
func encodeMetaKey(tableID uint32) []byte {
	buf := make([]byte, 6)
	buf[0] = 't'
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = 'm'
	return buf
}

// ─── Auto-increment RowID ───────────────────────────────────────────

// nextRowID returns the next rowID for a table, reading from KV on first use.
// Caller must hold te.mu.
func (te *tableEngine) nextRowID(tableID uint32) (uint64, error) {
	if rid, ok := te.rowCounters[tableID]; ok {
		return rid, nil
	}
	// Read from KV.
	metaKey := encodeMetaKey(tableID)
	data, err := te.store.Get(metaKey)
	if err == kvstoreapi.ErrKeyNotFound {
		te.rowCounters[tableID] = 1
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) < 8 {
		te.rowCounters[tableID] = 1
		return 1, nil
	}
	rid := binary.BigEndian.Uint64(data)
	te.rowCounters[tableID] = rid
	return rid, nil
}

// persistCounter writes the rowID counter into a WriteBatch.
func persistCounter(batch kvstoreapi.WriteBatch, tableID uint32, nextID uint64) error {
	metaKey := encodeMetaKey(tableID)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, nextID)
	return batch.Put(metaKey, buf)
}


// NextRowID returns the current counter value without advancing it.
func (te *tableEngine) NextRowID(tableID uint32) uint64 {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.rowCounters[tableID]
}

// PersistCounter writes the current row counter value into a WriteBatch.
func (te *tableEngine) PersistCounter(batch kvstoreapi.WriteBatch, tableID uint32) error {
	te.mu.Lock()
	nextID := te.rowCounters[tableID]
	te.mu.Unlock()
	metaKey := encodeMetaKey(tableID)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, nextID)
	return batch.Put(metaKey, buf)
}

// IncrementCounter atomically increments the row counter for a table.
// Used by the SQL executor for transactional inserts to advance the counter
// in-memory before persisting via PutWithXID.
func (te *tableEngine) IncrementCounter(tableID uint32) {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.rowCounters[tableID]++
}

// GetCounter returns the current counter value without modification.
// Used by the SQL executor to read the counter after IncrementCounter.
func (te *tableEngine) GetCounter(tableID uint32) uint64 {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.rowCounters[tableID]
}

// AllocRowID atomically allocates a new rowID for a table.
// Reads from KV if the counter is not cached, then increments and returns.
// Used by the SQL executor for transactional inserts that bypass WriteBatch.
func (te *tableEngine) AllocRowID(tableID uint32) (uint64, error) {
	te.mu.Lock()
	defer te.mu.Unlock()
	if rid, ok := te.rowCounters[tableID]; ok {
		return rid, nil
	}
	// Read from KV.
	metaKey := encodeMetaKey(tableID)
	data, err := te.store.Get(metaKey)
	if err == kvstoreapi.ErrKeyNotFound {
		te.rowCounters[tableID] = 2
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) < 8 {
		te.rowCounters[tableID] = 2
		return 1, nil
	}
	rid := binary.BigEndian.Uint64(data)
	te.rowCounters[tableID] = rid + 1
	return rid, nil
}

// EncodeRow serializes a row's values into a byte slice using the table's codec.
// Used by the SQL executor for transactional inserts that bypass WriteBatch.
func (te *tableEngine) EncodeRow(values []catalogapi.Value) []byte {
	return te.codec.EncodeRow(values)
}

// ─── TableEngine implementation ─────────────────────────────────────

func (te *tableEngine) Insert(table *catalogapi.TableSchema, values []catalogapi.Value) (uint64, error) {
	if table.TableID == 0 {
		return 0, engineapi.ErrTableIDNotSet
	}
	tableID := table.TableID

	te.mu.Lock()
	defer te.mu.Unlock()

	var rowID uint64

	// Check if table has an integer primary key.
	pkColIdx := -1
	if table.PrimaryKey != "" {
		for i, col := range table.Columns {
			if col.Name == table.PrimaryKey && col.Type == catalogapi.TypeInt {
				pkColIdx = i
				break
			}
		}
	}

	// F-C3: save counter BEFORE any update for rollback.
	oldCounter := te.rowCounters[tableID]

	if pkColIdx >= 0 && pkColIdx < len(values) && !values[pkColIdx].IsNull {
		// Use the PK value as rowID.
		// F-W2: validate non-negative.
		if values[pkColIdx].Int < 0 {
			return 0, fmt.Errorf("engine: primary key must be non-negative, got %d", values[pkColIdx].Int)
		}
		rowID = uint64(values[pkColIdx].Int)

		// Check for duplicates.
		rowKey := te.encoder.EncodeRowKey(tableID, rowID)
		_, err := te.store.Get(rowKey)
		if err == nil {
			return 0, engineapi.ErrDuplicateKey
		}
		if err != kvstoreapi.ErrKeyNotFound {
			return 0, err
		}

		// Update counter if needed (keep counter ahead of max used ID).
		next, err := te.nextRowID(tableID)
		if err != nil {
			return 0, err
		}
		if rowID >= next {
			te.rowCounters[tableID] = rowID + 1
		}
	} else {
		// Auto-increment.
		next, err := te.nextRowID(tableID)
		if err != nil {
			return 0, err
		}
		rowID = next
		te.rowCounters[tableID] = next + 1
		// Update the values slice so the caller gets the auto-generated ID.
		if pkColIdx >= 0 && pkColIdx < len(values) {
			values[pkColIdx] = catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(rowID)}
		}
	}

	// Encode and write atomically. Persist the current (possibly updated) counter.
	rowKey := te.encoder.EncodeRowKey(tableID, rowID)
	rowVal := te.codec.EncodeRow(values)

	batch := te.store.NewWriteBatch()
	if err := batch.Put(rowKey, rowVal); err != nil {
		batch.Discard()
		return 0, err
	}
	if err := persistCounter(batch, tableID, te.rowCounters[tableID]); err != nil {
		batch.Discard()
		return 0, err
	}
	if err := batch.Commit(); err != nil {
		// F-C3: rollback counter to saved value and discard batch.
		te.rowCounters[tableID] = oldCounter
		batch.Discard()
		return 0, err
	}

	return rowID, nil
}

// InsertInto inserts a row into a provided WriteBatch.
// Counter is updated in-memory but NOT persisted; caller must add
// counter persistence to the batch if needed.
// Returns the assigned rowID.
func (te *tableEngine) InsertInto(table *catalogapi.TableSchema, batch kvstoreapi.WriteBatch, values []catalogapi.Value) (uint64, error) {
	if table.TableID == 0 {
		return 0, engineapi.ErrTableIDNotSet
	}
	tableID := table.TableID

	te.mu.Lock()
	defer te.mu.Unlock()

	var rowID uint64

	pkColIdx := -1
	if table.PrimaryKey != "" {
		for i, col := range table.Columns {
			if col.Name == table.PrimaryKey && col.Type == catalogapi.TypeInt {
				pkColIdx = i
				break
			}
		}
	}
	// Also detect AUTOINCREMENT columns that may not have PRIMARY KEY set explicitly.
	// An AUTOINCREMENT column is always INT and always auto-generates IDs.
	if pkColIdx < 0 {
		for i, col := range table.Columns {
			if col.AutoInc && col.Type == catalogapi.TypeInt {
				pkColIdx = i
				break
			}
		}
	}

	if pkColIdx >= 0 && pkColIdx < len(values) && !values[pkColIdx].IsNull {
		if values[pkColIdx].Int < 0 {
			return 0, fmt.Errorf("engine: primary key must be non-negative, got %d", values[pkColIdx].Int)
		}
		rowID = uint64(values[pkColIdx].Int)
		// Check for duplicates via store.Get (not batch-safe for read-your-own-writes in batch)
		rowKey := te.encoder.EncodeRowKey(tableID, rowID)
		_, err := te.store.Get(rowKey)
		if err == nil {
			return 0, engineapi.ErrDuplicateKey
		}
		if err != kvstoreapi.ErrKeyNotFound {
			return 0, err
		}
		next, err := te.nextRowID(tableID)
		if err != nil {
			return 0, err
		}
		if rowID >= next {
			te.rowCounters[tableID] = rowID + 1
		}
	} else {
		next, err := te.nextRowID(tableID)
		if err != nil {
			return 0, err
		}
		rowID = next
		te.rowCounters[tableID] = next + 1
		// Update the values slice so the caller gets the auto-generated ID.
		if pkColIdx >= 0 && pkColIdx < len(values) {
			values[pkColIdx] = catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(rowID)}
		}
	}

	rowKey := te.encoder.EncodeRowKey(tableID, rowID)
	rowVal := te.codec.EncodeRow(values)
	if err := batch.Put(rowKey, rowVal); err != nil {
		return 0, err
	}

	return rowID, nil
}

func (te *tableEngine) Get(table *catalogapi.TableSchema, rowID uint64) (*engineapi.Row, error) {
	if table.TableID == 0 {
		return nil, engineapi.ErrTableIDNotSet
	}
	rowKey := te.encoder.EncodeRowKey(table.TableID, rowID)
	data, err := te.store.Get(rowKey)
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, engineapi.ErrRowNotFound
	}
	if err != nil {
		return nil, err
	}
	values, err := te.codec.DecodeRow(data, table.Columns)
	if err != nil {
		return nil, err
	}
	return &engineapi.Row{RowID: rowID, Values: values}, nil
}

func (te *tableEngine) Scan(table *catalogapi.TableSchema) (engineapi.RowIterator, error) {
	if table.TableID == 0 {
		return nil, engineapi.ErrTableIDNotSet
	}
	prefix := te.encoder.EncodeRowPrefix(table.TableID)
	prefixEnd := te.encoder.EncodeRowPrefixEnd(table.TableID)
	kvIter := te.store.Scan(prefix, prefixEnd)

	return &rowIterator{
		kvIter:  kvIter,
		encoder: te.encoder,
		codec:   te.codec,
		columns: table.Columns,
	}, nil
}

// ScanWithLimit returns an iterator over all rows in the table with optional LIMIT/OFFSET.
// If limit > 0, at most limit rows are returned.
// If offset > 0, the first offset rows are skipped.
// Combining offset and limit: returns rows [offset, offset+limit).
//
// This enables push-down optimization: the storage layer stops scanning early
// after returning the requested number of rows, avoiding unnecessary I/O.
func (te *tableEngine) ScanWithLimit(table *catalogapi.TableSchema, limit, offset int) (engineapi.RowIterator, error) {
	if table.TableID == 0 {
		return nil, engineapi.ErrTableIDNotSet
	}
	prefix := te.encoder.EncodeRowPrefix(table.TableID)
	prefixEnd := te.encoder.EncodeRowPrefixEnd(table.TableID)
	kvIter := te.store.ScanWithParams(prefix, prefixEnd, kvstoreapi.ScanParams{
		Limit:  limit,
		Offset: offset,
	})

	return &rowIterator{
		kvIter:  kvIter,
		encoder: te.encoder,
		codec:   te.codec,
		columns: table.Columns,
	}, nil
}

func (te *tableEngine) Delete(table *catalogapi.TableSchema, rowID uint64) error {
	if table.TableID == 0 {
		return engineapi.ErrTableIDNotSet
	}
	rowKey := te.encoder.EncodeRowKey(table.TableID, rowID)
	// Check existence first.
	_, err := te.store.Get(rowKey)
	if err == kvstoreapi.ErrKeyNotFound {
		return engineapi.ErrRowNotFound
	}
	if err != nil {
		return err
	}
	return te.store.Delete(rowKey)
}

// DeleteFrom deletes a row via a provided WriteBatch.
// Does NOT check existence. Caller is responsible for the batch lifecycle.
func (te *tableEngine) DeleteFrom(table *catalogapi.TableSchema, batch kvstoreapi.WriteBatch, rowID uint64) error {
	if table.TableID == 0 {
		return engineapi.ErrTableIDNotSet
	}
	rowKey := te.encoder.EncodeRowKey(table.TableID, rowID)
	return batch.Delete(rowKey)
}

func (te *tableEngine) Update(table *catalogapi.TableSchema, rowID uint64, values []catalogapi.Value) error {
	if table.TableID == 0 {
		return engineapi.ErrTableIDNotSet
	}
	rowKey := te.encoder.EncodeRowKey(table.TableID, rowID)
	// Check existence first.
	_, err := te.store.Get(rowKey)
	if err == kvstoreapi.ErrKeyNotFound {
		return engineapi.ErrRowNotFound
	}
	if err != nil {
		return err
	}
	rowVal := te.codec.EncodeRow(values)
	return te.store.Put(rowKey, rowVal)
}

// UpdateIn updates a row via a provided WriteBatch.
// Does NOT check existence. Caller is responsible for the batch lifecycle.
func (te *tableEngine) UpdateIn(table *catalogapi.TableSchema, batch kvstoreapi.WriteBatch, rowID uint64, values []catalogapi.Value) error {
	if table.TableID == 0 {
		return engineapi.ErrTableIDNotSet
	}
	rowKey := te.encoder.EncodeRowKey(table.TableID, rowID)
	rowVal := te.codec.EncodeRow(values)
	return batch.Put(rowKey, rowVal)
}

func (te *tableEngine) DropTableData(tableID uint32) error {
	// Delete all row data.
	prefix := te.encoder.EncodeRowPrefix(tableID)
	prefixEnd := te.encoder.EncodeRowPrefixEnd(tableID)
	if _, err := te.store.DeleteRange(prefix, prefixEnd); err != nil {
		return err
	}
	// Delete metadata key.
	metaKey := encodeMetaKey(tableID)
	_ = te.store.Delete(metaKey) // ignore not-found

	// Clear in-memory counter.
	te.mu.Lock()
	delete(te.rowCounters, tableID)
	te.mu.Unlock()

	return nil
}

// ─── RowIterator ────────────────────────────────────────────────────

type rowIterator struct {
	kvIter  kvstoreapi.Iterator
	encoder api.KeyEncoder
	codec   api.RowCodec
	columns []catalogapi.ColumnDef
	current *engineapi.Row
	err     error
}

func (ri *rowIterator) Next() bool {
	if ri.err != nil {
		return false
	}
	if !ri.kvIter.Next() {
		ri.err = ri.kvIter.Err()
		return false
	}
	// Decode row key to get rowID.
	_, rowID, err := ri.encoder.DecodeRowKey(ri.kvIter.Key())
	if err != nil {
		ri.err = err
		return false
	}
	// Decode row value.
	values, err := ri.codec.DecodeRow(ri.kvIter.Value(), ri.columns)
	if err != nil {
		ri.err = err
		return false
	}
	ri.current = &engineapi.Row{RowID: rowID, Values: values}
	return true
}

func (ri *rowIterator) Row() *engineapi.Row { return ri.current }
func (ri *rowIterator) Err() error          { return ri.err }
func (ri *rowIterator) Close()              { ri.kvIter.Close() }
