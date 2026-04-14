package internal

import (
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
)

// Compile-time interface check.
var _ engineapi.IndexEngine = (*indexEngine)(nil)

// indexEngine implements engineapi.IndexEngine.
type indexEngine struct {
	store   kvstoreapi.Store
	encoder encodingapi.KeyEncoder
}

// NewIndexEngine creates a new IndexEngine.
func NewIndexEngine(store kvstoreapi.Store, encoder encodingapi.KeyEncoder) engineapi.IndexEngine {
	return &indexEngine{
		store:   store,
		encoder: encoder,
	}
}

func (ie *indexEngine) Insert(index *catalogapi.IndexSchema, tableID uint32, indexID uint32,
	value catalogapi.Value, rowID uint64) error {
	key := ie.encoder.EncodeIndexKey(tableID, indexID, value, rowID)
	return ie.store.Put(key, []byte{}) // index value is empty
}

func (ie *indexEngine) Delete(index *catalogapi.IndexSchema, tableID uint32, indexID uint32,
	value catalogapi.Value, rowID uint64) error {
	key := ie.encoder.EncodeIndexKey(tableID, indexID, value, rowID)
	err := ie.store.Delete(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return nil // idempotent delete
	}
	return err
}

func (ie *indexEngine) InsertBatch(key []byte, batch kvstoreapi.WriteBatch) error {
	return batch.Put(key, []byte{})
}

func (ie *indexEngine) DeleteBatch(key []byte, batch kvstoreapi.WriteBatch) error {
	return batch.Delete(key)
}

func (ie *indexEngine) EncodeIndexKey(tableID uint32, indexID uint32,
	value catalogapi.Value, rowID uint64) []byte {
	return ie.encoder.EncodeIndexKey(tableID, indexID, value, rowID)
}

func (ie *indexEngine) Scan(tableID uint32, indexID uint32, op encodingapi.CompareOp,
	value catalogapi.Value) (engineapi.RowIDIterator, error) {

	var startKey, endKey []byte

	indexPrefix := ie.encoder.EncodeIndexPrefix(tableID, indexID)
	indexPrefixEnd := ie.encoder.EncodeIndexPrefixEnd(tableID, indexID)
	encodedValue := ie.encoder.EncodeValue(value)

	switch op {
	case encodingapi.OpEQ:
		// Scan all entries with exactly this value.
		// startKey = prefix + encodedValue
		// endKey = prefix + encodedValue + 0xFF×8 + 0x01 (past all rowIDs for this value)
		startKey = append(indexPrefix, encodedValue...)
		endKey = append(indexPrefix, encodedValue...)
		// Append 8 bytes of 0xFF to get past all possible rowIDs, then one more byte.
		for i := 0; i < 8; i++ {
			endKey = append(endKey, 0xFF)
		}
		endKey = append(endKey, 0x01)

	case encodingapi.OpLT:
		// All values < given value.
		startKey = indexPrefix
		endKey = append(append([]byte{}, indexPrefix...), encodedValue...)

	case encodingapi.OpLE:
		// All values <= given value.
		startKey = indexPrefix
		endKey = append(append([]byte{}, indexPrefix...), encodedValue...)
		for i := 0; i < 8; i++ {
			endKey = append(endKey, 0xFF)
		}
		endKey = append(endKey, 0x01)

	case encodingapi.OpGT:
		// All values > given value.
		// Start past the given value + all possible rowIDs.
		startKey = append(append([]byte{}, indexPrefix...), encodedValue...)
		for i := 0; i < 8; i++ {
			startKey = append(startKey, 0xFF)
		}
		startKey = append(startKey, 0x01)
		endKey = indexPrefixEnd

	case encodingapi.OpGE:
		// All values >= given value.
		startKey = append(append([]byte{}, indexPrefix...), encodedValue...)
		endKey = indexPrefixEnd

	default:
		// OpNE not supported for index scan.
		return &emptyRowIDIterator{}, nil
	}

	kvIter := ie.store.Scan(startKey, endKey)
	return &rowIDIterator{
		kvIter:  kvIter,
		encoder: ie.encoder,
	}, nil
}

func (ie *indexEngine) ScanRange(tableID uint32, indexID uint32,
	start *catalogapi.Value, end *catalogapi.Value) (engineapi.RowIDIterator, error) {

	indexPrefix := ie.encoder.EncodeIndexPrefix(tableID, indexID)
	indexPrefixEnd := ie.encoder.EncodeIndexPrefixEnd(tableID, indexID)

	var startKey, endKey []byte

	if start != nil {
		encodedStart := ie.encoder.EncodeValue(*start)
		startKey = append(append([]byte{}, indexPrefix...), encodedStart...)
	} else {
		startKey = indexPrefix
	}

	if end != nil {
		encodedEnd := ie.encoder.EncodeValue(*end)
		endKey = append(append([]byte{}, indexPrefix...), encodedEnd...)
	} else {
		endKey = indexPrefixEnd
	}

	kvIter := ie.store.Scan(startKey, endKey)
	return &rowIDIterator{
		kvIter:  kvIter,
		encoder: ie.encoder,
	}, nil
}

func (ie *indexEngine) DropIndexData(tableID uint32, indexID uint32) error {
	prefix := ie.encoder.EncodeIndexPrefix(tableID, indexID)
	prefixEnd := ie.encoder.EncodeIndexPrefixEnd(tableID, indexID)
	_, err := ie.store.DeleteRange(prefix, prefixEnd)
	return err
}

// ─── RowIDIterator ──────────────────────────────────────────────────

type rowIDIterator struct {
	kvIter  kvstoreapi.Iterator
	encoder encodingapi.KeyEncoder
	current uint64
	err     error
}

func (ri *rowIDIterator) Next() bool {
	if ri.err != nil {
		return false
	}
	if !ri.kvIter.Next() {
		ri.err = ri.kvIter.Err()
		return false
	}
	// Decode index key to extract rowID.
	_, _, _, rowID, err := ri.encoder.DecodeIndexKey(ri.kvIter.Key())
	if err != nil {
		ri.err = err
		return false
	}
	ri.current = rowID
	return true
}

func (ri *rowIDIterator) RowID() uint64 { return ri.current }
func (ri *rowIDIterator) Err() error    { return ri.err }
func (ri *rowIDIterator) Close()        { ri.kvIter.Close() }

// ─── Empty iterator ─────────────────────────────────────────────────

type emptyRowIDIterator struct{}

func (e *emptyRowIDIterator) Next() bool  { return false }
func (e *emptyRowIDIterator) RowID() uint64 { return 0 }
func (e *emptyRowIDIterator) Err() error  { return nil }
func (e *emptyRowIDIterator) Close()      {}
