// Package internal implements the CatalogManager interface.
package internal

import (
	"encoding/json"
	"strings"
	"sync"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"

	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// Compile-time interface check
var _ api.CatalogManager = (*Catalog)(nil)

// Catalog implements api.CatalogManager using kvstore for persistence.
// All exported methods are protected by a mutex to prevent TOCTOU races.
type Catalog struct {
	kv  kvstoreapi.Store
	mu sync.RWMutex
}

// New creates a new Catalog that persists to kv.
func New(kv kvstoreapi.Store) *Catalog {
	return &Catalog{kv: kv}
}

// ─── Key helpers ─────────────────────────────────────────────────

const (
	tablePrefix = "_sql:table:"
	indexPrefix = "_sql:index:"
)

// tableKey returns the KV key for a table schema.
func tableKey(name string) []byte {
	return []byte(tablePrefix + strings.ToUpper(name))
}

// indexKey returns the KV key for an index schema.
func indexKey(tableName, indexName string) []byte {
	return []byte(indexPrefix + strings.ToUpper(tableName) + ":" + strings.ToUpper(indexName))
}

// tableIndexPrefix returns the prefix for all indexes on a table.
func tableIndexPrefix(tableName string) []byte {
	return []byte(indexPrefix + strings.ToUpper(tableName) + ":")
}

// ─── CatalogManager implementation ─────────────────────────────────

func (c *Catalog) CreateTable(schema api.TableSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createTableImpl(schema)
}

func (c *Catalog) createTableImpl(schema api.TableSchema) error {
	// Check if table exists
	upperName := strings.ToUpper(schema.Name)
	key := tableKey(upperName)
	_, err := c.kv.Get(key)
	if err == nil {
		return api.ErrTableExists
	}
	if err != kvstoreapi.ErrKeyNotFound {
		return err
	}

	// Store schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.kv.Put(key, data)
}

func (c *Catalog) GetTable(name string) (*api.TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTableImpl(name)
}

func (c *Catalog) getTableImpl(name string) (*api.TableSchema, error) {
	key := tableKey(strings.ToUpper(name))
	data, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, api.ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}

	var schema api.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropTableImpl(name)
}

func (c *Catalog) dropTableImpl(name string) error {
	upperName := strings.ToUpper(name)

	// Delete table schema
	key := tableKey(upperName)
	_, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return api.ErrTableNotFound
	}
	if err != nil {
		return err
	}

	// Delete table
	if err := c.kv.Delete(key); err != nil {
		return err
	}

	// Delete all indexes on this table
	prefix := tableIndexPrefix(upperName)
	iter := c.kv.Scan(prefix, append(prefix, 0xFF))
	defer iter.Close()

	for iter.Next() {
		if err := c.kv.Delete(iter.Key()); err != nil {
			return err
		}
	}
	return iter.Err()
}

func (c *Catalog) CreateIndex(schema api.IndexSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createIndexImpl(schema)
}

// CreateIndexBatch writes an index catalog entry into a WriteBatch.
// Used by CREATE INDEX to make catalog entry atomic with index data.
// Caller holds c.mu.
func (c *Catalog) CreateIndexBatch(schema api.IndexSchema, batch kvstoreapi.WriteBatch) error {
	_, err := c.getTableImpl(schema.Table)
	if err != nil {
		return err
	}
	upperTable := strings.ToUpper(schema.Table)
	upperName := strings.ToUpper(schema.Name)
	key := indexKey(upperTable, upperName)
	_, err = c.kv.Get(key)
	if err == nil {
		return api.ErrIndexExists
	}
	if err != kvstoreapi.ErrKeyNotFound {
		return err
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return batch.Put(key, data)
}

func (c *Catalog) createIndexImpl(schema api.IndexSchema) error {
	// Verify table exists
	_, err := c.getTableImpl(schema.Table)
	if err != nil {
		return err
	}

	upperTable := strings.ToUpper(schema.Table)
	upperName := strings.ToUpper(schema.Name)
	key := indexKey(upperTable, upperName)

	// Check if index exists
	_, err = c.kv.Get(key)
	if err == nil {
		return api.ErrIndexExists
	}
	if err != kvstoreapi.ErrKeyNotFound {
		return err
	}

	// Store index schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.kv.Put(key, data)
}

func (c *Catalog) GetIndex(tableName, indexName string) (*api.IndexSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getIndexImpl(tableName, indexName)
}

func (c *Catalog) getIndexImpl(tableName, indexName string) (*api.IndexSchema, error) {
	key := indexKey(strings.ToUpper(tableName), strings.ToUpper(indexName))
	data, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, api.ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	var schema api.IndexSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func (c *Catalog) GetIndexByColumn(tableName, columnName string) (*api.IndexSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getIndexByColumnImpl(tableName, columnName)
}

func (c *Catalog) getIndexByColumnImpl(tableName, columnName string) (*api.IndexSchema, error) {
	upperTable := strings.ToUpper(tableName)
	upperCol := strings.ToUpper(columnName)
	prefix := tableIndexPrefix(upperTable)

	iter := c.kv.Scan(prefix, append(prefix, 0xFF))
	defer iter.Close()

	for iter.Next() {
		var schema api.IndexSchema
		if err := json.Unmarshal(iter.Value(), &schema); err != nil {
			continue
		}
		if strings.ToUpper(schema.Column) == upperCol {
			return &schema, nil
		}
	}
	return nil, nil // no index on this column
}

func (c *Catalog) DropIndex(tableName, indexName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropIndexImpl(tableName, indexName)
}

func (c *Catalog) dropIndexImpl(tableName, indexName string) error {
	key := indexKey(strings.ToUpper(tableName), strings.ToUpper(indexName))
	_, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return api.ErrIndexNotFound
	}
	if err != nil {
		return err
	}
	return c.kv.Delete(key)
}

func (c *Catalog) ListTables() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.listTablesImpl()
}

func (c *Catalog) listTablesImpl() ([]string, error) {
	prefix := []byte(tablePrefix)
	end := append(prefix, 0xFF)

	var tables []string
	iter := c.kv.Scan(prefix, end)
	defer iter.Close()

	for iter.Next() {
		// Extract table name from key "_sql:table:NAME"
		name := strings.TrimPrefix(string(iter.Key()), tablePrefix)
		tables = append(tables, name)
	}
	return tables, iter.Err()
}

func (c *Catalog) GetReferencingFKs(tableName string) ([]api.ForeignKeySchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getReferencingFKsImpl(tableName)
}

func (c *Catalog) getReferencingFKsImpl(tableName string) ([]api.ForeignKeySchema, error) {
	upperTable := strings.ToUpper(tableName)
	prefix := []byte(tablePrefix)
	end := append(prefix, 0xFF)

	var result []api.ForeignKeySchema
	iter := c.kv.Scan(prefix, end)
	defer iter.Close()

	for iter.Next() {
		var schema api.TableSchema
		if err := json.Unmarshal(iter.Value(), &schema); err != nil {
			return nil, err
		}
		for _, fk := range schema.ForeignKeys {
			if strings.ToUpper(fk.ReferencedTable) == upperTable {
				result = append(result, fk)
			}
		}
	}
	return result, iter.Err()
}

func (c *Catalog) ListIndexes(tableName string) ([]*api.IndexSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.listIndexesImpl(tableName)
}

func (c *Catalog) listIndexesImpl(tableName string) ([]*api.IndexSchema, error) {
	upperTable := strings.ToUpper(tableName)
	prefix := tableIndexPrefix(upperTable)
	end := append(append([]byte{}, prefix...), 0xFF)

	var indexes []*api.IndexSchema
	iter := c.kv.Scan(prefix, end)
	defer iter.Close()

	for iter.Next() {
		var schema api.IndexSchema
		if err := json.Unmarshal(iter.Value(), &schema); err != nil {
			return nil, err
		}
		indexes = append(indexes, &schema)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return indexes, nil
}

func (c *Catalog) AlterTable(schema api.TableSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alterTableImpl(schema)
}

func (c *Catalog) alterTableImpl(schema api.TableSchema) error {
	upperName := strings.ToUpper(schema.Name)
	key := tableKey(upperName)

	// Verify table exists
	_, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return api.ErrTableNotFound
	}
	if err != nil {
		return err
	}

	// Store updated schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.kv.Put(key, data)
}
