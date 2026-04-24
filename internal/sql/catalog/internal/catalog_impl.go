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

// DefaultSchemaCacheSize is the default maximum number of table schemas to cache.
const DefaultSchemaCacheSize = 128

// Catalog implements api.CatalogManager using kvstore for persistence.
// All exported methods are protected by a mutex to prevent TOCTOU races.
type Catalog struct {
	kv  kvstoreapi.Store
	mu sync.RWMutex

	// Schema cache: table name → cached TableSchema
	schemaCache    map[string]*api.TableSchema
	schemaCacheOrder []string // insertion order for LRU eviction
	schemaCacheSize int
}

// New creates a new Catalog that persists to kv.
func New(kv kvstoreapi.Store) *Catalog {
	return &Catalog{
		kv: kv,
		schemaCache:    make(map[string]*api.TableSchema),
		schemaCacheOrder: make([]string, 0, DefaultSchemaCacheSize),
		schemaCacheSize: DefaultSchemaCacheSize,
	}
}

// ─── Key helpers ─────────────────────────────────────────────────

const (
	tablePrefix  = "_sql:table:"
	indexPrefix  = "_sql:index:"
	triggerPrefix = "_sql:trigger:"
	viewPrefix    = "_sql:view:"
)

// tableKey returns the KV key for a table schema.
func tableKey(name string) []byte {
	return []byte(tablePrefix + strings.ToUpper(name))
}

// indexKey returns the KV key for an index schema.
func indexKey(tableName, indexName string) []byte {
	return []byte(indexPrefix + strings.ToUpper(tableName) + ":" + strings.ToUpper(indexName))
}

// triggerKey returns the KV key for a trigger schema.
func triggerKey(triggerName string) []byte {
	return []byte(triggerPrefix + strings.ToUpper(triggerName))
}

// viewKey returns the KV key for a view schema.
func viewKey(name string) []byte {
	return []byte(viewPrefix + strings.ToUpper(name))
}

// tableTriggersPrefix returns the prefix for all triggers on a table.
func tableTriggersPrefix(tableName string) []byte {
	return []byte(triggerPrefix + "table:" + strings.ToUpper(tableName) + ":")
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
	if err := c.kv.Put(key, data); err != nil {
		return err
	}

	// Pre-cache the new schema
	if len(c.schemaCache) >= c.schemaCacheSize {
		if len(c.schemaCacheOrder) > 0 {
			oldest := c.schemaCacheOrder[0]
			delete(c.schemaCache, oldest)
			c.schemaCacheOrder = c.schemaCacheOrder[1:]
		}
	}
	c.schemaCache[upperName] = &schema
	c.schemaCacheOrder = append(c.schemaCacheOrder, upperName)

	return nil
}

func (c *Catalog) GetTable(name string) (*api.TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTableImpl(name)
}

func (c *Catalog) getTableImpl(name string) (*api.TableSchema, error) {
	upperName := strings.ToUpper(name)

	// Check cache first
	if cached, ok := c.schemaCache[upperName]; ok {
		return cached, nil
	}

	key := tableKey(upperName)
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

	// Cache the schema (LRU eviction if needed)
	if len(c.schemaCache) >= c.schemaCacheSize {
		// Evict oldest entry (FIFO)
		if len(c.schemaCacheOrder) > 0 {
			oldest := c.schemaCacheOrder[0]
			delete(c.schemaCache, oldest)
			c.schemaCacheOrder = c.schemaCacheOrder[1:]
		}
	}
	c.schemaCache[upperName] = &schema
	c.schemaCacheOrder = append(c.schemaCacheOrder, upperName)

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

	// Invalidate cache entry
	delete(c.schemaCache, upperName)

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
	if err := c.kv.Put(key, data); err != nil {
		return err
	}

	// Update cache with new schema
	c.schemaCache[upperName] = &schema

	return nil
}

func (c *Catalog) RenameTable(oldName, newName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.renameTableImpl(oldName, newName)
}

func (c *Catalog) renameTableImpl(oldName, newName string) error {
	upperOld := strings.ToUpper(oldName)
	upperNew := strings.ToUpper(newName)

	// Check if old table exists
	oldKey := tableKey(upperOld)
	data, err := c.kv.Get(oldKey)
	if err == kvstoreapi.ErrKeyNotFound {
		return api.ErrTableNotFound
	}
	if err != nil {
		return err
	}

	// Check if new name already exists
	newKey := tableKey(upperNew)
	_, err = c.kv.Get(newKey)
	if err == nil {
		return api.ErrTableExists
	}
	if err != nil && err != kvstoreapi.ErrKeyNotFound {
		return err
	}

	// Parse existing schema
	var schema api.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return err
	}

	// Update the table name in the schema
	schema.Name = upperNew

	// Write new entry with updated name
	newData, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	if err := c.kv.Put(newKey, newData); err != nil {
		return err
	}

	// Delete old entry
	if err := c.kv.Delete(oldKey); err != nil {
		return err
	}

	// Update cache: remove old entry, add new entry
	delete(c.schemaCache, upperOld)
	c.schemaCache[upperNew] = &schema

	// Re-key all indexes from old table name to new table name.
	// Index keys are "_sql:index:TABLENAME:INDEXNAME" — must be moved.
	oldIdxPrefix := tableIndexPrefix(upperOld)
	idxIter := c.kv.Scan(oldIdxPrefix, append(oldIdxPrefix, 0xFF))
	defer idxIter.Close()

	for idxIter.Next() {
		// Parse index schema to update the Table field
		var idx api.IndexSchema
		if err := json.Unmarshal(idxIter.Value(), &idx); err != nil {
			return err
		}
		idx.Table = upperNew

		// Write under new key
		newIdxKey := indexKey(upperNew, strings.ToUpper(idx.Name))
		newData, err := json.Marshal(idx)
		if err != nil {
			return err
		}
		if err := c.kv.Put(newIdxKey, newData); err != nil {
			return err
		}

		// Delete old key
		if err := c.kv.Delete(idxIter.Key()); err != nil {
			return err
		}
	}
	if err := idxIter.Err(); err != nil {
		return err
	}

	return nil
}

// CreateTrigger creates a trigger.
func (c *Catalog) CreateTrigger(schema api.TriggerSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createTriggerImpl(schema)
}

func (c *Catalog) createTriggerImpl(schema api.TriggerSchema) error {
	// Check if trigger already exists
	upperName := strings.ToUpper(schema.Name)
	key := triggerKey(upperName)
	_, err := c.kv.Get(key)
	if err == nil {
		return api.ErrTriggerExists
	}
	if err != kvstoreapi.ErrKeyNotFound {
		return err
	}

	// Store trigger schema
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.kv.Put(key, data)
}

// GetTrigger returns a trigger by name.
func (c *Catalog) GetTrigger(triggerName string) (*api.TriggerSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTriggerImpl(triggerName)
}

func (c *Catalog) getTriggerImpl(triggerName string) (*api.TriggerSchema, error) {
	key := triggerKey(strings.ToUpper(triggerName))
	data, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, api.ErrTriggerNotFound
	}
	if err != nil {
		return nil, err
	}

	var schema api.TriggerSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// DropTrigger removes a trigger.
func (c *Catalog) DropTrigger(triggerName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropTriggerImpl(triggerName)
}

func (c *Catalog) dropTriggerImpl(triggerName string) error {
	key := triggerKey(strings.ToUpper(triggerName))
	_, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return api.ErrTriggerNotFound
	}
	if err != nil {
		return err
	}
	return c.kv.Delete(key)
}

// ListTriggers returns all triggers for a given table.
func (c *Catalog) ListTriggers(tableName string) ([]api.TriggerSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.listTriggersImpl(tableName)
}

func (c *Catalog) listTriggersImpl(tableName string) ([]api.TriggerSchema, error) {
	upperTable := strings.ToUpper(tableName)
	prefix := []byte(triggerPrefix)
	end := append(prefix, 0xFF)

	var triggers []api.TriggerSchema
	iter := c.kv.Scan(prefix, end)
	defer iter.Close()

	for iter.Next() {
		var schema api.TriggerSchema
		if err := json.Unmarshal(iter.Value(), &schema); err != nil {
			continue
		}
		if strings.ToUpper(schema.Table) == upperTable {
			triggers = append(triggers, schema)
		}
	}
	return triggers, iter.Err()
}

// ─── View Management ──────────────────────────────────────────────

func (c *Catalog) CreateView(schema api.ViewSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createViewImpl(schema)
}

func (c *Catalog) createViewImpl(schema api.ViewSchema) error {
	upperName := strings.ToUpper(schema.Name)
	key := viewKey(upperName)
	_, err := c.kv.Get(key)
	if err == nil {
		return api.ErrViewExists
	}
	if err != kvstoreapi.ErrKeyNotFound {
		return err
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	return c.kv.Put(key, data)
}

func (c *Catalog) GetView(name string) (*api.ViewSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getViewImpl(name)
}

func (c *Catalog) getViewImpl(name string) (*api.ViewSchema, error) {
	upperName := strings.ToUpper(name)
	key := viewKey(upperName)
	data, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, api.ErrViewNotFound
	}
	if err != nil {
		return nil, err
	}
	var schema api.ViewSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func (c *Catalog) DropView(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropViewImpl(name)
}

func (c *Catalog) dropViewImpl(name string) error {
	upperName := strings.ToUpper(name)
	key := viewKey(upperName)
	_, err := c.kv.Get(key)
	if err == kvstoreapi.ErrKeyNotFound {
		return api.ErrViewNotFound
	}
	if err != nil {
		return err
	}
	return c.kv.Delete(key)
}

func (c *Catalog) ListViews() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.listViewsImpl()
}

func (c *Catalog) listViewsImpl() ([]string, error) {
	prefix := []byte(viewPrefix)
	end := append(prefix, 0xFF)

	var views []string
	iter := c.kv.Scan(prefix, end)
	defer iter.Close()

	for iter.Next() {
		name := strings.TrimPrefix(string(iter.Key()), viewPrefix)
		views = append(views, name)
	}
	return views, iter.Err()
}
