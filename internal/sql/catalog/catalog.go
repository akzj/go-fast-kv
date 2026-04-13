package catalog

import (
	"encoding/json"
	"fmt"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

type Catalog struct {
	kv kvstoreapi.Store
}

func New(kv kvstoreapi.Store) *Catalog {
	return &Catalog{kv: kv}
}

func tableKey(tableName string) []byte {
	return []byte("_sql:table:" + tableName)
}

func indexKey(tableName, indexName string) []byte {
	return []byte("_sql:index:" + tableName + ":" + indexName)
}

func tableIndexPrefix(tableName string) []byte {
	return []byte("_sql:index:" + tableName + ":")
}

func (c *Catalog) CreateTable(schema TableSchema) error {
	_, err := c.GetTable(schema.Name)
	if err == nil {
		return ErrTableExists
	}
	if err != ErrTableNotFound {
		return err
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("catalog: marshal table schema: %w", err)
	}
	return c.kv.Put(tableKey(schema.Name), data)
}

func (c *Catalog) GetTable(name string) (*TableSchema, error) {
	data, err := c.kv.Get(tableKey(name))
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: get table: %w", err)
	}

	var schema TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("catalog: unmarshal table schema: %w", err)
	}
	return &schema, nil
}

func (c *Catalog) DropTable(name string) error {
	_, err := c.GetTable(name)
	if err == ErrTableNotFound {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}
	if err := c.kv.Delete(tableKey(name)); err != nil {
		return err
	}
	indexes, _ := c.ListIndexesByTable(name)
	for _, idx := range indexes {
		c.kv.Delete(indexKey(name, idx.Name))
	}
	return nil
}

func (c *Catalog) CreateIndex(schema IndexSchema) error {
	_, err := c.GetTable(schema.Table)
	if err != nil {
		return err
	}
	_, err = c.GetIndex(schema.Table, schema.Name)
	if err == nil {
		return ErrIndexExists
	}
	if err != ErrIndexNotFound {
		return err
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("catalog: marshal index schema: %w", err)
	}
	return c.kv.Put(indexKey(schema.Table, schema.Name), data)
}

func (c *Catalog) GetIndex(tableName, indexName string) (*IndexSchema, error) {
	data, err := c.kv.Get(indexKey(tableName, indexName))
	if err == kvstoreapi.ErrKeyNotFound {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: get index: %w", err)
	}
	var schema IndexSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("catalog: unmarshal index schema: %w", err)
	}
	return &schema, nil
}

func (c *Catalog) GetIndexByColumn(tableName, columnName string) (*IndexSchema, error) {
	indexes, err := c.ListIndexesByTable(tableName)
	if err != nil {
		return nil, err
	}
	for _, idx := range indexes {
		if idx.Column == columnName {
			return &idx, nil
		}
	}
	return nil, ErrIndexNotFound
}

func (c *Catalog) DropIndex(tableName, indexName string) error {
	_, err := c.GetIndex(tableName, indexName)
	if err == ErrIndexNotFound {
		return ErrIndexNotFound
	}
	if err != nil {
		return err
	}
	return c.kv.Delete(indexKey(tableName, indexName))
}

func (c *Catalog) ListTables() ([]string, error) {
	var tables []string
	iter := c.kv.Scan([]byte("_sql:table:"), []byte("_sql:table;\x00"))
	defer iter.Close()
	for iter.Next() {
		key := string(iter.Key())
		if len(key) > 11 {
			tables = append(tables, key[11:])
		}
	}
	return tables, iter.Err()
}

func (c *Catalog) ListIndexesByTable(tableName string) ([]IndexSchema, error) {
	var indexes []IndexSchema
	prefix := tableIndexPrefix(tableName)
	iter := c.kv.Scan(prefix, append(prefix, '\x00'))
	defer iter.Close()
	for iter.Next() {
		var schema IndexSchema
		if err := json.Unmarshal(iter.Value(), &schema); err != nil {
			continue
		}
		indexes = append(indexes, schema)
	}
	return indexes, iter.Err()
}
