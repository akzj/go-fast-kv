// Package gosql provides a database/sql/driver implementation for go-fast-kv.
//
// This package wraps the internal SQL layer (engine, executor, planner, etc.)
// to expose a standard database/sql driver interface.
//
// Usage with database/sql:
//
//	import (
//	    _ "github.com/akzj/go-fast-kv/internal/gosql"
//	    "database/sql"
//	)
//	
//	db, err := sql.Open("go-fast-kv", "/tmp/mydb")
//	
// Usage with sqlx:
//
//	import (
//	    "github.com/jmoiron/sqlx"
//	    gosqldriver "github.com/akzj/go-fast-kv/internal/gosql"
//	)
//	
//	db, err := sqlx.Open("go-fast-kv", "/tmp/mydb")
//
// DSN format: absolute path to the store directory (e.g., "/tmp/mydb")
//
// Supported placeholders: $1, $2, $3, ... (positional only, no named parameters)
package gosql

import (
	"database/sql"
	"database/sql/driver"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
)

// Compile-time interface checks.
var _ driver.Driver = (*Driver)(nil)

// Driver implements database/sql/driver.Driver.
// It accepts a store path as DSN and wraps the SQL layer.
type Driver struct{}

// Open implements driver.Driver.Open.
// DSN should be an absolute path to the store directory.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	// Open the KV store at the given path.
	// dsn is treated as the store directory path.
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dsn})
	if err != nil {
		return nil, err
	}

	// Create a new SQL DB wrapper around the store.
	db := newDB(store)

	// Create a connection using the sql.Conn wrapping our gosql.Conn.
	// We use sql.Conn to get the standard database/sql semantics
	// (Prepare, Begin, Close, etc.) while delegating to our driver types.
	conn, err := db.conn()
	if err != nil {
		store.Close()
		return nil, err
	}

	// Return the internal gosql.Conn wrapped in sql.Conn.
	return conn, nil
}

// Register registers this driver with database/sql.
// Called automatically via init(), but can be called manually as well.
func Register() {
	sql.Register("go-fast-kv", &Driver{})
}

func init() {
	// Auto-register the driver when the package is imported.
	Register()
}