// Package gosql provides a database/sql/driver implementation for go-fast-kv.
//
// This package wraps the internal SQL layer (engine, executor, planner, etc.)
// to expose a standard database/sql driver interface.
//
// # Usage with database/sql
//
//	import (
//	    _ "github.com/akzj/go-fast-kv/pkg/gosql"
//	    "database/sql"
//	)
//
//	db, err := sql.Open("go-fast-kv", "/tmp/mydb")
//
// # Usage with sqlx
//
//	import (
//	    "github.com/jmoiron/sqlx"
//	    gosqldriver "github.com/akzj/go-fast-kv/pkg/gosql"
//	)
//
//	db, err := sqlx.Open("go-fast-kv", "/tmp/mydb")
//
// # DSN Format
//
// The DSN (data source name) is the absolute path to the store directory,
// e.g., "/tmp/mydb" or "/home/user/mydb".
//
// # Placeholders
//
// gosql supports positional placeholders only: $1, $2, $3, ...
// Named parameters are not supported.
//
// # Example
//
//	import (
//	    _ "github.com/akzj/go-fast-kv/pkg/gosql"
//	    "database/sql"
//	)
//
//	func main() {
//	    db, err := sql.Open("go-fast-kv", "/tmp/mydb")
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer db.Close()
//
//	    // Create a table
//	    _, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//
//	    // Insert a row
//	    _, err = db.Exec("INSERT INTO users VALUES ($1, $2)", 1, "Alice")
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//
//	    // Query
//	    rows, err := db.Query("SELECT id, name FROM users WHERE id = $1", 1)
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer rows.Close()
//
//	    for rows.Next() {
//	        var id int
//	        var name string
//	        if err := rows.Scan(&id, &name); err != nil {
//	            log.Fatal(err)
//	        }
//	        fmt.Printf("id=%d name=%s\n", id, name)
//	    }
//	}
//
// Module: github.com/akzj/go-fast-kv
package gosql

import (
	// Re-export types from internal/gosql for user convenience.
	gosqldriver "github.com/akzj/go-fast-kv/internal/gosql"
)

var _ = gosqldriver.Driver{} // trigger import of internal package

// Register registers this driver with database/sql.
// This is called automatically via init(), but can be called manually as well.
//
//	import (
//	    _ "github.com/akzj/go-fast-kv/pkg/gosql" // auto-register
//	    "database/sql"
//	)
func Register() {
	gosqldriver.Register()
}