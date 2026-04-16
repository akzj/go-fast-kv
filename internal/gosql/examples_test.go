package gosql_test

import (
	_ "github.com/akzj/go-fast-kv/internal/gosql"
)

// Example demonstrates basic database/sql usage with go-fast-kv.
//
// The go-fast-kv driver is registered as "go-fast-kv" and accepts
// an absolute directory path as the DSN.
//
//	import _ "github.com/akzj/go-fast-kv/internal/gosql"
//	import "database/sql"
//
//	// Open database with store path as DSN
//	db, _ := sql.Open("go-fast-kv", "/tmp/mydb")
//
//	// Create table
//	db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
//
//	// Insert with positional parameters ($1, $2)
//	db.Exec("INSERT INTO users VALUES ($1, $2)", 1, "Alice")
//
//	// Query
//	rows, _ := db.Query("SELECT id, name FROM users")
//	defer rows.Close()
//
// For full test coverage, see TestDriverOpen, TestSqlxSelect, TestSqlxTransaction.
func Example() {
}
