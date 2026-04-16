// Package main demonstrates using go-fast-kv with database/sql.
//
// Run: go run github.com/akzj/go-fast-kv/examples/gosql
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/akzj/go-fast-kv/internal/gosql"
)

func main() {
	// Create a temp directory for the database.
	dir := "/tmp/gosql-example"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Open database with store path as DSN.
	db, err := sql.Open("go-fast-kv", dir)
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Verify connection.
	if err := db.Ping(); err != nil {
		log.Fatalf("Ping: %v", err)
	}
	fmt.Println("✓ Connected to go-fast-kv database")

	// Create table.
	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")
	if err != nil {
		log.Fatalf("CREATE TABLE: %v", err)
	}
	fmt.Println("✓ Created users table")

	// Insert rows with positional parameters.
	users := []struct {
		id    int
		name  string
		email string
	}{
		{1, "Alice", "alice@example.com"},
		{2, "Bob", "bob@example.com"},
		{3, "Charlie", "charlie@example.com"},
	}
	for _, u := range users {
		_, err = db.Exec("INSERT INTO users VALUES ($1, $2, $3)", u.id, u.name, u.email)
		if err != nil {
			log.Fatalf("INSERT: %v", err)
		}
	}
	fmt.Printf("✓ Inserted %d users\n", len(users))

	// Query rows.
	rows, err := db.Query("SELECT id, name, email FROM users ORDER BY id")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	fmt.Println("\n── All Users ──")
	for {
		if !rows.Next() {
			break
		}
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			log.Fatalf("Scan: %v", err)
		}
		fmt.Printf("  id=%d name=%s email=%s\n", id, name, email)
	}

	// Query single row.
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = $1", 1).Scan(&name)
	if err != nil {
		log.Fatalf("QueryRow: %v", err)
	}
	fmt.Printf("\n✓ User with id=1: %s\n", name)

	// Prepared statement.
	stmt, err := db.Prepare("SELECT name FROM users WHERE id = $1")
	if err != nil {
		log.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	for _, id := range []int{1, 2, 3} {
		var name string
		err = stmt.QueryRow(id).Scan(&name)
		if err != nil {
			log.Fatalf("QueryRow %d: %v", id, err)
		}
		fmt.Printf("✓ Prepared query for id=%d: %s\n", id, name)
	}

	// Count rows.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		log.Fatalf("Count: %v", err)
	}
	fmt.Printf("\n✓ Total users: %d\n", count)

	fmt.Println("\n✓ All database/sql operations completed successfully")
}
