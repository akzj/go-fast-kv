// Example: basic — Fundamental key-value operations
//
// This example demonstrates the core go-fast-kv API:
//   - Open a store
//   - Put, Get, Delete individual key-value pairs
//   - Range scan with iterator
//
// Run: go run github.com/akzj/go-fast-kv/examples/basic
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	api "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	// Create a temporary directory for the store.
	// In production, use a persistent directory.
	dir := os.TempDir() + "/go-fast-kv-basic-example"
	defer os.RemoveAll(dir)

	fmt.Println("=== go-fast-kv Basic Example ===")
	fmt.Println()

	// ─── Open the store ───────────────────────────────────────────
	fmt.Println("1. Opening store...")

	store, err := kvstore.Open(api.Config{Dir: dir})
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()
	fmt.Printf("   ✓ Store opened at: %s\n", dir)
	fmt.Println()

	// ─── Put key-value pairs ─────────────────────────────────────
	fmt.Println("2. Putting 5 key-value pairs...")

	pairs := [][2]string{
		{"name", "Alice"},
		{"age", "30"},
		{"city", "Shanghai"},
		{"country", "China"},
		{"lang", "Go"},
	}

	for _, pair := range pairs {
		key, value := pair[0], pair[1]
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			log.Fatalf("Put(%q, %q) failed: %v", key, value, err)
		}
		fmt.Printf("   ✓ Put(%q, %q)\n", key, value)
	}
	fmt.Println()

	// ─── Get values ──────────────────────────────────────────────
	fmt.Println("3. Getting values...")

	for _, pair := range pairs {
		key, expected := pair[0], pair[1]
		value, err := store.Get([]byte(key))
		if err != nil {
			log.Fatalf("Get(%q) failed: %v", key, err)
		}
		fmt.Printf("   ✓ Get(%q) = %q\n", key, value)
		if string(value) != expected {
			log.Fatalf("   value mismatch: got %q, want %q", value, expected)
		}
	}
	fmt.Println()

	// ─── Delete keys ─────────────────────────────────────────────
	fmt.Println("4. Deleting keys 'age' and 'city'...")

	deleted := []string{"age", "city"}
	for _, key := range deleted {
		if err := store.Delete([]byte(key)); err != nil {
			log.Fatalf("Delete(%q) failed: %v", key, err)
		}
		fmt.Printf("   ✓ Delete(%q)\n", key)
	}
	fmt.Println()

	// ─── Verify deletion ─────────────────────────────────────────
	fmt.Println("5. Verifying deleted keys return ErrKeyNotFound...")

	for _, key := range deleted {
		_, err := store.Get([]byte(key))
		if err != api.ErrKeyNotFound {
			log.Fatalf("Get(%q) after delete: got %v, want ErrKeyNotFound", key, err)
		}
		fmt.Printf("   ✓ Get(%q) → ErrKeyNotFound (correct!)\n", key)
	}
	fmt.Println()

	// ─── Scan with iterator ─────────────────────────────────────
	fmt.Println("6. Scanning keys in range ['a', 'z')...")

	iter := store.Scan([]byte("a"), []byte("z"))
	defer iter.Close()

	count := 0
	for iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())
		fmt.Printf("   ✓ Scan found: %q = %q\n", key, value)
		count++
	}
	if err := iter.Err(); err != nil {
		log.Fatalf("Scan error: %v", err)
	}

	fmt.Printf("   ✓ Scan complete: %d entries found\n", count)
	fmt.Println()

	// ─── Summary ─────────────────────────────────────────────────
	fmt.Println("=== Example Complete ===")
	fmt.Println()
	fmt.Println("Remaining keys in store (after delete):")
	fmt.Println("  • name    → Alice")
	fmt.Println("  • country → China")
	fmt.Println("  • lang    → Go")
}
