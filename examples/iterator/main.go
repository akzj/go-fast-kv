// iterator demonstrates range scans using kvstore.Scan.
//
// Scan returns an iterator over keys in [start, end).
// It provides true point-in-time isolation — the snapshot is taken
// at the moment Scan() is called, immune to concurrent writes.
//
// Run:
//   go run examples/iterator/main.go
package main

import (
	"fmt"
	"os"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	fmt.Println("=== go-fast-kv Iterator / Range Scan Demo ===")
	fmt.Println()

	dir := "/tmp/go-fast-kv-iterator-example"
	os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		panic(err)
	}
	defer store.Close()

	// Insert 100 keys: user:000, user:001, ..., user:099
	fmt.Println("── Inserting 100 keys ──")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("user:%03d", i)
		val := fmt.Sprintf("User %d", i)
		if err := store.Put([]byte(key), []byte(val)); err != nil {
			panic(err)
		}
	}
	fmt.Println("  Inserted: user:000 ... user:099")
	fmt.Println()

	// ── Scan range [user:020, user:030) ──────────────────────────
	fmt.Println("── Scan [user:020, user:030) ──")
	iter := store.Scan([]byte("user:020"), []byte("user:030"))
	count := 0
	for iter.Next() {
		key := string(iter.Key())
		val := string(iter.Value())
		fmt.Printf("  %s = %s\n", key, val)
		count++
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	iter.Close()
	fmt.Printf("  → %d keys in range\n", count)
	fmt.Println()

	// ── Scan all ──────────────────────────────────────────────────
	fmt.Println("── Scan [user:, user:~) — all keys ──")
	iter = store.Scan([]byte("user:"), []byte("user;"))
	count = 0
	for iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	iter.Close()
	fmt.Printf("  → %d total keys\n", count)
	fmt.Println()

	// ── Empty scan ────────────────────────────────────────────────
	fmt.Println("── Scan [z:, z:~) — empty ──")
	iter = store.Scan([]byte("z:"), []byte("z;"))
	if iter.Next() {
		fmt.Println("  ERROR: expected no results")
	} else {
		fmt.Printf("  ✓ Empty iterator (no errors: %v)\n", iter.Err())
	}
	iter.Close()

	fmt.Println()
	fmt.Println("=== Done ===")
}
