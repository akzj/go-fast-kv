// basic demonstrates the fundamental operations of go-fast-kv:
// Open, Put, Get, Delete, and Scan.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/basic
package main

import (
	"fmt"
	"log"
	"os"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	dir := "/tmp/go-fast-kv-basic"
	os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer store.Close()

	// ─── Put ───────────────────────────────────────────────────────
	if err := store.Put([]byte("name"), []byte("Alice")); err != nil {
		log.Fatalf("Put: %v", err)
	}
	if err := store.Put([]byte("age"), []byte("30")); err != nil {
		log.Fatalf("Put: %v", err)
	}
	fmt.Println("✓ Put 2 key-value pairs")

	// ─── Get ──────────────────────────────────────────────────────
	val, err := store.Get([]byte("name"))
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	fmt.Printf("✓ Get name: %s\n", val)

	_, err = store.Get([]byte("nonexistent"))
	if err == kvstoreapi.ErrKeyNotFound {
		fmt.Println("✓ Get nonexistent: key not found (expected)")
	}

	// ─── Update ───────────────────────────────────────────────────
	if err := store.Put([]byte("name"), []byte("Bob")); err != nil {
		log.Fatalf("Update: %v", err)
	}
	val, _ = store.Get([]byte("name"))
	fmt.Printf("✓ Updated name: %s\n", val)

	// ─── Delete ───────────────────────────────────────────────────
	if err := store.Delete([]byte("age")); err != nil {
		log.Fatalf("Delete: %v", err)
	}
	_, err = store.Get([]byte("age"))
	if err == kvstoreapi.ErrKeyNotFound {
		fmt.Println("✓ Delete age: key not found after delete")
	}

	// ─── Scan ──────────────────────────────────────────────────────
	fmt.Println("\n── Scan all keys ──")
	iter := store.Scan([]byte(""), []byte("\xff"))
	for iter.Next() {
		fmt.Printf("  key=%q value=%q\n", iter.Key(), iter.Value())
	}
	if err := iter.Err(); err != nil {
		log.Fatalf("Scan: %v", err)
	}
	iter.Close()

	fmt.Println("\n✓ All basic operations completed successfully")
}
