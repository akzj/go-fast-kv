// iterator demonstrates range queries with the Scan iterator.
//
// Scan returns a consistent snapshot of keys in [start, end).
// Each key appears at most once (latest visible version).
//
// Run: go run github.com/akzj/go-fast-kv/cmd/iterator
package main

import (
	"fmt"
	"log"
	"os"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	dir := "/tmp/go-fast-kv-iterator"
	os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	keys := []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace"}
	for i, k := range keys {
		store.Put([]byte(k), []byte(fmt.Sprintf("value-%d", i)))
	}
	fmt.Println("✓ Inserted 7 keys (unsorted order)")

	// Scan all — results are sorted.
	fmt.Println("\n── Scan all ──")
	iter := store.Scan([]byte(""), []byte("\xff"))
	count := 0
	for iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
		count++
	}
	iter.Close()
	fmt.Printf("✓ Scanned %d keys (sorted order)\n", count)

	// Scan range [a, d).
	fmt.Println("\n── Scan [a, d) ──")
	iter2 := store.Scan([]byte("a"), []byte("d"))
	for iter2.Next() {
		fmt.Printf("  %s\n", iter2.Key())
	}
	iter2.Close()

	// Empty range.
	fmt.Println("\n── Scan empty range [zzz, \\xff) ──")
	iter3 := store.Scan([]byte("zzz"), []byte("\xff"))
	if !iter3.Next() {
		fmt.Println("✓ No keys found (expected)")
	}
	iter3.Close()

	// Snapshot isolation: writes during iteration are invisible.
	fmt.Println("\n── Snapshot isolation during scan ──")
	iter4 := store.Scan([]byte(""), []byte("g"))
	c := 0
	for iter4.Next() {
		fmt.Printf("  sees: %s\n", iter4.Key())
		c++
		if c == 2 {
			store.Put([]byte("new-key"), []byte("new-val"))
			fmt.Println("  (inserted new-key during iteration)")
		}
	}
	iter4.Close()
	fmt.Printf("✓ Snapshot scan: %d keys (new-key invisible during scan)\n", c)

	val, _ := store.Get([]byte("new-key"))
	fmt.Printf("  After scan: new-key = %s\n", val)
}
