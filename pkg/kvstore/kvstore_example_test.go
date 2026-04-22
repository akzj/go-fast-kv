package kvstore_test

import (
	"fmt"
	"os"

	"github.com/akzj/go-fast-kv/pkg/kvstore"
)

// Example demonstrates basic usage of the go-fast-kv kvstore package.
func Example() {
	// Create a temporary directory for the store
	dir, err := os.MkdirTemp("", "kvstore-example-*")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer os.RemoveAll(dir)

	// Open a new KVStore
	store, err := kvstore.Open(kvstore.Config{Dir: dir})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer store.Close()

	// Put a key-value pair
	err = store.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Get the value back
	value, err := store.Get([]byte("key1"))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("Got value: %s\n", value)

	// Scan keys in a range
	iter := store.Scan([]byte("key0"), []byte("key9"))
	defer iter.Close()
	for iter.Next() {
		fmt.Printf("key=%s value=%s\n", iter.Key(), iter.Value())
	}

	// Output:
	// Got value: value1
	// key=key1 value=value1
}