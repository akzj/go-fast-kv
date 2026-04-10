// largevalue demonstrates transparent large value storage.
//
// Values larger than InlineThreshold (default 256 bytes) are automatically
// stored in BlobStore, keeping B-tree leaf pages compact.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/largevalue
package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	dir := "/tmp/go-fast-kv-largevalue"
	os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Small value (< 256 bytes) — stored inline in B-tree.
	smallVal := "Hello, World!"
	if err := store.Put([]byte("small"), []byte(smallVal)); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Put small value (%d bytes)\n", len(smallVal))

	// Large value (> 256 bytes) — transparently stored in BlobStore.
	largeVal := strings.Repeat("A", 1<<20) // 1 MB
	if err := store.Put([]byte("large"), []byte(largeVal)); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Put large value (%d bytes / 1 MB)\n", len(largeVal))

	// Retrieve and verify both.
	smallRet, _ := store.Get([]byte("small"))
	fmt.Printf("✓ Get small: %q\n", string(smallRet))

	largeRet, err := store.Get([]byte("large"))
	if err != nil {
		log.Fatal(err)
	}
	if !bytes.Equal([]byte(largeVal), largeRet) {
		log.Fatal("Large value mismatch!")
	}
	fmt.Printf("✓ Get large: %d bytes — content verified\n", len(largeRet))

	// Multiple large values.
	fmt.Println("\n── Writing 5 × 512KB values ──")
	start := time.Now()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("blob%02d", i)
		val := strings.Repeat(string(rune('A'+i)), 512*1024)
		store.Put([]byte(key), []byte(val))
	}
	fmt.Printf("✓ Wrote 5 × 512KB in %v\n", time.Since(start))

	for i := 0; i < 5; i++ {
		val, _ := store.Get([]byte(fmt.Sprintf("blob%02d", i)))
		if len(val) != 512*1024 {
			log.Fatalf("blob%02d: got %d bytes, want 512KB", i, len(val))
		}
	}
	fmt.Println("✓ All 5 large values verified")

	// Update large value.
	newVal := strings.Repeat("UPDATED", 128*1024)
	store.Put([]byte("large"), []byte(newVal))
	updated, _ := store.Get([]byte("large"))
	if !strings.HasPrefix(string(updated), "UPDATED") {
		log.Fatal("Update failed!")
	}
	fmt.Printf("✓ Updated large value: %d bytes\n", len(updated))

	fmt.Println("\n✓ Large value example completed")
}
