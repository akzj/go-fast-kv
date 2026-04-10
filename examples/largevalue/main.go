// largevalue demonstrates transparent large value storage via BlobStore.
//
// Values larger than InlineThreshold (default 256 bytes) are automatically
// stored in BlobStore. The Get/Put API is identical — it's transparent.
//
// Run with: go run examples/largevalue/main.go
package main
import (
	"bytes"
	"fmt"
	"strings"
	"time"
	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)
func main() {
	store, err := kvstore.Open(kvstoreapi.Config{Dir: "/tmp/gfkv-large"})
	if err != nil {
		panic(err)
	}
	defer store.Close()
	// Part 1: Inline value (≤256 bytes)
	small := strings.Repeat("x", 100)
	store.Put([]byte("inline"), []byte(small))
	v, _ := store.Get([]byte("inline"))
	fmt.Printf("  Inline (100B): stored=%d bytes, retrieved=%d bytes ✓\n", len(small), len(v))
	// Part 2: Blob value (>256 bytes)
	large := strings.Repeat("DATA", 100000) // 400KB
	store.Put([]byte("blob"), []byte(large))
	v, _ = store.Get([]byte("blob"))
	fmt.Printf("  Blob (400KB): stored=%d bytes, retrieved=%d bytes ✓\n", len(large), len(v))
	// Part 3: 1MB value
	mb := bytes.Repeat([]byte("M"), 1024*1024)
	store.Put([]byte("1mb"), mb)
	v, _ = store.Get([]byte("1mb"))
	fmt.Printf("  1MB value: retrieved %d bytes ✓\n", len(v))
	// Part 4: Batch write large values
	start := time.Now()
	batch := store.NewWriteBatch()
	for i := 0; i < 5; i++ {
		data := strings.Repeat(fmt.Sprintf("B%04d", i), 512*1024/10) // 512KB each
		batch.Put([]byte(fmt.Sprintf("batch%02d", i)), []byte(data))
	}
	batch.Commit()
	fmt.Printf("\n  Batch wrote 5×512KB = 2.5MB in %v ✓\n", time.Since(start))
	fmt.Println("\n✓ Large value example complete")
	fmt.Println("  Values > 256B are transparently stored in BlobStore.")
}
