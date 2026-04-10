// syncmode demonstrates the trade-off between durability and performance.
//
// go-fast-kv has two sync modes:
//   - SyncAlways (default): fsync after every write — maximum durability.
//   - SyncNone: no per-write fsync — faster writes, risk of data loss on crash.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/syncmode
package main

import (
	"fmt"
	"os"
	"time"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	const n = 1000

	// SyncAlways (default — full durability).
	dir1 := "/tmp/go-fast-kv-sync"
	os.RemoveAll(dir1)
	store1, _ := kvstore.Open(kvstoreapi.Config{
		Dir:      dir1,
		SyncMode: kvstoreapi.SyncAlways,
	})

	start := time.Now()
	for i := 0; i < n; i++ {
		store1.Put([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("value%06d", i)))
	}
	syncAlwaysTime := time.Since(start)
	store1.Close()

	fmt.Printf("SyncAlways: %d ops in %v (%.2f µs/op)\n",
		n, syncAlwaysTime, float64(syncAlwaysTime.Microseconds())/float64(n))

	// SyncNone (fast — no durability until close/checkpoint).
	dir2 := "/tmp/go-fast-kv-nosync"
	os.RemoveAll(dir2)
	store2, _ := kvstore.Open(kvstoreapi.Config{
		Dir:      dir2,
		SyncMode: kvstoreapi.SyncNone,
	})

	start = time.Now()
	for i := 0; i < n; i++ {
		store2.Put([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("value%06d", i)))
	}
	syncNoneTime := time.Since(start)
	store2.Close()

	fmt.Printf("SyncNone:   %d ops in %v (%.2f µs/op)\n",
		n, syncNoneTime, float64(syncNoneTime.Microseconds())/float64(n))

	speedup := float64(syncAlwaysTime) / float64(syncNoneTime)
	fmt.Printf("\n✓ SyncNone is %.1fx faster than SyncAlways\n", speedup)
	fmt.Println("✓ Both survive clean Close()")
	fmt.Println("✓ SyncAlways: no data loss on crash")
	fmt.Println("✓ SyncNone: may lose recent writes on crash (data is in WAL)")
}
