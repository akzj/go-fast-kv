// batch demonstrates WriteBatch — grouping multiple operations into a
// single atomic transaction with one WAL fsync.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/batch
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	const n = 1000

	// Individual Put.
	dir1 := "/tmp/go-fast-kv-batch/individual"
	os.RemoveAll(dir1)
	store1, _ := kvstore.Open(kvstoreapi.Config{Dir: dir1})
	defer store1.Close()

	start := time.Now()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := fmt.Sprintf("value%06d", i)
		if err := store1.Put([]byte(key), []byte(val)); err != nil {
			log.Fatal(err)
		}
	}
	individualTime := time.Since(start)
	fmt.Printf("Individual Put:  %d ops in %v (%.2f µs/op)\n",
		n, individualTime, float64(individualTime.Microseconds())/float64(n))

	// WriteBatch — all ops in ONE transaction + ONE WAL fsync.
	dir2 := "/tmp/go-fast-kv-batch/batch"
	os.RemoveAll(dir2)
	store2, _ := kvstore.Open(kvstoreapi.Config{Dir: dir2})
	defer store2.Close()

	start = time.Now()
	batch := store2.NewWriteBatch()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := fmt.Sprintf("value%06d", i)
		if err := batch.Put([]byte(key), []byte(val)); err != nil {
			log.Fatal(err)
		}
	}
	if err := batch.Commit(); err != nil {
		log.Fatal(err)
	}
	batchTime := time.Since(start)
	fmt.Printf("WriteBatch:     %d ops in %v (%.2f µs/op)\n",
		n, batchTime, float64(batchTime.Microseconds())/float64(n))

	speedup := float64(individualTime) / float64(batchTime)
	fmt.Printf("\n✓ WriteBatch is %.1fx faster than individual Put for %d ops\n", speedup, n)

	// Verify.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		val, err := store2.Get([]byte(key))
		if err != nil || string(val) != fmt.Sprintf("value%06d", i) {
			log.Fatalf("Data mismatch at %s", key)
		}
	}
	fmt.Println("✓ All data verified correct after WriteBatch commit")
}
