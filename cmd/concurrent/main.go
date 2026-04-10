// concurrent demonstrates thread-safe concurrent access from multiple goroutines.
//
// go-fast-kv uses per-page RwLocks — only pages being modified are locked,
// not the whole tree. Readers never block writers.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/concurrent
package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	dir := "/tmp/go-fast-kv-concurrent"
	os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		log.Fatal(err)
	}

	const (
		numWriters = 4
		numReaders = 4
		opsPerWriter = 500
		opsPerReader = 200
	)

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				key := fmt.Sprintf("w%d-k%06d", writerID, i)
				val := fmt.Sprintf("v%d-%06d", writerID, i)
				store.Put([]byte(key), []byte(val))
			}
		}(w)
	}

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			found := 0
			for i := 0; i < opsPerReader; i++ {
				key := fmt.Sprintf("w0-k%06d", i%opsPerWriter)
				val, err := store.Get([]byte(key))
				if err == nil && len(val) > 0 {
					found++
				}
			}
			fmt.Printf("  Reader %d: found %d/%d keys\n", readerID, found, opsPerReader)
		}(r)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := numWriters*opsPerWriter + numReaders*opsPerReader
	fmt.Printf("\n✓ Completed in %v\n", elapsed)
	fmt.Printf("  %d ops across %d goroutines (%.0f ops/sec)\n",
		totalOps, numWriters+numReaders, float64(totalOps)/elapsed.Seconds())

	// Verify data integrity.
	errors := 0
	for w := 0; w < numWriters; w++ {
		for i := 0; i < opsPerWriter; i++ {
			key := fmt.Sprintf("w%d-k%06d", w, i)
			val, err := store.Get([]byte(key))
			if err != nil {
				errors++
			}
			expected := fmt.Sprintf("v%d-%06d", w, i)
			if string(val) != expected {
				errors++
			}
		}
	}
	if errors == 0 {
		fmt.Printf("✓ All %d keys verified correct\n", numWriters*opsPerWriter)
	} else {
		fmt.Printf("✗ %d errors\n", errors)
	}

	store.Close()
}
