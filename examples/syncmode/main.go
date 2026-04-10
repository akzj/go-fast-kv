// syncmode demonstrates the performance difference between SyncAlways
// (fsync per write, maximum durability) and SyncNone (no per-write fsync,
// faster writes but risk of data loss on crash).
//
// Run:
//   go run examples/syncmode/main.go
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
	fmt.Println("=== SyncMode Performance Demo ===")
	fmt.Println()

	// ─── SyncAlways (default) ─────────────────────────────────────
	dir1 := "/tmp/go-fast-kv-syncmode-always"
	os.RemoveAll(dir1)
	store1, err := kvstore.Open(kvstoreapi.Config{Dir: dir1, SyncMode: kvstoreapi.SyncAlways})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("── SyncAlways (fsync per write) ─────────────────")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := fmt.Sprintf("value%06d", i)
		if err := store1.Put([]byte(key), []byte(val)); err != nil {
			log.Fatal(err)
		}
	}
	elapsedAlways := time.Since(start)
	store1.Close()
	fmt.Printf("  1000 Put ops in %v\n", elapsedAlways)
	fmt.Printf("  Throughput: %.0f ops/sec\n\n", float64(1000)/elapsedAlways.Seconds())

	// ─── SyncNone ─────────────────────────────────────────────────
	dir2 := "/tmp/go-fast-kv-syncmode-none"
	os.RemoveAll(dir2)
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: dir2, SyncMode: kvstoreapi.SyncNone})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("── SyncNone (no per-write fsync) ────────────────")
	start = time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := fmt.Sprintf("value%06d", i)
		if err := store2.Put([]byte(key), []byte(val)); err != nil {
			log.Fatal(err)
		}
	}
	elapsedNone := time.Since(start)
	fmt.Printf("  1000 Put ops in %v\n", elapsedNone)
	fmt.Printf("  Throughput: %.0f ops/sec\n\n", float64(1000)/elapsedNone.Seconds())

	// ─── Summary ──────────────────────────────────────────────────
	speedup := float64(elapsedAlways) / float64(elapsedNone)
	fmt.Printf("✓ SyncNone is %.1fx faster than SyncAlways\n", speedup)
	fmt.Println()

	// Note: SyncNone risks data loss on crash. Close() always fsyncs,
	// and Checkpoint() does a full fsync, so periodic Checkpoint()
	// calls reduce the risk window.
	fmt.Println("⚠  Trade-off:")
	fmt.Println("   SyncAlways: every write is durable immediately.")
	fmt.Println("              Crash = at most 1 write lost.")
	fmt.Println()
	fmt.Println("   SyncNone:   writes go to OS page cache, not disk.")
	fmt.Println("              Crash between Checkpoint/Close = data loss.")
	fmt.Println("              Best for: bulk import, benchmarks, rebuildable data.")
	fmt.Println()
	fmt.Println("   Safety tip: call Checkpoint() periodically with SyncNone")
	fmt.Println("   to flush all data to disk.")
}
