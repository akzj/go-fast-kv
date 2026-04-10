// batch demonstrates WriteBatch vs single Put performance.
//
// WriteBatch groups multiple Put/Delete operations into a single atomic
// transaction with ONE WAL fsync, dramatically reducing per-operation
// overhead for bulk writes.
//
// Run with: go run examples/batch/main.go
package main
import (
	"fmt"
	"time"
	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)
func main() {
	const n = 1000
	// ── Single Put (baseline) ────────────────────────────────────────
	fmt.Printf("Writing %d key-value pairs with single Put()...\n", n)
	start := time.Now()
	store1, err := kvstore.Open(kvstoreapi.Config{Dir: "/tmp/gfkv-batch-single"})
	if err != nil {
		panic(err)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := store1.Put([]byte(key), []byte(val)); err != nil {
			panic(err)
		}
	}
	singleMs := time.Since(start).Milliseconds()
	store1.Close()
	// ── WriteBatch ───────────────────────────────────────────────────
	fmt.Printf("Writing %d key-value pairs with WriteBatch...\n", n)
	start = time.Now()
	store2, err := kvstore.Open(kvstoreapi.Config{Dir: "/tmp/gfkv-batch-batch"})
	if err != nil {
		panic(err)
	}
	batch := store2.NewWriteBatch()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := batch.Put([]byte(key), []byte(val)); err != nil {
			panic(err)
		}
	}
	if err := batch.Commit(); err != nil {
		panic(err)
	}
	batchMs := time.Since(start).Milliseconds()
	store2.Close()
	// ── Results ──────────────────────────────────────────────────────
	fmt.Printf("\n  Single Put: %dms (%d µs/op)\n", singleMs, time.Duration(singleMs)*time.Millisecond/time.Duration(n))
	fmt.Printf("  WriteBatch: %dms (%d µs/op)\n", batchMs, time.Duration(batchMs)*time.Millisecond/time.Duration(n))
	if singleMs > 0 && batchMs > 0 {
		fmt.Printf("\n  Speedup: %.1fx\n", float64(singleMs)/float64(batchMs))
	}
	fmt.Println("\n✓ Batch example complete")
	fmt.Println("  WriteBatch is faster because all ", n, " ops share ONE transaction + ONE WAL fsync.")
}
