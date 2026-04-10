// vacuum demonstrates MVCC old version cleanup.
//
// In go-fast-kv, Delete marks a key as logically invisible (MVCC),
// but the old version remains in B-tree leaf pages until vacuum runs.
// Vacuum physically removes entries that are no longer visible.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/vacuum
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
	// ─── Auto-vacuum ───────────────────────────────────────────────
	// With default settings, vacuum triggers after 1000 ops.
	// We use a low threshold for demonstration.
	dir := "/tmp/go-fast-kv-vacuum"
	os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{
		Dir:                 dir,
		AutoVacuumThreshold: 5, // trigger after 5 ops
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%03d", i)
		if err := store.Put([]byte(key), []byte(fmt.Sprintf("v%03d", i))); err != nil {
			log.Fatal(err)
		}
		if err := store.Delete([]byte(key)); err != nil {
			log.Fatal(err)
		}
	}
	// Auto-vacuum goroutine will have triggered. Give it time to finish.
	time.Sleep(500 * time.Millisecond)
	fmt.Println("✓ Put+deleted 20 keys — auto-vacuum triggered at threshold")

	// ─── Manual vacuum ─────────────────────────────────────────────
	// Useful when auto-vacuum is disabled.
	dir2 := "/tmp/go-fast-kv-vacuum-manual"
	os.RemoveAll(dir2)

	store2, _ := kvstore.Open(kvstoreapi.Config{
		Dir:                 dir2,
		AutoVacuumThreshold: 0, // disabled
	})
	defer store2.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("dead%03d", i)
		store2.Put([]byte(key), []byte(fmt.Sprintf("v%03d", i)))
		store2.Delete([]byte(key))
	}
	fmt.Println("✓ Put+deleted 10 keys with auto-vacuum disabled")

	stats, err := store2.RunVacuum()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Manual RunVacuum: removed %d entries, scanned %d leaves\n",
		stats.EntriesRemoved, stats.LeavesScanned)

	// Verify all deleted keys are gone.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("dead%03d", i)
		_, err := store2.Get([]byte(key))
		if err != kvstoreapi.ErrKeyNotFound {
			log.Fatalf("Key %s still visible after vacuum", key)
		}
	}
	fmt.Println("✓ All deleted keys physically removed by vacuum")

	// Live keys are unaffected.
	store2.Put([]byte("live"), []byte("value"))
	store2.Delete([]byte("live"))
	store2.Put([]byte("live"), []byte("new-value"))
	liveStats, _ := store2.RunVacuum()
	fmt.Printf("\n✓ Vacuum with live keys: removed %d entries (only truly dead ones)\n", liveStats.EntriesRemoved)
	val, _ := store2.Get([]byte("live"))
	fmt.Printf("  live key still visible: %s\n", val)
}
