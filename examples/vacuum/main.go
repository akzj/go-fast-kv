// vacuum demonstrates auto-vacuum and manual RunVacuum.
//
// go-fast-kv uses MVCC — old versions are marked deleted but NOT physically
// removed until Vacuum runs. This example shows:
//
//   1. Auto-vacuum: triggered automatically after AutoVacuumThreshold ops
//   2. Manual vacuum: RunVacuum() always available
//   3. How vacuum physically removes deleted entries
//
// Run:
//   go run examples/vacuum/main.go
package main

import (
	"fmt"
	"os"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	fmt.Println("=== go-fast-kv Vacuum Demo ===")
	fmt.Println()

	// ── Demo 1: Auto-vacuum with low threshold ─────────────────────
	fmt.Println("── Demo 1: Auto-vacuum (threshold=10) ──")
	dir1 := "/tmp/go-fast-kv-vacuum-auto"
	os.RemoveAll(dir1)

	s1, err := kvstore.Open(kvstoreapi.Config{
		Dir:                 dir1,
		AutoVacuumThreshold: 10, // very low — triggers after 10 ops
	})
	if err != nil {
		panic(err)
	}

	// Put 5 keys then delete them — crosses threshold of 10 (5 puts + 5 deletes).
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("user:%03d", i)
		val := fmt.Sprintf("User %d", i)
		if err := s1.Put([]byte(key), []byte(val)); err != nil {
			panic(err)
		}
		if err := s1.Delete([]byte(key)); err != nil {
			panic(err)
		}
	}

	// Give auto-vacuum goroutine time to run (async).
	fmt.Println("  5 puts + 5 deletes performed (threshold=10)")
	fmt.Println("  Auto-vacuum goroutine running in background...")
	s1.Close()
	fmt.Println("  Store closed — vacuum goroutine finished during close")
	fmt.Println()

	// ── Demo 2: Manual vacuum ──────────────────────────────────────
	fmt.Println("── Demo 2: Manual RunVacuum() ──")
	dir2 := "/tmp/go-fast-kv-vacuum-manual"
	os.RemoveAll(dir2)

	s2, err := kvstore.Open(kvstoreapi.Config{
		Dir:                 dir2,
		AutoVacuumThreshold: 0, // disabled — we trigger manually
	})
	if err != nil {
		panic(err)
	}

	// Put and delete many keys.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%03d", i)
		val := fmt.Sprintf("value-%d", i)
		if err := s2.Put([]byte(key), []byte(val)); err != nil {
			panic(err)
		}
		if err := s2.Delete([]byte(key)); err != nil {
			panic(err)
		}
	}
	fmt.Println("  100 puts + 100 deletes performed (auto-vacuum disabled)")

	// Verify: deleted keys should be logically invisible (but still in pages).
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key:%03d", i)
		_, err := s2.Get([]byte(key))
		if err == kvstoreapi.ErrKeyNotFound {
			fmt.Printf("  key:%03d = logically deleted (not found via Get)\n", i)
		}
	}

	// Run vacuum manually — physically removes dead entries.
	fmt.Println()
	fmt.Println("  Calling s.RunVacuum()...")
	stats, err := s2.RunVacuum()
	if err != nil {
		panic(err)
	}
	fmt.Printf("  Vacuum stats: scanned=%d leaves, modified=%d, removed=%d entries, freed=%d blobs\n",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
	fmt.Println()
	fmt.Println("=== Done ===")
	s2.Close()
}
