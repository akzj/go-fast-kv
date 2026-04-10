// checkpoint demonstrates Checkpoint and crash recovery.
//
// Checkpoint writes a full snapshot of the current state to disk.
// On next Open(), recovery replays WAL entries from the last checkpoint.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/checkpoint
package main

import (
	"fmt"
	"log"
	"os"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	dir := "/tmp/go-fast-kv-checkpoint"

	// ─── Phase 1: Write + checkpoint ──────────────────────────────
	os.RemoveAll(dir)
	fmt.Println("── Phase 1: Write and checkpoint ──")
	store1, _ := kvstore.Open(kvstoreapi.Config{Dir: dir})

	for i := 0; i < 5; i++ {
		store1.Put([]byte(fmt.Sprintf("key%02d", i)), []byte(fmt.Sprintf("value%02d", i)))
	}
	fmt.Println("✓ Wrote 5 keys")

	if err := store1.Checkpoint(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Checkpoint written")
	store1.Close()

	// ─── Phase 2: Simulate crash, recover ─────────────────────────
	fmt.Println("\n── Phase 2: Simulate crash + recovery ──")
	// Delete WAL to simulate crash mid-flight (data is in checkpoint).
	os.RemoveAll(dir + "/wal")
	fmt.Println("✓ WAL deleted (simulated crash)")

	store2, _ := kvstore.Open(kvstoreapi.Config{Dir: dir})
	defer store2.Close()
	fmt.Println("✓ Store reopened — recovery succeeded")

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%02d", i)
		val, err := store2.Get([]byte(key))
		if err != nil {
			log.Fatalf("Recovery lost %s: %v", key, err)
		}
		fmt.Printf("  %s = %s\n", key, val)
	}
	fmt.Println("✓ All 5 keys recovered correctly")

	// ─── Phase 3: No checkpoint = data loss ───────────────────────
	fmt.Println("\n── Phase 3: No checkpoint (data loss on crash) ──")
	dir3 := "/tmp/go-fast-kv-checkpoint-no-ckpt"
	os.RemoveAll(dir3)

	store3, _ := kvstore.Open(kvstoreapi.Config{Dir: dir3})
	store3.Put([]byte("ckpt-key"), []byte("checkpointed"))
	store3.Checkpoint()
	store3.Put([]byte("after-ckpt-key"), []byte("not-checkpointed"))
	store3.Close()

	// Simulate crash: delete checkpoint, keep WAL.
	os.RemoveAll(dir3 + "/checkpoint")

	store4, _ := kvstore.Open(kvstoreapi.Config{Dir: dir3})
	defer store4.Close()

	_, err := store4.Get([]byte("after-ckpt-key"))
	if err == kvstoreapi.ErrKeyNotFound {
		fmt.Println("✓ after-ckpt-key: LOST on crash (expected — not checkpointed)")
	}
	v, _ := store4.Get([]byte("ckpt-key"))
	fmt.Printf("✓ ckpt-key: %s (survived — was checkpointed)\n", v)
}
