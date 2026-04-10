// config demonstrates all configuration options.
//
// Run: go run github.com/akzj/go-fast-kv/cmd/config
package main

import (
	"fmt"
	"log"
	"os"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
	fmt.Println("── Default config ──")
	fmt.Println("  MaxSegmentSize:       64 MB")
	fmt.Println("  InlineThreshold:       256 bytes")
	fmt.Println("  SyncMode:            SyncAlways (full durability)")
	fmt.Println("  AutoVacuumThreshold: 1000 ops")

	dir := "/tmp/go-fast-kv-config"
	os.RemoveAll(dir)

	cfg := kvstoreapi.Config{
		Dir:                 dir,
		MaxSegmentSize:      16 * 1024 * 1024, // 16 MB per segment
		InlineThreshold:     512,                // store > 512B in BlobStore
		SyncMode:           kvstoreapi.SyncAlways,
		AutoVacuumThreshold: 500,               // vacuum every 500 ops
	}

	store, err := kvstore.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	store.Put([]byte("key"), []byte("value"))
	val, _ := store.Get([]byte("key"))
	fmt.Printf("\n✓ Opened with custom config: MaxSegmentSize=16MB, InlineThreshold=512B, AutoVacuumThreshold=500\n")
	fmt.Printf("  Verified: Get('key') = %s\n", val)

	fmt.Println("\n── Config options ──")
	fmt.Println("  ┌──────────────────────┬─────────────┬───────────────────────────────────┐")
	fmt.Println("  │ Option               │ Default    │ Description                       │")
	fmt.Println("  ├──────────────────────┼─────────────┼───────────────────────────────────┤")
	fmt.Println("  │ Dir                  │ (required) │ Root data directory               │")
	fmt.Println("  │ MaxSegmentSize       │ 64 MB      │ Max segment file size            │")
	fmt.Println("  │ InlineThreshold      │ 256 bytes  │ Large value → BlobStore         │")
	fmt.Println("  │ SyncMode            │ SyncAlways │ SyncAlways / SyncNone           │")
	fmt.Println("  │ AutoVacuumThreshold │ 1000 ops   │ 0 = disabled                    │")
	fmt.Println("  └──────────────────────┴─────────────┴───────────────────────────────────┘")
}
