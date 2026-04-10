// config demonstrates all kvstore configuration options.
//
// Run with: go run examples/config/main.go
package main
import (
	"fmt"
	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)
func main() {
	fmt.Println("=== go-fast-kv Configuration Reference ===\n")
	configs := []struct {
		name string
		cfg  kvstoreapi.Config
		note string
	}{
		{
			name: "defaults",
			cfg:  kvstoreapi.Config{Dir: "/tmp/gfkv-cfg-defaults"},
			note: "MaxSegmentSize=64MB, InlineThreshold=256B, SyncAlways, AutoVacuum=1000",
		},
		{
			name: "small segments",
			cfg: kvstoreapi.Config{
				Dir:            "/tmp/gfkv-cfg-small-seg",
				MaxSegmentSize: 1 << 20, // 1MB segments
			},
			note: "MaxSegmentSize=1MB — useful for memory-constrained environments",
		},
		{
			name: "small inline",
			cfg: kvstoreapi.Config{
				Dir:               "/tmp/gfkv-cfg-small-inline",
				InlineThreshold:   64, // store >64B in BlobStore
			},
			note: "InlineThreshold=64B — more values go to BlobStore",
		},
		{
			name: "fast writes",
			cfg: kvstoreapi.Config{
				Dir:      "/tmp/gfkv-cfg-fast",
				SyncMode: kvstoreapi.SyncNone,
			},
			note: "SyncMode=SyncNone — faster writes, risk of data loss on crash",
		},
		{
			name: "aggressive vacuum",
			cfg: kvstoreapi.Config{
				Dir:                 "/tmp/gfkv-cfg-vac",
				AutoVacuumThreshold: 100, // trigger after 100 ops
			},
			note: "AutoVacuumThreshold=100 — more frequent cleanup",
		},
		{
			name: "vacuum disabled",
			cfg: kvstoreapi.Config{
				Dir:                 "/tmp/gfkv-cfg-novac",
				AutoVacuumThreshold: 0, // disable auto-vacuum
			},
			note: "AutoVacuumThreshold=0 — auto-vacuum off, manual RunVacuum() still works",
		},
	}
	for _, c := range configs {
		fmt.Printf("Config: %s\n", c.name)
		fmt.Printf("  Note: %s\n", c.note)
		store, err := kvstore.Open(c.cfg)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			continue
		}
		// Quick smoke test
		store.Put([]byte("test"), []byte("ok"))
		if v, err := store.Get([]byte("test")); err == nil {
			fmt.Printf("  Test: Put+Get OK (%q)\n", v)
		}
		store.Close()
		fmt.Println()
	}
	fmt.Println("=== Config Defaults ===")
	d := kvstoreapi.Config{}
	fmt.Printf("  MaxSegmentSize:    %d (default)\n", d.MaxSegmentSize)
	fmt.Printf("  InlineThreshold:   %d (default)\n", d.InlineThreshold)
	fmt.Printf("  SyncMode:          %d (SyncAlways=0)\n", d.SyncMode)
	fmt.Printf("  AutoVacuumThreshold: %d (0=disabled, >0=enabled)\n", d.AutoVacuumThreshold)
	fmt.Printf("  AutoVacuumRatio:    %.2f (0=disabled)\n", d.AutoVacuumRatio)
	fmt.Println("\n✓ Config example complete")
}
