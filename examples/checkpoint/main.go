// checkpoint demonstrates durability and crash recovery.
//
// Checkpoint writes a full snapshot to disk, allowing WAL truncation.
// After a checkpoint, the WAL contains only entries since the last checkpoint,
// making recovery faster.
//
// Run with: go run examples/checkpoint/main.go
package main
import (
	"fmt"
	"os"
	"path/filepath"
	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)
func main() {
	dir := "/tmp/gfkv-checkpoint"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)
	// Part 1: Without checkpoint — data recovered from WAL
	fmt.Println("=== Part 1: Recovery WITHOUT Checkpoint ===")
	s1, _ := kvstore.Open(kvstoreapi.Config{Dir: filepath.Join(dir, "nocheckpoint")})
	for i := 0; i < 10; i++ {
		s1.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}
	// Close without checkpoint — WAL still has all entries
	s1.Close()
	// Reopen — WAL replay recovers all data
	s1r, _ := kvstore.Open(kvstoreapi.Config{Dir: filepath.Join(dir, "nocheckpoint")})
	v, _ := s1r.Get([]byte("k05"))
	fmt.Printf("  After recovery (no checkpoint): k05 = %s ✓\n", v)
	s1r.Close()
	// Part 2: With checkpoint — WAL truncated
	fmt.Println("\n=== Part 2: Recovery WITH Checkpoint ===")
	s2, _ := kvstore.Open(kvstoreapi.Config{Dir: filepath.Join(dir, "withcheckpoint")})
	for i := 0; i < 10; i++ {
		s2.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}
	// Checkpoint persists state and truncates WAL
	if err := s2.Checkpoint(); err != nil {
		panic(err)
	}
	// Write more data after checkpoint
	for i := 10; i < 20; i++ {
		s2.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}
	s2.Close()
	// Reopen — data before checkpoint from snapshot file, after from WAL
	s2r, _ := kvstore.Open(kvstoreapi.Config{Dir: filepath.Join(dir, "withcheckpoint")})
	v1, _ := s2r.Get([]byte("k05"))  // from checkpoint
	v2, _ := s2r.Get([]byte("k15"))  // from WAL
	fmt.Printf("  After recovery (with checkpoint): k05 = %s, k15 = %s ✓\n", v1, v2)
	s2r.Close()
	fmt.Println("\n  Checkpoint: writes full snapshot → faster recovery, WAL truncated")
	fmt.Println("  No checkpoint: WAL replay → works but slower on large DBs")
	fmt.Println("\n✓ Checkpoint example complete")
}
