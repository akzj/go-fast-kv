package main

import (
    "fmt"
    "math/rand"
    "os"
    "runtime"
    "runtime/pprof"
    "time"
    
    tkvstore "github.com/akzj/go-fast-kv/internal/kvstore"
    kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
    const N = 1000000
    fmt.Printf("=== Performance Test with pprof: %d operations ===\n", N)
    
    // Clean up
    os.RemoveAll("/tmp/kv_pprof_test")
    
    // Start CPU profile
    f, err := os.Create("/tmp/cpu.prof")
    if err != nil {
        panic(err)
    }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
    
    // Open store
    s, err := tkvstore.Open(kvstoreapi.Config{
        Dir:      "/tmp/kv_pprof_test",
        SyncMode: kvstoreapi.SyncNone,
    })
    if err != nil {
        panic(err)
    }
    defer s.Close()
    
    // Generate random keys
    keys := make([][]byte, N)
    values := make([][]byte, N)
    for i := 0; i < N; i++ {
        keys[i] = []byte(fmt.Sprintf("key_%08d_%08d", i, rand.Int()))
        values[i] = []byte(fmt.Sprintf("value_%08d", i))
    }
    
    // Shuffle
    rand.Seed(42)
    rand.Shuffle(N, func(i, j int) {
        keys[i], keys[j] = keys[j], keys[i]
        values[i], values[j] = values[j], values[i]
    })
    
    // Write test
    fmt.Printf("\n=== Write Test: %d random writes ===\n", N)
    start := time.Now()
    for i := 0; i < N; i++ {
        if err := s.Put(keys[i], values[i]); err != nil {
            panic(err)
        }
        if (i+1)%100000 == 0 {
            fmt.Printf("Written: %d / %d\n", i+1, N)
        }
    }
    writeDur := time.Since(start)
    fmt.Printf("Write: %d ops in %v (%.2f ops/s)\n", N, writeDur, float64(N)/writeDur.Seconds())
    
    // Stop CPU profile, start mem profile
    pprof.StopCPUProfile()
    f.Close()
    
    f2, err := os.Create("/tmp/mem.prof")
    if err != nil {
        panic(err)
    }
    runtime.GC()
    pprof.WriteHeapProfile(f2)
    f2.Close()
    
    // Read test with CPU profile
    fmt.Printf("\n=== Read Test: %d random reads ===\n", N)
    f3, err := os.Create("/tmp/cpu_read.prof")
    if err != nil {
        panic(err)
    }
    pprof.StartCPUProfile(f3)
    defer pprof.StopCPUProfile()
    
    start = time.Now()
    hits := 0
    for i := 0; i < N; i++ {
        v, err := s.Get(keys[i])
        if err == nil && v != nil {
            hits++
        }
        if (i+1)%100000 == 0 {
            fmt.Printf("Read: %d / %d\n", i+1, N)
        }
    }
    readDur := time.Since(start)
    fmt.Printf("Read: %d ops in %v (%.2f ops/s), Hit rate: %.2f%%\n", N, readDur, float64(N)/readDur.Seconds(), float64(hits)/float64(N)*100)
    
    pprof.StopCPUProfile()
    f3.Close()
    
    // Metrics
    m := s.GetMetrics()
    fmt.Printf("\n=== Metrics ===\n")
    fmt.Printf("PageReads: %d, PageWrites: %d, PageSplits: %d\n", m.PageReads, m.PageWrites, m.PageSplits)
    fmt.Printf("PageCacheHits: %d, HitRate: %.2f%%\n", m.PageCacheHits, float64(m.PageCacheHits)/float64(m.PageReads)*100)
    
    fmt.Printf("\n=== pprof files saved ===\n")
    fmt.Printf("  /tmp/cpu.prof - Write CPU profile\n")
    fmt.Printf("  /tmp/mem.prof - Heap profile\n")
    fmt.Printf("  /tmp/cpu_read.prof - Read CPU profile\n")
    fmt.Printf("\nTo analyze: go tool pprof /tmp/cpu.prof\n")
}
