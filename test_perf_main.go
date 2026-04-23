package main

import (
    "fmt"
    "math/rand"
    "os"
    "time"
    
    tkvstore "github.com/akzj/go-fast-kv/internal/kvstore"
    kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func main() {
    const N = 100000
    fmt.Printf("=== Performance Test: %d operations ===\n", N)
    
    // Clean up
    os.RemoveAll("/tmp/kv_perf_test")
    
    // Open store
    s, err := tkvstore.Open(kvstoreapi.Config{
        Dir:      "/tmp/kv_perf_test",
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
    
    // Shuffle for random order
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
        if (i+1)%20000 == 0 {
            fmt.Printf("Written: %d / %d\n", i+1, N)
        }
    }
    writeDur := time.Since(start)
    fmt.Printf("Write: %d ops in %v (%.2f ops/s)\n", N, writeDur, float64(N)/writeDur.Seconds())
    
    // Read test
    fmt.Printf("\n=== Read Test: %d random reads ===\n", N)
    start = time.Now()
    hits := 0
    for i := 0; i < N; i++ {
        v, err := s.Get(keys[i])
        if err == nil && v != nil {
            hits++
        }
        if (i+1)%20000 == 0 {
            fmt.Printf("Read: %d / %d\n", i+1, N)
        }
    }
    readDur := time.Since(start)
    fmt.Printf("Read: %d ops in %v (%.2f ops/s), Hit rate: %.2f%%\n", N, readDur, float64(N)/readDur.Seconds(), float64(hits)/float64(N)*100)
    
    // Metrics
    m := s.GetMetrics()
    fmt.Printf("\n=== Metrics ===\n")
    fmt.Printf("PageReads: %d, PageWrites: %d, PageSplits: %d\n", m.PageReads, m.PageWrites, m.PageSplits)
    fmt.Printf("PageCacheHits: %d, HitRate: %.2f%%\n", m.PageCacheHits, float64(m.PageCacheHits)/float64(m.PageReads)*100)
}
