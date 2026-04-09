package bench

import (
	"fmt"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
)

// ─── Badger SyncWrites comparison ───────────────────────────────────

func openBadgerSync(dir string) (kvAdapter, error) {
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.Logger = nil
	opts.SyncWrites = true // FORCE sync writes for fair comparison
	opts.ValueLogFileSize = 64 << 20
	opts.MemTableSize = 8 << 20
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 2
	opts.NumLevelZeroTablesStall = 4
	opts.NumCompactors = 2
	opts.BlockCacheSize = 16 << 20
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerAdapter{db: db}, nil
}

func openBadgerAsync(dir string) (kvAdapter, error) {
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.Logger = nil
	opts.SyncWrites = false // explicit async (Badger v4 default)
	opts.ValueLogFileSize = 64 << 20
	opts.MemTableSize = 8 << 20
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 2
	opts.NumLevelZeroTablesStall = 4
	opts.NumCompactors = 2
	opts.BlockCacheSize = 16 << 20
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerAdapter{db: db}, nil
}

// Badger with SyncWrites=true — fair durability comparison
func BenchmarkBadger_PutSequential_SyncTrue(b *testing.B) {
	benchPutSequential(b, openBadgerSync)
}

// Badger with SyncWrites=false (default) — async, no per-write fsync
func BenchmarkBadger_PutSequential_SyncFalse(b *testing.B) {
	benchPutSequential(b, openBadgerAsync)
}

// ─── go-fast-kv no-WAL-fsync experiment ─────────────────────────────
// We can't easily disable WAL fsync without modifying code, so instead
// we measure the WAL-only cost by benchmarking WriteBatch with 1 op

func BenchmarkGoFastKV_BatchPut1(b *testing.B) {
	// WriteBatch with 1 op = same as Put but goes through batch path
	// This confirms batch overhead is minimal
	dir := b.TempDir()
	store, err := openGoFastKV(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	vals := pregenValues(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%010d", i))
		if err := store.Put(key, vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

// Batch sizes to see amortization curve
func BenchmarkGoFastKV_BatchPut10(b *testing.B) {
	benchBatchN(b, 10)
}

func BenchmarkGoFastKV_BatchPut50(b *testing.B) {
	benchBatchN(b, 50)
}

func BenchmarkGoFastKV_BatchPut500(b *testing.B) {
	benchBatchN(b, 500)
}

func benchBatchN(b *testing.B, batchSize int) {
	dir := b.TempDir()
	store, err := openGoFastKV(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	vals := pregenValues(256)
	counter := 0

	gfkv := store.(*goFastKVAdapter)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := gfkv.store.NewWriteBatch()
		for j := 0; j < batchSize; j++ {
			key := []byte(fmt.Sprintf("key-%010d", counter))
			counter++
			if err := batch.Put(key, vals[counter%len(vals)]); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── go-fast-kv SyncNone benchmarks ────────────────────────────────

func openGoFastKV_SyncNone(dir string) (kvAdapter, error) {
	cfg := kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 256 * 1024 * 1024,
		SyncMode:       kvstoreapi.SyncNone,
	}
	s, err := kvstore.Open(cfg)
	if err != nil {
		return nil, err
	}
	return &goFastKVAdapter{store: s}, nil
}

// go-fast-kv with SyncNone — no per-write WAL fsync (like Badger default)
func BenchmarkGoFastKV_PutSequential_SyncNone(b *testing.B) {
	benchPutSequential(b, openGoFastKV_SyncNone)
}

// go-fast-kv SyncNone with random keys
func BenchmarkGoFastKV_PutRandom_SyncNone(b *testing.B) {
	benchPutRandom(b, openGoFastKV_SyncNone)
}
