// Package bench provides comparative benchmarks for go-fast-kv vs BoltDB vs Badger.
//
// Run:
//
//	go test ./internal/bench/ -bench=. -benchtime=3s -count=1 -timeout=30m
//
// For a quick smoke test:
//
//	go test ./internal/bench/ -bench=. -benchtime=1s -count=1 -timeout=10m
package bench

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	badger "github.com/dgraph-io/badger/v4"
	bolt "go.etcd.io/bbolt"
)

// ─── Constants ──────────────────────────────────────────────────────

const (
	numKeys    = 10_000
	valueSize  = 100
	scanCount  = 100
	concurrent = 10
)

var bucketName = []byte("bench")

// ─── Key/Value Generators ───────────────────────────────────────────

func makeKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%010d", i))
}

func makeValue(rng *rand.Rand) []byte {
	v := make([]byte, valueSize)
	rng.Read(v)
	return v
}

func pregenValues(n int) [][]byte {
	rng := rand.New(rand.NewSource(42))
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = makeValue(rng)
	}
	return vals
}

// ─── Store Adapters ─────────────────────────────────────────────────

type kvAdapter interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Scan(start, end []byte) (int, error)
	Close() error
}

// --- go-fast-kv adapter ---

type goFastKVAdapter struct {
	store kvstoreapi.Store
}

func openGoFastKV(dir string) (kvAdapter, error) {
	cfg := kvstoreapi.Config{
		Dir:            dir,
		MaxSegmentSize: 256 * 1024 * 1024,
	}
	s, err := kvstore.Open(cfg)
	if err != nil {
		return nil, err
	}
	return &goFastKVAdapter{store: s}, nil
}

func (a *goFastKVAdapter) Put(key, value []byte) error {
	return a.store.Put(key, value)
}

func (a *goFastKVAdapter) Get(key []byte) ([]byte, error) {
	return a.store.Get(key)
}

func (a *goFastKVAdapter) Scan(start, end []byte) (int, error) {
	iter := a.store.Scan(start, end)
	defer iter.Close()
	count := 0
	for iter.Next() {
		_ = iter.Key()
		_ = iter.Value()
		count++
	}
	return count, iter.Err()
}

func (a *goFastKVAdapter) Close() error {
	return a.store.Close()
}

// --- BoltDB adapter ---

type boltAdapter struct {
	db *bolt.DB
}

func openBoltDB(dir string) (kvAdapter, error) {
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &boltAdapter{db: db}, nil
}

func (a *boltAdapter) Put(key, value []byte) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put(key, value)
	})
}

func (a *boltAdapter) Get(key []byte) ([]byte, error) {
	var result []byte
	err := a.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketName).Get(key)
		if v != nil {
			result = make([]byte, len(v))
			copy(result, v)
		}
		return nil
	})
	return result, err
}

func (a *boltAdapter) Scan(start, end []byte) (int, error) {
	count := 0
	err := a.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
			_ = k
			_ = v
			count++
		}
		return nil
	})
	return count, err
}

func (a *boltAdapter) Close() error {
	return a.db.Close()
}

// --- Badger adapter ---

type badgerAdapter struct {
	db *badger.DB
}

func openBadger(dir string) (kvAdapter, error) {
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.Logger = nil
	opts.ValueLogFileSize = 64 << 20   // 64MB — avoid "file too large" under ulimit
	opts.MemTableSize = 8 << 20        // 8MB memtable (default 64MB)
	opts.NumMemtables = 2              // reduce memory (default 5)
	opts.NumLevelZeroTables = 2        // reduce memory
	opts.NumLevelZeroTablesStall = 4   // reduce memory
	opts.NumCompactors = 2             // minimum required by Badger (default 4)
	opts.BlockCacheSize = 16 << 20     // 16MB block cache (required when compression enabled)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerAdapter{db: db}, nil
}

func (a *badgerAdapter) Put(key, value []byte) error {
	return a.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (a *badgerAdapter) Get(key []byte) ([]byte, error) {
	var result []byte
	err := a.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	return result, err
}

func (a *badgerAdapter) Scan(start, end []byte) (int, error) {
	count := 0
	err := a.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = scanCount
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if bytes.Compare(k, end) >= 0 {
				break
			}
			_, _ = item.ValueCopy(nil)
			count++
		}
		return nil
	})
	return count, err
}

func (a *badgerAdapter) Close() error {
	return a.db.Close()
}

// ─── Helpers ────────────────────────────────────────────────────────

type openerFunc func(dir string) (kvAdapter, error)

func populate(store kvAdapter, n int, vals [][]byte) error {
	for i := 0; i < n; i++ {
		if err := store.Put(makeKey(i), vals[i%len(vals)]); err != nil {
			return err
		}
	}
	return nil
}

// populateBoltBatch populates BoltDB using batched transactions (100 keys per tx)
// to avoid the 10,000 × fsync overhead during benchmark setup.
func populateBoltBatch(adapter kvAdapter, n int, vals [][]byte) error {
	ba, ok := adapter.(*boltAdapter)
	if !ok {
		return populate(adapter, n, vals)
	}
	batchSize := 100
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		err := ba.db.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(bucketName)
			for i := start; i < end; i++ {
				if err := bkt.Put(makeKey(i), vals[i%len(vals)]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// BENCHMARKS
// ═══════════════════════════════════════════════════════════════════

// ─── PutSequential ──────────────────────────────────────────────────

func benchPutSequential(b *testing.B, open openerFunc) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Put(makeKey(i), vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoFastKV_PutSequential(b *testing.B) { benchPutSequential(b, openGoFastKV) }
func BenchmarkBoltDB_PutSequential(b *testing.B)    { benchPutSequential(b, openBoltDB) }
func BenchmarkBadger_PutSequential(b *testing.B)    { benchPutSequential(b, openBadger) }

// ─── PutRandom ──────────────────────────────────────────────────────

func benchPutRandom(b *testing.B, open openerFunc) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	rng := rand.New(rand.NewSource(99))
	keys := make([]int, b.N+1)
	for i := range keys {
		keys[i] = rng.Intn(1_000_000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Put(makeKey(keys[i]), vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoFastKV_PutRandom(b *testing.B) { benchPutRandom(b, openGoFastKV) }
func BenchmarkBoltDB_PutRandom(b *testing.B)    { benchPutRandom(b, openBoltDB) }
func BenchmarkBadger_PutRandom(b *testing.B)    { benchPutRandom(b, openBadger) }

// ─── PutConcurrent10 ───────────────────────────────────────────────

func benchPutConcurrent(b *testing.B, open openerFunc, workers int) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	var counter atomic.Int64
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerWorker := b.N / workers
	if opsPerWorker < 1 {
		opsPerWorker = 1
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				idx := int(counter.Add(1))
				if err := store.Put(makeKey(idx), vals[idx%len(vals)]); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkGoFastKV_PutConcurrent10(b *testing.B) {
	benchPutConcurrent(b, openGoFastKV, 10)
}
func BenchmarkBoltDB_PutConcurrent10(b *testing.B) {
	benchPutConcurrent(b, openBoltDB, 10)
}
func BenchmarkBadger_PutConcurrent10(b *testing.B) {
	benchPutConcurrent(b, openBadger, 10)
}

// ─── GetSequential ──────────────────────────────────────────────────

func benchGetSequential(b *testing.B, open openerFunc) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	if err := populateBoltBatch(store, numKeys, vals); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(makeKey(i % numKeys)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoFastKV_GetSequential(b *testing.B) { benchGetSequential(b, openGoFastKV) }
func BenchmarkBoltDB_GetSequential(b *testing.B)    { benchGetSequential(b, openBoltDB) }
func BenchmarkBadger_GetSequential(b *testing.B)    { benchGetSequential(b, openBadger) }

// ─── GetRandom ──────────────────────────────────────────────────────

func benchGetRandom(b *testing.B, open openerFunc) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	if err := populateBoltBatch(store, numKeys, vals); err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewSource(77))
	keys := make([]int, b.N+1)
	for i := range keys {
		keys[i] = rng.Intn(numKeys)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(makeKey(keys[i])); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoFastKV_GetRandom(b *testing.B) { benchGetRandom(b, openGoFastKV) }
func BenchmarkBoltDB_GetRandom(b *testing.B)    { benchGetRandom(b, openBoltDB) }
func BenchmarkBadger_GetRandom(b *testing.B)    { benchGetRandom(b, openBadger) }

// ─── GetConcurrent10 ───────────────────────────────────────────────

func benchGetConcurrent(b *testing.B, open openerFunc, workers int) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	if err := populateBoltBatch(store, numKeys, vals); err != nil {
		b.Fatal(err)
	}

	var counter atomic.Int64
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerWorker := b.N / workers
	if opsPerWorker < 1 {
		opsPerWorker = 1
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				idx := int(counter.Add(1)) % numKeys
				if _, err := store.Get(makeKey(idx)); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkGoFastKV_GetConcurrent10(b *testing.B) {
	benchGetConcurrent(b, openGoFastKV, 10)
}
func BenchmarkBoltDB_GetConcurrent10(b *testing.B) {
	benchGetConcurrent(b, openBoltDB, 10)
}
func BenchmarkBadger_GetConcurrent10(b *testing.B) {
	benchGetConcurrent(b, openBadger, 10)
}

// ─── Mixed 50% Read 50% Write ──────────────────────────────────────

func benchMixed(b *testing.B, open openerFunc) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	if err := populateBoltBatch(store, numKeys, vals); err != nil {
		b.Fatal(err)
	}

	var writeCounter atomic.Int64
	var readCounter atomic.Int64
	workers := 10

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerWorker := b.N / workers
	if opsPerWorker < 1 {
		opsPerWorker = 1
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		isWriter := w < workers/2
		go func(writer bool) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				if writer {
					idx := int(writeCounter.Add(1))
					if err := store.Put(makeKey(numKeys+idx), vals[idx%len(vals)]); err != nil {
						b.Error(err)
						return
					}
				} else {
					idx := int(readCounter.Add(1)) % numKeys
					if _, err := store.Get(makeKey(idx)); err != nil {
						b.Error(err)
						return
					}
				}
			}
		}(isWriter)
	}
	wg.Wait()
}

func BenchmarkGoFastKV_Mixed50R50W(b *testing.B) { benchMixed(b, openGoFastKV) }
func BenchmarkBoltDB_Mixed50R50W(b *testing.B)    { benchMixed(b, openBoltDB) }
func BenchmarkBadger_Mixed50R50W(b *testing.B)    { benchMixed(b, openBadger) }

// ─── Scan100 ────────────────────────────────────────────────────────

func benchScan(b *testing.B, open openerFunc) {
	vals := pregenValues(256)
	dir := b.TempDir()
	store, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	if err := populateBoltBatch(store, numKeys, vals); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startIdx := i % (numKeys - scanCount)
		count, err := store.Scan(makeKey(startIdx), makeKey(startIdx+scanCount))
		if err != nil {
			b.Fatal(err)
		}
		if count == 0 {
			b.Fatal("scan returned 0 results")
		}
	}
}

func BenchmarkGoFastKV_Scan100(b *testing.B) { benchScan(b, openGoFastKV) }
func BenchmarkBoltDB_Scan100(b *testing.B)    { benchScan(b, openBoltDB) }
func BenchmarkBadger_Scan100(b *testing.B)    { benchScan(b, openBadger) }

// ─── BatchPut100 ────────────────────────────────────────────────────
// Tests batch-mode writes where available.
// BoltDB: multiple puts in a single Update tx (1 fsync per 100 puts)
// Badger: WriteBatch API
// go-fast-kv: WriteBatch API (1 transaction + 1 WAL fsync per 100 puts)

func BenchmarkGoFastKV_BatchPut100(b *testing.B) {
	vals := pregenValues(256)
	dir := b.TempDir()
	adapter, err := openGoFastKV(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer adapter.Close()

	gfkv := adapter.(*goFastKVAdapter).store

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * 100
		batch := gfkv.NewWriteBatch()
		for j := 0; j < 100; j++ {
			idx := base + j
			if err := batch.Put(makeKey(idx), vals[idx%len(vals)]); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBoltDB_BatchPut100(b *testing.B) {
	vals := pregenValues(256)
	dir := b.TempDir()
	adapter, err := openBoltDB(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer adapter.Close()

	boltDB := adapter.(*boltAdapter).db
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * 100
		err := boltDB.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(bucketName)
			for j := 0; j < 100; j++ {
				idx := base + j
				if err := bkt.Put(makeKey(idx), vals[idx%len(vals)]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadger_BatchPut100(b *testing.B) {
	vals := pregenValues(256)
	dir := b.TempDir()
	adapter, err := openBadger(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer adapter.Close()

	badgerDB := adapter.(*badgerAdapter).db
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * 100
		wb := badgerDB.NewWriteBatch()
		for j := 0; j < 100; j++ {
			idx := base + j
			if err := wb.Set(makeKey(idx), vals[idx%len(vals)]); err != nil {
				b.Fatal(err)
			}
		}
		if err := wb.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}
