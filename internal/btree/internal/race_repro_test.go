package internal

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

func TestConcurrentPutStress(t *testing.T) {
	for run := 0; run < 50; run++ {
		provider := NewCachedMemPageProvider()
		tree := New(btreeapi.Config{}, provider, nil)

		N := 100000
		workers := 4
		var wg sync.WaitGroup
		perWorker := N / workers

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					k := []byte(fmt.Sprintf("key_%08d_%08d", i, rand.Int()))
					v := []byte(fmt.Sprintf("value_%08d", i))
					if err := tree.Put(k, v, uint64(i+1)); err != nil {
						t.Errorf("Put failed: %v", err)
						return
					}
				}
			}(w*perWorker, (w+1)*perWorker)
		}
		wg.Wait()
		t.Logf("Run %d: OK", run)
	}
}
