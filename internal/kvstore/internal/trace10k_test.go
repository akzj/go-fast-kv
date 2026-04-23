package internal

import (
	"fmt"
	"testing"
)

func TestTrace10kSequential(t *testing.T) {
	dir := t.TempDir()
	s := openTestStoreAt(t, dir)

	// 10k sequential Put
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Put error at %d: %v", i, err)
		}
	}
	fmt.Println("=== Put 10k complete ===")

	// 10k Get
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		_, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get error at %d: %v", i, err)
		}
	}
	fmt.Println("=== Get 10k complete ===")

	s.Close()
}
