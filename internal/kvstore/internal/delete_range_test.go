package internal

import (
	"bytes"
	"fmt"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func TestDeleteRangeBasic(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// Insert 10 test keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [key000, key005)
	count, err := s.DeleteRange([]byte("key000"), []byte("key005"))
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Errorf("expected 5 deleted, got %d", count)
	}

	// Verify first 5 are gone
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		_, err := s.Get(key)
		if err != kvstoreapi.ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound for %s, got %v", key, err)
		}
	}

	// Verify remaining exist
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Errorf("expected to get %s, got err %v", key, err)
		}
		expected := []byte(fmt.Sprintf("value%d", i))
		if !bytes.Equal(val, expected) {
			t.Errorf("%s: got %s, want %s", key, val, expected)
		}
	}
}

func TestDeleteRangeEmpty(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	count, err := s.DeleteRange([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 deleted, got %d", count)
	}
}

func TestDeleteRangeAll(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		if err := s.Put(key, []byte(fmt.Sprintf("value%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	count, err := s.DeleteRange([]byte("key000"), []byte("key005"))
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Errorf("expected 5 deleted, got %d", count)
	}

	_, err = s.Get([]byte("key000"))
	if err != kvstoreapi.ErrKeyNotFound {
		t.Error("expected all keys deleted")
	}
}
