// Package internal provides the KVStore implementation.
package internal

import (
	"context"
	"testing"

	"github.com/akzj/go-fast-kv/pkg/kvstore/api"
)

func TestKVStoreImplOpen(t *testing.T) {
	ctx := context.Background()
	
	// Test with empty config
	_, err := Open(ctx, api.Config{})
	if err == nil {
		t.Error("Open with empty Dir should fail")
	}

	// Test with valid config - mock always succeeds
	cfg := api.Config{
		Dir: "/tmp/testkv",
	}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}
	if kv == nil {
		t.Fatal("Open returned nil")
	}
	
	// Close should succeed
	if err := kv.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestKVStoreImplPutGet(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer kv.Close()

	// Put some values
	tests := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, tt := range tests {
		if err := kv.Put(ctx, []byte(tt.key), []byte(tt.value)); err != nil {
			t.Fatalf("Put(%s) failed: %v", tt.key, err)
		}
	}

	// Get them back
	for _, tt := range tests {
		val, found, err := kv.Get(ctx, []byte(tt.key))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", tt.key, err)
		}
		if !found {
			t.Fatalf("Get(%s): not found", tt.key)
		}
		if string(val) != tt.value {
			t.Fatalf("Get(%s): got %q, want %q", tt.key, val, tt.value)
		}
	}
}

func TestKVStoreImplDelete(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer kv.Close()

	// Put a value
	if err := kv.Put(ctx, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify it exists
	_, found, _ := kv.Get(ctx, []byte("key1"))
	if !found {
		t.Fatal("key1 not found after put")
	}

	// Delete it
	if err := kv.Delete(ctx, []byte("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, found, _ = kv.Get(ctx, []byte("key1"))
	if found {
		t.Fatal("key1 still found after delete")
	}
}

func TestKVStoreImplScan(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer kv.Close()

	// Put some values
	kv.Put(ctx, []byte("a"), []byte("1"))
	kv.Put(ctx, []byte("b"), []byte("2"))
	kv.Put(ctx, []byte("c"), []byte("3"))

	// Scan
	var scanned []string
	err = kv.Scan(ctx, []byte("a"), []byte("c"), func(key, value []byte) bool {
		scanned = append(scanned, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(scanned) != 3 {
		t.Errorf("Scan: got %d keys, want 3", len(scanned))
	}
}

func TestKVStoreImplSync(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer kv.Close()

	if err := kv.Sync(ctx); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

func TestKVStoreImplStats(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer kv.Close()

	stats, err := kv.Stats()
	if err != nil {
		t.Errorf("Stats failed: %v", err)
	}
	_ = stats // Just verify it doesn't panic
}

func TestKVStoreImplClosed(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Close the store
	kv.Close()

	// Operations should fail
	_, _, err = kv.Get(ctx, []byte("key1"))
	if err == nil {
		t.Error("Get after Close should fail")
	}

	err = kv.Put(ctx, []byte("key1"), []byte("value1"))
	if err == nil {
		t.Error("Put after Close should fail")
	}

	err = kv.Delete(ctx, []byte("key1"))
	if err == nil {
		t.Error("Delete after Close should fail")
	}

	err = kv.Scan(ctx, nil, nil, func(k, v []byte) bool { return true })
	if err == nil {
		t.Error("Scan after Close should fail")
	}
}

func TestKVStoreImplDoubleClose(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Close twice should not panic
	kv.Close()
	kv.Close()
}

func TestKVStoreImplUpdate(t *testing.T) {
	ctx := context.Background()
	cfg := api.Config{Dir: "/tmp/testkv"}
	kv, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer kv.Close()

	// Put initial value
	kv.Put(ctx, []byte("key1"), []byte("value1"))

	// Update value
	kv.Put(ctx, []byte("key1"), []byte("value2"))

	// Get updated value
	val, _, _ := kv.Get(ctx, []byte("key1"))
	if string(val) != "value2" {
		t.Errorf("Update: got %q, want %q", val, "value2")
	}
}
