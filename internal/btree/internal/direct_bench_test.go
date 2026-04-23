package internal

import (
	"bytes"
	"fmt"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// BenchmarkDirectMem benchmarks the DirectMemPageProvider + nodeSerializer approach.
// This tests the overhead of serialization with in-memory storage.
func BenchmarkDirectMemPut(b *testing.B) {
	provider := NewDirectMemPageProvider()
	serializer := NewNodeSerializer()
	
	pid := provider.AllocPage()
	root := &btreeapi.Node{IsLeaf: true}
	data, _ := serializer.Serialize(root)
	provider.WritePage(pid, data)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		
		// Read
		data, _ := provider.ReadPage(pid)
		node, _ := serializer.Deserialize(data)
		
		// Insert
		entry := btreeapi.LeafEntry{
			Key:    cloneBytes(key),
			TxnMin: uint64(i),
			TxnMax: btreeapi.TxnMaxInfinity,
			Value:  btreeapi.Value{Inline: cloneBytes(value)},
		}
		
		pos := 0
		for pos < len(node.Entries) && bytes.Compare(key, node.Entries[pos].Key) > 0 {
			pos++
		}
		node.Entries = append(node.Entries, btreeapi.LeafEntry{})
		copy(node.Entries[pos+1:], node.Entries[pos:])
		node.Entries[pos] = entry
		
		// Serialize
		newData, _ := serializer.Serialize(node)
		provider.WritePage(pid, newData)
	}
}

// BenchmarkDirectMemGet benchmarks Get operations with DirectMemPageProvider.
func BenchmarkDirectMemGet(b *testing.B) {
	provider := NewDirectMemPageProvider()
	serializer := NewNodeSerializer()

	pid := provider.AllocPage()

	// Populate with 1000 entries
	node := &btreeapi.Node{IsLeaf: true}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("value-%04d", i))
		node.Entries = append(node.Entries, btreeapi.LeafEntry{
			Key:    key,
			TxnMin: uint64(i),
			TxnMax: btreeapi.TxnMaxInfinity,
			Value:  btreeapi.Value{Inline: value},
		})
	}
	node.Count = uint16(len(node.Entries))
	data, _ := serializer.Serialize(node)
	provider.WritePage(pid, data)

	txnID := uint64(999)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i%1000))

		data, _ := provider.ReadPage(pid)
		if data == nil {
			b.Fatal("page not found")
		}
		node, _ := serializer.Deserialize(data)

		for j := range node.Entries {
			e := &node.Entries[j]
			if bytes.Equal(e.Key, key) && e.TxnMin <= txnID && e.TxnMax > txnID {
				_ = e.Value.Inline
				break
			}
		}
	}
}

// BenchmarkDirectBTreePut benchmarks the full DirectBTree Put operation.
func BenchmarkDirectBTreePut(b *testing.B) {
	tree := NewDirectBTree()
	tree.Bootstrap()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		tree.Put(key, value, uint64(i))
	}
}

// BenchmarkDirectBTreeGet benchmarks the full DirectBTree Get operation.
func BenchmarkDirectBTreeGet(b *testing.B) {
	tree := NewDirectBTree()
	
	// Populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("value-%04d", i))
		tree.Put(key, value, uint64(i))
	}
	
	txnID := uint64(9999)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i%10000))
		tree.Get(key, txnID)
	}
}

// BenchmarkPageAccessor benchmarks the PageAccessor zero-copy reader.
func BenchmarkPageAccessor(b *testing.B) {
	serializer := NewNodeSerializer()
	
	// Create a page with entries
	node := &btreeapi.Node{IsLeaf: true}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("value-%04d", i))
		node.Entries = append(node.Entries, btreeapi.LeafEntry{
			Key:    key,
			TxnMin: uint64(i),
			TxnMax: btreeapi.TxnMaxInfinity,
			Value:  btreeapi.Value{Inline: value},
		})
	}
	node.Count = uint16(len(node.Entries))
	data, _ := serializer.Serialize(node)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		acc, _ := NewPageAccessor(data)
		_, _, _, _, _ = acc.LeafEntryAt(i % 100)
	}
}
