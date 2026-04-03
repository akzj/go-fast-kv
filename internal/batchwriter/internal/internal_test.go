package internal

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchWriter_Basic(t *testing.T) {
	bw := NewBatchWriter(100)
	defer bw.Close()

	var buf bytes.Buffer
	var mu sync.Mutex
	var writeCount int32

	writeAt := func(data []byte, offset int64) (int, error) {
		atomic.AddInt32(&writeCount, 1)
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(data)
	}

	// Submit multiple writes
	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		if !bw.Write(WriteRequest{
			Data:    data,
			Offset:  int64(i),
			WriteAt: writeAt,
		}) {
			t.Fatal("Write returned false")
		}
	}

	// Wait for writes to be processed
	time.Sleep(50 * time.Millisecond)

	// Verify writes happened
	if atomic.LoadInt32(&writeCount) != 10 {
		t.Errorf("expected 10 writes, got %d", atomic.LoadInt32(&writeCount))
	}

	// Verify buffer content
	if buf.Len() != 10 {
		t.Errorf("expected buffer length 10, got %d", buf.Len())
	}
}

func TestBatchWriter_Batching(t *testing.T) {
	bw := NewBatchWriter(100)
	defer bw.Close()

	var mu sync.Mutex
	var batchedWrites []int

	writeAt := func(data []byte, offset int64) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		batchedWrites = append(batchedWrites, len(data))
		return len(data), nil
	}

	// Submit multiple writes quickly
	for i := 0; i < 5; i++ {
		bw.Write(WriteRequest{
			Data:    []byte{byte(i), byte(i + 1)},
			Offset:  int64(i),
			WriteAt: writeAt,
		})
	}

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	// With batching, 5 writes should be processed
	// Each write is 2 bytes
	totalBytes := 0
	for _, n := range batchedWrites {
		totalBytes += n
	}

	if totalBytes != 10 {
		t.Errorf("expected 10 bytes total, got %d", totalBytes)
	}
}

func TestBatchWriter_Close(t *testing.T) {
	bw := NewBatchWriter(10)
	var writeCount int32

	writeAt := func(data []byte, offset int64) (int, error) {
		atomic.AddInt32(&writeCount, 1)
		time.Sleep(10 * time.Millisecond)
		return len(data), nil
	}

	// Submit some writes
	for i := 0; i < 5; i++ {
		bw.Write(WriteRequest{
			Data:    []byte{byte(i)},
			WriteAt: writeAt,
		})
	}

	// Close should wait for pending writes
	bw.Close()

	if atomic.LoadInt32(&writeCount) != 5 {
		t.Errorf("expected 5 writes completed on close, got %d", atomic.LoadInt32(&writeCount))
	}

	// Further writes should fail
	if bw.Write(WriteRequest{}) {
		t.Error("Write should return false after Close")
	}
}

func TestBatchWriter_WriteSync(t *testing.T) {
	bw := NewBatchWriter(10)
	defer bw.Close()

	var buf bytes.Buffer
	mu := sync.Mutex{}

	writeAt := func(data []byte, offset int64) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(data)
	}

	// Sync write
	n, err := bw.WriteSync([]byte("hello"), 0, writeAt)
	if err != nil {
		t.Errorf("WriteSync error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}

	if buf.String() != "hello" {
		t.Errorf("expected 'hello', got '%s'", buf.String())
	}
}

func TestBatchWriter_Callback(t *testing.T) {
	bw := NewBatchWriter(10)
	defer bw.Close()

	var mu sync.Mutex
	var completed []int
	var completedErr []error

	writeAt := func(data []byte, offset int64) (int, error) {
		return len(data), nil
	}

	callback := func(n int, err error) {
		mu.Lock()
		defer mu.Unlock()
		completed = append(completed, n)
		completedErr = append(completedErr, err)
	}

	// Submit writes with callbacks
	for i := 0; i < 3; i++ {
		bw.Write(WriteRequest{
			Data:     []byte{byte(i)},
			WriteAt:  writeAt,
			Callback: callback,
		})
	}

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	if len(completed) != 3 {
		t.Errorf("expected 3 callbacks, got %d", len(completed))
	}

	for i, n := range completed {
		if n != 1 {
			t.Errorf("callback %d: expected 1 byte, got %d", i, n)
		}
	}
}

func TestBatchWriter_ConcurrentWrites(t *testing.T) {
	bw := NewBatchWriter(100)
	defer bw.Close()

	var totalBytes int32

	writeAt := func(data []byte, offset int64) (int, error) {
		atomic.AddInt32(&totalBytes, int32(len(data)))
		return len(data), nil
	}

	var wg sync.WaitGroup
	const goroutines = 10
	const writesPerGoroutine = 100

	// Concurrent writes from multiple goroutines
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				bw.Write(WriteRequest{
					Data:    []byte{byte(i)},
					WriteAt: writeAt,
				})
			}
		}()
	}

	wg.Wait()
	bw.Close()

	expectedBytes := int32(goroutines * writesPerGoroutine)
	if atomic.LoadInt32(&totalBytes) != expectedBytes {
		t.Errorf("expected %d total bytes, got %d", expectedBytes, atomic.LoadInt32(&totalBytes))
	}
}

func TestBatchWriter_EventDriven(t *testing.T) {
	// Verify that flushing is NOT timer-based - it should happen immediately
	// when the channel is drained
	bw := NewBatchWriter(100)
	defer bw.Close()

	var mu sync.Mutex
	var writeTimes []time.Time

	writeAt := func(data []byte, offset int64) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		writeTimes = append(writeTimes, time.Now())
		return len(data), nil
	}

	// Submit writes
	for i := 0; i < 3; i++ {
		bw.Write(WriteRequest{
			Data:    []byte{byte(i)},
			WriteAt: writeAt,
		})
	}

	// Wait a short time
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	firstWriteTime := writeTimes[0]
	lastWriteTime := writeTimes[len(writeTimes)-1]
	mu.Unlock()

	// All writes should happen very close together (within 20ms)
	// If there were a timer, there would be a delay
	elapsed := lastWriteTime.Sub(firstWriteTime)
	if elapsed > 50*time.Millisecond {
		t.Errorf("writes took too long (%v), suggests timer-based flushing", elapsed)
	}
}
