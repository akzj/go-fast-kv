// Package internal contains internal implementation details for batchwriter.
// This package is not intended to be imported directly.
package internal

import (
	"sync"
)

// BatchWriter is the exported type alias for the internal batchWriter implementation.
type BatchWriter = batchWriter

// batchWriter provides buffered batch writing with channel-based event-driven flushing.
// Writes are sent to a buffered channel and processed by a single consumer goroutine.
// When the channel is empty, the consumer flushes pending writes.
type batchWriter struct {
	ch      chan WriteRequest
	wg      sync.WaitGroup
	closed  bool
	closeMu sync.Mutex
}

// WriteRequest represents a single write to be batched.
type WriteRequest struct {
	Data     []byte
	Offset   int64
	WriteAt  func(data []byte, offset int64) (int, error)
	Callback func(n int, err error)
}

// NewBatchWriter creates a new BatchWriter with the given buffer size.
// The buffer size determines how many writes can be pending before senders block.
func NewBatchWriter(bufferSize int) *batchWriter {
	if bufferSize <= 0 {
		bufferSize = 1024
	}

	bw := &batchWriter{
		ch: make(chan WriteRequest, bufferSize),
	}

	// Start the consumer goroutine
	bw.wg.Add(1)
	go bw.consumeLoop()

	return bw
}

// Write submits a write request to be batched.
// The write will be processed asynchronously. The callback is called when complete.
// If the BatchWriter is closed, Write returns false immediately.
func (bw *batchWriter) Write(req WriteRequest) bool {
	bw.closeMu.Lock()
	if bw.closed {
		bw.closeMu.Unlock()
		return false
	}
	bw.closeMu.Unlock()

	bw.ch <- req
	return true
}

// WriteSync submits a write and waits for it to complete.
func (bw *batchWriter) WriteSync(data []byte, offset int64, writeAt func([]byte, int64) (int, error)) (int, error) {
	var result struct {
		n   int
		err error
	}
	done := make(chan struct{})

	req := WriteRequest{
		Data:   data,
		Offset: offset,
		WriteAt: func(d []byte, o int64) (int, error) {
			n, err := writeAt(d, o)
			result.n = n
			result.err = err
			close(done)
			return n, err
		},
	}

	if !bw.Write(req) {
		return 0, ErrClosed
	}

	<-done
	return result.n, result.err
}

// Close gracefully shuts down the batch writer.
// It waits for all pending writes to complete before returning.
func (bw *batchWriter) Close() error {
	bw.closeMu.Lock()
	if bw.closed {
		bw.closeMu.Unlock()
		return nil
	}
	bw.closed = true
	bw.closeMu.Unlock()

	// Close the channel to signal the consumer to exit
	close(bw.ch)

	// Wait for consumer to finish processing
	bw.wg.Wait()

	return nil
}

// consumeLoop is the consumer goroutine that processes writes.
// It uses the "read → loop read more → exit when empty → flush" pattern.
// This is EVENT-DRIVEN: no timers, no periodic flushing.
func (bw *batchWriter) consumeLoop() {
	defer bw.wg.Done()

	for {
		// Read one write from the channel
		req, ok := <-bw.ch
		if !ok {
			// Channel closed, exit
			return
		}

		// Process the first write
		bw.processWrite(&req)

		// Loop: drain remaining writes from channel
		// This batches multiple writes together
		for {
			select {
			case nextReq, ok := <-bw.ch:
				if !ok {
					// Channel closed after we drained it, exit
					return
				}
				bw.processWrite(&nextReq)
			default:
				// Channel empty, exit inner loop
				goto flushed
			}
		}
	flushed:
		// Channel is drained, next iteration will read new writes
	}
}

// processWrite executes a single write request and calls its callback.
func (bw *batchWriter) processWrite(req *WriteRequest) {
	if req.WriteAt != nil {
		n, err := req.WriteAt(req.Data, req.Offset)
		if req.Callback != nil {
			req.Callback(n, err)
		}
	} else if req.Callback != nil {
		req.Callback(0, ErrNoWriteFunc)
	}
}

// Error types.

var ErrClosed = &batchError{"batch writer is closed"}
var ErrNoWriteFunc = &batchError{"no write function provided"}

type batchError struct {
	msg string
}

func (e *batchError) Error() string {
	return e.msg
}
