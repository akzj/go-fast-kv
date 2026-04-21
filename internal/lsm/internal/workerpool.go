package internal

import (
	"sync"
)

// workerPool is a simple fixed-size goroutine pool for parallel SSTable reads.
type workerPool struct {
	tasks   chan func()
	workers int
	wg      sync.WaitGroup
}

// newWorkerPool creates a worker pool with the given number of workers.
func newWorkerPool(workers int) *workerPool {
	if workers <= 0 {
		workers = 1
	}
	p := &workerPool{
		tasks:   make(chan func(), workers*4),
		workers: workers,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for task := range p.tasks {
				task()
			}
		}()
	}
	return p
}

// Submit adds a task to the pool. Blocks if the task queue is full.
// For non-blocking submission, use TrySubmit.
func (p *workerPool) Submit(task func()) {
	p.tasks <- task
}

// TrySubmit attempts to submit a task without blocking.
// Returns true if the task was submitted, false if the queue is full.
func (p *workerPool) TrySubmit(task func()) bool {
	select {
	case p.tasks <- task:
		return true
	default:
		return false
	}
}

// Wait waits for all submitted tasks to complete.
func (p *workerPool) Wait() {
	close(p.tasks)
	p.wg.Wait()
}

// resultCollector collects results from parallel workers.
type resultCollector[T any] struct {
	mu       sync.Mutex
	results  []T
	expected int
}

// newResultCollector creates a result collector for the given expected count.
func newResultCollector[T any](expected int) *resultCollector[T] {
	return &resultCollector[T]{
		results:  make([]T, 0, expected),
		expected: expected,
	}
}

// Add adds a result to the collector.
func (c *resultCollector[T]) Add(result T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results = append(c.results, result)
}

// Results returns all collected results.
func (c *resultCollector[T]) Results() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.results
}

// boolCollector collects boolean results with early-exit support.
type boolCollector struct {
	mu      sync.Mutex
	found   bool
	waiting int32 // atomic-like counter for waiters
}

// newBoolCollector creates a collector for boolean results.
func newBoolCollector() *boolCollector {
	return &boolCollector{}
}

// SetFound marks that a positive result was found.
func (c *boolCollector) SetFound() {
	c.mu.Lock()
	c.found = true
	c.mu.Unlock()
}

// IsFound returns whether a positive result was found.
func (c *boolCollector) IsFound() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.found
}
