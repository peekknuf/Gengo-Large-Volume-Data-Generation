package common

import (
	"context"
	"sync"
)

// GenericWorkerPool implements WorkerPool interface with configurable workers
type GenericWorkerPool struct {
	workers   int
	taskQueue chan *future
	submitMu  sync.RWMutex
	closeOnce sync.Once
	workerWg  sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewGenericWorkerPool creates a new worker pool with specified number of workers
func NewGenericWorkerPool(workers int) *GenericWorkerPool {
	workers = max(1, workers)
	ctx, cancel := context.WithCancel(context.Background())

	pool := &GenericWorkerPool{
		workers:   workers,
		taskQueue: make(chan *future, workers*2), // Buffered queue
		ctx:       ctx,
		cancel:    cancel,
	}

	pool.startWorkers()
	return pool
}

// startWorkers launches worker goroutines
func (p *GenericWorkerPool) startWorkers() {
	for i := 0; i < p.workers; i++ {
		p.workerWg.Add(1)
		go p.worker()
	}
}

// worker processes tasks from the queue
func (p *GenericWorkerPool) worker() {
	defer p.workerWg.Done()
	for f := range p.taskQueue {
		if err := p.ctx.Err(); err != nil {
			f.err = err
			close(f.done)
			continue
		}
		f.Execute(p.ctx)
	}
}

func (p *GenericWorkerPool) Submit(task Task) Future {
	f := &future{task: task, done: make(chan struct{})}
	p.submitMu.RLock()
	defer p.submitMu.RUnlock()
	if p.ctx.Err() == nil {
		select {
		case p.taskQueue <- f:
			return f
		case <-p.ctx.Done():
		}
	}
	f.err = p.ctx.Err()
	close(f.done)
	return f
}

func (p *GenericWorkerPool) Close() error {
	p.closeOnce.Do(func() {
		p.cancel()
		p.submitMu.Lock()
		close(p.taskQueue)
		p.submitMu.Unlock()
		p.workerWg.Wait()
	})
	return nil
}

// WorkerCount returns the number of workers in the pool
func (p *GenericWorkerPool) WorkerCount() int {
	return p.workers
}

// SimpleTask is a basic implementation of Task interface for testing
type SimpleTask struct {
	ExecuteFunc func(ctx context.Context) (interface{}, error)
}

// Execute runs the task's function
func (t *SimpleTask) Execute(ctx context.Context) (interface{}, error) {
	return t.ExecuteFunc(ctx)
}

// NewSimpleTask creates a new simple task from a function
func NewSimpleTask(fn func(ctx context.Context) (interface{}, error)) Task {
	return &SimpleTask{ExecuteFunc: fn}
}
