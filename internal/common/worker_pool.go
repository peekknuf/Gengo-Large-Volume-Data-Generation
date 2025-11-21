package common

import (
	"context"
	"sync"
)

// GenericWorkerPool implements WorkerPool interface with configurable workers
type GenericWorkerPool struct {
	workers   int
	taskQueue chan Task
	workerWg  sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewGenericWorkerPool creates a new worker pool with specified number of workers
func NewGenericWorkerPool(workers int) *GenericWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &GenericWorkerPool{
		workers:   workers,
		taskQueue: make(chan Task, workers*2), // Buffered queue
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
		go p.worker(i)
	}
}

// worker processes tasks from the queue
func (p *GenericWorkerPool) worker(workerID int) {
	defer p.workerWg.Done()

	for {
		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				// Queue closed, exit worker
				return
			}
			// Execute task (in a real implementation, we might want to capture results)
			task.Execute(p.ctx)

		case <-p.ctx.Done():
			// Context cancelled, exit worker
			return
		}
	}
}

// Submit submits a task to the worker pool and returns a Future
func (p *GenericWorkerPool) Submit(task Task) Future {
	future := &future{
		task: task,
		done: make(chan struct{}),
	}

	select {
	case p.taskQueue <- task:
		// Task submitted successfully
		// In a real implementation, we'd need a way to get the result back
		// For now, execute immediately and return result
		go func() {
			result, err := task.Execute(p.ctx)
			future.result = result
			future.err = err
			close(future.done)
		}()
		return future

	case <-p.ctx.Done():
		// Pool is shutting down
		future.err = p.ctx.Err()
		close(future.done)
		return future
	}
}

// Close gracefully shuts down the worker pool
func (p *GenericWorkerPool) Close() error {
	// Cancel context to signal workers to stop
	p.cancel()

	// Close task queue to prevent new tasks
	close(p.taskQueue)

	// Wait for all workers to finish
	p.workerWg.Wait()

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
