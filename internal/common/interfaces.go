package common

import (
	"context"
	"math/rand"
)

// DataGenerator defines the interface for generating synthetic data
type DataGenerator interface {
	GenerateDimensions(ctx context.Context, config GenerationConfig) ([]DimensionData, error)
	GenerateFacts(ctx context.Context, config GenerationConfig, dimensions []DimensionData) ([]FactData, error)
}

// DataWriter defines the interface for writing data to different formats
type DataWriter interface {
	Write(ctx context.Context, data interface{}, format string, target string) error
	WriteStream(ctx context.Context, data <-chan interface{}, format string, target string) error
}

// IDGenerator defines the interface for generating unique IDs
type IDGenerator interface {
	NextID() int64
	NextBatch(size int) []int64
}

// WeightedSampler defines the interface for weighted random sampling
type WeightedSampler interface {
	Sample(rng *rand.Rand) int
	SampleBatch(count int, rng *rand.Rand) []int
}

// WorkerPool defines the interface for concurrent task execution
type WorkerPool interface {
	Submit(task Task) Future
	Close() error
}

// Task defines the interface for a unit of work
type Task interface {
	Execute(ctx context.Context) (interface{}, error)
}

// Future represents the result of an asynchronous task
type Future interface {
	Get() (interface{}, error)
	Done() <-chan struct{}
}

// GenerationConfig holds configuration for data generation
type GenerationConfig struct {
	TargetSizeGB float64
	Format       string
	OutputDir    string
	NumWorkers   int
	BatchSize    int
	Seed         int64
}

// DimensionData represents generated dimension table data
type DimensionData struct {
	Type  string
	Data  interface{}
	Count int
}

// FactData represents generated fact table data
type FactData struct {
	Type   string
	Data   interface{}
	Count  int
	Chunks [][]interface{} // For parallel processing
}

// future implements the Future interface
type future struct {
	task   Task
	done   chan struct{}
	result interface{}
	err    error
}

// NewFuture creates a new Future
func NewFuture(task Task) Future {
	f := &future{
		task: task,
		done: make(chan struct{}),
	}
	return f
}

// Execute runs the task and stores the result
func (f *future) Execute(ctx context.Context) {
	result, err := f.task.Execute(ctx)
	f.result = result
	f.err = err
	close(f.done)
}

// Get returns the result of the task, blocking until complete
func (f *future) Get() (interface{}, error) {
	<-f.done
	return f.result, f.err
}

// Done returns a channel that's closed when the task is complete
func (f *future) Done() <-chan struct{} {
	return f.done
}
