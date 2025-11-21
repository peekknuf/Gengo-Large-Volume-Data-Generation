package generators

import (
	"context"
	"math/rand"
	"runtime"

	"github.com/peekknuf/Gengo/internal/common"
)

// BaseGenerator provides common functionality for all domain generators
type BaseGenerator struct {
	idGen   common.IDGenerator
	sampler common.WeightedSampler
	pool    common.WorkerPool
	config  common.GenerationConfig
	rng     *rand.Rand
}

// NewBaseGenerator creates a new base generator with the given configuration
func NewBaseGenerator(config common.GenerationConfig) *BaseGenerator {
	// Use reasonable defaults if not specified
	if config.NumWorkers == 0 {
		config.NumWorkers = runtime.NumCPU()
	}
	if config.BatchSize == 0 {
		config.BatchSize = 10000
	}
	if config.Seed == 0 {
		config.Seed = rand.Int63()
	}

	return &BaseGenerator{
		idGen:  common.NewUnifiedIDGenerator(1, 100000),
		pool:   common.NewGenericWorkerPool(config.NumWorkers),
		config: config,
		rng:    rand.New(rand.NewSource(config.Seed)),
	}
}

// GenerateDimensionData provides a generic way to generate dimension data using worker pool
func (b *BaseGenerator) GenerateDimensionData(dimensionType string, count int, generatorFunc func() interface{}) ([]interface{}, error) {
	// Partition work into chunks for parallel processing
	chunks := b.partitionWork(count, b.config.BatchSize)

	var results [][]interface{}
	for _, chunk := range chunks {
		task := common.NewSimpleTask(func(ctx context.Context) (interface{}, error) {
			return b.generateChunk(chunk, generatorFunc), nil
		})

		future := b.pool.Submit(task)
		result, err := future.Get()
		if err != nil {
			return nil, err
		}

		chunkResult := result.([]interface{})
		results = append(results, chunkResult)
	}

	return b.mergeResults(results), nil
}

// partitionWork divides count into chunks of size batchSize
func (b *BaseGenerator) partitionWork(count, batchSize int) []int {
	if count <= 0 {
		return nil
	}

	var chunks []int
	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}
		chunks = append(chunks, end-i)
	}
	return chunks
}

// generateChunk generates a chunk of data using the provided generator function
func (b *BaseGenerator) generateChunk(chunkSize int, generatorFunc func() interface{}) []interface{} {
	result := make([]interface{}, chunkSize)
	for i := 0; i < chunkSize; i++ {
		result[i] = generatorFunc()
	}
	return result
}

// mergeResults flattens a 2D slice of results into a single slice
func (b *BaseGenerator) mergeResults(results [][]interface{}) []interface{} {
	var totalLen int
	for _, chunk := range results {
		totalLen += len(chunk)
	}

	merged := make([]interface{}, 0, totalLen)
	for _, chunk := range results {
		merged = append(merged, chunk...)
	}
	return merged
}

// GetIDGenerator returns the ID generator
func (b *BaseGenerator) GetIDGenerator() common.IDGenerator {
	return b.idGen
}

// GetWorkerPool returns the worker pool
func (b *BaseGenerator) GetWorkerPool() common.WorkerPool {
	return b.pool
}

// GetRNG returns the random number generator
func (b *BaseGenerator) GetRNG() *rand.Rand {
	return b.rng
}

// GetConfig returns the configuration
func (b *BaseGenerator) GetConfig() common.GenerationConfig {
	return b.config
}

// Close cleans up resources
func (b *BaseGenerator) Close() error {
	return b.pool.Close()
}

// ExtractDimensionMap extracts a map of dimension data for efficient lookups
func (b *BaseGenerator) ExtractDimensionMap(dimensions []common.DimensionData, dimensionType string) map[int]interface{} {
	dimMap := make(map[int]interface{})

	for _, dim := range dimensions {
		if dim.Type == dimensionType {
			// Convert slice to map for O(1) lookups
			switch data := dim.Data.(type) {
			case []interface{}:
				for _, item := range data {
					// This is a simplified approach - in real implementation,
					// we'd need to extract ID based on the specific type
					if itemWithID, ok := item.(interface{ GetID() int }); ok {
						dimMap[itemWithID.GetID()] = item
					}
				}
			}
			break
		}
	}

	return dimMap
}

// DimensionWithID is an interface for types that have GetID() method
type DimensionWithID interface {
	GetID() int
}
