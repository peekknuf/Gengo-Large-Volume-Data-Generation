package common

import (
	"sync"
)

// UnifiedIDGenerator implements the IDGenerator interface with block-based allocation
// to reduce atomic contention in concurrent scenarios
type UnifiedIDGenerator struct {
	next      int64
	end       int64
	blockSize int64
	mu        sync.Mutex
}

// NewUnifiedIDGenerator creates a new ID generator with the specified start and block size
func NewUnifiedIDGenerator(start int64, blockSize int64) *UnifiedIDGenerator {
	return &UnifiedIDGenerator{
		next:      start,
		end:       start + blockSize,
		blockSize: blockSize,
	}
}

// NextID returns the next available ID, allocating a new block when needed
func (g *UnifiedIDGenerator) NextID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.next >= g.end {
		// Allocate new block
		g.end += g.blockSize
	}

	id := g.next
	g.next++
	return id
}

// NextBatch returns a slice of consecutive IDs
func (g *UnifiedIDGenerator) NextBatch(size int) []int64 {
	if size <= 0 {
		return nil
	}

	batch := make([]int64, size)
	g.mu.Lock()
	defer g.mu.Unlock()

	for i := 0; i < size; i++ {
		if g.next >= g.end {
			// Allocate new block
			g.end += g.blockSize
		}

		batch[i] = g.next
		g.next++
	}

	return batch
}

// LocalIDGenerator provides a lock-free ID generator for worker-local use
type LocalIDGenerator struct {
	next int64
	end  int64
}

// NewLocalIDGenerator creates a local ID generator for a specific range
func NewLocalIDGenerator(start int64, end int64) *LocalIDGenerator {
	return &LocalIDGenerator{
		next: start,
		end:  end,
	}
}

// NextID returns the next available ID from the local range
func (g *LocalIDGenerator) NextID() int64 {
	if g.next >= g.end {
		return -1 // Range exhausted
	}

	id := g.next
	g.next++
	return id
}

// HasMore returns true if there are more IDs available in the range
func (g *LocalIDGenerator) HasMore() bool {
	return g.next < g.end
}
