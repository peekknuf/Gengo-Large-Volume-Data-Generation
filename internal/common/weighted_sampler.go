package common

import (
	"fmt"
	"math"
	"math/rand"
)

// UnifiedWeightedSampler implements O(1) weighted sampling using Vose's alias method
// Supports both int and int64 ID types for flexibility across domains
type UnifiedWeightedSampler struct {
	prob  []float64
	alias []int
	ids   []int // Using int for compatibility with existing code
}

// NewUnifiedWeightedSampler creates a sampler from IDs with implicit weights (1/sqrt(i+1))
func NewUnifiedWeightedSampler(ids []int) (*UnifiedWeightedSampler, error) {
	n := len(ids)
	if n == 0 {
		return nil, fmt.Errorf("cannot build sampler from empty ID list")
	}
	if n == 1 {
		// trivial case
		return &UnifiedWeightedSampler{
			ids:   ids,
			prob:  []float64{1.0},
			alias: []int{0},
		}, nil
	}

	// --- 1. Build raw weights ---
	weights := make([]float64, n)
	var total float64
	for i := 0; i < n; i++ {
		// same weighting scheme as before: 1/sqrt(i+1)
		w := 1.0 / math.Sqrt(float64(i+1))
		weights[i] = w
		total += w
	}

	// normalize
	for i := 0; i < n; i++ {
		weights[i] = weights[i] * float64(n) / total
	}

	// --- 2. Initialize structures ---
	prob := make([]float64, n)
	alias := make([]int, n)

	// two stacks: small and large
	small := make([]int, 0, n)
	large := make([]int, 0, n)

	for i, w := range weights {
		if w < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	// --- 3. Build tables ---
	for len(small) > 0 && len(large) > 0 {
		s := small[len(small)-1]
		small = small[:len(small)-1]
		l := large[len(large)-1]
		large = large[:len(large)-1]

		prob[s] = weights[s]
		alias[s] = l

		weights[l] = weights[l] + weights[s] - 1.0
		if weights[l] < 1.0 {
			small = append(small, l)
		} else {
			large = append(large, l)
		}
	}

	// whatever remains has prob = 1
	for _, i := range append(small, large...) {
		prob[i] = 1.0
		alias[i] = i
	}

	return &UnifiedWeightedSampler{
		prob:  prob,
		alias: alias,
		ids:   append([]int(nil), ids...), // copy
	}, nil
}

// NewUnifiedWeightedSampler64 creates a sampler from int64 IDs with implicit weights
func NewUnifiedWeightedSampler64(ids []int64) (*UnifiedWeightedSampler64, error) {
	n := len(ids)
	if n == 0 {
		return nil, fmt.Errorf("cannot build sampler from empty ID list")
	}
	if n == 1 {
		// trivial case
		return &UnifiedWeightedSampler64{
			ids:   ids,
			prob:  []float64{1.0},
			alias: []int{0},
		}, nil
	}

	// --- 1. Build raw weights ---
	weights := make([]float64, n)
	var total float64
	for i := 0; i < n; i++ {
		// same weighting scheme: 1/sqrt(i+1)
		w := 1.0 / math.Sqrt(float64(i+1))
		weights[i] = w
		total += w
	}

	// normalize
	for i := 0; i < n; i++ {
		weights[i] = weights[i] * float64(n) / total
	}

	// --- 2. Initialize structures ---
	prob := make([]float64, n)
	alias := make([]int, n)

	// two stacks: small and large
	small := make([]int, 0, n)
	large := make([]int, 0, n)

	for i, w := range weights {
		if w < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	// --- 3. Build tables ---
	for len(small) > 0 && len(large) > 0 {
		s := small[len(small)-1]
		small = small[:len(small)-1]
		l := large[len(large)-1]
		large = large[:len(large)-1]

		prob[s] = weights[s]
		alias[s] = l

		weights[l] = weights[l] + weights[s] - 1.0
		if weights[l] < 1.0 {
			small = append(small, l)
		} else {
			large = append(large, l)
		}
	}

	// whatever remains has prob = 1
	for _, i := range append(small, large...) {
		prob[i] = 1.0
		alias[i] = i
	}

	return &UnifiedWeightedSampler64{
		prob:  prob,
		alias: alias,
		ids:   append([]int64(nil), ids...), // copy
	}, nil
}

// Sample draws one ID using O(1) time
func (s *UnifiedWeightedSampler) Sample(rng *rand.Rand) int {
	if len(s.ids) == 1 {
		return s.ids[0]
	}
	// pick a column
	i := rng.Intn(len(s.ids))
	// biased coin flip
	if rng.Float64() < s.prob[i] {
		return s.ids[i]
	}
	return s.ids[s.alias[i]]
}

// SampleBatch draws multiple IDs efficiently
func (s *UnifiedWeightedSampler) SampleBatch(count int, rng *rand.Rand) []int {
	if count <= 0 {
		return nil
	}

	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = s.Sample(rng)
	}
	return result
}

// UnifiedWeightedSampler64 is the int64 version of the weighted sampler
type UnifiedWeightedSampler64 struct {
	prob  []float64
	alias []int
	ids   []int64
}

// Sample draws one ID using O(1) time (int64 version)
func (s *UnifiedWeightedSampler64) Sample(rng *rand.Rand) int64 {
	if len(s.ids) == 1 {
		return s.ids[0]
	}
	// pick a column
	i := rng.Intn(len(s.ids))
	// biased coin flip
	if rng.Float64() < s.prob[i] {
		return s.ids[i]
	}
	return s.ids[s.alias[i]]
}

// SampleBatch draws multiple IDs efficiently (int64 version)
func (s *UnifiedWeightedSampler64) SampleBatch(count int, rng *rand.Rand) []int64 {
	if count <= 0 {
		return nil
	}

	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = s.Sample(rng)
	}
	return result
}
