package ecommerce

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestAliasSamplerWeights(t *testing.T) {
	sampler, err := NewAliasSampler([]int{0, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	var counts [3]int
	const samples = 100000
	for range samples {
		counts[sampler.Sample(rng)]++
	}
	total := 1 + 1/math.Sqrt(2) + 1/math.Sqrt(3)
	for i, count := range counts {
		want := 1 / math.Sqrt(float64(i+1)) / total
		if math.Abs(float64(count)/samples-want) > 0.01 {
			t.Fatalf("ID %d frequency %.4f, want %.4f", i, float64(count)/samples, want)
		}
	}
}
