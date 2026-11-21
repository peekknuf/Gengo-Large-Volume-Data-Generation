package core

import (
	"math"
	"testing"
)

func TestInputRejectsInvalidSize(t *testing.T) {
	for _, size := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, _, _, _, err := GetUserInput("medical", size, "csv", "unused"); err == nil {
			t.Errorf("accepted invalid size %v", size)
		}
	}
}

func TestInputRejectsUnsupportedJSON(t *testing.T) {
	if _, _, _, _, err := GetUserInput("financial", 0.01, "json", "unused"); err == nil {
		t.Fatal("accepted unsupported JSON output")
	}
}
