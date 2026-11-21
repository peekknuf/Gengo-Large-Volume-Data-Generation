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

func TestInputRejectsUnrepresentableRows(t *testing.T) {
	for _, model := range []string{"ecommerce", "ecommerce-ds", "medical", "financial"} {
		if _, _, _, _, err := GetUserInput(model, math.MaxFloat64, "csv", "unused"); err == nil {
			t.Errorf("%s accepted overflow", model)
		}
	}
}
