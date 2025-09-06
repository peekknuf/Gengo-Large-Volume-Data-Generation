package tests

import (
	"os"
	"testing"

	"github.com/peekknuf/Gengo/internal/core"
)

// BenchmarkEcommerceGeneration benchmarks the e-commerce data generation pipeline.
// Run with: go test -cpuprofile=cpu.prof -bench=BenchmarkEcommerce -benchtime=1x ./tests
func BenchmarkEcommerceGeneration(b *testing.B) {
	// Setup output directory
	outputDir := "benchmark_output"
	defer func() {
		os.RemoveAll(outputDir)
	}()

	// Calculate counts for a moderate-sized dataset (good for profiling)
	counts, err := core.CalculateECommerceRowCounts(1.0) // 1GB target
	if err != nil {
		b.Fatalf("Failed to calculate row counts: %v", err)
	}

	b.ResetTimer() // Don't include setup time in benchmark

	for i := 0; i < b.N; i++ {
		// Create fresh output directory for each iteration
		iterationDir := outputDir + "_" + string(rune('0'+i))
		
		err := core.GenerateModelData("ecommerce", counts, "csv", iterationDir)
		if err != nil {
			b.Fatalf("Failed to generate ecommerce data: %v", err)
		}
		
		// Cleanup after each iteration
		os.RemoveAll(iterationDir)
	}
}

// BenchmarkEcommerceGenerationSmall benchmarks with a smaller dataset for faster iterations.
// Run with: go test -cpuprofile=cpu.prof -bench=BenchmarkEcommerceSmall -benchtime=5x ./tests
func BenchmarkEcommerceGenerationSmall(b *testing.B) {
	outputDir := "benchmark_output_small"
	defer func() {
		os.RemoveAll(outputDir)
	}()

	// Smaller dataset for faster profiling iterations
	counts, err := core.CalculateECommerceRowCounts(0.1) // 100MB target
	if err != nil {
		b.Fatalf("Failed to calculate row counts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iterationDir := outputDir + "_small_" + string(rune('0'+i))
		
		err := core.GenerateModelData("ecommerce", counts, "csv", iterationDir)
		if err != nil {
			b.Fatalf("Failed to generate small ecommerce data: %v", err)
		}
		
		os.RemoveAll(iterationDir)
	}
}

// BenchmarkEcommerceGenerationLarge benchmarks with a larger dataset for realistic profiling.
// Run with: go test -cpuprofile=cpu.prof -bench=BenchmarkEcommerceLarge -benchtime=1x ./tests
func BenchmarkEcommerceGenerationLarge(b *testing.B) {
	outputDir := "benchmark_output_large"
	defer func() {
		os.RemoveAll(outputDir)
	}()

	// Larger dataset for comprehensive profiling
	counts, err := core.CalculateECommerceRowCounts(5.0) // 5GB target
	if err != nil {
		b.Fatalf("Failed to calculate row counts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iterationDir := outputDir + "_large_" + string(rune('0'+i))
		
		err := core.GenerateModelData("ecommerce", counts, "csv", iterationDir)
		if err != nil {
			b.Fatalf("Failed to generate large ecommerce data: %v", err)
		}
		
		os.RemoveAll(iterationDir)
	}
}

// BenchmarkFactGenerationOnly benchmarks just the fact table generation (most CPU-intensive part).
// Run with: go test -cpuprofile=cpu.prof -bench=BenchmarkFactGeneration -benchtime=3x ./tests
func BenchmarkFactGenerationOnly(b *testing.B) {
	outputDir := "benchmark_facts_only"
	defer func() {
		os.RemoveAll(outputDir)
	}()

	// Medium-sized dataset focused on fact generation
	counts, err := core.CalculateECommerceRowCounts(2.0) // 2GB target
	if err != nil {
		b.Fatalf("Failed to calculate row counts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iterationDir := outputDir + "_facts_" + string(rune('0'+i))
		
		// Generate only CSV format to focus on the core generation logic
		err := core.GenerateModelData("ecommerce", counts, "csv", iterationDir)
		if err != nil {
			b.Fatalf("Failed to generate fact data: %v", err)
		}
		
		os.RemoveAll(iterationDir)
	}
}

// BenchmarkFinancialGeneration benchmarks the financial data generation pipeline.
// Run with: go test -cpuprofile=cpu.prof -bench=BenchmarkFinancial -benchtime=1x ./tests
func BenchmarkFinancialGeneration(b *testing.B) {
	outputDir := "benchmark_financial"
	defer func() {
		os.RemoveAll(outputDir)
	}()

	counts, err := core.CalculateFinancialRowCounts(1.0) // 1GB target
	if err != nil {
		b.Fatalf("Failed to calculate financial row counts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iterationDir := outputDir + "_financial_" + string(rune('0'+i))
		
		err := core.GenerateModelData("financial", counts, "csv", iterationDir)
		if err != nil {
			b.Fatalf("Failed to generate financial data: %v", err)
		}
		
		os.RemoveAll(iterationDir)
	}
}

// BenchmarkMedicalGeneration benchmarks the medical data generation pipeline.
// Run with: go test -cpuprofile=cpu.prof -bench=BenchmarkMedical -benchtime=1x ./tests
func BenchmarkMedicalGeneration(b *testing.B) {
	outputDir := "benchmark_medical"
	defer func() {
		os.RemoveAll(outputDir)
	}()

	counts, err := core.CalculateMedicalRowCounts(1.0) // 1GB target
	if err != nil {
		b.Fatalf("Failed to calculate medical row counts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iterationDir := outputDir + "_medical_" + string(rune('0'+i))
		
		err := core.GenerateModelData("medical", counts, "csv", iterationDir)
		if err != nil {
			b.Fatalf("Failed to generate medical data: %v", err)
		}
		
		os.RemoveAll(iterationDir)
	}
}