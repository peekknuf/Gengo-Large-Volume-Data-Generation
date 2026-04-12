package main

import (
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/peekknuf/Gengo/internal/core"
)

func main() {
	counts, err := core.CalculateECommerceRowCounts(10)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sizing error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Row counts: Customers=%d, Addresses=%d, Suppliers=%d, Products=%d, OrderHeaders=%d, OrderItems=%d\n",
		counts.Customers, counts.CustomerAddresses, counts.Suppliers, counts.Products, counts.OrderHeaders, counts.OrderItems)

	f, err := os.Create("cpu.prof")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
		os.Exit(1)
	}
	defer pprof.StopCPUProfile()

	fmt.Println("Starting profiled ecommerce 10GB CSV generation...")
	if err := core.GenerateModelData("ecommerce", counts, "csv", "10"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Done. Profile saved to cpu.prof")
}
