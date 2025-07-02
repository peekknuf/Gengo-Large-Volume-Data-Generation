package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/core"
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "Gengo",
	Short: "A brief description of your application",
	Long: `Welcome to Gengo.
Create fake datasets quickly.

Just type in:
go build
./Gengo gen
and follow through`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

var generateCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate synthetic e-commerce data model",
	Long: `Generates a synthetic e-commerce data model (dimensions and facts)
based on an estimated target size and saves it to the specified format
(CSV, JSON Lines, Parquet) within a directory.

Example:
  ./Gengo gen`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting data model generation process...")

		// --- Get User Input ---
		modelType, counts, outputFormat, outputDir, err := core.GetUserInputForModel() // Assumes getUserInputForModel is in input.go or similar
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError getting user input: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\nConfiguration:\n Target Format: %s\n Output Directory: %s\n", outputFormat, outputDir)

		// --- Seed Random Generators Once ---
		// Seeding here ensures consistency for a single run across different generators.
		seed := time.Now().UnixNano()
		rand.Seed(seed) // Seed math/rand used by weighted sampler & potentially others
		gf.Seed(seed)   // Seed gofakeit used in simulation functions
		fmt.Println("Random generators seeded.")

		fmt.Println("\nStarting generation (this might take a while)...")

		// --- Call the Main Generation Orchestrator ---
		// Assumes GenerateModelData is in orchestrator.go or similar
		err = core.GenerateModelData(modelType, counts, outputFormat, outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError during data generation: %v\n", err)
			os.Exit(1)
		}

		// If no error returned from GenerateModelData
		fmt.Println("\nProcess completed successfully.")
	},
}

func init() {
	RootCmd.AddCommand(generateCmd)
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
