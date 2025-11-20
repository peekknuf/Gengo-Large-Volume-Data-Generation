package main

import (
	"fmt"
	"os"

	"github.com/peekknuf/Gengo/internal/core"
	"github.com/spf13/cobra"
)

var (
	modelType  string
	targetGB   float64
	format     string
	outputDir  string
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
		cmd.Help()
	},
}

var generateCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate synthetic e-commerce data model",
	Long: `Generates a synthetic e-commerce data model (dimensions and facts)
based on an estimated target size and saves it to the specified format
(CSV, JSON Lines, Parquet) within a directory.

Example:
  ./Gengo gen --model ecommerce-ds --size 0.5 --format parquet --output my-data`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting data model generation process...")

		// --- Get User Input from flags or interactive prompts ---
		model, counts, outputFormat, dir, err := core.GetUserInput(modelType, targetGB, format, outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError getting user input: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\nConfiguration:\n Target Format: %s\n Output Directory: %s\n", outputFormat, dir)

		fmt.Println("\nStarting generation (this might take a while)...")

		// --- Call the Main Generation Orchestrator ---
		err = core.GenerateModelData(model, counts, outputFormat, dir)
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
	generateCmd.Flags().StringVarP(&modelType, "model", "m", "", "Data model to generate (ecommerce, ecommerce-ds, financial, medical)")
	generateCmd.Flags().Float64VarP(&targetGB, "size", "s", 0, "Approximate target size in GB (e.g., 0.5, 10)")
	generateCmd.Flags().StringVarP(&format, "format", "f", "", "Output format (csv, json, parquet)")
	generateCmd.Flags().StringVarP(&outputDir, "output", "o", "", "Output directory name")
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
