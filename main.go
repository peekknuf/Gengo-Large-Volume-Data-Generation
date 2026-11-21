package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/peekknuf/Gengo/internal/core"
	"github.com/peekknuf/Gengo/internal/utils"
	"github.com/spf13/cobra"
)

var (
	modelType string
	targetGB  float64
	format    string
	outputDir string
	dryRun    bool
	workers   int
)

var RootCmd = &cobra.Command{
	Use:          "gengo",
	Short:        "Large-scale synthetic relational data generator",
	SilenceUsage: true,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var generateCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate a synthetic relational dataset",
	Long: `Generates normalized (3NF) synthetic datasets across four domains:
  ecommerce, ecommerce-ds (TPC-DS), financial, and medical.

Supports CSV and Apache Parquet output formats.

Example:
  gengo gen --model ecommerce-ds --size 10 --format parquet --output my-data`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if workers < 1 {
			return fmt.Errorf("workers must be at least 1")
		}
		previous := runtime.GOMAXPROCS(workers)
		defer runtime.GOMAXPROCS(previous)
		fmt.Println("Starting data model generation process...")

		// --- Get User Input from flags or interactive prompts ---
		dirFlag := outputDir
		if dryRun && dirFlag == "" {
			dirFlag = "data"
		}
		model, counts, outputFormat, dir, err := core.GetUserInput(modelType, targetGB, format, dirFlag)
		if err != nil {
			return err
		}

		fmt.Printf("\nConfiguration:\n Target Format: %s\n Output Directory: %s\n", outputFormat, dir)
		fmt.Printf(" Workers per fact table: %d\n", workers)
		if dryRun {
			fmt.Println("\nDry run complete. No files were created.")
			return nil
		}

		fmt.Println("\nStarting generation (this might take a while)...")

		// --- Call the Main Generation Orchestrator ---
		err = core.GenerateModelData(model, counts, outputFormat, dir)
		if err != nil {
			return err
		}

		// If no error returned from GenerateModelData
		fmt.Println("\nProcess completed successfully.")
		return nil
	},
}

func init() {
	RootCmd.AddCommand(generateCmd)
	generateCmd.Flags().StringVarP(&modelType, "model", "m", "", "Data model to generate (ecommerce, ecommerce-ds, financial, medical)")
	generateCmd.Flags().Float64VarP(&targetGB, "size", "s", 0, "Approximate target size in GB (e.g., 0.5, 10)")
	generateCmd.Flags().StringVarP(&format, "format", "f", "", "Output format (csv, parquet)")
	generateCmd.Flags().StringVarP(&outputDir, "output", "o", "", "Output directory name")
	generateCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show estimated row counts without generating files")
	generateCmd.Flags().IntVarP(&workers, "workers", "w", runtime.GOMAXPROCS(0), "Maximum workers per fact table")
}

func main() {
	fmt.Println(utils.Logo())
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
