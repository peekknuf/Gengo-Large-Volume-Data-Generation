package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "Data Gen",
	Short: "A brief description of your application",
	Long: `Welcome to Gengo.
Create fake datasets quickly.

Just type in: 
go build
./Gengo gen
and follow through`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%s\n", logoStyle.Render(logo))
		fmt.Println(cmd.Long)
	},
}

var generateCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate synthetic e-commerce data",
	Long:  `Generates synthetic e-commerce data with predefined columns and saves it to the specified format (CSV, JSON, Parquet).`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting data generation process...")

		numRows, outputTarget, format, err := getUserInput()
		if err != nil {
			fmt.Println("\nError getting user input:", err)
			os.Exit(1)
		}

		fmt.Printf("Configuration: %s rows, Format: %s, Output: %s\n", addUnderscores(numRows), format, outputTarget)
		fmt.Println("Generating data (this might take a while for large row counts)...")

		GenerateData(numRows, outputTarget, format, selectedCols)

		fmt.Println("\nProcess completed.")
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
