package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "Data Gen",
	Short: "A brief description of your application",
	Long:  `This tool is designed and created for people to be able to create fake datasets quickly.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate e-commerce data",
	Long:  `Generate e-commerce data based on selected columns.`,
	Run: func(cmd *cobra.Command, args []string) {
		GenerateData(numRows, outputFilename, selectedCols)
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(generateCmd)

	rootCmd.Flags().IntVarP(&numRows, "rows", "r", 3000000, "Number of rows to generate")
	rootCmd.Flags().StringVarP(&outputFilename, "output", "o", "ecommerce_data.csv", "Output filename")
	rootCmd.Flags().StringSliceVarP(&selectedCols, "columns", "c", []string{
		"ID", "Timestamp", "ProductName", "Company", "Price", "Quantity", "Discount", "TotalPrice", "CustomerID", "FirstName", "LastName", "Email", "Address", "City", "State", "Zip", "Country"},
		"Selected columns to generate")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
