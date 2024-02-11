/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "Data Gen",
	Short: "A brief description of your application",
	Long: `This tool is designed and created for people to create fake datasets.
The outputs are kinda hilarious sometimes`,
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
// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.go_cli.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	
	rootCmd.AddCommand(generateCmd)

	generateCmd.Flags().IntVarP(&numRows, "rows", "r", 3000000, "Number of rows to generate")
	generateCmd.Flags().StringVarP(&outputFilename, "output", "o", "ecommerce_data.csv", "Output filename")
	generateCmd.Flags().StringSliceVarP(&selectedCols, "columns", "c", []string{"ID", "Timestamp", "ProductName", "Company", "Price", "Quantity", "Discount", "TotalPrice", "CustomerID", "FirstName", "LastName", "Email", "Address", "City", "State", "Zip", "Country"}, "Selected columns to generate")
	
}
