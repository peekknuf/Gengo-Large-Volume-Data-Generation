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
Just type in ./Gengo gen`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%s\n", logoStyle.Render(logo))
		fmt.Println(cmd.Long)
	},
}

var generateCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate e-commerce data",
	Long:  `Generate e-commerce data based on predefine columns.`,
	Run: func(cmd *cobra.Command, args []string) {
		numRows, outputFilename, err := getUserInput()
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
		GenerateData(numRows, outputFilename, selectedCols)
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
