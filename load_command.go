package main

import (
	"fmt"

	"github.com/peekknuf/Gengo/internal/core"
	"github.com/spf13/cobra"
)

func newLoadCommand() *cobra.Command {
	load := &cobra.Command{Use: "load", Short: "Prepare PostgreSQL dataset imports"}
	var dataset, schemaName, destination string
	plan := &cobra.Command{
		Use:   "plan",
		Short: "Generate PostgreSQL import scripts for an ecommerce CSV dataset",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := core.WriteCSVLoadPlan(dataset, schemaName, destination); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Created PostgreSQL import scripts in %s\n", destination)
			return nil
		},
	}
	plan.Flags().StringVar(&dataset, "dataset", "", "Completed Gengo CSV dataset directory")
	plan.Flags().StringVar(&schemaName, "schema", "gengo", "Destination PostgreSQL schema to create")
	plan.Flags().StringVar(&destination, "output", "", "Empty or new directory for import scripts")
	_ = plan.MarkFlagRequired("dataset")
	_ = plan.MarkFlagRequired("output")
	load.AddCommand(plan)
	return load
}
