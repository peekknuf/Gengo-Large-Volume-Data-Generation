package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/peekknuf/Gengo/internal/core"
	"github.com/peekknuf/Gengo/internal/postgres"
	"github.com/spf13/cobra"
)

func newLoadCommand() *cobra.Command {
	load := &cobra.Command{Use: "load", Short: "Prepare and execute PostgreSQL dataset imports"}
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
	load.AddCommand(plan, newLoadRunCommand())
	return load
}

func newLoadRunCommand() *cobra.Command {
	var dataset, schemaName, destination, target, envFile string
	var timeout time.Duration
	run := &cobra.Command{
		Use:   "run",
		Short: "Plan and execute an ecommerce CSV import using psql",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if timeout <= 0 {
				return fmt.Errorf("timeout must be positive")
			}
			if target != "direct" && target != "libpq" {
				return fmt.Errorf("imports require target direct or libpq; use poolers for workload connections")
			}
			client, err := postgres.NewClient(target, envFile)
			if err != nil {
				return err
			}
			if err = core.WriteCSVLoadPlan(dataset, schemaName, destination); err != nil {
				return err
			}
			script, err := filepath.Abs(filepath.Join(destination, "run.psql"))
			if err != nil {
				return err
			}
			ctx, cancel := postgresOperationContext(cmd.Context(), timeout)
			defer cancel()
			if err = client.Run(ctx, []string{"--file", script}, cmd.OutOrStdout(), cmd.ErrOrStderr()); err != nil {
				return fmt.Errorf("import stopped; completed stages and scripts remain for inspection: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "PostgreSQL import verified in schema %s.\n", schemaName)
			return nil
		},
	}
	run.Flags().StringVar(&dataset, "dataset", "", "Completed Gengo ecommerce CSV dataset directory")
	run.Flags().StringVar(&schemaName, "schema", "gengo", "New destination PostgreSQL schema")
	run.Flags().StringVar(&destination, "output", "", "Empty or new directory to retain import scripts")
	run.Flags().StringVar(&target, "target", "direct", "Import connection target: direct or libpq")
	run.Flags().StringVar(&envFile, "env-file", "", "Optional dotenv file; existing process variables take precedence")
	run.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "Maximum import execution duration")
	_ = run.MarkFlagRequired("dataset")
	_ = run.MarkFlagRequired("output")
	return run
}
