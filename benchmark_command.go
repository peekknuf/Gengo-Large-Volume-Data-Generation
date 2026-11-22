package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/peekknuf/Gengo/internal/benchmark"
	"github.com/spf13/cobra"
)

func newBenchmarkCommand() *cobra.Command {
	bench := &cobra.Command{Use: "bench", Short: "Run controlled PostgreSQL workloads with pgbench"}
	var scenarioPath, envFile, output string
	run := &cobra.Command{
		Use:   "run",
		Short: "Run a versioned YAML customer-lookup scenario",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			plan, err := benchmark.LoadScenario(scenarioPath)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			result, err := benchmark.Run(ctx, plan, envFile, output, cmd.OutOrStdout())
			if result != nil {
				benchmark.PrintSummary(cmd.OutOrStdout(), result)
				fmt.Fprintf(cmd.OutOrStdout(), "Saved benchmark result in %s/result.json\n", output)
			}
			return err
		},
	}
	run.Flags().StringVar(&scenarioPath, "scenario", "", "Version-1 YAML scenario file")
	run.Flags().StringVar(&envFile, "env-file", "", "Optional dotenv file; process variables take precedence")
	run.Flags().StringVar(&output, "output", "", "New directory for results, workload, and client logs")
	_ = run.MarkFlagRequired("scenario")
	_ = run.MarkFlagRequired("output")
	bench.AddCommand(run)
	return bench
}
