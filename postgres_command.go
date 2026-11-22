package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/peekknuf/Gengo/internal/postgres"
	"github.com/spf13/cobra"
)

func newDatabaseCommand() *cobra.Command {
	database := &cobra.Command{Use: "db", Short: "Check PostgreSQL connections"}
	var target, envFile string
	var timeout time.Duration
	check := &cobra.Command{
		Use:   "check",
		Short: "Query database identity and version without changing data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if timeout <= 0 {
				return fmt.Errorf("timeout must be positive")
			}
			client, err := postgres.NewClient(target, envFile)
			if err != nil {
				return err
			}
			ctx, cancel := postgresOperationContext(cmd.Context(), timeout)
			defer cancel()
			if err = client.Run(ctx, []string{"--command", "SELECT current_database() AS database, current_user AS role, current_setting('server_version') AS server_version, 1 AS connection_check;"}, cmd.OutOrStdout(), cmd.ErrOrStderr()); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "PostgreSQL connection verified (%s).\n", target)
			return nil
		},
	}
	check.Flags().StringVar(&target, "target", "direct", "Connection target: direct, shared, dedicated, or libpq")
	check.Flags().StringVar(&envFile, "env-file", "", "Optional dotenv file; existing process variables take precedence")
	check.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Maximum connection-check duration")
	database.AddCommand(check)
	return database
}

func postgresOperationContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	interruptContext, stop := signal.NotifyContext(parent, os.Interrupt)
	ctx, cancel := context.WithTimeout(interruptContext, timeout)
	return ctx, func() {
		cancel()
		stop()
	}
}
