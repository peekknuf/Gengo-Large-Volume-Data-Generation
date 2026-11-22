package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/postgres"
)

func TestLivePostgresCSVLoad(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into live PostgreSQL import tests")
	}
	client, err := postgres.NewClient("direct", envFile)
	if err != nil {
		t.Fatal(err)
	}
	namespace := fmt.Sprintf("gengo_test_%d", time.Now().UnixNano())
	qualified := quotePostgresIdentifier(namespace)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var output bytes.Buffer
		if err := client.Run(ctx, []string{"--command", "DROP SCHEMA IF EXISTS " + qualified + " CASCADE;"}, &output, &output); err != nil {
			t.Errorf("clean up isolated schema %s: %v\n%s", namespace, err, output.String())
		}
	})
	dataset := loadFixture(t)
	plan := filepath.Join(t.TempDir(), "import")
	if err := WriteCSVLoadPlan(dataset, namespace, plan); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var output bytes.Buffer
	if err := client.Run(ctx, []string{"--file", filepath.Join(plan, "run.psql")}, &output, &output); err != nil {
		t.Fatalf("live import: %v\n%s", err, output.String())
	}
	query := fmt.Sprintf(`SELECT
  (SELECT count(*) FROM pg_constraint WHERE connamespace = '%s'::regnamespace AND contype = 'p'),
  (SELECT count(*) FROM pg_constraint WHERE connamespace = '%s'::regnamespace AND contype = 'f' AND convalidated),
  (SELECT count(*) FROM %s.fact_order_items items
    JOIN %s.fact_orders_header orders USING (order_id)
    JOIN %s.dim_customers customers ON customers.customer_id = orders.customer_id
    JOIN %s.dim_products products USING (product_id));`, namespace, namespace, qualified, qualified, qualified, qualified)
	output.Reset()
	if err := client.Run(ctx, []string{"-At", "--command", query}, &output, &output); err != nil {
		t.Fatalf("verify constraints and joins: %v\n%s", err, output.String())
	}
	if strings.TrimSpace(output.String()) != "7|8|39" {
		t.Fatalf("constraints and joined items = %q, want 7|8|39", output.String())
	}
	output.Reset()
	if err := client.Run(ctx, []string{"--file", filepath.Join(plan, "run.psql")}, &output, &output); err == nil {
		t.Fatal("import overwrote an existing schema")
	}
	if !strings.Contains(output.String(), "already exists") {
		t.Fatalf("expected existing-schema rejection, got %s", output.String())
	}
	t.Run("count_mismatch_rolls_back", func(t *testing.T) {
		failedNamespace := namespace + "_bad"
		failedQualified := quotePostgresIdentifier(failedNamespace)
		t.Cleanup(func() {
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			var cleanupOutput bytes.Buffer
			if err := client.Run(cleanupContext, []string{"--command", "DROP SCHEMA IF EXISTS " + failedQualified + " CASCADE;"}, &cleanupOutput, &cleanupOutput); err != nil {
				t.Errorf("cleanup %s: %v\n%s", failedNamespace, err, cleanupOutput.String())
			}
		})
		path := filepath.Join(dataset, "manifest.json")
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var manifest Manifest
		if err := json.Unmarshal(contents, &manifest); err != nil {
			t.Fatal(err)
		}
		manifest.Identity.Counts["dim_customers"]++
		for index := range manifest.Files {
			if manifest.Files[index].Table == "dim_customers" {
				manifest.Files[index].Rows++
				break
			}
		}
		contents, err = json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, contents, 0600); err != nil {
			t.Fatal(err)
		}
		failedPlan := filepath.Join(t.TempDir(), "import")
		if err := WriteCSVLoadPlan(dataset, failedNamespace, failedPlan); err != nil {
			t.Fatal(err)
		}
		output.Reset()
		if err := client.Run(ctx, []string{"--file", filepath.Join(failedPlan, "run.psql")}, &output, &output); err == nil || !strings.Contains(output.String(), "Row count mismatch for dim_customers") {
			t.Fatalf("expected database count mismatch, got %v\n%s", err, output.String())
		}
		output.Reset()
		query := "SELECT count(*) FROM " + failedQualified + ".dim_customers;"
		if err := client.Run(ctx, []string{"-At", "--command", query}, &output, &output); err != nil || strings.TrimSpace(output.String()) != "0" {
			t.Fatalf("failed load did not roll back: %v\n%s", err, output.String())
		}
	})
}
