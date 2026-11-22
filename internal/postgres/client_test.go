package postgres

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func init() {
	if mode := os.Getenv("GENGO_TEST_PSQL_HELPER"); mode != "" {
		switch mode {
		case "echo":
			fmt.Println(strings.Join(os.Args[1:], "\n"))
			fmt.Println("password=" + os.Getenv("PGPASSWORD"))
			fmt.Println("sslpassword=" + os.Getenv("PGSSLPASSWORD"))
			fmt.Println("database=" + os.Getenv("PGDATABASE"))
			fmt.Println("api_key=" + os.Getenv("SUPABASE_API_KEY"))
		case "fail":
			fmt.Fprintln(os.Stderr, "connection failed: "+os.Getenv("PGPASSWORD"))
			os.Exit(2)
		case "wait":
			time.Sleep(time.Minute)
		}
		os.Exit(0)
	}
}

func TestClientTargets(t *testing.T) {
	for _, target := range []struct{ name, variable, address string }{
		{"direct", "GENGO_DIRECT_DATABASE_URL", "postgresql://postgres@direct.example:5432/postgres"},
		{"shared", "GENGO_TRANSACTION_DATABASE_URL", "postgresql://postgres.project@shared.example:6543/postgres"},
		{"dedicated", "GENGO_DEDICATED_DATABASE_URL", "postgresql://postgres@direct.example:6543/postgres"},
	} {
		t.Run(target.name, func(t *testing.T) {
			t.Setenv(target.variable, target.address)
			t.Setenv("PGPASSWORD", "test-password")
			t.Setenv("GENGO_TEST_PSQL_HELPER", "echo")
			client, err := NewClient(target.name, "")
			if err != nil {
				t.Fatal(err)
			}
			if client.database != target.address {
				t.Fatalf("connection target = %q, want %q", client.database, target.address)
			}
			client.executable, err = os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err = client.Run(t.Context(), []string{"--command", "SELECT 1"}, &output, &output); err != nil {
				t.Fatal(err)
			}
			for _, expected := range []string{"-X\n-w\n--set=ON_ERROR_STOP=on\n--dbname\n[REDACTED]", "--command\nSELECT 1", "password=[REDACTED]"} {
				if !strings.Contains(output.String(), expected) {
					t.Fatalf("output %q does not contain %q", output.String(), expected)
				}
			}
			if strings.Contains(output.String(), "test-password") {
				t.Fatal("password leaked")
			}
		})
	}
}

func TestClientEnvironmentAndPasswords(t *testing.T) {
	t.Setenv("GENGO_TEST_PSQL_HELPER", "echo")
	t.Setenv("PGPASSWORD", "process-password")
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("PGPASSWORD=file-password\nGENGO_DEDICATED_DATABASE_URL=postgresql://postgres@dedicated.example:6543/postgres?sslmode=require\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient("dedicated", path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(client.environment, "\n"), "PGPASSWORD=process-password") {
		t.Fatal("dotenv overrode the process environment")
	}
	for _, address := range []string{
		"postgresql://postgres:p%40ss%3Aword@db.example/postgres?sslmode=require",
		"postgresql://postgres@db.example/postgres?password=p%40ss%3Aword&sslpassword=key-secret",
	} {
		t.Setenv("GENGO_DIRECT_DATABASE_URL", address)
		client, err = NewClient("direct", "")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(client.database, "password") || strings.Contains(client.database, "p%40ss") {
			t.Fatal("password present in process arguments")
		}
		if !strings.Contains(strings.Join(client.environment, "\n"), "PGPASSWORD=p@ss:word") {
			t.Fatal("URL password was not decoded into environment")
		}
		client.executable, err = os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		if err = client.Run(t.Context(), nil, &output, &output); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(output.String(), "p@ss:word") || strings.Contains(output.String(), "key-secret") {
			t.Fatalf("credentials leaked: %s", output.String())
		}
	}
}

func TestClientRedactsSupabaseKey(t *testing.T) {
	t.Setenv("SUPABASE_API_KEY", "sb_secret_private-test-key")
	t.Setenv("GENGO_TEST_PSQL_HELPER", "echo")
	client, err := NewClient("libpq", "")
	if err != nil {
		t.Fatal(err)
	}
	client.executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := client.Run(t.Context(), nil, &output, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "sb_secret_private-test-key") || !strings.Contains(output.String(), "api_key=[REDACTED]") {
		t.Fatal("Supabase key was not redacted")
	}
}

func TestClientRejectsInvalidSettings(t *testing.T) {
	for _, address := range []string{"", "not-a-url", "https://db.example", "postgresql://postgres:secret@db.example/?bad=%ZZ"} {
		t.Setenv("GENGO_DIRECT_DATABASE_URL", address)
		if _, err := NewClient("direct", ""); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("expected a credential-safe validation error, got %v", err)
		}
	}
	if _, err := NewClient("unknown", ""); err == nil {
		t.Fatal("accepted unknown target")
	}
	if _, err := NewClient("direct", filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("accepted missing dotenv file")
	}
	if _, err := NewClient("libpq", ""); err != nil {
		t.Fatal(err)
	}
}

func TestClientFailureAndCancellation(t *testing.T) {
	for _, mode := range []string{"fail", "wait"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("PGPASSWORD", "never-print-this")
			t.Setenv("GENGO_TEST_PSQL_HELPER", mode)
			client, err := NewClient("libpq", "")
			if err != nil {
				t.Fatal(err)
			}
			client.executable, err = os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			var output bytes.Buffer
			err = client.Run(ctx, nil, &output, &output)
			if err == nil || strings.Contains(output.String()+err.Error(), "never-print-this") {
				t.Fatalf("expected safe failure: %v, %s", err, output.String())
			}
			if mode == "wait" && !strings.Contains(err.Error(), "deadline exceeded") {
				t.Fatalf("cancellation not reported: %v", err)
			}
		})
	}
}
