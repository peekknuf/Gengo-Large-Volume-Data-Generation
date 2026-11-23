package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"

	"github.com/peekknuf/Gengo/internal/benchmark"
)

//go:embed profile-report.html
var profileReportHTML string

func writeProfileReport(directory string) error {
	runs := map[string]*benchmark.Result{}
	for _, name := range []string{"quick", "thorough"} {
		result, err := benchmark.ReadResult(filepath.Join(directory, "profile-"+name, "result.json"))
		if err != nil {
			return err
		}
		if result.Scenario.Profile != name || result.Scenario.Workload != "customer-lookup" {
			return fmt.Errorf("expected %s profile in report source", name)
		}
		runs[name] = result
	}
	if runs["quick"].DatasetID != runs["thorough"].DatasetID {
		return fmt.Errorf("profile reports require the same dataset")
	}
	contents, err := json.MarshalIndent(runs, "", "  ")
	if err != nil {
		return err
	}
	page, err := template.New("profiles").Parse(profileReportHTML)
	if err != nil {
		return err
	}
	var rendered bytes.Buffer
	if err := page.Execute(&rendered, string(contents)); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "profile-report.json"), append(contents, '\n'), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "profile-report.html"), rendered.Bytes(), 0600); err != nil {
		return err
	}
	fmt.Println("Profile report:", filepath.Join(directory, "profile-report.html"))
	return nil
}
