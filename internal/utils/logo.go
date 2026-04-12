package utils

import (
	"fmt"

	lipgloss "github.com/charmbracelet/lipgloss"
)

var (
	purple = lipgloss.Color("#8A2BE2")
	cyan   = lipgloss.Color("#00D4FF")
	gray   = lipgloss.Color("#555555")
	gold   = lipgloss.Color("#FFD700")
)

func Logo() string {
	g := []string{" █████", "██    ", "██ ███", "██  ██", " █████"}
	e := []string{"█████ ", "██    ", "█████ ", "██    ", "█████ "}
	n := []string{"██  ██", "███ ██", "██████", "██ ███", "██  ██"}
	o := []string{" ████ ", "██  ██", "██  ██", "██  ██", " ████ "}

	letters := [][]string{g, e, n, g, o}
	var lines [5]string
	for row := 0; row < 5; row++ {
		for _, l := range letters {
			lines[row] += l[row] + " "
		}
	}

	ascii := lipgloss.NewStyle().
		Foreground(purple).
		Bold(true).
		Render(fmt.Sprintf("\n  %s\n  %s\n  %s\n  %s\n  %s", lines[0], lines[1], lines[2], lines[3], lines[4]))

	tagline := lipgloss.NewStyle().
		Foreground(cyan).
		Italic(true).
		Render("    Large-Scale Synthetic Data Generation")

	divider := lipgloss.NewStyle().
		Foreground(gray).
		Render("    ─────────────────────────────────")

	footer := lipgloss.NewStyle().
		Foreground(gold).
		Render("    Blazingly fast. Relational. Realistic.")

	return ascii + "\n" + tagline + "\n" + divider + "\n" + footer + "\n"
}
