package utils

import lipgloss "github.com/charmbracelet/lipgloss"


var logoStyle = lipgloss.NewStyle().
	Foreground(lipgloss.Color("#8A2BE2")).
	Italic(true).
	Bold(true)

const logo = `
___          ___
/ __|___ _ _ / __|___
| (_ / -_) ' \ (_ / _ \
\___\___|_||_\___\___/

`