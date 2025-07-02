package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func AddUnderscores(n int) string {
	str := strconv.Itoa(n)
	ln := len(str)
	if ln <= 3 {
		return str
	}

	var parts []string
	for ln > 3 {
		parts = append(parts, str[ln-3:])
		str = str[:ln-3]
		ln = len(str)
	}
	if ln > 0 {
		parts = append(parts, str)
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, "_")
}

func PrintProgress(current, total int, label string) {
	if total == 0 {
		return
	}
	percentage := float64(current) / float64(total) * 100
	barLength := 40
	filledLength := int(float64(barLength) * percentage / 100)
	bar := strings.Repeat("=", filledLength) + strings.Repeat("-", barLength-filledLength)
	fmt.Printf("\r%s: [%s] %d/%d (%.2f%%)", label, bar, current, total, percentage)
	if current == total {
		fmt.Println()
	}
}