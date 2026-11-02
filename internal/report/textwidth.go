// Package report renders human-readable text reports from complete analysis results.
package report

import (
	"strings"

	"golang.org/x/text/width"
)

// displayWidth returns the number of terminal cells s occupies. East Asian wide
// and fullwidth runes take two cells; everything else takes one. The text tables
// pad with it so a localized header, or a schema or table name written in CJK,
// keeps the columns under their headings.
func displayWidth(s string) int {
	cells := 0
	for _, r := range s {
		switch width.LookupRune(r).Kind() {
		case width.EastAsianWide, width.EastAsianFullwidth:
			cells += 2
		default:
			cells++
		}
	}
	return cells
}

// padRight pads s on the right to exactly cells; a longer s is returned as is.
func padRight(s string, cells int) string {
	if n := cells - displayWidth(s); n > 0 {
		return s + strings.Repeat(" ", n)
	}
	return s
}

// padLeft pads s on the left to exactly cells; a longer s is returned as is.
func padLeft(s string, cells int) string {
	if n := cells - displayWidth(s); n > 0 {
		return strings.Repeat(" ", n) + s
	}
	return s
}
