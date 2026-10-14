// Package binlogviz decides what goes to stderr besides errors and warnings.
// input: --quiet, whether stderr is a terminal, and TERM.
// output: whether the progress bar and line erase codes may be written, and the writer for info lines.
// pos: one place for terminal hygiene (#253, ADR 0008); agents and CI get plain stderr.
// note: if this file changes, update this header and README.md.
package binlogviz

import (
	"io"
	"os"
)

// quietFlag is --quiet: no progress, no info lines. Errors and warnings stay.
var quietFlag bool

// stderrIsTerminal is a variable so tests can stand in for a TTY.
var stderrIsTerminal = func() bool { return isTerminal(os.Stderr) }

// interactiveStderr reports whether stderr may carry a progress bar,
// carriage returns and ANSI erase codes.
func interactiveStderr() bool {
	return !quietFlag && os.Getenv("TERM") != "dumb" && stderrIsTerminal()
}

// infoWriter is where progress-adjacent info lines go ("Finalizing
// analysis...", resolved paths, save confirmations). --quiet drops them.
func infoWriter(w io.Writer) io.Writer {
	if quietFlag {
		return io.Discard
	}
	return w
}
