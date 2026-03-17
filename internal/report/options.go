// Package report defines presentation controls for text and JSON analysis output.
// input: CLI-selected presentation options and analyzer-produced bounded analysis results.
// output: normalized report rendering options with validated SQL context modes.
// pos: renderer configuration layer between command flag parsing and text/JSON serializers.
// note: if this file changes, update this header and module README.md.
package report

import "fmt"

// SQLContextMode controls how transaction SQL context is rendered.
type SQLContextMode string

const (
	SQLContextSummary SQLContextMode = "summary"
	SQLContextOff     SQLContextMode = "off"
	SQLContextFull    SQLContextMode = "full"
)

// Options controls report presentation without changing analysis semantics.
type Options struct {
	SQLContextMode SQLContextMode
}

// DefaultOptions returns the backwards-compatible report presentation defaults.
func DefaultOptions() Options {
	return Options{SQLContextMode: SQLContextSummary}
}

// ParseSQLContextMode validates a CLI-provided sql-context mode.
func ParseSQLContextMode(raw string) (SQLContextMode, error) {
	mode := SQLContextMode(raw)
	switch mode {
	case "", SQLContextSummary:
		return SQLContextSummary, nil
	case SQLContextOff:
		return SQLContextOff, nil
	case SQLContextFull:
		return SQLContextFull, nil
	default:
		return "", fmt.Errorf("invalid --sql-context %q (allowed: summary, off, full)", raw)
	}
}

func normalizeOptions(opts Options) Options {
	mode, err := ParseSQLContextMode(string(opts.SQLContextMode))
	if err != nil {
		return DefaultOptions()
	}
	return Options{SQLContextMode: mode}
}
