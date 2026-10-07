// Package report defines presentation controls for text, JSON, markdown, and HTML analysis output.
// input: CLI-selected presentation options and analyzer-produced complete analysis results.
// output: normalized report rendering options with validated SQL context modes and output formats.
// pos: renderer configuration layer between command flag parsing and text/JSON/markdown/HTML serializers.
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

// Format controls the output format of the report.
type Format string

const (
	FormatText     Format = "text"
	FormatJSON     Format = "json"
	FormatMarkdown Format = "markdown"
	FormatHTML     Format = "html"
)

// ParseFormat validates and returns a Format from a CLI string.
func ParseFormat(raw string) (Format, error) {
	switch Format(raw) {
	case FormatText, FormatJSON, FormatMarkdown, FormatHTML:
		return Format(raw), nil
	case "":
		return FormatText, nil
	}
	return "", fmt.Errorf("unknown format %q: must be one of text, json, markdown, html", raw)
}

// Options controls report presentation without changing analysis semantics.
type Options struct {
	SQLContextMode SQLContextMode
	Format         Format
	TopN           int
	// TopTables limits human-readable table sections; zero is unlimited when TopTablesSet is true.
	TopTables int
	// TopTablesSet distinguishes an explicit zero limit from an omitted value.
	TopTablesSet bool
	// TopThreads limits the Top Threads section in every format. Zero is unlimited when TopThreadsSet is true.
	TopThreads int
	// TopThreadsSet distinguishes an explicit zero limit from an omitted value.
	TopThreadsSet bool
	// TopRows limits the Hot Rows section in every format. Zero is unlimited when TopRowsSet is true.
	TopRows int
	// TopRowsSet distinguishes an explicit zero limit from an omitted value.
	TopRowsSet   bool
	Details      bool
	ShowMinutes  bool
	ShowPatterns bool
	// ShowRows prints bounded row images for listed transactions.
	// --sql-context off suppresses the values and says so.
	ShowRows bool
}

// DefaultOptions returns the backwards-compatible report presentation defaults.
func DefaultOptions() Options {
	return Options{SQLContextMode: SQLContextSummary, TopN: DefaultTopN, TopTables: DefaultTopN}
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
		opts.SQLContextMode = SQLContextSummary
	} else {
		opts.SQLContextMode = mode
	}
	if opts.TopN <= 0 {
		opts.TopN = DefaultTopN
	}
	if !opts.TopTablesSet && opts.TopTables <= 0 {
		opts.TopTables = opts.TopN
	}
	if !opts.TopThreadsSet && opts.TopThreads <= 0 {
		opts.TopThreads = opts.TopN
	}
	if !opts.TopRowsSet && opts.TopRows <= 0 {
		opts.TopRows = opts.TopN
	}
	if opts.Details {
		opts.ShowMinutes = true
		opts.ShowPatterns = true
	}
	return opts
}
