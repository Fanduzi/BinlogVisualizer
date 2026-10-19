// Package report defines presentation controls for text, JSON, markdown, and HTML analysis output.
// input: CLI-selected presentation options and analyzer-produced complete analysis results.
// output: normalized report rendering options with validated SQL context modes, diagnostics modes, output formats, and full-diagnostics archive options.
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

// DiagnosticsMode controls how much of diagnostics analyze JSON includes.
// Text, Markdown, and HTML ignore it.
type DiagnosticsMode string

const (
	DiagnosticsFull    DiagnosticsMode = "full"
	DiagnosticsSummary DiagnosticsMode = "summary"
	DiagnosticsOff     DiagnosticsMode = "off"
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
	// DiagnosticsMode caps analyze JSON diagnostics. Zero means full.
	DiagnosticsMode DiagnosticsMode
	Format          Format
	TopN            int
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
	return Options{SQLContextMode: SQLContextSummary, DiagnosticsMode: DiagnosticsFull, TopN: DefaultTopN, TopTables: DefaultTopN}
}

// ArchiveOptions returns opts with full diagnostics. A snapshot or workflow
// artifact is an archive that compare and trend read later, so it never
// takes the stdout --diagnostics budget (ADR 0011 §3).
func ArchiveOptions(opts Options) Options {
	opts.DiagnosticsMode = DiagnosticsFull
	return opts
}

// ParseDiagnosticsMode validates a diagnostics mode.
// Empty is full, the report.Options zero value and the historical analyze
// JSON shape. The CLI rejects an explicitly set --diagnostics "" before
// calling this (exit 3); do not map "" to an error here.
func ParseDiagnosticsMode(raw string) (DiagnosticsMode, error) {
	switch DiagnosticsMode(raw) {
	case "", DiagnosticsFull:
		return DiagnosticsFull, nil
	case DiagnosticsSummary:
		return DiagnosticsSummary, nil
	case DiagnosticsOff:
		return DiagnosticsOff, nil
	default:
		return "", fmt.Errorf("invalid --diagnostics %q (allowed: full, summary, off)", raw)
	}
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
	diagnostics, err := ParseDiagnosticsMode(string(opts.DiagnosticsMode))
	if err != nil {
		opts.DiagnosticsMode = DiagnosticsFull
	} else {
		opts.DiagnosticsMode = diagnostics
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
