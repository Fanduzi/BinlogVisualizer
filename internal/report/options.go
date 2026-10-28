// Package report defines presentation controls for text, JSON, markdown, and HTML analysis output.
// input: CLI-selected presentation options and analyzer-produced complete analysis results.
// output: normalized report rendering options with validated SQL context modes, diagnostics modes, output formats, and full-diagnostics archive options.
// pos: renderer configuration layer between command flag parsing and text/JSON/markdown/HTML serializers.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"time"

	"binlogviz/internal/model"
)

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

// knownFormats is the list ParseFormat accepts. An empty CLI string is text,
// not a fifth value. Write renders each entry.
var knownFormats = []Format{
	FormatText,
	FormatJSON,
	FormatMarkdown,
	FormatHTML,
}

// ParseFormat validates and returns a Format from a CLI string.
func ParseFormat(raw string) (Format, error) {
	if raw == "" {
		return FormatText, nil
	}
	for _, format := range knownFormats {
		if Format(raw) == format {
			return format, nil
		}
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
	// Display is the zone for rendered timestamps. Nil means the historical
	// UTC spelling (a trailing " UTC", or Z in JSON), so a report with no
	// --tz flag stays byte-identical. It does not change row-cell values.
	Display *time.Location
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

// chartTimeLayout picks the x-axis label layout for minute charts: HH:MM
// when every point lands on one calendar day in the display zone, with the
// date prefixed when the span crosses midnight, so the same time cannot
// label two different days.
func (o Options) chartTimeLayout(times []time.Time) string {
	if len(times) == 0 {
		return "15:04"
	}
	loc := o.Display
	if loc == nil {
		loc = time.UTC
	}
	first := times[0].In(loc)
	for _, t := range times[1:] {
		d := t.In(loc)
		if d.Year() != first.Year() || d.YearDay() != first.YearDay() {
			return "01-02 15:04"
		}
	}
	return "15:04"
}

// chartMinuteLayout is chartTimeLayout for minute buckets.
func (o Options) chartMinuteLayout(minutes []model.MinuteBucket) string {
	times := make([]time.Time, 0, len(minutes))
	for _, m := range minutes {
		times = append(times, m.Minute)
	}
	return o.chartTimeLayout(times)
}

// chartActivityLayout is chartMinuteLayout for a per-table activity series.
func (o Options) chartActivityLayout(points []model.TableActivityPoint) string {
	times := make([]time.Time, 0, len(points))
	for _, p := range points {
		times = append(times, p.Minute)
	}
	return o.chartTimeLayout(times)
}
