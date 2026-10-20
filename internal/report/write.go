// Package report writes one analyze report.
// input: a finished AnalysisResult, presentation options, and a format name.
// output: text, JSON, Markdown, or HTML written to a writer. An unknown format writes nothing.
// pos: the format dispatch under the analyze command. The view is built before the switch.
// note: if this file changes, update this header and README.md.
package report

import (
	"bytes"
	"fmt"
	"io"

	"binlogviz/internal/model"
	"binlogviz/internal/report/view"
)

// Write builds one view, then writes format to w.
// An unknown format returns the ParseFormat error and writes nothing.
// There is no default format.
func Write(w io.Writer, result model.AnalysisResult, opts Options, format string) error {
	if w == nil {
		return fmt.Errorf("nil writer")
	}
	opts = normalizeOptions(opts)
	built := newView(result, opts)
	parsed, err := ParseFormat(format)
	if err != nil {
		return err
	}
	switch parsed {
	case FormatJSON:
		return writeJSON(w, result, opts, built)
	case FormatText:
		text, err := renderText(result, opts, built)
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, text)
		return err
	case FormatMarkdown:
		text, err := renderMarkdown(result, opts, built)
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, text)
		return err
	case FormatHTML:
		text, err := renderHTML(result, opts, built)
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, text)
		return err
	default:
		return fmt.Errorf("unknown format %q: must be one of text, json, markdown, html", format)
	}
}

// Render is Write into a string. JSON includes the trailing newline Encode adds.
func Render(result model.AnalysisResult, opts Options, format string) (string, error) {
	var buf bytes.Buffer
	if err := Write(&buf, result, opts, format); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func newView(result model.AnalysisResult, opts Options) view.View {
	opts = normalizeOptions(opts)
	mode := opts.SQLContextMode
	return view.Build(result, view.Limits{
		TopN:            opts.TopN,
		DiagnosticLists: opts.DiagnosticsMode == DiagnosticsFull,
		HasQuery: func(txn model.Transaction) bool {
			return transactionTextQuery(txn, mode) != ""
		},
	})
}
